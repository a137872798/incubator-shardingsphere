/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.shardingsphere.sharding.route.engine.type.standard;

import com.google.common.base.Preconditions;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.api.hint.HintManager;
import org.apache.shardingsphere.core.rule.BindingTableRule;
import org.apache.shardingsphere.core.rule.DataNode;
import org.apache.shardingsphere.core.rule.ShardingRule;
import org.apache.shardingsphere.core.rule.TableRule;
import org.apache.shardingsphere.core.strategy.route.ShardingStrategy;
import org.apache.shardingsphere.core.strategy.route.hint.HintShardingStrategy;
import org.apache.shardingsphere.core.strategy.route.value.ListRouteValue;
import org.apache.shardingsphere.core.strategy.route.value.RouteValue;
import org.apache.shardingsphere.sharding.route.engine.condition.ShardingCondition;
import org.apache.shardingsphere.sharding.route.engine.condition.ShardingConditions;
import org.apache.shardingsphere.sharding.route.engine.type.ShardingRouteEngine;
import org.apache.shardingsphere.sql.parser.relation.type.TableAvailable;
import org.apache.shardingsphere.sql.parser.relation.statement.SQLStatementContext;
import org.apache.shardingsphere.sql.parser.relation.statement.dml.DeleteStatementContext;
import org.apache.shardingsphere.sql.parser.relation.statement.dml.InsertStatementContext;
import org.apache.shardingsphere.sql.parser.relation.statement.dml.UpdateStatementContext;
import org.apache.shardingsphere.underlying.common.constant.properties.ShardingSphereProperties;
import org.apache.shardingsphere.underlying.common.exception.ShardingSphereException;
import org.apache.shardingsphere.underlying.route.context.RouteResult;
import org.apache.shardingsphere.underlying.route.context.RouteUnit;
import org.apache.shardingsphere.underlying.route.context.TableUnit;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

/**
 * Sharding standard routing engine.
 * 标准路由引擎
 */
@RequiredArgsConstructor
public final class ShardingStandardRoutingEngine implements ShardingRouteEngine {

    /**
     * 本次分表时   只包含一个表
     */
    private final String logicTableName;

    /**
     * 维护了本次会话的相关信息
     */
    private final SQLStatementContext sqlStatementContext;

    /**
     * 会影响到分库分表规则的 一组条件
     */
    private final ShardingConditions shardingConditions;

    private final ShardingSphereProperties properties;

    /**
     * 传入规则对象 并进行分表
     * @param shardingRule sharding rule  包含了各种全局配置
     * @return
     */
    @Override
    public RouteResult route(final ShardingRule shardingRule) {
        // 能够创建该对象的前提就是 单表 或者 binding 表 那么在绑定规则下不允许执行insert 语句
        if (isDMLForModify(sqlStatementContext) && 1 != ((TableAvailable) sqlStatementContext).getAllTables().size()) {
            throw new ShardingSphereException("Cannot support Multiple-Table for '%s'.", sqlStatementContext.getSqlStatement());
        }
        // getDataNodes 代表获取一组备选的物理节点 如果本次sql没有shardingColumn 会返回所有节点 (因为数据本身就是所有物理表的集合 所以要查询所有物理表)
        return generateRouteResult(getDataNodes(shardingRule, shardingRule.getTableRule(logicTableName)));
    }
    
    private boolean isDMLForModify(final SQLStatementContext sqlStatementContext) {
        return sqlStatementContext instanceof InsertStatementContext || sqlStatementContext instanceof UpdateStatementContext || sqlStatementContext instanceof DeleteStatementContext;
    }

    /**
     * 将一组实际物理节点封装成 routeResult
     * @param routedDataNodes
     * @return
     */
    private RouteResult generateRouteResult(final Collection<DataNode> routedDataNodes) {
        RouteResult result = new RouteResult();
        // 比如返回的结果是  1号数据源下1表  和 1号数据源下2表   2号数据源下1表  2号数据源下2表
        for (DataNode each : routedDataNodes) {
            RouteUnit routeUnit = new RouteUnit(each.getDataSourceName());
            routeUnit.getTableUnits().add(new TableUnit(logicTableName, each.getTableName()));
            result.getRouteUnits().add(routeUnit);
        }
        return result;
    }

    /**
     * 返回逻辑表对应的所有物理表
     * @param shardingRule
     * @param tableRule
     * @return
     */
    private Collection<DataNode> getDataNodes(final ShardingRule shardingRule, final TableRule tableRule) {
        if (isRoutingByHint(shardingRule, tableRule)) {
            return routeByHint(shardingRule, tableRule);
        }
        // 根据条件对象来分表
        if (isRoutingByShardingConditions(shardingRule, tableRule)) {
            return routeByShardingConditions(shardingRule, tableRule);
        }
        return routeByMixedConditions(shardingRule, tableRule);
    }

    /**
     * 用户强制指定了 路由信息
     * @param shardingRule
     * @param tableRule
     * @return
     */
    private boolean isRoutingByHint(final ShardingRule shardingRule, final TableRule tableRule) {
        return shardingRule.getDatabaseShardingStrategy(tableRule) instanceof HintShardingStrategy && shardingRule.getTableShardingStrategy(tableRule) instanceof HintShardingStrategy;
    }

    /**
     * 根据用户传入的线索 进行分表
     * @param shardingRule
     * @param tableRule
     * @return
     */
    private Collection<DataNode> routeByHint(final ShardingRule shardingRule, final TableRule tableRule) {
        return route0(shardingRule, tableRule, getDatabaseShardingValuesFromHint(), getTableShardingValuesFromHint());
    }
    
    private boolean isRoutingByShardingConditions(final ShardingRule shardingRule, final TableRule tableRule) {
        return !(shardingRule.getDatabaseShardingStrategy(tableRule) instanceof HintShardingStrategy || shardingRule.getTableShardingStrategy(tableRule) instanceof HintShardingStrategy);
    }
    
    private Collection<DataNode> routeByShardingConditions(final ShardingRule shardingRule, final TableRule tableRule) {
        return shardingConditions.getConditions().isEmpty()
                // 没有传入其他参数时 直接返回所有物理数据源以及下面的所有物理节点
                ? route0(shardingRule, tableRule, Collections.emptyList(), Collections.emptyList()) : routeByShardingConditionsWithCondition(shardingRule, tableRule);
    }

    /**
     * 根据条件对象 进行分表
     * @param shardingRule
     * @param tableRule
     * @return
     */
    private Collection<DataNode> routeByShardingConditionsWithCondition(final ShardingRule shardingRule, final TableRule tableRule) {
        Collection<DataNode> result = new LinkedList<>();
        // 遍历本次会影响到分表结果的一系列条件
        for (ShardingCondition each : shardingConditions.getConditions()) {
            Collection<DataNode> dataNodes = route0(shardingRule, tableRule,
                    // 在 dataSource 级别 允许分表的列
                    getShardingValuesFromShardingConditions(shardingRule, shardingRule.getDatabaseShardingStrategy(tableRule).getShardingColumns(), each),
                    // 在 table 级别 允许分表的列
                    getShardingValuesFromShardingConditions(shardingRule, shardingRule.getTableShardingStrategy(tableRule).getShardingColumns(), each));
            // 这里在回填物理节点信息
            each.getDataNodes().addAll(dataNodes);
            // 使用的是 LinkedList 那么某些节点就会重复
            result.addAll(dataNodes);
        }
        return result;
    }

    /**
     * @param shardingRule
     * @param tableRule
     * @return
     */
    private Collection<DataNode> routeByMixedConditions(final ShardingRule shardingRule, final TableRule tableRule) {
        return shardingConditions.getConditions().isEmpty() ? routeByMixedConditionsWithHint(shardingRule, tableRule) : routeByMixedConditionsWithCondition(shardingRule, tableRule);
    }
    
    private Collection<DataNode> routeByMixedConditionsWithCondition(final ShardingRule shardingRule, final TableRule tableRule) {
        Collection<DataNode> result = new LinkedList<>();
        for (ShardingCondition each : shardingConditions.getConditions()) {
            Collection<DataNode> dataNodes = route0(shardingRule, tableRule, getDatabaseShardingValues(shardingRule, tableRule, each), getTableShardingValues(shardingRule, tableRule, each));
            each.getDataNodes().addAll(dataNodes);
            result.addAll(dataNodes);
        }
        return result;
    }
    
    private Collection<DataNode> routeByMixedConditionsWithHint(final ShardingRule shardingRule, final TableRule tableRule) {
        if (shardingRule.getDatabaseShardingStrategy(tableRule) instanceof HintShardingStrategy) {
            return route0(shardingRule, tableRule, getDatabaseShardingValuesFromHint(), Collections.emptyList());
        }
        return route0(shardingRule, tableRule, Collections.emptyList(), getTableShardingValuesFromHint());
    }
    
    private List<RouteValue> getDatabaseShardingValues(final ShardingRule shardingRule, final TableRule tableRule, final ShardingCondition shardingCondition) {
        // 获取分表策略
        ShardingStrategy dataBaseShardingStrategy = shardingRule.getDatabaseShardingStrategy(tableRule);
        return isGettingShardingValuesFromHint(dataBaseShardingStrategy)
                ? getDatabaseShardingValuesFromHint() : getShardingValuesFromShardingConditions(shardingRule, dataBaseShardingStrategy.getShardingColumns(), shardingCondition);
    }
    
    private List<RouteValue> getTableShardingValues(final ShardingRule shardingRule, final TableRule tableRule, final ShardingCondition shardingCondition) {
        ShardingStrategy tableShardingStrategy = shardingRule.getTableShardingStrategy(tableRule);
        return isGettingShardingValuesFromHint(tableShardingStrategy)
                ? getTableShardingValuesFromHint() : getShardingValuesFromShardingConditions(shardingRule, tableShardingStrategy.getShardingColumns(), shardingCondition);
    }
    
    private boolean isGettingShardingValuesFromHint(final ShardingStrategy shardingStrategy) {
        return shardingStrategy instanceof HintShardingStrategy;
    }
    
    private List<RouteValue> getDatabaseShardingValuesFromHint() {
        return getRouteValues(HintManager.isDatabaseShardingOnly() ? HintManager.getDatabaseShardingValues() : HintManager.getDatabaseShardingValues(logicTableName));
    }

    /**
     * 返回分表的结果
     * @return
     */
    private List<RouteValue> getTableShardingValuesFromHint() {
        return getRouteValues(HintManager.getTableShardingValues(logicTableName));
    }
    
    private List<RouteValue> getRouteValues(final Collection<Comparable<?>> shardingValue) {
        return shardingValue.isEmpty() ? Collections.emptyList() : Collections.singletonList(new ListRouteValue<>("", logicTableName, shardingValue));
    }

    /**
     *
     * @param shardingRule  规则对象 内部可以获取到一些全局信息
     * @param shardingColumns  某张表对应的分表策略 下会影响到分表结果的列
     * @param shardingCondition   会影响到结果的条件对象
     * @return
     */
    private List<RouteValue> getShardingValuesFromShardingConditions(final ShardingRule shardingRule, final Collection<String> shardingColumns, final ShardingCondition shardingCondition) {
        List<RouteValue> result = new ArrayList<>(shardingColumns.size());
        // 获取原始信息 比如 a = 3 就是一个 RouteValue
        for (RouteValue each : shardingCondition.getRouteValues()) {
            // 获取该逻辑表的绑定规则
            Optional<BindingTableRule> bindingTableRule = shardingRule.findBindingTableRule(logicTableName);
            // 这里有2种情况 一种是 只有一张表 刚好就是逻辑表 还有一种就是 bindingTable 而且关联的列也是 shardingColumns
            if ((logicTableName.equals(each.getTableName()) || bindingTableRule.isPresent() && bindingTableRule.get().hasLogicTable(logicTableName)) 
                    && shardingColumns.contains(each.getColumnName())) {
                result.add(each);
            }
        }
        return result;
    }

    /**
     * 根据规则对象进行分表
     * @param shardingRule
     * @param tableRule
     * @param databaseShardingValues   database  级别相关的列值
     * @param tableShardingValues   table 级别相关的列值
     * @return   返回的结果代表本次实际的物理表
     */
    private Collection<DataNode> route0(final ShardingRule shardingRule, final TableRule tableRule, final List<RouteValue> databaseShardingValues, final List<RouteValue> tableShardingValues) {
        Collection<String> routedDataSources = routeDataSources(shardingRule, tableRule, databaseShardingValues);
        Collection<DataNode> result = new LinkedList<>();
        // 遍历每个数据源
        for (String each : routedDataSources) {
            // 根据数据源再路由到某个物理表
            result.addAll(routeTables(shardingRule, tableRule, each, tableShardingValues));
        }
        return result;
    }

    /**
     * 选择符合条件的所有数据源
     * @param shardingRule
     * @param tableRule
     * @param databaseShardingValues
     * @return
     */
    private Collection<String> routeDataSources(final ShardingRule shardingRule, final TableRule tableRule, final List<RouteValue> databaseShardingValues) {
        // 如果没有条件信息 那么允许使用任何一个数据源 (尽可能的发挥分数据源的功能)
        if (databaseShardingValues.isEmpty()) {
            return tableRule.getActualDatasourceNames();
        }
        // 根据策略返回本次符合条件的所有数据源
        Collection<String> result = new LinkedHashSet<>(shardingRule.getDatabaseShardingStrategy(tableRule).doSharding(tableRule.getActualDatasourceNames(), databaseShardingValues, this.properties));
        Preconditions.checkState(!result.isEmpty(), "no database route info");
        Preconditions.checkState(tableRule.getActualDatasourceNames().containsAll(result), 
                "Some routed data sources do not belong to configured data sources. routed data sources: `%s`, configured data sources: `%s`", result, tableRule.getActualDatasourceNames());
        return result;
    }

    /**
     * 根据规则信息 路由到某些 物理表(DataNode)
     * @param shardingRule
     * @param tableRule
     * @param routedDataSource
     * @param tableShardingValues
     * @return
     */
    private Collection<DataNode> routeTables(final ShardingRule shardingRule, final TableRule tableRule, final String routedDataSource, final List<RouteValue> tableShardingValues) {
        // 本数据源下所有的物理表
        Collection<String> availableTargetTables = tableRule.getActualTableNames(routedDataSource);
        // 没有路由的关键信息的话 返回所有的物理表 否则根据值 配合策略对象 返回一组物理表  (尽可能的分散到不同的物理表)
        Collection<String> routedTables = new LinkedHashSet<>(tableShardingValues.isEmpty() ? availableTargetTables
                : shardingRule.getTableShardingStrategy(tableRule).doSharding(availableTargetTables, tableShardingValues, this.properties));
        Preconditions.checkState(!routedTables.isEmpty(), "no table route info");
        Collection<DataNode> result = new LinkedList<>();
        for (String each : routedTables) {
            // 本次的物理数据源 以及物理表
            result.add(new DataNode(routedDataSource, each));
        }
        return result;
    }
}
