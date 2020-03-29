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

package org.apache.shardingsphere.sharding.route.engine;

import com.google.common.base.Preconditions;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.api.hint.HintManager;
import org.apache.shardingsphere.core.rule.BindingTableRule;
import org.apache.shardingsphere.core.rule.ShardingRule;
import org.apache.shardingsphere.core.rule.TableRule;
import org.apache.shardingsphere.core.strategy.route.hint.HintShardingStrategy;
import org.apache.shardingsphere.core.strategy.route.value.ListRouteValue;
import org.apache.shardingsphere.core.strategy.route.value.RouteValue;
import org.apache.shardingsphere.sharding.route.engine.condition.ShardingCondition;
import org.apache.shardingsphere.sharding.route.engine.condition.ShardingConditions;
import org.apache.shardingsphere.sharding.route.engine.condition.engine.InsertClauseShardingConditionEngine;
import org.apache.shardingsphere.sharding.route.engine.condition.engine.WhereClauseShardingConditionEngine;
import org.apache.shardingsphere.sharding.route.engine.context.ShardingRouteContext;
import org.apache.shardingsphere.sharding.route.engine.keygen.GeneratedKey;
import org.apache.shardingsphere.sharding.route.engine.type.ShardingRouteEngine;
import org.apache.shardingsphere.sharding.route.engine.type.ShardingRouteEngineFactory;
import org.apache.shardingsphere.sharding.route.engine.validator.ShardingStatementValidatorFactory;
import org.apache.shardingsphere.sql.parser.SQLParserEngine;
import org.apache.shardingsphere.sql.parser.relation.SQLStatementContextFactory;
import org.apache.shardingsphere.sql.parser.relation.metadata.RelationMetas;
import org.apache.shardingsphere.sql.parser.relation.statement.SQLStatementContext;
import org.apache.shardingsphere.sql.parser.relation.statement.dml.InsertStatementContext;
import org.apache.shardingsphere.sql.parser.relation.statement.dml.SelectStatementContext;
import org.apache.shardingsphere.sql.parser.sql.statement.SQLStatement;
import org.apache.shardingsphere.sql.parser.sql.statement.dml.DMLStatement;
import org.apache.shardingsphere.sql.parser.sql.statement.dml.InsertStatement;
import org.apache.shardingsphere.underlying.common.constant.properties.ShardingSphereProperties;
import org.apache.shardingsphere.underlying.common.metadata.ShardingSphereMetaData;
import org.apache.shardingsphere.underlying.route.DateNodeRouter;
import org.apache.shardingsphere.underlying.route.context.RouteResult;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Sharding router.
 * 分表的路由器对象
 */
@RequiredArgsConstructor
public final class ShardingRouter implements DateNodeRouter {

    /**
     * 使用的规则对象
     */
    private final ShardingRule shardingRule;

    private final ShardingSphereProperties properties;
    
    private final ShardingSphereMetaData metaData;

    /**
     * 解析sql 的引擎对象  会生成ast
     */
    private final SQLParserEngine sqlParserEngine;

    /**
     * 根据一组 参数 以及sql语句 生成一个路由上下文
     * @param sql SQL
     * @param parameters SQL parameters
     * @param useCache whether use cache to save SQL parse result   是否需要缓存结果
     * @return
     */
    @Override
    @SuppressWarnings("unchecked")
    public ShardingRouteContext route(final String sql, final List<Object> parameters, final boolean useCache) {
        // 解析sql 生成语法树后 通过sql类型生成对应的会话  此时sql相关元素已经解析到sqlStatement内部了
        SQLStatement sqlStatement = parse(sql, useCache);
        // 校验会话本身是否合法 (主要是检查 insert 和 update)
        ShardingStatementValidatorFactory.newInstance(sqlStatement).ifPresent(shardingStatementValidator -> shardingStatementValidator.validate(shardingRule, sqlStatement, parameters));
        // 根据sql的类型生成具体的 statement  比如 InsertStatementContext
        SQLStatementContext sqlStatementContext = SQLStatementContextFactory.newInstance(metaData.getRelationMetas(), sql, parameters, sqlStatement);
        // 本次自增键 如果没有就是empty 也可以是用户设置 又或者是通过 UUID 和 雪花算法生成
        Optional<GeneratedKey> generatedKey = sqlStatement instanceof InsertStatement
                ? GeneratedKey.getGenerateKey(shardingRule, metaData.getTables(), parameters, (InsertStatement) sqlStatement) : Optional.empty();

        // 获取会影响到本次 分表的所有条件对象
        ShardingConditions shardingConditions = getShardingConditions(parameters, sqlStatementContext, generatedKey.orElse(null), metaData.getRelationMetas());
        // 如果是select 语句 并且包含子查询 认为需要合并  TODO 这里先不看
        boolean needMergeShardingValues = isNeedMergeShardingValues(sqlStatementContext);
        if (sqlStatementContext.getSqlStatement() instanceof DMLStatement && needMergeShardingValues) {
            // TODO  这里要求子查询必须有分表键条件 且 条件必须相同 场景比较难想 先跳过
            checkSubqueryShardingValues(sqlStatementContext, shardingConditions);
            mergeShardingConditions(shardingConditions);
        }
        // 生成路由引擎对象
        ShardingRouteEngine shardingRouteEngine = ShardingRouteEngineFactory.newInstance(shardingRule, metaData, sqlStatementContext, shardingConditions, properties);
        // 将全局规则传入 并生成分表结果
        RouteResult routeResult = shardingRouteEngine.route(shardingRule);
        if (needMergeShardingValues) {
            Preconditions.checkState(1 == routeResult.getRouteUnits().size(), "Must have one sharding with subquery.");
        }
        // 将本次路由结果 以及一些其他上下文信息包装
        return new ShardingRouteContext(sqlStatementContext, routeResult, shardingConditions, generatedKey.orElse(null));
    }
    
    /*
     * To make sure SkyWalking will be available at the next release of ShardingSphere,
     * a new plugin should be provided to SkyWalking project if this API changed.
     *
     * @see <a href="https://github.com/apache/skywalking/blob/master/docs/en/guides/Java-Plugin-Development-Guide.md#user-content-plugin-development-guide">Plugin Development Guide</a>
     * 使用引擎解析
     */
    private SQLStatement parse(final String sql, final boolean useCache) {
        return sqlParserEngine.parse(sql, useCache);
    }

    /**
     * 获取分表条件规则对象
     * @param parameters
     * @param sqlStatementContext
     * @param generatedKey
     * @param relationMetas
     * @return
     */
    private ShardingConditions getShardingConditions(final List<Object> parameters, final SQLStatementContext sqlStatementContext, final GeneratedKey generatedKey, final RelationMetas relationMetas) {
        // 只针对 DML 在这里 DML 除了 增删改 还包含 查
        if (sqlStatementContext.getSqlStatement() instanceof DMLStatement) {
            if (sqlStatementContext instanceof InsertStatementContext) {
                InsertStatementContext shardingInsertStatement = (InsertStatementContext) sqlStatementContext;
                // 这里会返回一组 condition 对象 它们都会成为影响 实际sql的因素
                return new ShardingConditions(new InsertClauseShardingConditionEngine(shardingRule).createShardingConditions(shardingInsertStatement, generatedKey, parameters));
            }
            // 针对其余 增删改的情况
            return new ShardingConditions(new WhereClauseShardingConditionEngine(shardingRule, relationMetas).createShardingConditions(sqlStatementContext, parameters));
        }
        return new ShardingConditions(Collections.emptyList());
    }

    /**
     * 如果是查询语句 且包含子查询 且本次涉及到的逻辑表的 规则不为空
     * @param sqlStatementContext
     * @return
     */
    private boolean isNeedMergeShardingValues(final SQLStatementContext sqlStatementContext) {
        return sqlStatementContext instanceof SelectStatementContext && ((SelectStatementContext) sqlStatementContext).isContainsSubquery() 
                && !shardingRule.getShardingLogicTableNames(sqlStatementContext.getTablesContext().getTableNames()).isEmpty();
    }

    /**
     * 检查子查询的值
     * @param sqlStatementContext
     * @param shardingConditions  本次会影响到分表逻辑的所有条件项
     */
    private void checkSubqueryShardingValues(final SQLStatementContext sqlStatementContext, final ShardingConditions shardingConditions) {
        for (String each : sqlStatementContext.getTablesContext().getTableNames()) {
            // 找到该表相关的规则对象
            Optional<TableRule> tableRule = shardingRule.findTableRule(each);
            // 如果由用户指定了分表规则 那么直接返回
            if (tableRule.isPresent() && isRoutingByHint(tableRule.get()) && !HintManager.getDatabaseShardingValues(each).isEmpty() && !HintManager.getTableShardingValues(each).isEmpty()) {
                return;
            }
        }
        // 当包含子查询时 子查询必须包含 shardingColumn  并且 这些列的值会被包装成 shardingCondition
        Preconditions.checkState(!shardingConditions.getConditions().isEmpty(), "Must have sharding column with subquery.");
        if (shardingConditions.getConditions().size() > 1) {
            // 条件必须相同
            Preconditions.checkState(isSameShardingCondition(shardingConditions), "Sharding value must same with subquery.");
        }
    }

    /**
     * 是否通过用户自定义了分表规则
     * @param tableRule
     * @return
     */
    private boolean isRoutingByHint(final TableRule tableRule) {
        return shardingRule.getDatabaseShardingStrategy(tableRule) instanceof HintShardingStrategy && shardingRule.getTableShardingStrategy(tableRule) instanceof HintShardingStrategy;
    }

    /**
     * 是否是相同的分表条件
     * @param shardingConditions
     * @return
     */
    private boolean isSameShardingCondition(final ShardingConditions shardingConditions) {
        // 默认移除最后一个条件对象
        ShardingCondition example = shardingConditions.getConditions().remove(shardingConditions.getConditions().size() - 1);
        for (ShardingCondition each : shardingConditions.getConditions()) {
            if (!isSameShardingCondition(example, each)) {
                return false;
            }
        }
        return true;
    }
    
    private boolean isSameShardingCondition(final ShardingCondition shardingCondition1, final ShardingCondition shardingCondition2) {
        if (shardingCondition1.getRouteValues().size() != shardingCondition2.getRouteValues().size()) {
            return false;
        }
        for (int i = 0; i < shardingCondition1.getRouteValues().size(); i++) {
            RouteValue shardingValue1 = shardingCondition1.getRouteValues().get(i);
            RouteValue shardingValue2 = shardingCondition2.getRouteValues().get(i);
            if (!isSameRouteValue((ListRouteValue) shardingValue1, (ListRouteValue) shardingValue2)) {
                return false;
            }
        }
        return true;
    }
    
    private boolean isSameRouteValue(final ListRouteValue routeValue1, final ListRouteValue routeValue2) {
        return isSameLogicTable(routeValue1, routeValue2)
                && routeValue1.getColumnName().equals(routeValue2.getColumnName()) && routeValue1.getValues().equals(routeValue2.getValues());
    }
    
    private boolean isSameLogicTable(final ListRouteValue shardingValue1, final ListRouteValue shardingValue2) {
        return shardingValue1.getTableName().equals(shardingValue2.getTableName()) || isBindingTable(shardingValue1, shardingValue2);
    }
    
    private boolean isBindingTable(final ListRouteValue shardingValue1, final ListRouteValue shardingValue2) {
        Optional<BindingTableRule> bindingRule = shardingRule.findBindingTableRule(shardingValue1.getTableName());
        return bindingRule.isPresent() && bindingRule.get().hasLogicTable(shardingValue2.getTableName());
    }

    /**
     * 这里只保留最后一组 啥意思???
     * @param shardingConditions
     */
    private void mergeShardingConditions(final ShardingConditions shardingConditions) {
        if (shardingConditions.getConditions().size() > 1) {
            ShardingCondition shardingCondition = shardingConditions.getConditions().remove(shardingConditions.getConditions().size() - 1);
            shardingConditions.getConditions().clear();
            shardingConditions.getConditions().add(shardingCondition);
        }
    }
}
