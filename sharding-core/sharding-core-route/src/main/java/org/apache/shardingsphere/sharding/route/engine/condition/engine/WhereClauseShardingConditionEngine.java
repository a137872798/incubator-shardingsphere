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

package org.apache.shardingsphere.sharding.route.engine.condition.engine;

import com.google.common.collect.Range;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.core.rule.ShardingRule;
import org.apache.shardingsphere.core.strategy.route.value.ListRouteValue;
import org.apache.shardingsphere.core.strategy.route.value.RangeRouteValue;
import org.apache.shardingsphere.core.strategy.route.value.RouteValue;
import org.apache.shardingsphere.sharding.route.engine.condition.AlwaysFalseRouteValue;
import org.apache.shardingsphere.sharding.route.engine.condition.AlwaysFalseShardingCondition;
import org.apache.shardingsphere.sharding.route.engine.condition.Column;
import org.apache.shardingsphere.sharding.route.engine.condition.ShardingCondition;
import org.apache.shardingsphere.sharding.route.engine.condition.generator.ConditionValueGeneratorFactory;
import org.apache.shardingsphere.sql.parser.relation.metadata.RelationMetas;
import org.apache.shardingsphere.sql.parser.relation.type.WhereAvailable;
import org.apache.shardingsphere.sql.parser.relation.statement.SQLStatementContext;
import org.apache.shardingsphere.sql.parser.sql.segment.dml.predicate.AndPredicate;
import org.apache.shardingsphere.sql.parser.sql.segment.dml.predicate.PredicateSegment;
import org.apache.shardingsphere.sql.parser.sql.segment.dml.predicate.WhereSegment;
import org.apache.shardingsphere.underlying.common.exception.ShardingSphereException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

/**
 * Sharding condition engine for where clause.
 */
@RequiredArgsConstructor
public final class WhereClauseShardingConditionEngine {
    
    private final ShardingRule shardingRule;

    /**
     * 该对象标注了 本次执行的sql使用了哪些表 以及它们的列
     */
    private final RelationMetas relationMetas;
    
    /**
     * Create sharding conditions.
     * 
     * @param sqlStatementContext SQL statement context
     * @param parameters SQL parameters
     * @return sharding conditions
     * 创建本次会影响代理逻辑的所有因素
     */
    public List<ShardingCondition> createShardingConditions(final SQLStatementContext sqlStatementContext, final List<Object> parameters) {
        if (!(sqlStatementContext instanceof WhereAvailable)) {
            return Collections.emptyList();
        }
        List<ShardingCondition> result = new ArrayList<>();
        // 获取 "where" 相关的信息
        Optional<WhereSegment> whereSegment = ((WhereAvailable) sqlStatementContext).getWhere();
        if (whereSegment.isPresent()) {
            result.addAll(createShardingConditions(sqlStatementContext, whereSegment.get().getAndPredicates(), parameters));
        }
        // FIXME process subquery
//        Collection<SubqueryPredicateSegment> subqueryPredicateSegments = sqlStatementContext.findSQLSegments(SubqueryPredicateSegment.class);
//        for (SubqueryPredicateSegment each : subqueryPredicateSegments) {
//            Collection<ShardingCondition> subqueryShardingConditions = createShardingConditions((WhereSegmentAvailable) sqlStatementContext, each.getAndPredicates(), parameters);
//            if (!result.containsAll(subqueryShardingConditions)) {
//                result.addAll(subqueryShardingConditions);
//            }
//        }
        return result;
    }

    /**
     *
     * @param sqlStatementContext  本次会话相关的信息
     * @param andPredicates  一组谓语信息 对应 where xxx = xxx
     * @param parameters  对应 where 传入的参数
     * @return
     */
    private Collection<ShardingCondition> createShardingConditions(final SQLStatementContext sqlStatementContext, final Collection<AndPredicate> andPredicates, final List<Object> parameters) {
        Collection<ShardingCondition> result = new LinkedList<>();
        // where (a.id = 3 or a.id = 5) and (b.id = 3 or b.id = 5)  2个 and 条件
        for (AndPredicate each : andPredicates) {
            // 代表针对某个列的多个限制条件  比如 where  (a.id = 3 or a.id = 5) 这里就是2个条件
            Map<Column, Collection<RouteValue>> routeValueMap = createRouteValueMap(sqlStatementContext, each, parameters);
            if (routeValueMap.isEmpty()) {
                return Collections.emptyList();
            }
            // 将某个子条件包装成 condition 并添加到列表中
            result.add(createShardingCondition(routeValueMap));
        }
        return result;
    }

    /**
     *
     * @param sqlStatementContext   本次会话信息
     * @param andPredicate   and 信息
     * @param parameters   使用的参数
     * @return
     */
    private Map<Column, Collection<RouteValue>> createRouteValueMap(final SQLStatementContext sqlStatementContext, final AndPredicate andPredicate, final List<Object> parameters) {
        Map<Column, Collection<RouteValue>> result = new HashMap<>();
        // 代表每个 and 谓语内部还有多个 片段 比如 and (xxx or yyy)
        for (PredicateSegment each : andPredicate.getPredicates()) {
            // 如果谓语对象设置了表名 那么直接获取表名 否则 通过谓语设置的列名 配合 relationMeta 找到对应的表名
            Optional<String> tableName = sqlStatementContext.getTablesContext().findTableName(each, relationMetas);
            // 如果没有找到表名  或者该列 跟分表规则无关
            if (!tableName.isPresent() || !shardingRule.isShardingColumn(each.getColumn().getIdentifier().getValue(), tableName.get())) {
                continue;
            }
            // 根据列名 和表名 生成 column 对象
            Column column = new Column(each.getColumn().getIdentifier().getValue(), tableName.get());
            // 该对象内部就是 xx字段必须是什么值
            Optional<RouteValue> routeValue = ConditionValueGeneratorFactory.generate(each.getRightValue(), column, parameters);
            if (!routeValue.isPresent()) {
                continue;
            }
            if (!result.containsKey(column)) {
                result.put(column, new LinkedList<>());
            }
            result.get(column).add(routeValue.get());
        }
        return result;
    }

    /**
     * 根据某组路由规则 生成condition 对象
     * @param routeValueMap
     * @return
     */
    private ShardingCondition createShardingCondition(final Map<Column, Collection<RouteValue>> routeValueMap) {
        ShardingCondition result = new ShardingCondition();
        for (Entry<Column, Collection<RouteValue>> entry : routeValueMap.entrySet()) {
            try {
                // 合并成单个 RouteValue
                RouteValue routeValue = mergeRouteValues(entry.getKey(), entry.getValue());
                if (routeValue instanceof AlwaysFalseRouteValue) {
                    return new AlwaysFalseShardingCondition();
                }
                result.getRouteValues().add(routeValue);
            } catch (final ClassCastException ex) {
                throw new ShardingSphereException("Found different types for sharding value `%s`.", entry.getKey());
            }
        }
        return result;
    }

    /**
     *
     * @param column  某一列
     * @param routeValues  该列需要满足的约束条件
     * @return
     */
    @SuppressWarnings("unchecked")
    private RouteValue mergeRouteValues(final Column column, final Collection<RouteValue> routeValues) {
        Collection<Comparable<?>> listValue = null;
        Range<Comparable<?>> rangeValue = null;
        for (RouteValue each : routeValues) {
            // 整合所有 “=” “in” 条件
            if (each instanceof ListRouteValue) {
                listValue = mergeListRouteValues(((ListRouteValue) each).getValues(), listValue);
                if (listValue.isEmpty()) {
                    return new AlwaysFalseRouteValue();
                }
            // 整合所有范围条件
            } else if (each instanceof RangeRouteValue) {
                try {
                    rangeValue = mergeRangeRouteValues(((RangeRouteValue) each).getValueRange(), rangeValue);
                } catch (final IllegalArgumentException ex) {
                    return new AlwaysFalseRouteValue();
                }
            }
        }
        if (null == listValue) {
            return new RangeRouteValue<>(column.getName(), column.getTableName(), rangeValue);
        }
        if (null == rangeValue) {
            return new ListRouteValue<>(column.getName(), column.getTableName(), listValue);
        }
        // 整合 2种类型的限制
        listValue = mergeListAndRangeRouteValues(listValue, rangeValue);
        return listValue.isEmpty() ? new AlwaysFalseRouteValue() : new ListRouteValue<>(column.getName(), column.getTableName(), listValue);
    }
    
    private Collection<Comparable<?>> mergeListRouteValues(final Collection<Comparable<?>> value1, final Collection<Comparable<?>> value2) {
        if (null == value2) {
            return value1;
        }
        value1.retainAll(value2);
        return value1;
    }
    
    private Range<Comparable<?>> mergeRangeRouteValues(final Range<Comparable<?>> value1, final Range<Comparable<?>> value2) {
        return null == value2 ? value1 : value1.intersection(value2);   // 计算交集
    }

    // 同时满足范围条件和  "=" 条件的
    private Collection<Comparable<?>> mergeListAndRangeRouteValues(final Collection<Comparable<?>> listValue, final Range<Comparable<?>> rangeValue) {
        Collection<Comparable<?>> result = new LinkedList<>();
        for (Comparable<?> each : listValue) {
            if (rangeValue.contains(each)) {
                result.add(each);
            }
        }
        return result;
    }
}
