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

import com.google.common.base.Preconditions;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.sql.parser.relation.segment.insert.InsertValueContext;
import org.apache.shardingsphere.sql.parser.relation.statement.dml.InsertStatementContext;
import org.apache.shardingsphere.sql.parser.sql.segment.dml.expr.ExpressionSegment;
import org.apache.shardingsphere.sql.parser.sql.segment.dml.expr.simple.LiteralExpressionSegment;
import org.apache.shardingsphere.sql.parser.sql.segment.dml.expr.simple.ParameterMarkerExpressionSegment;
import org.apache.shardingsphere.sql.parser.sql.segment.dml.expr.simple.SimpleExpressionSegment;
import org.apache.shardingsphere.sharding.route.engine.condition.ExpressionConditionUtils;
import org.apache.shardingsphere.sharding.route.engine.condition.ShardingCondition;
import org.apache.shardingsphere.sharding.route.engine.keygen.GeneratedKey;
import org.apache.shardingsphere.sharding.route.spi.SPITimeService;
import org.apache.shardingsphere.core.rule.ShardingRule;
import org.apache.shardingsphere.core.strategy.route.value.ListRouteValue;
import org.apache.shardingsphere.underlying.common.exception.ShardingSphereException;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Sharding condition engine for insert clause.
 * 该对象用于处理 insert 情况 并使之使用于分表的情况
 */
@RequiredArgsConstructor
public final class InsertClauseShardingConditionEngine {
    
    private final ShardingRule shardingRule;
    
    /**
     * Create sharding conditions.
     * 
     * @param insertStatementContext insert statement context  本次ast树抽取出来的信息
     * @param generatedKey generated key   本次包含的自增键
     * @param parameters SQL parameters   insert 语句相关的所有参数
     * @return sharding conditions
     */
    public List<ShardingCondition> createShardingConditions(final InsertStatementContext insertStatementContext, final GeneratedKey generatedKey, final List<Object> parameters) {
        List<ShardingCondition> result = new LinkedList<>();
        // 获取逻辑表信息
        String tableName = insertStatementContext.getSqlStatement().getTable().getTableName().getIdentifier().getValue();
        // 获取所有列名
        Collection<String> columnNames = getColumnNames(insertStatementContext, generatedKey);
        // 每个each 对应一个insert 的子插入 (针对批量插入的情况 如果是普通插入 那么就是参数以及字段信息)
        // 每个子句都对应一个条件对象 同时该condition 中包含了 列名 参数值等
        for (InsertValueContext each : insertStatementContext.getInsertValueContexts()) {
            result.add(createShardingCondition(tableName, columnNames.iterator(), each, parameters));
        }
        // 如果是本次还生成了 自增键 那么要将信息追加到条件对象中  因为如果本来插入的语句中就有主键 那么 就不需要在下面再插入一次了
        if (null != generatedKey && generatedKey.isGenerated() && shardingRule.isShardingColumn(generatedKey.getColumnName(), tableName)) {
            appendGeneratedKeyCondition(generatedKey, tableName, result);
        }
        return result;
    }

    /**
     * 获取所有列名
     */
    private Collection<String> getColumnNames(final InsertStatementContext insertStatementContext, final GeneratedKey generatedKey) {
        // 返回所有插入的列
        if (null == generatedKey || !generatedKey.isGenerated()) {
            return insertStatementContext.getColumnNames();
        }
        // 如果自增键是通过shardingSphere 那么移除掉这部分
        Collection<String> result = new LinkedList<>(insertStatementContext.getColumnNames());
        result.remove(generatedKey.getColumnName());
        return result;
    }

    /**
     * 每个 ShardingCondition 对应一个子句的 所有影响分表信息的列
     * @param tableName
     * @param columnNames   每次传进来的迭代器都是新的
     * @param insertValueContext
     * @param parameters
     * @return
     */
    private ShardingCondition createShardingCondition(final String tableName, final Iterator<String> columnNames, final InsertValueContext insertValueContext, final List<Object> parameters) {
        ShardingCondition result = new ShardingCondition();
        // 时间相关的服务 有通过 new Date 或者从数据库查询
        SPITimeService timeService = new SPITimeService();
        // 遍历每个子部分
        for (ExpressionSegment each : insertValueContext.getValueExpressions()) {
            // 每个部分刚好对应某个插入的列
            String columnName = columnNames.next();
            // 该列有在配置文件中设置分表规则
            if (shardingRule.isShardingColumn(columnName, tableName)) {
                if (each instanceof SimpleExpressionSegment) {
                    // 通过 getRouteValue 定位到了某个具体的参数 之后配合columnName 包装成routeValue
                    result.getRouteValues().add(new ListRouteValue<>(columnName, tableName, Collections.singletonList(getRouteValue((SimpleExpressionSegment) each, parameters))));
                    // 如果是 "now()" 那么创建当前时间作为参数
                } else if (ExpressionConditionUtils.isNowExpression(each)) {
                    result.getRouteValues().add(new ListRouteValue<>(columnName, tableName, Collections.singletonList(timeService.getTime())));
                } else if (ExpressionConditionUtils.isNullExpression(each)) {
                    throw new ShardingSphereException("Insert clause sharding column can't be null.");
                }
            }
        }
        return result;
    }
    
    private Comparable<?> getRouteValue(final SimpleExpressionSegment expressionSegment, final List<Object> parameters) {
        Object result;
        // 根据下标从 param 中获取合适的元素
        if (expressionSegment instanceof ParameterMarkerExpressionSegment) {
            result = parameters.get(((ParameterMarkerExpressionSegment) expressionSegment).getParameterMarkerIndex());
        } else {
            // TODO 这啥呀
            result = ((LiteralExpressionSegment) expressionSegment).getLiterals();
        }
        Preconditions.checkArgument(result instanceof Comparable, "Sharding value must implements Comparable.");
        return (Comparable) result;
    }

    /**
     * 触发该方法的前提是该列属于 sharding 列
     * @param generatedKey
     * @param tableName
     * @param shardingConditions
     */
    private void appendGeneratedKeyCondition(final GeneratedKey generatedKey, final String tableName, final List<ShardingCondition> shardingConditions) {
        // 有多少个 ShardingCondition 就有多少个子句 就有多少个 generatedValues  所以是一一对应的关系
        Iterator<Comparable<?>> generatedValues = generatedKey.getGeneratedValues().iterator();
        // 补偿一个routeValue 信息
        for (ShardingCondition each : shardingConditions) {
            each.getRouteValues().add(new ListRouteValue<>(generatedKey.getColumnName(), tableName, Collections.<Comparable<?>>singletonList(generatedValues.next())));
        }
    }
}
