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

package org.apache.shardingsphere.sharding.rewrite.parameter.impl;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import lombok.Setter;
import org.apache.shardingsphere.sharding.route.engine.context.ShardingRouteContext;
import org.apache.shardingsphere.sql.parser.relation.statement.SQLStatementContext;
import org.apache.shardingsphere.sql.parser.relation.statement.dml.InsertStatementContext;
import org.apache.shardingsphere.sharding.rewrite.aware.ShardingRouteContextAware;
import org.apache.shardingsphere.underlying.rewrite.parameter.builder.ParameterBuilder;
import org.apache.shardingsphere.underlying.rewrite.parameter.builder.impl.GroupedParameterBuilder;
import org.apache.shardingsphere.underlying.rewrite.parameter.rewriter.ParameterRewriter;

import java.util.Iterator;
import java.util.List;

/**
 * Sharding generated key insert value parameter rewriter.
 * 该参数修改器是用来插入自建主键的
 */
@Setter
public final class ShardingGeneratedKeyInsertValueParameterRewriter implements ParameterRewriter<InsertStatementContext>, ShardingRouteContextAware {
    
    private ShardingRouteContext shardingRouteContext;

    /**
     * 首先判断是否满足重写参数的条件   首先本次执行的会话是 insert 且确保了需要插入自增主键
     * @param sqlStatementContext SQL statement context
     * @return
     */
    @Override
    public boolean isNeedRewrite(final SQLStatementContext sqlStatementContext) {
        return sqlStatementContext instanceof InsertStatementContext && shardingRouteContext.getGeneratedKey().isPresent() && shardingRouteContext.getGeneratedKey().get().isGenerated();
    }

    /**
     * 将主键插入到  params 中
     * @param parameterBuilder parameter builder  该对象便于快速修改参数
     * @param insertStatementContext   本次insert 详情
     * @param parameters SQL parameters  本次携带的参数
     */
    @Override
    public void rewrite(final ParameterBuilder parameterBuilder, final InsertStatementContext insertStatementContext, final List<Object> parameters) {
        Preconditions.checkState(shardingRouteContext.getGeneratedKey().isPresent());
        // 设置派生列名   哦哦因为本次自动生成了要插入的主键 所以这里要有一个派生列
        ((GroupedParameterBuilder) parameterBuilder).setDerivedColumnName(shardingRouteContext.getGeneratedKey().get().getColumnName());
        // 所有生成的主键
        Iterator<Comparable<?>> generatedValues = shardingRouteContext.getGeneratedKey().get().getGeneratedValues().descendingIterator();
        int count = 0;
        int parametersCount = 0;
        for (List<Object> each : insertStatementContext.getGroupedParameters()) {
            // 代表每个子插入 对应的使用的参数
            parametersCount += insertStatementContext.getInsertValueContexts().get(count).getParametersCount();
            // 获取一个主键
            Comparable<?> generatedValue = generatedValues.next();
            if (!each.isEmpty()) {
                // 这里将参数追加到第一个
                ((GroupedParameterBuilder) parameterBuilder).getParameterBuilders().get(count).addAddedParameters(parametersCount, Lists.<Object>newArrayList(generatedValue));
            }
            count++;
        }
    }
}
