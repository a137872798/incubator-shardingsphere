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

package org.apache.shardingsphere.sharding.rewrite.context;

import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.sharding.route.engine.context.ShardingRouteContext;
import org.apache.shardingsphere.core.rule.ShardingRule;
import org.apache.shardingsphere.sharding.rewrite.parameter.ShardingParameterRewriterBuilder;
import org.apache.shardingsphere.sharding.rewrite.token.pojo.impl.ShardingTokenGenerateBuilder;
import org.apache.shardingsphere.underlying.common.constant.properties.ShardingSphereProperties;
import org.apache.shardingsphere.underlying.rewrite.context.SQLRewriteContext;
import org.apache.shardingsphere.underlying.rewrite.context.SQLRewriteContextDecorator;
import org.apache.shardingsphere.underlying.rewrite.parameter.rewriter.ParameterRewriter;

/**
 * SQL rewrite context decorator for sharding.
 * 改写上下文的 装饰对象
 */
@RequiredArgsConstructor
public final class ShardingSQLRewriteContextDecorator implements SQLRewriteContextDecorator<ShardingRule> {
    
    private final ShardingRouteContext shardingRouteContext;

    /**
     * 这里针对 插入语句自动生成主键  以及分页取消 limit 改写了原有参数
     * @param shardingRule
     * @param properties ShardingSphere properties
     * @param sqlRewriteContext SQL rewrite context to be decorated
     */
    @Override
    public void decorate(final ShardingRule shardingRule, final ShardingSphereProperties properties, final SQLRewriteContext sqlRewriteContext) {
        for (ParameterRewriter each : new ShardingParameterRewriterBuilder(shardingRule, shardingRouteContext).getParameterRewriters(sqlRewriteContext.getRelationMetas())) {
            // 当判断参数需要被重写时 进行重写
            if (!sqlRewriteContext.getParameters().isEmpty() && each.isNeedRewrite(sqlRewriteContext.getSqlStatementContext())) {
                each.rewrite(sqlRewriteContext.getParameterBuilder(), sqlRewriteContext.getSqlStatementContext(), sqlRewriteContext.getParameters());
            }
        }
        // 这里添加一组token 生成器   逻辑sql 配合本次分表结果会需要这些token 生成器 变成多个物理sql
        sqlRewriteContext.addSQLTokenGenerators(new ShardingTokenGenerateBuilder(shardingRule, shardingRouteContext).getSQLTokenGenerators());
    }
}
