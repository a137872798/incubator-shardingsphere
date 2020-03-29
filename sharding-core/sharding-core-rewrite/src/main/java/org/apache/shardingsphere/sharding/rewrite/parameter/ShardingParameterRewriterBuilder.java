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

package org.apache.shardingsphere.sharding.rewrite.parameter;

import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.sharding.route.engine.context.ShardingRouteContext;
import org.apache.shardingsphere.core.rule.ShardingRule;
import org.apache.shardingsphere.sharding.rewrite.parameter.impl.ShardingGeneratedKeyInsertValueParameterRewriter;
import org.apache.shardingsphere.sharding.rewrite.parameter.impl.ShardingPaginationParameterRewriter;
import org.apache.shardingsphere.sql.parser.relation.metadata.RelationMetas;
import org.apache.shardingsphere.underlying.rewrite.parameter.rewriter.ParameterRewriter;
import org.apache.shardingsphere.underlying.rewrite.parameter.rewriter.ParameterRewriterBuilder;
import org.apache.shardingsphere.sharding.rewrite.aware.ShardingRouteContextAware;
import org.apache.shardingsphere.core.rule.aware.ShardingRuleAware;
import org.apache.shardingsphere.underlying.rewrite.sql.token.generator.aware.RelationMetasAware;

import java.util.Collection;
import java.util.LinkedList;

/**
 * Parameter rewriter builder for sharding.
 * 参数修改器
 */
@RequiredArgsConstructor
public final class ShardingParameterRewriterBuilder implements ParameterRewriterBuilder {

    /**
     * 该对象内部包含了一系列的rule 对象
     */
    private final ShardingRule shardingRule;
    
    private final ShardingRouteContext shardingRouteContext;

    /**
     * 获取2个重写参数的对象 并返回
     * @param relationMetas relation metas   记录了所有表 以及它们有哪些列
     * @return
     */
    @Override
    public Collection<ParameterRewriter> getParameterRewriters(final RelationMetas relationMetas) {
        Collection<ParameterRewriter> result = getParameterRewriters();
        for (ParameterRewriter each : result) {
            setUpParameterRewriters(each, relationMetas);
        }
        return result;
    }

    /**
     * 获取用于修改参数的插件
     * @return0
     */
    private static Collection<ParameterRewriter> getParameterRewriters() {
        Collection<ParameterRewriter> result = new LinkedList<>();
        // 用于插入 主键 和分页的
        result.add(new ShardingGeneratedKeyInsertValueParameterRewriter());
        result.add(new ShardingPaginationParameterRewriter());
        return result;
    }

    /**
     * 填充一些必备的信息
     * @param parameterRewriter
     * @param relationMetas
     */
    private void setUpParameterRewriters(final ParameterRewriter parameterRewriter, final RelationMetas relationMetas) {
        if (parameterRewriter instanceof RelationMetasAware) {
            ((RelationMetasAware) parameterRewriter).setRelationMetas(relationMetas);
        }
        if (parameterRewriter instanceof ShardingRuleAware) {
            ((ShardingRuleAware) parameterRewriter).setShardingRule(shardingRule);
        }
        if (parameterRewriter instanceof ShardingRouteContextAware) {
            ((ShardingRouteContextAware) parameterRewriter).setShardingRouteContext(shardingRouteContext);
        }
    }
}
