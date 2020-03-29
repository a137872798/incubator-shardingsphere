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

package org.apache.shardingsphere.sharding.route.engine.type.complex;

import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.core.rule.ShardingRule;
import org.apache.shardingsphere.core.rule.TableRule;
import org.apache.shardingsphere.sharding.route.engine.condition.ShardingConditions;
import org.apache.shardingsphere.sharding.route.engine.type.ShardingRouteEngine;
import org.apache.shardingsphere.sharding.route.engine.type.standard.ShardingStandardRoutingEngine;
import org.apache.shardingsphere.sql.parser.relation.statement.SQLStatementContext;
import org.apache.shardingsphere.underlying.common.constant.properties.ShardingSphereProperties;
import org.apache.shardingsphere.underlying.common.exception.ShardingSphereException;
import org.apache.shardingsphere.underlying.route.context.RouteResult;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Sharding complex routing engine.
 * 复合路由引擎
 */
@RequiredArgsConstructor
public final class ShardingComplexRoutingEngine implements ShardingRouteEngine {
    
    private final Collection<String> logicTables;
    
    private final SQLStatementContext sqlStatementContext;
    
    private final ShardingConditions shardingConditions;

    private final ShardingSphereProperties properties;
    
    @Override
    public RouteResult route(final ShardingRule shardingRule) {
        // 针对每组逻辑表(这里的组代表某个逻辑表使用了同一个表规则 那么它们路由的结果是一致的) 包含不同的 路由结果
        Collection<RouteResult> result = new ArrayList<>(logicTables.size());
        Collection<String> bindingTableNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        for (String each : logicTables) {
            // 找到某个逻辑表对应的 规则对象 该对象内部包含了 针对 database 级别 和 table级别的分表策略
            Optional<TableRule> tableRule = shardingRule.findTableRule(each);
            if (tableRule.isPresent()) {
                // 绑定在一起的表规则下所有的逻辑表 只要路由一次就可以 比如 一共5个字段 其中3个是绑定同一个路由规则
                // 那么只要针对某个字段进行路由后 其他2个实际上就不需要再路由了 (这3个路由结果是相同的)
                if (!bindingTableNames.contains(each)) {
                    // 每个逻辑表对应一组 分表结果
                    result.add(new ShardingStandardRoutingEngine(tableRule.get().getLogicTable(), sqlStatementContext, shardingConditions, properties).route(shardingRule));
                }
                // 在这里添加后 上面就不会单独生成 ShardingStandardRoutingEngine 了
                shardingRule.findBindingTableRule(each).ifPresent(bindingTableRule -> bindingTableNames.addAll(
                    bindingTableRule.getTableRules().stream().map(TableRule::getLogicTable).collect(Collectors.toList())));
            }
        }
        if (result.isEmpty()) {
            throw new ShardingSphereException("Cannot find table rule and default data source with logic tables: '%s'", logicTables);
        }
        if (1 == result.size()) {
            return result.iterator().next();
        }
        // 将路由结果 通过笛卡尔路由对象 进行组合
        return new ShardingCartesianRoutingEngine(result).route(shardingRule);
    }
}
