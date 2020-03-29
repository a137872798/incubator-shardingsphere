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

import com.google.common.collect.Sets;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.core.rule.ShardingRule;
import org.apache.shardingsphere.sharding.route.engine.type.ShardingRouteEngine;
import org.apache.shardingsphere.underlying.route.context.RouteResult;
import org.apache.shardingsphere.underlying.route.context.RouteUnit;
import org.apache.shardingsphere.underlying.route.context.TableUnit;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Sharding cartesian routing engine.
 * 使用笛卡尔积 组合本次路由结果
 */
@RequiredArgsConstructor
public final class ShardingCartesianRoutingEngine implements ShardingRouteEngine {

    /**
     * 本次路由的多组结果
     */
    private final Collection<RouteResult> routeResults;

    /**
     * 根据某个组规则 组合 Collection<RouteResult>
     * @param shardingRule sharding rule
     * @return
     */
    @Override
    public RouteResult route(final ShardingRule shardingRule) {
        RouteResult result = new RouteResult();
        // 获取数据源交集下所有的逻辑表
        for (Entry<String, Set<String>> entry : getDataSourceLogicTablesMap().entrySet()) {
            List<Set<String>> actualTableGroups = getActualTableGroups(entry.getKey(), entry.getValue());
            // 将物理源  和 物理表组 结合成 tableUnit
            List<Set<TableUnit>> routingTableGroups = toRoutingTableGroups(entry.getKey(), actualTableGroups);
            // Sets.cartesianProduct 将 [1,2,3] [x,y,z] 变成了  (x,1),(x,2) ....  也就是各种物理表可能的结合
            result.getRouteUnits().addAll(getRouteUnits(entry.getKey(), Sets.cartesianProduct(routingTableGroups)));
        }
        return result;
    }

    /**
     * 返回以物理数据源为key  逻辑表为value 的map
     * @return
     */
    private Map<String, Set<String>> getDataSourceLogicTablesMap() {
        // 获取数据源交集 
        Collection<String> intersectionDataSources = getIntersectionDataSources();
        Map<String, Set<String>> result = new HashMap<>(routeResults.size());
        // 这里遍历 之前每个逻辑表对应的分组结果
        for (RouteResult each : routeResults) {
            // key 是物理数据源 value 是对应的所有逻辑表
            for (Entry<String, Set<String>> entry : each.getDataSourceLogicTablesMap(intersectionDataSources).entrySet()) {
                if (result.containsKey(entry.getKey())) {
                    result.get(entry.getKey()).addAll(entry.getValue());
                } else {
                    result.put(entry.getKey(), entry.getValue());
                }
            }
        }
        return result;
    }

    /**
     * 获取所有路由结果的  数据源的交集部分 比如 col1, col2 ,col3 会被路由到数据源1，2 下的各个物理表
     * 而 col4 col5 只被路由到了数据源1 那么它们的交集就是 数据源1
     * @return
     */
    private Collection<String> getIntersectionDataSources() {
        Collection<String> result = new HashSet<>();
        for (RouteResult each : routeResults) {
            if (result.isEmpty()) {
                result.addAll(each.getDataSourceNames());
            }
            result.retainAll(each.getDataSourceNames());
        }
        return result;
    }

    /**
     *
     * @param dataSourceName  物理数据源
     * @param logicTables   一组对应的逻辑表
     * @return
     */
    private List<Set<String>> getActualTableGroups(final String dataSourceName, final Set<String> logicTables) {
        // 某数据源下所有逻辑表对应的物理表  Set 代表物理表集合
        List<Set<String>> result = new ArrayList<>(logicTables.size());
        for (RouteResult each : routeResults) {
            // 逻辑表到物理表的转换
            result.addAll(each.getActualTableNameGroups(dataSourceName, logicTables));
        }
        return result;
    }

    /**
     *
     * @param dataSource  物理数据源
     * @param actualTableGroups   物理表组
     * @return
     */
    private List<Set<TableUnit>> toRoutingTableGroups(final String dataSource, final List<Set<String>> actualTableGroups) {
        List<Set<TableUnit>> result = new ArrayList<>(actualTableGroups.size());
        for (Set<String> each : actualTableGroups) {
            // 转换成  (物理数据源 + 物理表的结合单元)
            result.add(new HashSet<>(new ArrayList<>(each).stream().map(input -> findRoutingTable(dataSource, input)).collect(Collectors.toList())));
        }
        return result;
    }

    /**
     *
     * @param dataSource  物理数据源
     * @param actualTable   物理表名
     * @return
     */
    private TableUnit findRoutingTable(final String dataSource, final String actualTable) {
        for (RouteResult each : routeResults) {
            Optional<TableUnit> result = each.getTableUnit(dataSource, actualTable);
            if (result.isPresent()) {
                return result.get();
            }
        }
        throw new IllegalStateException(String.format("Cannot found routing table factor, data source: %s, actual table: %s", dataSource, actualTable));
    }

    /**
     *
     * @param dataSource
     * @param cartesianRoutingTableGroups   set 代表多组  List 代表每组都是多个物理数据源的结合体
     * @return
     */
    private Collection<RouteUnit> getRouteUnits(final String dataSource, final Set<List<TableUnit>> cartesianRoutingTableGroups) {
        Collection<RouteUnit> result = new LinkedHashSet<>();
        for (List<TableUnit> each : cartesianRoutingTableGroups) {
            RouteUnit routeUnit = new RouteUnit(dataSource);
            routeUnit.getTableUnits().addAll(each);
            result.add(routeUnit);
        }
        return result;
    }
}
