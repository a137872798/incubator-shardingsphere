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

package org.apache.shardingsphere.core.strategy.route.complex;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Range;

import lombok.Getter;
import org.apache.shardingsphere.api.config.sharding.strategy.ComplexShardingStrategyConfiguration;
import org.apache.shardingsphere.api.sharding.complex.ComplexKeysShardingAlgorithm;
import org.apache.shardingsphere.api.sharding.complex.ComplexKeysShardingValue;
import org.apache.shardingsphere.core.strategy.route.ShardingStrategy;
import org.apache.shardingsphere.core.strategy.route.value.ListRouteValue;
import org.apache.shardingsphere.core.strategy.route.value.RangeRouteValue;
import org.apache.shardingsphere.core.strategy.route.value.RouteValue;
import org.apache.shardingsphere.underlying.common.constant.properties.ShardingSphereProperties;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

/**
 * Complex sharding strategy.
 * 复合键 代理策略 (基于多个键来改写sql 从而关联到多个物理表)
 */
public final class ComplexShardingStrategy implements ShardingStrategy {

    /**
     * 涉及到的所有键的列名
     */
    @Getter
    private final Collection<String> shardingColumns;
    
    private final ComplexKeysShardingAlgorithm shardingAlgorithm;
    
    public ComplexShardingStrategy(final ComplexShardingStrategyConfiguration complexShardingStrategyConfig) {
        Preconditions.checkNotNull(complexShardingStrategyConfig.getShardingColumns(), "Sharding columns cannot be null.");
        Preconditions.checkNotNull(complexShardingStrategyConfig.getShardingAlgorithm(), "Sharding algorithm cannot be null.");
        shardingColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        // 将包含多个列的字符串拆分后 每个元素都进行去空操作 填充到shardingColumns
        shardingColumns.addAll(Splitter.on(",").trimResults().splitToList(complexShardingStrategyConfig.getShardingColumns()));
        shardingAlgorithm = complexShardingStrategyConfig.getShardingAlgorithm();
    }

    /**
     *
     * @param availableTargetNames available data sources or tables's names  候选的一组数据源 或者表名  本次代理后就会从这里面的表中查询数据
     * @param shardingValues sharding values  代表一系列路由的结果
     * @param properties ShardingSphere properties   存放一些 ShardingSphere相关的属性
     * @return
     */
    @SuppressWarnings("unchecked")
    @Override
    public Collection<String> doSharding(final Collection<String> availableTargetNames, final Collection<RouteValue> shardingValues, final ShardingSphereProperties properties) {
        Map<String, Collection<Comparable<?>>> columnShardingValues = new HashMap<>(shardingValues.size(), 1);
        Map<String, Range<Comparable<?>>> columnRangeValues = new HashMap<>(shardingValues.size(), 1);
        String logicTableName = "";
        // 先遍历所有的路由结果 应该是这样 一个逻辑sql 本身只定位到一个范围 而在进行改写后就会生成多个routeValue 代表指向多个结果
        for (RouteValue each : shardingValues) {
            // 根据类型存放到不同的容器中 (Range/List)
            if (each instanceof ListRouteValue) {
                columnShardingValues.put(each.getColumnName(), ((ListRouteValue) each).getValues());
            } else if (each instanceof RangeRouteValue) {
                columnRangeValues.put(each.getColumnName(), ((RangeRouteValue) each).getValueRange());
            }
            // 难道它们对应的表都是一样的吗
            logicTableName = each.getTableName();
        }
        Collection<String> shardingResult = shardingAlgorithm.doSharding(availableTargetNames, new ComplexKeysShardingValue(logicTableName, columnShardingValues, columnRangeValues));
        Collection<String> result = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        result.addAll(shardingResult);
        return result;
    }
}
