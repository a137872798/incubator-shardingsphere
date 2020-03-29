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

package org.apache.shardingsphere.core.strategy.route.standard;

import com.google.common.base.Preconditions;
import org.apache.shardingsphere.api.config.sharding.strategy.StandardShardingStrategyConfiguration;
import org.apache.shardingsphere.api.sharding.standard.PreciseShardingAlgorithm;
import org.apache.shardingsphere.api.sharding.standard.PreciseShardingValue;
import org.apache.shardingsphere.api.sharding.standard.RangeShardingAlgorithm;
import org.apache.shardingsphere.api.sharding.standard.RangeShardingValue;
import org.apache.shardingsphere.core.strategy.route.ShardingStrategy;
import org.apache.shardingsphere.core.strategy.route.value.ListRouteValue;
import org.apache.shardingsphere.core.strategy.route.value.RangeRouteValue;
import org.apache.shardingsphere.core.strategy.route.value.RouteValue;
import org.apache.shardingsphere.underlying.common.constant.properties.ShardingSphereProperties;

import java.util.Collection;
import java.util.LinkedList;
import java.util.TreeSet;

/**
 * Standard sharding strategy.
 * 标准策略
 */
public final class StandardShardingStrategy implements ShardingStrategy {

    /**
     * 逻辑sql 转换成 物理sql 的核心列 通过该列判断该怎样改写sql
     */
    private final String shardingColumn;

    // 内部同时包含2套算法  一个是针对 "="  "in"  一个是针对 < > <= >=
    private final PreciseShardingAlgorithm preciseShardingAlgorithm;
    private final RangeShardingAlgorithm rangeShardingAlgorithm;
    
    public StandardShardingStrategy(final StandardShardingStrategyConfiguration standardShardingStrategyConfig) {
        Preconditions.checkNotNull(standardShardingStrategyConfig.getShardingColumn(), "Sharding column cannot be null.");
        Preconditions.checkNotNull(standardShardingStrategyConfig.getPreciseShardingAlgorithm(), "precise sharding algorithm cannot be null.");
        shardingColumn = standardShardingStrategyConfig.getShardingColumn();
        preciseShardingAlgorithm = standardShardingStrategyConfig.getPreciseShardingAlgorithm();
        rangeShardingAlgorithm = standardShardingStrategyConfig.getRangeShardingAlgorithm();
    }

    /**
     *
     * @param availableTargetNames available data sources or tables's names  所有候选者
     * @param shardingValues sharding values
     * @param properties ShardingSphere properties
     * @return
     */
    @Override
    public Collection<String> doSharding(final Collection<String> availableTargetNames, final Collection<RouteValue> shardingValues, final ShardingSphereProperties properties) {
        // 遍历每个会影响分表结果 的 value
        RouteValue shardingValue = shardingValues.iterator().next();
        // 传入所有备选的 物理节点 并返回实际会用到的物理表名
        Collection<String> shardingResult = shardingValue instanceof ListRouteValue
                ? doSharding(availableTargetNames, (ListRouteValue) shardingValue) : doSharding(availableTargetNames, (RangeRouteValue) shardingValue);
        Collection<String> result = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        result.addAll(shardingResult);
        return result;
    }

    // 针对不同的入参 分别使用不同的算法对象进行处理
    @SuppressWarnings("unchecked")
    private Collection<String> doSharding(final Collection<String> availableTargetNames, final RangeRouteValue<?> shardingValue) {
        if (null == rangeShardingAlgorithm) {
            throw new UnsupportedOperationException("Cannot find range sharding strategy in sharding rule.");
        }
        return rangeShardingAlgorithm.doSharding(availableTargetNames, 
                new RangeShardingValue(shardingValue.getTableName(), shardingValue.getColumnName(), shardingValue.getValueRange()));
    }

    /**
     * 进行实际的分表操作
     * @param availableTargetNames
     * @param shardingValue
     * @return
     */
    @SuppressWarnings("unchecked")
    private Collection<String> doSharding(final Collection<String> availableTargetNames, final ListRouteValue<?> shardingValue) {
        Collection<String> result = new LinkedList<>();
        // 如果是 "=" 那么只有一个值 如果是 "in"  代表有多个值  注意这里可能 某个 target 会重复添加 比如 in (1,3,5) 它们都会对应到单数表
        for (Comparable<?> each : shardingValue.getValues()) {
            // 返回一个实际节点  (简单而言 PreciseShardingAlgorithm 就是将 物理表 一分为2/一分为4)
            String target = preciseShardingAlgorithm.doSharding(availableTargetNames, new PreciseShardingValue(shardingValue.getTableName(), shardingValue.getColumnName(), each));
            if (null != target) {
                result.add(target);
            }
        }
        return result;
    }
    
    @Override
    public Collection<String> getShardingColumns() {
        Collection<String> result = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        result.add(shardingColumn);
        return result;
    }
}
