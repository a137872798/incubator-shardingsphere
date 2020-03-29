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

package org.apache.shardingsphere.core.rule;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import lombok.Getter;
import org.apache.shardingsphere.api.config.masterslave.MasterSlaveRuleConfiguration;
import org.apache.shardingsphere.api.config.sharding.ShardingRuleConfiguration;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Sharding data source names.
 * 
 * <p>Will convert actual data source names to master-slave data source name.</p>
 * 一组dataSourceName
 */
public final class ShardingDataSourceNames {

    /**
     * 类似一个配置总集  内部包含了分表的所有配置
     */
    private final ShardingRuleConfiguration shardingRuleConfig;

    /**
     * 一组数据源的名称
     */
    @Getter
    private final Collection<String> dataSourceNames;
    
    public ShardingDataSourceNames(final ShardingRuleConfiguration shardingRuleConfig, final Collection<String> rawDataSourceNames) {
        Preconditions.checkArgument(null != shardingRuleConfig, "can not construct ShardingDataSourceNames with null ShardingRuleConfig");
        this.shardingRuleConfig = shardingRuleConfig;
        dataSourceNames = getAllDataSourceNames(rawDataSourceNames);
    }

    /**
     * @param dataSourceNames
     * @return
     */
    private Collection<String> getAllDataSourceNames(final Collection<String> dataSourceNames) {
        Collection<String> result = new LinkedHashSet<>(dataSourceNames);
        // TODO 如果存在主从配置信息才要做处理 现在看sharding模式 先忽略
        for (MasterSlaveRuleConfiguration each : shardingRuleConfig.getMasterSlaveRuleConfigs()) {
            result.remove(each.getMasterDataSourceName());
            result.removeAll(each.getSlaveDataSourceNames());
            result.add(each.getName());
        }
        return result;
    }
    
    /**
     * Get default data source name.
     *
     * @return default data source name
     * 如果包含多个 dataSourceName  那么返回一个默认的数据源名称
     */
    public String getDefaultDataSourceName() {
        return 1 == dataSourceNames.size() ? dataSourceNames.iterator().next() : shardingRuleConfig.getDefaultDataSourceName();
    }
    
    /**
     * Get raw master data source name.
     *
     * @param dataSourceName data source name
     * @return raw master data source name
     * 找到该数据源名称对应的  masterDataSourceName
     */
    public String getRawMasterDataSourceName(final String dataSourceName) {
        for (MasterSlaveRuleConfiguration each : shardingRuleConfig.getMasterSlaveRuleConfigs()) {
            if (each.getName().equals(dataSourceName)) {
                return each.getMasterDataSourceName();
            }
        }
        return dataSourceName;
    }
    
    /**
     * Get random data source name.
     *
     * @return random data source name
     */
    public String getRandomDataSourceName() {
        return getRandomDataSourceName(dataSourceNames);
    }
    
    /**
     * Get random data source name.
     *
     * @param dataSourceNames available data source names
     * @return random data source name
     * 返回一个随机的dataSourceName
     */
    public String getRandomDataSourceName(final Collection<String> dataSourceNames) {
        return Lists.newArrayList(dataSourceNames).get(ThreadLocalRandom.current().nextInt(dataSourceNames.size()));
    }
}
