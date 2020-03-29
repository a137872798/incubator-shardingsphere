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

import lombok.Getter;
import org.apache.shardingsphere.api.config.masterslave.LoadBalanceStrategyConfiguration;
import org.apache.shardingsphere.api.config.masterslave.MasterSlaveRuleConfiguration;
import org.apache.shardingsphere.spi.algorithm.masterslave.MasterSlaveLoadBalanceAlgorithmServiceLoader;
import org.apache.shardingsphere.spi.masterslave.MasterSlaveLoadBalanceAlgorithm;
import org.apache.shardingsphere.underlying.common.rule.BaseRule;

import java.util.List;

/**
 * Databases and tables master-slave rule.
 * 主从规则
 */
@Getter
public class MasterSlaveRule implements BaseRule {
    
    private final String name;

    /**
     * 主数据源
     */
    private final String masterDataSourceName;

    /**
     * 一组从数据源
     */
    private final List<String> slaveDataSourceNames;

    /**
     * 主从间均衡负载的策略对象
     */
    private final MasterSlaveLoadBalanceAlgorithm loadBalanceAlgorithm;

    /**
     * 主从机规则对象
     */
    private final MasterSlaveRuleConfiguration ruleConfiguration;

    /**
     * 使用指定的数据来初始化
     * @param name
     * @param masterDataSourceName
     * @param slaveDataSourceNames
     * @param loadBalanceAlgorithm
     */
    public MasterSlaveRule(final String name, final String masterDataSourceName, final List<String> slaveDataSourceNames, final MasterSlaveLoadBalanceAlgorithm loadBalanceAlgorithm) {
        this.name = name;
        this.masterDataSourceName = masterDataSourceName;
        this.slaveDataSourceNames = slaveDataSourceNames;
        // 如果没有指定均衡负载算法 那么使用一个默认的算法 否则使用指定的算法
        this.loadBalanceAlgorithm = null == loadBalanceAlgorithm ? new MasterSlaveLoadBalanceAlgorithmServiceLoader().newService() : loadBalanceAlgorithm;
        ruleConfiguration = new MasterSlaveRuleConfiguration(name, masterDataSourceName, slaveDataSourceNames, 
                new LoadBalanceStrategyConfiguration(this.loadBalanceAlgorithm.getType(), this.loadBalanceAlgorithm.getProperties()));
    }

    /**
     * 从配置文件中 获取相关属性进行初始化
     * @param config
     */
    public MasterSlaveRule(final MasterSlaveRuleConfiguration config) {
        name = config.getName();
        masterDataSourceName = config.getMasterDataSourceName();
        slaveDataSourceNames = config.getSlaveDataSourceNames();
        loadBalanceAlgorithm = createMasterSlaveLoadBalanceAlgorithm(config.getLoadBalanceStrategyConfiguration());
        ruleConfiguration = config;
    }

    /**
     * 根据类型创建均衡负载实现类
     * @param loadBalanceStrategyConfiguration
     * @return
     */
    private MasterSlaveLoadBalanceAlgorithm createMasterSlaveLoadBalanceAlgorithm(final LoadBalanceStrategyConfiguration loadBalanceStrategyConfiguration) {
        MasterSlaveLoadBalanceAlgorithmServiceLoader serviceLoader = new MasterSlaveLoadBalanceAlgorithmServiceLoader();
        return null == loadBalanceStrategyConfiguration
                ? serviceLoader.newService() : serviceLoader.newService(loadBalanceStrategyConfiguration.getType(), loadBalanceStrategyConfiguration.getProperties());
    }
    
    /**
     * Judge whether contain data source name.
     *
     * @param dataSourceName data source name
     * @return contain or not.
     */
    public boolean containDataSourceName(final String dataSourceName) {
        return masterDataSourceName.equals(dataSourceName) || slaveDataSourceNames.contains(dataSourceName);
    }
}
