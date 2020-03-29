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

package org.apache.shardingsphere.core.strategy.masterslave;

import lombok.Getter;
import lombok.Setter;
import org.apache.shardingsphere.spi.masterslave.MasterSlaveLoadBalanceAlgorithm;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Random slave database load-balance algorithm.
 * 均衡负载规则 这里使用随机策略
 */
@Getter
@Setter
public final class RandomMasterSlaveLoadBalanceAlgorithm implements MasterSlaveLoadBalanceAlgorithm {
    
    private Properties properties = new Properties();
    
    @Override
    public String getType() {
        return "RANDOM";
    }

    /**
     * 当遇到读操作时 从SlaveDataSource 中随机返回一个
     * @param name master-slave logic data source name
     * @param masterDataSourceName name of master data sources
     * @param slaveDataSourceNames names of slave data sources
     * @return
     */
    @Override
    public String getDataSource(final String name, final String masterDataSourceName, final List<String> slaveDataSourceNames) {
        return slaveDataSourceNames.get(ThreadLocalRandom.current().nextInt(slaveDataSourceNames.size()));
    }
}
