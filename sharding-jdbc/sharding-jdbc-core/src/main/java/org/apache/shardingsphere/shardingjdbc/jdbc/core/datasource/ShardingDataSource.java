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

package org.apache.shardingsphere.shardingjdbc.jdbc.core.datasource;

import com.google.common.base.Preconditions;
import lombok.Getter;
import org.apache.shardingsphere.core.rule.ShardingRule;
import org.apache.shardingsphere.shardingjdbc.jdbc.adapter.AbstractDataSourceAdapter;
import org.apache.shardingsphere.shardingjdbc.jdbc.core.connection.ShardingConnection;
import org.apache.shardingsphere.shardingjdbc.jdbc.core.context.ShardingRuntimeContext;
import org.apache.shardingsphere.transaction.core.TransactionTypeHolder;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

/**
 * Sharding data source.
 * 该数据源包装了 用户使用的数据源 在代理过程中实现了分库分表以及结果集合并的工作
 */
@Getter
public class ShardingDataSource extends AbstractDataSourceAdapter {

    /**
     * 相关的上下文信息
     */
    private final ShardingRuntimeContext runtimeContext;

    /**
     *
     * @param dataSourceMap   key 数据源名  value 填充数据后的dataSource
     * @param shardingRule    内部包含一些分表策略 bindingTable 策略 主键策略
     * @param props     额外的属性
     * @throws SQLException
     */
    public ShardingDataSource(final Map<String, DataSource> dataSourceMap, final ShardingRule shardingRule, final Properties props) throws SQLException {
        super(dataSourceMap);
        // 检查数据源信息
        checkDataSourceType(dataSourceMap);
        // 通过规则对象 和 prop 创建运行时上下文
        runtimeContext = new ShardingRuntimeContext(dataSourceMap, shardingRule, props, getDatabaseType());
    }
    
    private void checkDataSourceType(final Map<String, DataSource> dataSourceMap) {
        for (DataSource each : dataSourceMap.values()) {
            Preconditions.checkArgument(!(each instanceof MasterSlaveDataSource), "Initialized data sources can not be master-slave data sources.");
        }
    }

    /**
     * 使用该数据源 返回的连接对象也是包装后的
     * @return
     */
    @Override
    public final ShardingConnection getConnection() {
        return new ShardingConnection(getDataSourceMap(), runtimeContext, TransactionTypeHolder.get());
    }
}
