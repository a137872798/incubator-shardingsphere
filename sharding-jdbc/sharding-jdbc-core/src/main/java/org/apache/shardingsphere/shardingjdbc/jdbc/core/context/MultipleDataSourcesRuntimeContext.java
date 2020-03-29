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

package org.apache.shardingsphere.shardingjdbc.jdbc.core.context;

import lombok.Getter;
import org.apache.shardingsphere.spi.database.type.DatabaseType;
import org.apache.shardingsphere.underlying.common.config.DatabaseAccessConfiguration;
import org.apache.shardingsphere.underlying.common.log.MetaDataLogger;
import org.apache.shardingsphere.underlying.common.metadata.ShardingSphereMetaData;
import org.apache.shardingsphere.underlying.common.metadata.datasource.DataSourceMetas;
import org.apache.shardingsphere.underlying.common.metadata.table.TableMetas;
import org.apache.shardingsphere.underlying.common.metadata.table.init.TableMetaDataInitializerEntry;
import org.apache.shardingsphere.underlying.common.rule.BaseRule;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

/**
 * Runtime context for multiple data sources.
 *
 * @param <T> type of rule
 *           代表多数据源运行时上下文
 */
@Getter
public abstract class MultipleDataSourcesRuntimeContext<T extends BaseRule> extends AbstractRuntimeContext<T> {

    /**
     * 相关的元数据信息
     */
    private final ShardingSphereMetaData metaData;

    /**
     *
     * @param dataSourceMap  本次分表涉及到的所有数据源
     * @param rule  本次指定的分表策略
     * @param props
     * @param databaseType
     * @throws SQLException
     */
    protected MultipleDataSourcesRuntimeContext(final Map<String, DataSource> dataSourceMap, final T rule, final Properties props, final DatabaseType databaseType) throws SQLException {
        // 公共的部分封装到上层 比如线程池 和 解析引擎 而该层要维护多个数据源的 metaData
        super(rule, props, databaseType);
        long start = System.currentTimeMillis();
        MetaDataLogger.log("Start loading meta data.");
        metaData = createMetaData(dataSourceMap, databaseType);
        MetaDataLogger.log("Meta data loading finished, cost {} milliseconds.", System.currentTimeMillis() - start);
    }

    /**
     * 根据本次传入的所有数据源 抽取它们的元数据信息
     * @param dataSourceMap
     * @param databaseType
     * @return
     * @throws SQLException
     */
    private ShardingSphereMetaData createMetaData(final Map<String, DataSource> dataSourceMap, final DatabaseType databaseType) throws SQLException {
        // 第一步 根据 dataSource的 Connection 获取 url 并解析出信息
        DataSourceMetas dataSourceMetas = new DataSourceMetas(databaseType, getDatabaseAccessConfigurationMap(dataSourceMap));
        // 第二步 开始获取所有表相关元数据
        TableMetas tableMetas = createTableMetaDataInitializerEntry(dataSourceMap, dataSourceMetas).initAll();
        // 最后一步 将url信息和表信息结合
        return new ShardingSphereMetaData(dataSourceMetas, tableMetas);
    }

    /**
     * 获取访问相关的配置
     * @param dataSourceMap   key 是物理表名称
     * @return
     * @throws SQLException
     */
    private Map<String, DatabaseAccessConfiguration> getDatabaseAccessConfigurationMap(final Map<String, DataSource> dataSourceMap) throws SQLException {
        Map<String, DatabaseAccessConfiguration> result = new LinkedHashMap<>(dataSourceMap.size(), 1);
        for (Entry<String, DataSource> entry : dataSourceMap.entrySet()) {
            DataSource dataSource = entry.getValue();
            // 通过 JDBC连接对象 获取元数据信息
            try (Connection connection = dataSource.getConnection()) {
                DatabaseMetaData metaData = connection.getMetaData();
                // 这里是把 url username password 这种信息提取出来
                result.put(entry.getKey(), new DatabaseAccessConfiguration(metaData.getURL(), metaData.getUserName(), null));
            }
        }
        return result;
    }
    
    protected abstract TableMetaDataInitializerEntry createTableMetaDataInitializerEntry(Map<String, DataSource> dataSourceMap, DataSourceMetas dataSourceMetas);
}
