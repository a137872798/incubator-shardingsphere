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
import org.apache.shardingsphere.core.rule.ShardingRule;
import org.apache.shardingsphere.encrypt.metadata.decorator.EncryptTableMetaDataDecorator;
import org.apache.shardingsphere.sharding.execute.metadata.loader.ShardingTableMetaDataLoader;
import org.apache.shardingsphere.shardingjdbc.jdbc.core.datasource.metadata.CachedDatabaseMetaData;
import org.apache.shardingsphere.shardingjdbc.jdbc.metadata.JDBCDataSourceMapConnectionManager;
import org.apache.shardingsphere.spi.database.type.DatabaseType;
import org.apache.shardingsphere.transaction.ShardingTransactionManagerEngine;
import org.apache.shardingsphere.underlying.common.constant.properties.PropertiesConstant;
import org.apache.shardingsphere.underlying.common.metadata.datasource.DataSourceMetas;
import org.apache.shardingsphere.underlying.common.metadata.table.init.TableMetaDataInitializer;
import org.apache.shardingsphere.underlying.common.metadata.table.init.TableMetaDataInitializerEntry;
import org.apache.shardingsphere.underlying.common.rule.BaseRule;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Runtime context for sharding.
 * 代表分库分表时的 上下文信息
 */
@Getter
public final class ShardingRuntimeContext extends MultipleDataSourcesRuntimeContext<ShardingRule> {

    /**
     * JDBC 编程的接口
     */
    private final DatabaseMetaData cachedDatabaseMetaData;
    
    private final ShardingTransactionManagerEngine shardingTransactionManagerEngine;

    /**
     *
     * @param dataSourceMap  本次配置中使用的所有数据源
     * @param shardingRule  这里明确规定了此时运行的规则
     * @param props   额外配置
     * @param databaseType   数据源的类型
     * @throws SQLException
     */
    public ShardingRuntimeContext(final Map<String, DataSource> dataSourceMap, final ShardingRule shardingRule, final Properties props, final DatabaseType databaseType) throws SQLException {
        // 在父类初始化的过程中 已经获取到了本次涉及的所有表的元数据信息 (如果有必要的话 还会获取相同逻辑表下所有物理表的元数据进行对比校验)
        super(dataSourceMap, shardingRule, props, databaseType);
        cachedDatabaseMetaData = createCachedDatabaseMetaData(dataSourceMap, shardingRule);
        // 创建事务相关的管理器   这里就是让seats 基于该数据源进行分布式事务处理
        shardingTransactionManagerEngine = new ShardingTransactionManagerEngine();
        shardingTransactionManagerEngine.init(databaseType, dataSourceMap);
    }

    /**
     * @param dataSourceMap
     * @param rule
     * @return
     * @throws SQLException
     */
    private DatabaseMetaData createCachedDatabaseMetaData(final Map<String, DataSource> dataSourceMap, final ShardingRule rule) throws SQLException {
        // 看来已经默认该组 dataSource 据有相同的元数据信息了 所以只获取第一个连接对象就可以
        try (Connection connection = dataSourceMap.values().iterator().next().getConnection()) {
            return new CachedDatabaseMetaData(connection.getMetaData(), dataSourceMap, rule);
        }
    }

    /**
     * 获取数据源下所有表信息
     * @param dataSourceMap      数据源map
     * @param dataSourceMetas    有关连接的元数据 比如 url username password 等
     * @return
     */
    @Override
    protected TableMetaDataInitializerEntry createTableMetaDataInitializerEntry(final Map<String, DataSource> dataSourceMap, final DataSourceMetas dataSourceMetas) {
        Map<BaseRule, TableMetaDataInitializer> tableMetaDataInitializes = new HashMap<>(2, 1);
        // 这个对象可以根据 jdbcConnection 拉取表的元数据
        tableMetaDataInitializes.put(getRule(), new ShardingTableMetaDataLoader(dataSourceMetas, getExecutorEngine(), new JDBCDataSourceMapConnectionManager(dataSourceMap),
                // 每次查询使用几个连接  以及是否要检查表的元数据信息
                getProperties().<Integer>getValue(PropertiesConstant.MAX_CONNECTIONS_SIZE_PER_QUERY), getProperties().<Boolean>getValue(PropertiesConstant.CHECK_TABLE_METADATA_ENABLED)));
        // TODO 这里先忽略
        if (!getRule().getEncryptRule().getEncryptTableNames().isEmpty()) {
            tableMetaDataInitializes.put(getRule().getEncryptRule(), new EncryptTableMetaDataDecorator());
        }
        return new TableMetaDataInitializerEntry(tableMetaDataInitializes);
    }

    /**
     * 关闭上下文时 同时关闭事务  上下文是在什么时候开启的  同时又是在什么时候关闭的???
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        shardingTransactionManagerEngine.close();
        super.close();
    }
}
