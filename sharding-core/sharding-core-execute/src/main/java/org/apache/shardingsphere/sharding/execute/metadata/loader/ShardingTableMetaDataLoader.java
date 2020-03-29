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

package org.apache.shardingsphere.sharding.execute.metadata.loader;

import com.google.common.collect.Lists;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.core.metadata.column.ShardingGeneratedKeyColumnMetaData;
import org.apache.shardingsphere.core.rule.DataNode;
import org.apache.shardingsphere.core.rule.ShardingRule;
import org.apache.shardingsphere.core.rule.TableRule;
import org.apache.shardingsphere.spi.database.metadata.DataSourceMetaData;
import org.apache.shardingsphere.underlying.common.exception.ShardingSphereException;
import org.apache.shardingsphere.underlying.common.log.MetaDataLogger;
import org.apache.shardingsphere.underlying.common.metadata.column.ColumnMetaData;
import org.apache.shardingsphere.underlying.common.metadata.column.loader.ColumnMetaDataLoader;
import org.apache.shardingsphere.underlying.common.metadata.datasource.DataSourceMetas;
import org.apache.shardingsphere.underlying.common.metadata.table.TableMetaData;
import org.apache.shardingsphere.underlying.common.metadata.table.TableMetas;
import org.apache.shardingsphere.underlying.common.metadata.table.init.loader.ConnectionManager;
import org.apache.shardingsphere.underlying.common.metadata.table.init.loader.TableMetaDataLoader;
import org.apache.shardingsphere.underlying.executor.engine.ExecutorEngine;
import org.apache.shardingsphere.underlying.executor.engine.InputGroup;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

/**
 * Table meta data loader for sharding.
 */
@RequiredArgsConstructor
public final class ShardingTableMetaDataLoader implements TableMetaDataLoader<ShardingRule> {
    
    private static final String INDEX_NAME = "INDEX_NAME";

    /**
     * 从 Connection 的url 信息上解析出参数
     */
    private final DataSourceMetas dataSourceMetas;

    /**
     * 相关的线程池
     */
    private final ExecutorEngine executorEngine;

    /**
     * 可以通过 dataSourceName 快捷定位到某个Connection
     */
    private final ConnectionManager connectionManager;

    /**
     * 每次查询允许使用多少个 jdbcConnection
     */
    private final int maxConnectionsSizePerQuery;

    /**
     * 是否需要先校验元数据
     */
    private final boolean isCheckingMetaData;

    /**
     * 用于从表中读取列数据
     */
    private final ColumnMetaDataLoader columnMetaDataLoader = new ColumnMetaDataLoader();

    /**
     * 传入某个逻辑数据源的名称 来获取下面表的元数据信息
     * @param logicTableName
     * @param shardingRule
     * @return
     * @throws SQLException
     */
    @Override
    public TableMetaData load(final String logicTableName, final ShardingRule shardingRule) throws SQLException {
        // shardingRule 一开始读取配置文件后 如果某些表指定了 主键列以及使用的生成规则 那么就可以找到
        final String generateKeyColumnName = shardingRule.findGenerateKeyColumnName(logicTableName).orElse(null);
        List<TableMetaData> actualTableMetaDataList = load(getDataNodeGroups(shardingRule.getTableRule(logicTableName)), shardingRule, generateKeyColumnName);
        // 确保物理表的元数据是一致的
        checkUniformed(logicTableName, actualTableMetaDataList);
        // 返回某个逻辑表对应的表元数据信息 只要通过了上面的校验 返回 一个就行了  (因为该批物理表的元数据是一致的)
        return actualTableMetaDataList.iterator().next();
    }

    /**
     * 根据指定的 dataNode 信息加载表相关元数据
     * @param dataNodeGroups  key 是物理数据源名 value 是 一组物理节点   因为需要取物理数据源 那里拉取元数据
     * @param shardingRule   全局规则
     * @param generateKeyColumnName  本表对应的自增主键
     * @return
     * @throws SQLException
     */
    private List<TableMetaData> load(final Map<String, List<DataNode>> dataNodeGroups, final ShardingRule shardingRule, final String generateKeyColumnName) throws SQLException {
        // 粗略的行为就是 获取tableMetaData 是尽可能平分到多条Connection 来执行的
        return executorEngine.execute(getDataNodeInputGroups(dataNodeGroups),
                // 这里传入的是回调对象   默认情况下 firstCall 也是该对象
                (dataNodes, isTrunkThread, dataMap) -> {
            // 找到主数据源  没有配置主从信息时 忽略
            String masterDataSourceName = shardingRule.getShardingDataSourceNames().getRawMasterDataSourceName(dataNodes.iterator().next().getDataSourceName());
            // 获取对应的元数据信息
            DataSourceMetaData dataSourceMetaData = ShardingTableMetaDataLoader.this.dataSourceMetas.getDataSourceMetaData(masterDataSourceName);
            // 加载某张物理表的元数据信息
            return load(masterDataSourceName, dataSourceMetaData, dataNodes, generateKeyColumnName);
        });
    }

    /**
     *
     * @param dataSourceName  某个数据源
     * @param dataSourceMetaData  对应的元数据 (从连接的 url上解析出来)
     * @param dataNodes  本次要处理的一组dataNode
     * @param generateKeyColumnName   自动补全的列
     * @return
     * @throws SQLException
     */
    private Collection<TableMetaData> load(final String dataSourceName, 
                                           final DataSourceMetaData dataSourceMetaData, final Collection<DataNode> dataNodes, final String generateKeyColumnName) throws SQLException {
        Collection<TableMetaData> result = new LinkedList<>();
        // 找到连接到该数据源的 connection 对象  注意这里获取多个dataNode 的元数据信息使用了同一个connection
        try (Connection connection = connectionManager.getConnection(dataSourceName)) {
            for (DataNode each : dataNodes) {
                result.add(createTableMetaData(connection, dataSourceMetaData, each.getTableName(), generateKeyColumnName));
            }
        }
        return result;
    }

    /**
     *
     * @param tableRule
     * @return
     */
    private Map<String, List<DataNode>> getDataNodeGroups(final TableRule tableRule) {
        // 如果需要检查元数据 那么获取 TableRule 内部 dataNode  并按照 dataSource分组  否则只要第一个dataNode 就可以  哦 如果要检查点的话 要获取所有物理表
        // 不需要检查 而他们的表元数据又应该相同的情况 那么只要以一个物理表信息就够了
        return isCheckingMetaData ? tableRule.getDataNodeGroups() : getFirstDataNodeGroups(tableRule);
    }
    
    private Map<String, List<DataNode>> getFirstDataNodeGroups(final TableRule tableRule) {
        DataNode firstDataNode = tableRule.getActualDataNodes().get(0);
        return Collections.singletonMap(firstDataNode.getDataSourceName(), Collections.singletonList(firstDataNode));
    }

    /**
     * DataNode  代表指向一个物理表的  数据源以及物理表名的信息
     * @param dataNodeGroups  key 代表某个数据源
     * @return
     */
    private Collection<InputGroup<DataNode>> getDataNodeInputGroups(final Map<String, List<DataNode>> dataNodeGroups) {
        Collection<InputGroup<DataNode>> result = new LinkedList<>();
        for (Entry<String, List<DataNode>> entry : dataNodeGroups.entrySet()) {
            result.addAll(getDataNodeInputGroups(entry.getValue()));
        }
        return result;
    }

    /**
     * 这里是尝试通过多创建一些 connection 来提高效率 所以根据能分配的 connection 数量来尝试分组
     * @param dataNodes 属于某一物理数据源下的所有 datanode信息
     * @return
     */
    private Collection<InputGroup<DataNode>> getDataNodeInputGroups(final List<DataNode> dataNodes) {
        Collection<InputGroup<DataNode>> result = new LinkedList<>();
        // 将一组数据节点 按照连接数来分组  默认情况所有sql 有一条线程来执行 而一条线程对应一个 JDBC 连接 这里将任务平分后 那么相当于每个JDBC连接分摊了请求
        for (List<DataNode> each : Lists.partition(dataNodes, Math.max(dataNodes.size() / maxConnectionsSizePerQuery, 1))) {
            result.add(new InputGroup<>(each));
        }
        return result;
    }

    /**
     * 包装成表的元数据信息
     * @param connection
     * @param dataSourceMetaData
     * @param actualTableName
     * @param generateKeyColumnName
     * @return
     * @throws SQLException
     */
    private TableMetaData createTableMetaData(final Connection connection,
                                              final DataSourceMetaData dataSourceMetaData, final String actualTableName, final String generateKeyColumnName) throws SQLException {
        String catalog = dataSourceMetaData.getCatalog();
        String schema = dataSourceMetaData.getSchema();
        MetaDataLogger.logTableMetaData(catalog, schema, actualTableName);
        return isTableExist(connection, catalog, actualTableName)
                // 通过 索引信息和列信息生成tableMetaData 对象
                ? new TableMetaData(getColumnMetaDataList(connection, catalog, actualTableName, generateKeyColumnName), getLogicIndexes(connection, catalog, schema, actualTableName))
                // connection 没有找到该表 创建一个空实体
                : new TableMetaData(Collections.emptyList(), Collections.emptySet());
    }
    
    private boolean isTableExist(final Connection connection, final String catalog, final String actualTableName) throws SQLException {
        try (ResultSet resultSet = connection.getMetaData().getTables(catalog, null, actualTableName, null)) {
            return resultSet.next();
        }
    }

    /**
     * 根据表名和连接对象 生成列的元数据信息
     * @param connection
     * @param catalog
     * @param actualTableName
     * @param generateKeyColumnName
     * @return
     * @throws SQLException
     */
    private Collection<ColumnMetaData> getColumnMetaDataList(final Connection connection, final String catalog, final String actualTableName, final String generateKeyColumnName) throws SQLException {
        Collection<ColumnMetaData> result = new LinkedList<>();
        for (ColumnMetaData each : columnMetaDataLoader.load(connection, catalog, actualTableName)) {
            result.add(filterColumnMetaData(each, generateKeyColumnName));
        }
        return result;
    }

    /**
     * 这里对获取到的 列元数据信息 要做一层处理
     * @param columnMetaData
     * @param generateKeyColumnName
     * @return
     */
    private ColumnMetaData filterColumnMetaData(final ColumnMetaData columnMetaData, final String generateKeyColumnName) {
        // 所有列都会传入 同时又有 普通主键和 分表场景下主键  这里只能有一个主键是支持shardingSphere 自动生成的
        return columnMetaData.getName().equalsIgnoreCase(generateKeyColumnName) 
                ? new ShardingGeneratedKeyColumnMetaData(columnMetaData.getName(), columnMetaData.getDataType(), columnMetaData.isPrimaryKey())
                : columnMetaData;
    }

    /**
     * 获取所有逻辑索引
     * @param connection
     * @param catalog
     * @param schema
     * @param actualTableName
     * @return
     * @throws SQLException
     */
    private Collection<String> getLogicIndexes(final Connection connection, final String catalog, final String schema, final String actualTableName) throws SQLException {
        Collection<String> result = new HashSet<>();
        // 获取某table 所有索引 不包含 unique键
        try (ResultSet resultSet = connection.getMetaData().getIndexInfo(catalog, schema, actualTableName, false, false)) {
            while (resultSet.next()) {
                getLogicIndex(resultSet.getString(INDEX_NAME), actualTableName).ifPresent(result::add);
            }
        }
        return result;
    }

    /**
     *
     * @param actualIndexName  索引名
     * @param actualTableName  对应的表imng
     * @return
     */
    private Optional<String> getLogicIndex(final String actualIndexName, final String actualTableName) {
        if (null == actualIndexName) {
            return Optional.empty();
        }
        String indexNameSuffix = "_" + actualTableName;
        // !!这里要包含前缀才行
        return actualIndexName.contains(indexNameSuffix) ? Optional.of(actualIndexName.replace(indexNameSuffix, "")) : Optional.empty();
    }

    /**
     * 这是某个逻辑表名 以及对应的所有实体表的元数据信息
     * @param logicTableName
     * @param actualTableMetaDataList
     */
    private void checkUniformed(final String logicTableName, final List<TableMetaData> actualTableMetaDataList) {
        if (!isCheckingMetaData) {
            return;
        }
        // 每张实体表除了表名外 它们的列数据信息必须要一致
        TableMetaData sample = actualTableMetaDataList.iterator().next();
        for (TableMetaData each : actualTableMetaDataList) {
            if (!sample.equals(each)) {
                throw new ShardingSphereException("Cannot get uniformed table structure for `%s`. The different meta data of actual tables are as follows:\n%s\n%s.", logicTableName, sample, each);
            }
        }
    }

    /**
     * 加载所有的(逻辑)表元数据
     * @param shardingRule
     * @return
     * @throws SQLException
     */
    @Override
    public TableMetas loadAll(final ShardingRule shardingRule) throws SQLException {
        Map<String, TableMetaData> result = new HashMap<>();
        // 这里是加载sharding 表相关的元数据  这里的 entry 是以逻辑表名为key的 然后value 代表逻辑表下所有物理表的元数据信息都一致 所以用同一份
        result.putAll(loadShardingTables(shardingRule));
        // 这里是加载默认数据源下所有 表的tableMetas 以及列信息的 这里就是没有分表的场景 key 就是物理表的名称
        result.putAll(loadDefaultTables(shardingRule));
        return new TableMetas(result);
    }

    /**
     * shardingRule 内部包含了 本次启用 shardingSphere 下面的所有表信息
     * @param shardingRule
     * @return
     * @throws SQLException
     */
    private Map<String, TableMetaData> loadShardingTables(final ShardingRule shardingRule) throws SQLException {
        // key 是
        Map<String, TableMetaData> result = new HashMap<>(shardingRule.getTableRules().size(), 1);
        MetaDataLogger.log("There are {} sharding table(s) will be loaded.", shardingRule.getTableRules().size());
        for (TableRule each : shardingRule.getTableRules()) {
            // 有了表规则 换句话就知道哪些表需要被加载元数据
            result.put(each.getLogicTable(), load(each.getLogicTable(), shardingRule));
        }
        return result;
    }

    /**
     * 加载默认数据源下所有的表信息
     * @param shardingRule
     * @return
     * @throws SQLException
     */
    private Map<String, TableMetaData> loadDefaultTables(final ShardingRule shardingRule) throws SQLException {
        // 获取配置中的默认数据源名
        Optional<String> actualDefaultDataSourceName = shardingRule.findActualDefaultDataSourceName();
        if (!actualDefaultDataSourceName.isPresent()) {
            return Collections.emptyMap();
        }
        // 获取所有表名
        Collection<String> tableNames = loadAllTableNames(actualDefaultDataSourceName.get());
        MetaDataLogger.log("There are {} default table(s) will be loaded.", tableNames.size());
        List<TableMetaData> tableMetaDataList = getTableMetaDataList(shardingRule, tableNames);
        return loadDefaultTables(tableNames, tableMetaDataList);
    }
    
    private Map<String, TableMetaData> loadDefaultTables(final Collection<String> tableNames, final List<TableMetaData> tableMetaDataList) {
        Map<String, TableMetaData> result = new HashMap<>(tableNames.size(), 1);
        Iterator<String> tabNameIterator = tableNames.iterator();
        for (TableMetaData each : tableMetaDataList) {
            result.put(tabNameIterator.next(), each);
        }
        return result;
    }
    
    private Collection<String> loadAllTableNames(final String dataSourceName) throws SQLException {
        DataSourceMetaData dataSourceMetaData = dataSourceMetas.getDataSourceMetaData(dataSourceName);
        String catalog = null == dataSourceMetaData ? null : dataSourceMetaData.getCatalog();
        String schemaName = null == dataSourceMetaData ? null : dataSourceMetaData.getSchema();
        return loadAllTableNames(dataSourceName, catalog, schemaName);
    }

    /**
     * 获取某个数据源下所有表名
     * @param dataSourceName
     * @param catalog
     * @param schemaName
     * @return
     * @throws SQLException
     */
    private Collection<String> loadAllTableNames(final String dataSourceName, final String catalog, final String schemaName) throws SQLException {
        Collection<String> result = new LinkedHashSet<>();
        try (Connection connection = connectionManager.getConnection(dataSourceName);
             ResultSet resultSet = connection.getMetaData().getTables(catalog, schemaName, null, new String[]{"TABLE"})) {
            result.addAll(loadTableName(resultSet));
        }
        return result;
    }
    
    private Collection<String> loadTableName(final ResultSet resultSet) throws SQLException {
        Collection<String> result = new LinkedHashSet<>();
        while (resultSet.next()) {
            String tableName = resultSet.getString("TABLE_NAME");
            if (!tableName.contains("$") && !tableName.contains("/")) {
                result.add(tableName);
            }
        }
        return result;
    }

    /**
     *
     * @param shardingRule
     * @param tableNames
     * @return
     * @throws SQLException
     */
    private List<TableMetaData> getTableMetaDataList(final ShardingRule shardingRule, final Collection<String> tableNames) throws SQLException {
        Map<String, List<DataNode>> result = new LinkedHashMap<>();
        String defaultDataSource = shardingRule.getShardingDataSourceNames().getDefaultDataSourceName();
        result.put(defaultDataSource, new LinkedList<>());
        for (String each : tableNames) {
            // 这里所有表名 都与defaultDataSourceName 包装成 TableRule 对象
            result.get(defaultDataSource).addAll(getDataNodeGroups(shardingRule.getTableRule(each)).get(defaultDataSource));
        }
        // 把每个表 以及 下面的所有物理表信息(DataNode) 通过Connection 拉取元数据信息
        return load(result, shardingRule, null);
    }
}
