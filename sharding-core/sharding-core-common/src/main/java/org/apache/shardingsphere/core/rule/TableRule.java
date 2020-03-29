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

import com.google.common.base.Strings;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.ToString;
import org.apache.shardingsphere.api.config.sharding.KeyGeneratorConfiguration;
import org.apache.shardingsphere.api.config.sharding.TableRuleConfiguration;
import org.apache.shardingsphere.underlying.common.config.exception.ShardingSphereConfigurationException;
import org.apache.shardingsphere.underlying.common.exception.ShardingSphereException;
import org.apache.shardingsphere.core.strategy.route.none.NoneShardingStrategy;
import org.apache.shardingsphere.spi.algorithm.keygen.ShardingKeyGeneratorServiceLoader;
import org.apache.shardingsphere.core.strategy.route.ShardingStrategy;
import org.apache.shardingsphere.core.strategy.route.ShardingStrategyFactory;
import org.apache.shardingsphere.underlying.common.config.inline.InlineExpressionParser;
import org.apache.shardingsphere.spi.keygen.ShardingKeyGenerator;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Table rule.
 * 表规则
 */
@Getter
@ToString(exclude = {"dataNodeIndexMap", "actualTables", "actualDatasourceNames", "datasourceToTablesMap"})
public final class TableRule {

    /**
     * 逻辑表名 (非物理表)
     */
    private final String logicTable;

    /**
     * 当改写后的每个表 都作为一个 DataNode
     */
    private final List<DataNode> actualDataNodes;

    /**
     * 对应 actualDataNodes 的表名
     */
    @Getter(AccessLevel.NONE)
    private final Set<String> actualTables;
    
    @Getter(AccessLevel.NONE)
    private final Map<DataNode, Integer> dataNodeIndexMap;

    // 分别对应数据源的改写策略 和 表的改写策略

    private final ShardingStrategy databaseShardingStrategy;
    
    private final ShardingStrategy tableShardingStrategy;
    
    private final String generateKeyColumn;

    /**
     * 主键生成器
     */
    private final ShardingKeyGenerator shardingKeyGenerator;

    /**
     * 对应连接到物理表的数据源
     */
    private final Collection<String> actualDatasourceNames = new LinkedHashSet<>();

    /**
     * 一个datasource 对应多个物理表 (分表场景必须要配置 如果未配置就将逻辑表看作物理表 )
     */
    private final Map<String, Collection<String>> datasourceToTablesMap = new HashMap<>();

    /**
     * 代表设置默认数据源  默认数据源下的表被看作是逻辑表 (虽然可能是物理表名)
     * @param defaultDataSourceName
     * @param logicTableName
     */
    public TableRule(final String defaultDataSourceName, final String logicTableName) {
        logicTable = logicTableName.toLowerCase();
        actualDataNodes = Collections.singletonList(new DataNode(defaultDataSourceName, logicTableName));
        // 将真正的表名设置到容器中
        actualTables = getActualTables();
        cacheActualDatasourcesAndTables();
        // 节点以及 排列的位置么???
        dataNodeIndexMap = Collections.emptyMap();
        // 应该是看作没有分表
        databaseShardingStrategy = null;
        tableShardingStrategy = null;
        generateKeyColumn = null;
        shardingKeyGenerator = null;
    }
    
    public TableRule(final Collection<String> dataSourceNames, final String logicTableName) {
        logicTable = logicTableName.toLowerCase();
        dataNodeIndexMap = new HashMap<>(dataSourceNames.size(), 1);
        actualDataNodes = generateDataNodes(logicTableName, dataSourceNames);
        actualTables = getActualTables();
        databaseShardingStrategy = null;
        tableShardingStrategy = null;
        generateKeyColumn = null;
        shardingKeyGenerator = null;
    }

    /**
     * 一个TableRule 对应一个 TableRuleConfiguration   (也就是一个逻辑表的维度)
     * 这里是把有关选择某个表 或者说生成主键的信息全部抽取出来 之后在需要的时候协助判断该如何分表
     * @param tableRuleConfig
     * @param shardingDataSourceNames 本次涉及到的所有数据源名 以及一个全规则对象
     * @param defaultGenerateKeyColumn 全局范围的默认生成列 一般是不配置的  不灵活
     */
    public TableRule(final TableRuleConfiguration tableRuleConfig, final ShardingDataSourceNames shardingDataSourceNames, final String defaultGenerateKeyColumn) {
        logicTable = tableRuleConfig.getLogicTable().toLowerCase();
        // 物理节点也是用 特殊的表达式 需要先解析  解析后得到一组物理节点
        List<String> dataNodes = new InlineExpressionParser(tableRuleConfig.getActualDataNodes()).splitAndEvaluate();
        dataNodeIndexMap = new HashMap<>(dataNodes.size(), 1);
        // 生成一组节点信息
        actualDataNodes = isEmptyDataNodes(dataNodes)
                // 在分表的前提下 一般都会设置 dataNodes 信息 否则无法进行分表
            ? generateDataNodes(tableRuleConfig.getLogicTable(), shardingDataSourceNames.getDataSourceNames())
                : generateDataNodes(dataNodes, shardingDataSourceNames.getDataSourceNames()); // 使用解析表达式后得到的子表达式进行初始化  这里应该是实际的名称(推测)
        actualTables = getActualTables();
        // 选择数据源的策略 也就是2个维度 一个是datasource 一个是table
        databaseShardingStrategy = null == tableRuleConfig.getDatabaseShardingStrategyConfig() ? null : ShardingStrategyFactory.newInstance(tableRuleConfig.getDatabaseShardingStrategyConfig());
        tableShardingStrategy = null == tableRuleConfig.getTableShardingStrategyConfig() ? null : ShardingStrategyFactory.newInstance(tableRuleConfig.getTableShardingStrategyConfig());
        // 通过配置对象 获取要创建主键的列名
        generateKeyColumn = getGenerateKeyColumn(tableRuleConfig.getKeyGeneratorConfig(), defaultGenerateKeyColumn);
        // 通过SPI机制生成主键的对象
        shardingKeyGenerator = containsKeyGeneratorConfiguration(tableRuleConfig)
                // 生成主键算法, 一些额外的属性
                ? new ShardingKeyGeneratorServiceLoader().newService(tableRuleConfig.getKeyGeneratorConfig().getType(), tableRuleConfig.getKeyGeneratorConfig().getProperties())
                : null; // 代表没有指定 唯一主键生成器
        // 校验此时根据 ruleConfig配置的一系列参数是否合法
        checkRule(dataNodes);
    }
    
    private void cacheActualDatasourcesAndTables() {
        for (DataNode each : actualDataNodes) {
            actualDatasourceNames.add(each.getDataSourceName());
            addActualTable(each.getDataSourceName(), each.getTableName());
        }
    }

    /**
     * 从node 中获取表名
     * @return
     */
    private Set<String> getActualTables() {
        // 因为元素本身就少吧 (不怕浪费内存) 所以loadFactor 设置成1 为了减少碰撞
        Set<String> result = new HashSet<>(actualDataNodes.size(), 1);
        // 但是如果没有指定 actual-data-nodes 那么就是逻辑表  如果 ds0.table0,ds0.table1 ds1.table0,ds1.table1 那么表名不是自动去重了吗???
        for (DataNode each : actualDataNodes) {
            result.add(each.getTableName());
        }
        return result;
    }
    
    private void addActualTable(final String datasourceName, final String tableName) {
        datasourceToTablesMap.computeIfAbsent(datasourceName, k -> new LinkedHashSet<>()).add(tableName);
    }
    
    private boolean containsKeyGeneratorConfiguration(final TableRuleConfiguration tableRuleConfiguration) {
        return null != tableRuleConfiguration.getKeyGeneratorConfig() && !Strings.isNullOrEmpty(tableRuleConfiguration.getKeyGeneratorConfig().getType());
    }

    /**
     * 判断哪行需要自动生成id
     * @param keyGeneratorConfiguration
     * @param defaultGenerateKeyColumn
     * @return
     */
    private String getGenerateKeyColumn(final KeyGeneratorConfiguration keyGeneratorConfiguration, final String defaultGenerateKeyColumn) {
        if (null != keyGeneratorConfiguration && !Strings.isNullOrEmpty(keyGeneratorConfiguration.getColumn())) {
            return keyGeneratorConfiguration.getColumn();
        }
        return defaultGenerateKeyColumn;
    }
    
    private boolean isEmptyDataNodes(final List<String> dataNodes) {
        return null == dataNodes || dataNodes.isEmpty();
    }

    /**
     * 当没有直接在配置中找到某个逻辑表的物理节点时
     * @param logicTable
     * @param dataSourceNames
     * @return
     */
    private List<DataNode> generateDataNodes(final String logicTable, final Collection<String> dataSourceNames) {
        List<DataNode> result = new LinkedList<>();
        int index = 0;
        for (String each : dataSourceNames) {
            DataNode dataNode = new DataNode(each, logicTable);
            result.add(dataNode);
            dataNodeIndexMap.put(dataNode, index);
            // 填充物理数据源
            actualDatasourceNames.add(each);
            addActualTable(dataNode.getDataSourceName(), dataNode.getTableName());
            index++;
        }
        return result;
    }

    /**
     * 通过一组物理节点进行初始化  另一种情况是使用逻辑表进行初始化
     * @param actualDataNodes
     * @param dataSourceNames
     * @return
     */
    private List<DataNode> generateDataNodes(final List<String> actualDataNodes, final Collection<String> dataSourceNames) {
        List<DataNode> result = new LinkedList<>();
        int index = 0;
        for (String each : actualDataNodes) {
            DataNode dataNode = new DataNode(each);
            // 代表配置的 dataNode 有问题 出现了未登记的dataSource
            if (!dataSourceNames.contains(dataNode.getDataSourceName())) {
                throw new ShardingSphereException("Cannot find data source in sharding rule, invalid actual data node is: '%s'", each);
            }
            result.add(dataNode);
            dataNodeIndexMap.put(dataNode, index);
            actualDatasourceNames.add(dataNode.getDataSourceName());
            addActualTable(dataNode.getDataSourceName(), dataNode.getTableName());
            index++;
        }
        return result;
    }
    
    /**
     * Get data node groups.
     *
     * @return data node groups, key is data source name, value is data nodes belong to this data source
     * 将数据分组  以dataSourceName 作为key
     */
    public Map<String, List<DataNode>> getDataNodeGroups() {
        // 生成 dataNodes 等大的list
        Map<String, List<DataNode>> result = new LinkedHashMap<>(actualDataNodes.size(), 1);
        for (DataNode each : actualDataNodes) {
            // key 是物理数据源
            String dataSourceName = each.getDataSourceName();
            if (!result.containsKey(dataSourceName)) {
                result.put(dataSourceName, new LinkedList<>());
            }
            result.get(dataSourceName).add(each);
        }
        return result;
    }
    
    /**
     * Get actual data source names.
     *
     * @return actual data source names
     */
    public Collection<String> getActualDatasourceNames() {
        return actualDatasourceNames;
    }
    
    /**
     * Get actual table names via target data source name.
     *
     * @param targetDataSource target data source name
     * @return names of actual tables
     */
    public Collection<String> getActualTableNames(final String targetDataSource) {
        Collection<String> result = datasourceToTablesMap.get(targetDataSource);
        if (null == result) {
            result = Collections.emptySet();
        }
        return result;
    }
    
    int findActualTableIndex(final String dataSourceName, final String actualTableName) {
        return dataNodeIndexMap.getOrDefault(new DataNode(dataSourceName, actualTableName), -1);
    }
    
    boolean isExisted(final String actualTableName) {
        return actualTables.contains(actualTableName);
    }

    /**
     * 这里确保必须要设置物理节点 除非本身就没有 分表策略 或者说本身逻辑表就是物理表
     * @param dataNodes
     */
    private void checkRule(final List<String> dataNodes) {
        if (isEmptyDataNodes(dataNodes) && null != tableShardingStrategy && !(tableShardingStrategy instanceof NoneShardingStrategy)) {
            throw new ShardingSphereConfigurationException("ActualDataNodes must be configured if want to shard tables for logicTable [%s]", logicTable);
        }
    }
}
