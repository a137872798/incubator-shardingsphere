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
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import lombok.Getter;
import org.apache.shardingsphere.api.config.masterslave.MasterSlaveRuleConfiguration;
import org.apache.shardingsphere.api.config.sharding.KeyGeneratorConfiguration;
import org.apache.shardingsphere.api.config.sharding.ShardingRuleConfiguration;
import org.apache.shardingsphere.api.config.sharding.TableRuleConfiguration;
import org.apache.shardingsphere.api.config.sharding.strategy.ShardingStrategyConfiguration;
import org.apache.shardingsphere.core.strategy.route.ShardingStrategy;
import org.apache.shardingsphere.core.strategy.route.ShardingStrategyFactory;
import org.apache.shardingsphere.core.strategy.route.none.NoneShardingStrategy;
import org.apache.shardingsphere.encrypt.api.EncryptRuleConfiguration;
import org.apache.shardingsphere.encrypt.rule.EncryptRule;
import org.apache.shardingsphere.spi.algorithm.keygen.ShardingKeyGeneratorServiceLoader;
import org.apache.shardingsphere.spi.keygen.ShardingKeyGenerator;
import org.apache.shardingsphere.underlying.common.config.exception.ShardingSphereConfigurationException;
import org.apache.shardingsphere.underlying.common.rule.BaseRule;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.TreeSet;

/**
 * Databases and tables sharding rule.
 * 分库分表规则
 */
@Getter
public class ShardingRule implements BaseRule {
    
    private final ShardingRuleConfiguration ruleConfiguration;
    
    private final ShardingDataSourceNames shardingDataSourceNames;
    
    private final Collection<TableRule> tableRules;
    
    private final Collection<BindingTableRule> bindingTableRules;
    
    private final Collection<String> broadcastTables;
    
    private final ShardingStrategy defaultDatabaseShardingStrategy;
    
    private final ShardingStrategy defaultTableShardingStrategy;
    
    private final ShardingKeyGenerator defaultShardingKeyGenerator;
    
    private final Collection<MasterSlaveRule> masterSlaveRules;
    
    private final EncryptRule encryptRule;


    /**
     * @param shardingRuleConfig  分表相关所有配置都在里面
     * @param dataSourceNames  本次配置中所有数据源名称
     */
    public ShardingRule(final ShardingRuleConfiguration shardingRuleConfig, final Collection<String> dataSourceNames) {
        Preconditions.checkArgument(null != shardingRuleConfig, "ShardingRuleConfig cannot be null.");
        Preconditions.checkArgument(null != dataSourceNames && !dataSourceNames.isEmpty(), "Data sources cannot be empty.");
        this.ruleConfiguration = shardingRuleConfig;
        // 该对象就是2个参数的包装对象
        shardingDataSourceNames = new ShardingDataSourceNames(shardingRuleConfig, dataSourceNames);
        // 根据table级别规则配置来生成规则对象  注意这里内部配置的dataNode 可能是物理表名 也可能是逻辑表名 主要看配置文件有没有设置 dataNode
        tableRules = createTableRules(shardingRuleConfig);
        // 获取配置下所有的广播表
        broadcastTables = shardingRuleConfig.getBroadcastTables();
        // 绑定表可以避免笛卡尔积 也就是将多张关联性很强的表绑定在一起  0表对0表 1表对1表 本来排列组合 是4种情况
        bindingTableRules = createBindingTableRules(shardingRuleConfig.getBindingTableGroups());

        // 根据配置生成相关策略 (降级策略)
        defaultDatabaseShardingStrategy = createDefaultShardingStrategy(shardingRuleConfig.getDefaultDatabaseShardingStrategyConfig());
        defaultTableShardingStrategy = createDefaultShardingStrategy(shardingRuleConfig.getDefaultTableShardingStrategyConfig());
        defaultShardingKeyGenerator = createDefaultKeyGenerator(shardingRuleConfig.getDefaultKeyGeneratorConfig());
        // TODO 没设置主从的情况就先忽略
        masterSlaveRules = createMasterSlaveRules(shardingRuleConfig.getMasterSlaveRuleConfigs());
        // TODO 数据脱敏的忽略
        encryptRule = createEncryptRule(shardingRuleConfig.getEncryptRuleConfig());
    }

    /**
     * 从规则配置中获取表的配置 并生成 TableRule (该对象内部包含了某个逻辑表下所有的dataSource)
     * @param shardingRuleConfig
     * @return
     */
    private Collection<TableRule> createTableRules(final ShardingRuleConfiguration shardingRuleConfig) {
        // 以表为维度 声明了一些逻辑表的规则
        Collection<TableRuleConfiguration> tableRuleConfigurations = shardingRuleConfig.getTableRuleConfigs();
        Collection<TableRule> result = new ArrayList<>(tableRuleConfigurations.size());
        for (TableRuleConfiguration each : tableRuleConfigurations) {
            // 每个规则对象又和 dataSources 以及 全局规则绑定在一起   还有默认生成的列 一般都不配置
            result.add(new TableRule(each, shardingDataSourceNames, getDefaultGenerateKeyColumn(shardingRuleConfig)));
        }
        return result;
    }

    /**
     * 通过配置中包含的KeyGeneratorConfig 得知本次自动生成键的列名
     * @param shardingRuleConfig
     * @return
     */
    private String getDefaultGenerateKeyColumn(final ShardingRuleConfiguration shardingRuleConfig) {
        return null == shardingRuleConfig.getDefaultKeyGeneratorConfig() ? null : shardingRuleConfig.getDefaultKeyGeneratorConfig().getColumn();
    }

    /**
     *
     * @param bindingTableGroups 每个字符串是绑定的一组
     * @return
     */
    private Collection<BindingTableRule> createBindingTableRules(final Collection<String> bindingTableGroups) {
        Collection<BindingTableRule> result = new ArrayList<>(bindingTableGroups.size());
        for (String each : bindingTableGroups) {
            // 将结果包装成 bindingTable
            result.add(createBindingTableRule(each));
        }
        return result;
    }

    /**
     * 根据绑定表组来创建规则对象
     * @param bindingTableGroup
     * @return
     */
    private BindingTableRule createBindingTableRule(final String bindingTableGroup) {
        List<TableRule> tableRules = new LinkedList<>();
        for (String each : Splitter.on(",").trimResults().splitToList(bindingTableGroup)) {
            tableRules.add(getTableRule(each));
        }
        return new BindingTableRule(tableRules);
    }
    
    private ShardingStrategy createDefaultShardingStrategy(final ShardingStrategyConfiguration shardingStrategyConfiguration) {
        return null == shardingStrategyConfiguration ? new NoneShardingStrategy() : ShardingStrategyFactory.newInstance(shardingStrategyConfiguration);
    }
    
    private ShardingKeyGenerator createDefaultKeyGenerator(final KeyGeneratorConfiguration keyGeneratorConfiguration) {
        ShardingKeyGeneratorServiceLoader serviceLoader = new ShardingKeyGeneratorServiceLoader();
        return containsKeyGeneratorConfiguration(keyGeneratorConfiguration)
                ? serviceLoader.newService(keyGeneratorConfiguration.getType(), keyGeneratorConfiguration.getProperties()) : serviceLoader.newService();
    }
    
    private boolean containsKeyGeneratorConfiguration(final KeyGeneratorConfiguration keyGeneratorConfiguration) {
        return null != keyGeneratorConfiguration && !Strings.isNullOrEmpty(keyGeneratorConfiguration.getType());
    }
    
    private Collection<MasterSlaveRule> createMasterSlaveRules(final Collection<MasterSlaveRuleConfiguration> masterSlaveRuleConfigurations) {
        Collection<MasterSlaveRule> result = new ArrayList<>(masterSlaveRuleConfigurations.size());
        for (MasterSlaveRuleConfiguration each : masterSlaveRuleConfigurations) {
            result.add(new MasterSlaveRule(each));
        }
        return result;
    }
    
    private EncryptRule createEncryptRule(final EncryptRuleConfiguration encryptRuleConfig) {
        return null == encryptRuleConfig ? new EncryptRule() : new EncryptRule(ruleConfiguration.getEncryptRuleConfig());
    }
    
    /**
     * Find table rule.
     *
     * @param logicTableName logic table name
     * @return table rule
     */
    public Optional<TableRule> findTableRule(final String logicTableName) {
        for (TableRule each : tableRules) {
            if (each.getLogicTable().equalsIgnoreCase(logicTableName)) {
                return Optional.of(each);
            }
        }
        return Optional.empty();
    }
    
    /**
     * Find table rule via actual table name.
     *
     * @param actualTableName actual table name
     * @return table rule
     */
    public Optional<TableRule> findTableRuleByActualTable(final String actualTableName) {
        for (TableRule each : tableRules) {
            if (each.isExisted(actualTableName)) {
                return Optional.of(each);
            }
        }
        return Optional.empty();
    }
    
    /**
     * Get table rule.
     *
     * @param logicTableName logic table name
     * @return table rule
     */
    public TableRule getTableRule(final String logicTableName) {
        // 上一步 已经根据所有TableConfig信息生成了 TableRule 对象了 那么这里只是找到该对象
        Optional<TableRule> tableRule = findTableRule(logicTableName);
        if (tableRule.isPresent()) {
            return tableRule.get();
        }
        // 如果该表被指定为广播表
        if (isBroadcastTable(logicTableName)) {
            return new TableRule(shardingDataSourceNames.getDataSourceNames(), logicTableName);
        }
        // 默认情况 就当作不需要分表  这里会获取该dataSource 下所有的table信息
        if (!Strings.isNullOrEmpty(shardingDataSourceNames.getDefaultDataSourceName())) {
            return new TableRule(shardingDataSourceNames.getDefaultDataSourceName(), logicTableName);
        }
        throw new ShardingSphereConfigurationException("Cannot find table rule and default data source with logic table: '%s'", logicTableName);
    }
    
    /**
     * Get database sharding strategy.
     *
     * <p>
     * Use default database sharding strategy if not found.
     * </p>
     *
     * @param tableRule table rule
     * @return database sharding strategy
     */
    public ShardingStrategy getDatabaseShardingStrategy(final TableRule tableRule) {
        return null == tableRule.getDatabaseShardingStrategy() ? defaultDatabaseShardingStrategy : tableRule.getDatabaseShardingStrategy();
    }
    
    /**
     * Get table sharding strategy.
     *
     * <p>
     * Use default table sharding strategy if not found.
     * </p>
     *
     * @param tableRule table rule
     * @return table sharding strategy
     * 获取table 级别的分表策略
     */
    public ShardingStrategy getTableShardingStrategy(final TableRule tableRule) {
        return null == tableRule.getTableShardingStrategy() ? defaultTableShardingStrategy : tableRule.getTableShardingStrategy();
    }
    
    /**
     * Judge logic tables is all belong to binding encryptors.
     *
     * @param logicTableNames logic table names   某组逻辑表
     * @return logic tables is all belong to binding encryptors or not
     */
    public boolean isAllBindingTables(final Collection<String> logicTableNames) {
        if (logicTableNames.isEmpty()) {
            return false;
        }
        // 判断本组逻辑表是否使用同一个 TableRule
        Optional<BindingTableRule> bindingTableRule = findBindingTableRule(logicTableNames);
        if (!bindingTableRule.isPresent()) {
            return false;
        }
        // 这里只针对匹配的第一个 bindingTableRule 判断 本次所有逻辑表能否在 内部所有的 tableRule 找到对应者
        Collection<String> result = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        result.addAll(bindingTableRule.get().getAllLogicTables());
        return !result.isEmpty() && result.containsAll(logicTableNames);
    }

    /**
     * 只要能通过某个逻辑表名找到对应的 bindingRule 就直接返回
     * @param logicTableNames
     * @return
     */
    private Optional<BindingTableRule> findBindingTableRule(final Collection<String> logicTableNames) {
        for (String each : logicTableNames) {
            Optional<BindingTableRule> result = findBindingTableRule(each);
            if (result.isPresent()) {
                return result;
            }
        }
        return Optional.empty();
    }
    
    /**
     * Find binding table rule via logic table name.
     *
     * @param logicTableName logic table name
     * @return binding table rule
     */
    public Optional<BindingTableRule> findBindingTableRule(final String logicTableName) {
        for (BindingTableRule each : bindingTableRules) {
            if (each.hasLogicTable(logicTableName)) {
                return Optional.of(each);
            }
        }
        return Optional.empty();
    }
    
    /**
     * Judge logic tables is all belong to broadcast encryptors.
     *
     * @param logicTableNames logic table names
     * @return logic tables is all belong to broadcast encryptors or not
     */
    public boolean isAllBroadcastTables(final Collection<String> logicTableNames) {
        if (logicTableNames.isEmpty()) {
            return false;
        }
        for (String each : logicTableNames) {
            if (!isBroadcastTable(each)) {
                return false;
            }
        }
        return true;
    }
    
    /**
     * Judge logic table is belong to broadcast tables.
     *
     * @param logicTableName logic table name
     * @return logic table is belong to broadcast tables or not
     */
    public boolean isBroadcastTable(final String logicTableName) {
        for (String each : broadcastTables) {
            if (each.equalsIgnoreCase(logicTableName)) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * Judge logic tables is all belong to default data source.
     *
     * @param logicTableNames logic table names
     * @return logic tables is all belong to default data source
     */
    public boolean isAllInDefaultDataSource(final Collection<String> logicTableNames) {
        if (!hasDefaultDataSourceName()) {
            return false;
        }
        for (String each : logicTableNames) {
            if (findTableRule(each).isPresent() || isBroadcastTable(each)) {
                return false;
            }
        }
        return !logicTableNames.isEmpty();
    }
    
    /**
     * Judge if there is at least one table rule for logic tables.
     *
     * @param logicTableNames logic table names
     * @return whether a table rule exists for logic tables
     */
    public boolean tableRuleExists(final Collection<String> logicTableNames) {
        for (String each : logicTableNames) {
            if (findTableRule(each).isPresent() || isBroadcastTable(each)) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * Judge is sharding column or not.
     *
     * @param columnName column name
     * @param tableName table name
     * @return is sharding column or not
     */
    public boolean isShardingColumn(final String columnName, final String tableName) {
        for (TableRule each : tableRules) {
            if (each.getLogicTable().equalsIgnoreCase(tableName) && isShardingColumn(each, columnName)) {
                return true;
            }
        }
        return false;
    }
    
    private boolean isShardingColumn(final TableRule tableRule, final String columnName) {
        return getDatabaseShardingStrategy(tableRule).getShardingColumns().contains(columnName) || getTableShardingStrategy(tableRule).getShardingColumns().contains(columnName);
    }
    
    /**
     * Find column name of generated key.
     *
     * @param logicTableName logic table name
     * @return column name of generated key
     * 确保查询的 逻辑表名 保存在rule 中
     */
    public Optional<String> findGenerateKeyColumnName(final String logicTableName) {
        for (TableRule each : tableRules) {
            if (each.getLogicTable().equalsIgnoreCase(logicTableName) && null != each.getGenerateKeyColumn()) {
                return Optional.of(each.getGenerateKeyColumn());
            }
        }
        return Optional.empty();
    }
    
    /**
     * Generate key.
     *
     * @param logicTableName logic table name
     * @return generated key
     * 通过逻辑表名 确定某一组分表 并生成全局唯一键
     */
    public Comparable<?> generateKey(final String logicTableName) {
        // 获取该逻辑表相关的规则
        Optional<TableRule> tableRule = findTableRule(logicTableName);
        if (!tableRule.isPresent()) {
            throw new ShardingSphereConfigurationException("Cannot find strategy for generate keys.");
        }
        // 获取唯一键生成器
        ShardingKeyGenerator shardingKeyGenerator = null == tableRule.get().getShardingKeyGenerator() ? defaultShardingKeyGenerator : tableRule.get().getShardingKeyGenerator();
        return shardingKeyGenerator.generateKey();
    }
    
    /**
     * Get logic table names based on actual table name.
     *
     * @param actualTableName actual table name
     * @return logic table name
     */
    public Collection<String> getLogicTableNames(final String actualTableName) {
        Collection<String> result = new LinkedList<>();
        for (TableRule each : tableRules) {
            if (each.isExisted(actualTableName)) {
                result.add(each.getLogicTable());
            }
        }
        return result;
    }
    
    /**
     * Find data node by logic table name.
     *
     * @param logicTableName logic table name
     * @return data node
     * 这里只返回一个dataNode
     */
    public DataNode getDataNode(final String logicTableName) {
        TableRule tableRule = getTableRule(logicTableName);
        return tableRule.getActualDataNodes().get(0);
    }
    
    /**
     * Find data node by data source and logic table.
     *
     * @param dataSourceName data source name
     * @param logicTableName logic table name
     * @return data node
     */
    public DataNode getDataNode(final String dataSourceName, final String logicTableName) {
        TableRule tableRule = getTableRule(logicTableName);
        for (DataNode each : tableRule.getActualDataNodes()) {
            if (shardingDataSourceNames.getDataSourceNames().contains(each.getDataSourceName()) && each.getDataSourceName().equals(dataSourceName)) {
                return each;
            }
        }
        throw new ShardingSphereConfigurationException("Cannot find actual data node for data source name: '%s' and logic table name: '%s'", dataSourceName, logicTableName);
    }
    
    /**
     * Judge if default data source mame exists.
     *
     * @return if default data source name exists
     */
    public boolean hasDefaultDataSourceName() {
        String defaultDataSourceName = shardingDataSourceNames.getDefaultDataSourceName();
        return !Strings.isNullOrEmpty(defaultDataSourceName);
    }
    
    /**
     * Find actual default data source name.
     *
     * <p>If use master-slave rule, return master data source name.</p>
     *
     * @return actual default data source name
     */
    public Optional<String> findActualDefaultDataSourceName() {
        String defaultDataSourceName = shardingDataSourceNames.getDefaultDataSourceName();
        if (Strings.isNullOrEmpty(defaultDataSourceName)) {
            return Optional.empty();
        }
        Optional<String> masterDefaultDataSourceName = findMasterDataSourceName(defaultDataSourceName);
        return masterDefaultDataSourceName.isPresent() ? masterDefaultDataSourceName : Optional.of(defaultDataSourceName);
    }
    
    private Optional<String> findMasterDataSourceName(final String masterSlaveRuleName) {
        for (MasterSlaveRule each : masterSlaveRules) {
            if (each.getName().equals(masterSlaveRuleName)) {
                return Optional.of(each.getMasterDataSourceName());
            }
        }
        return Optional.empty();
    }
    
    /**
     * Find master slave rule.
     *
     * @param dataSourceName data source name
     * @return master slave rule
     */
    public Optional<MasterSlaveRule> findMasterSlaveRule(final String dataSourceName) {
        for (MasterSlaveRule each : masterSlaveRules) {
            if (each.containDataSourceName(dataSourceName)) {
                return Optional.of(each);
            }
        }
        return Optional.empty();
    }
    
    /**
     * Get sharding logic table names.
     *
     * @param logicTableNames logic table names
     * @return sharding logic table names
     */
    public Collection<String> getShardingLogicTableNames(final Collection<String> logicTableNames) {
        Collection<String> result = new LinkedList<>();
        for (String each : logicTableNames) {
            if (findTableRule(each).isPresent()) {
                result.add(each);
            }
        }
        return result;
    }
}
