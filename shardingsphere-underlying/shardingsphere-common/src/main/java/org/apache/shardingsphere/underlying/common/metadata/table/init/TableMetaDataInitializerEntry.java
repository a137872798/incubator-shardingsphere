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

package org.apache.shardingsphere.underlying.common.metadata.table.init;

import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.underlying.common.metadata.table.TableMetaData;
import org.apache.shardingsphere.underlying.common.metadata.table.TableMetas;
import org.apache.shardingsphere.underlying.common.metadata.table.init.decorator.TableMetaDataDecorator;
import org.apache.shardingsphere.underlying.common.metadata.table.init.loader.TableMetaDataLoader;
import org.apache.shardingsphere.underlying.common.rule.BaseRule;

import java.sql.SQLException;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Table meta data initializer entry.
 */
@RequiredArgsConstructor
public final class TableMetaDataInitializerEntry {

    /**
     * key 代表加载规则  value 可以根据逻辑表信息 获取到所有物理表的列信息 并且还进行校验 确保所有物理表的列信息一致
     * 一般map 只有一组键值对
     */
    private final Map<BaseRule, TableMetaDataInitializer> initializes;
    
    /**
     * Initialize table meta data.
     *
     * @param tableName table name
     * @return table meta data
     * @throws SQLException SQL exception
     */
    public TableMetaData init(final String tableName) throws SQLException {
        return decorate(tableName, load(tableName));
    }
    
    @SuppressWarnings("unchecked")
    private TableMetaData load(final String tableName) throws SQLException {
        for (Entry<BaseRule, TableMetaDataInitializer> entry : initializes.entrySet()) {
            if (entry.getValue() instanceof TableMetaDataLoader) {
                return ((TableMetaDataLoader) entry.getValue()).load(tableName, entry.getKey());
            }
        }
        throw new IllegalStateException("Cannot find class `TableMetaDataLoader`");
    }
    
    @SuppressWarnings("unchecked")
    private TableMetaData decorate(final String tableName, final TableMetaData tableMetaData) {
        TableMetaData result = tableMetaData;
        for (Entry<BaseRule, TableMetaDataInitializer> entry : initializes.entrySet()) {
            if (entry.getValue() instanceof TableMetaDataDecorator) {
                result = ((TableMetaDataDecorator) entry.getValue()).decorate(result, tableName, entry.getKey());
            }
        }
        return result;
    }
    
    /**
     * Initialize all table meta data.
     *
     * @return table metas
     * @throws SQLException SQL exception
     * 初始化时 装填所有数据源 这里初始化就要获取数据源的列信息
     */
    public TableMetas initAll() throws SQLException {
        return decorateAll(loadAll());
    }

    /**
     * 开始加载所有表元数据
     * @return
     * @throws SQLException
     */
    @SuppressWarnings("unchecked")
    private TableMetas loadAll() throws SQLException {
        for (Entry<BaseRule, TableMetaDataInitializer> entry : initializes.entrySet()) {
            if (entry.getValue() instanceof TableMetaDataLoader) {
                return ((TableMetaDataLoader) entry.getValue()).loadAll(entry.getKey());
            }
        }
        throw new IllegalStateException("Cannot find class `TableMetaDataLoader`");
    }

    /**
     * 生成一层包装
     * @param tableMetas
     * @return
     */
    @SuppressWarnings("unchecked")
    private TableMetas decorateAll(final TableMetas tableMetas) {
        TableMetas result = tableMetas;
        for (Entry<BaseRule, TableMetaDataInitializer> entry : initializes.entrySet()) {
            // 如果初始化对象 具备装饰的功能 那么在装饰后返回  ShardingTableMetaDataLoader
            if (entry.getValue() instanceof TableMetaDataDecorator) {
                result = ((TableMetaDataDecorator) entry.getValue()).decorate(result, entry.getKey());
            }
        }
        return result;
    }
}
