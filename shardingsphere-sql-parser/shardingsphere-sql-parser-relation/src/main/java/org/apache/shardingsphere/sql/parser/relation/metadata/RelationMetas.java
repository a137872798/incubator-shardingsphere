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

package org.apache.shardingsphere.sql.parser.relation.metadata;

import lombok.RequiredArgsConstructor;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Relation metas.
 * 一组关联元数据
 */
@RequiredArgsConstructor
public final class RelationMetas {

    /**
     * key: tableName value: 元数据信息 (实际上是某个表的全列名)
     */
    private final Map<String, RelationMetaData> relations;
    
    /**
     * Judge whether contains table.
     *
     * @param tableName table name
     * @return contains table or not
     */
    public boolean containsTable(final String tableName) {
        return relations.containsKey(tableName);
    }
    
    /**
     * Judge whether contains column name.
     * 
     * @param tableName table name
     * @param columnName column name
     * @return contains column or not
     * 判断某个表是否有某个字段
     */
    public boolean containsColumn(final String tableName, final String columnName) {
        return relations.containsKey(tableName) && relations.get(tableName).getColumnNames().contains(columnName.toLowerCase());
    }
    
    /**
     * Get all column names via table.
     *
     * @param tableName table name
     * @return column names
     * 获取某个表下所有的列名
     */
    public List<String> getAllColumnNames(final String tableName) {
        return relations.containsKey(tableName) ? relations.get(tableName).getColumnNames() : Collections.<String>emptyList();
    }
}
