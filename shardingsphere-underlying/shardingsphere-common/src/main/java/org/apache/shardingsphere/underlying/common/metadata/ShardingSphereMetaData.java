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

package org.apache.shardingsphere.underlying.common.metadata;

import lombok.Getter;
import org.apache.shardingsphere.underlying.common.metadata.datasource.DataSourceMetas;
import org.apache.shardingsphere.underlying.common.metadata.table.TableMetaData;
import org.apache.shardingsphere.underlying.common.metadata.table.TableMetas;
import org.apache.shardingsphere.sql.parser.relation.metadata.RelationMetaData;
import org.apache.shardingsphere.sql.parser.relation.metadata.RelationMetas;

import java.util.HashMap;
import java.util.Map;

/**
 * ShardingSphere meta data.
 * 主要的元数据信息
 */
@Getter
public final class ShardingSphereMetaData {

    /**
     * 数据源元数据
     */
    private final DataSourceMetas dataSources;

    /**
     * 涉及到的表的元数据
     */
    private final TableMetas tables;

    /**
     * 关联关系的元数据
     */
    private final RelationMetas relationMetas;
    
    public ShardingSphereMetaData(final DataSourceMetas dataSources, final TableMetas tables) {
        this.dataSources = dataSources;
        this.tables = tables;
        // 初始化关联信息
        relationMetas = createRelationMetas();
    }
    
    private RelationMetas createRelationMetas() {
        // 获取所有表名 如果设置了 defaultDataSource 那么会将该数据源下所有表的元数据都拉取下来同时key 是物理表
        Map<String, RelationMetaData> result = new HashMap<>(tables.getAllTableNames().size());
        for (String each : tables.getAllTableNames()) {
            // 获取对应的表元数据
            TableMetaData tableMetaData = tables.get(each);
            result.put(each, new RelationMetaData(tableMetaData.getColumns().keySet()));
        }
        return new RelationMetas(result);
    }
}
