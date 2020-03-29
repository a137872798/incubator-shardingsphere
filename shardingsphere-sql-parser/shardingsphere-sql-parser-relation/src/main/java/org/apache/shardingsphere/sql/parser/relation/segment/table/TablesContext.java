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

package org.apache.shardingsphere.sql.parser.relation.segment.table;

import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.sql.parser.relation.metadata.RelationMetas;
import org.apache.shardingsphere.sql.parser.sql.segment.dml.predicate.PredicateSegment;
import org.apache.shardingsphere.sql.parser.sql.segment.generic.table.SimpleTableSegment;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Optional;

/**
 * Tables context.
 * 表相关的上下文信息
 */
@RequiredArgsConstructor
public final class TablesContext {

    /**
     * 表信息 不仅意味着表名  还包含着 别名等信息
     */
    private final Collection<SimpleTableSegment> tables;
    
    public TablesContext(final SimpleTableSegment tableSegment) {
        this(null == tableSegment ? Collections.emptyList() : Collections.singletonList(tableSegment));
    }
    
    /**
     * Get table names.
     * 
     * @return table names
     * 将当前所有表名保存到list 中
     */
    public Collection<String> getTableNames() {
        Collection<String> result = new LinkedHashSet<>(tables.size(), 1);
        for (SimpleTableSegment each : tables) {
            result.add(each.getTableName().getIdentifier().getValue());
        }
        return result;
    }
    
    /**
     * Find table name.
     *
     * @param predicate predicate
     * @param relationMetas relation metas
     * @return table name
     * 尝试查找某个表名
     */
    public Optional<String> findTableName(final PredicateSegment predicate, final RelationMetas relationMetas) {
        // 只有一个表名时 直接返回
        if (1 == tables.size()) {
            return Optional.of(tables.iterator().next().getTableName().getIdentifier().getValue());
        }
        // 如果该列已经设置了 owner  代表已经标记了该列属于哪个表
        if (predicate.getColumn().getOwner().isPresent()) {
            return Optional.of(findTableNameFromSQL(predicate.getColumn().getOwner().get().getIdentifier().getValue()));
        }
        return findTableNameFromMetaData(predicate.getColumn().getIdentifier().getValue(), relationMetas);
    }

    /**
     * 找到匹配的表名
     * @param tableNameOrAlias
     * @return
     */
    private String findTableNameFromSQL(final String tableNameOrAlias) {
        for (SimpleTableSegment each : tables) {
            if (tableNameOrAlias.equalsIgnoreCase(each.getTableName().getIdentifier().getValue()) || tableNameOrAlias.equals(each.getAlias().orElse(null))) {
                return each.getTableName().getIdentifier().getValue();
            }
        }
        throw new IllegalStateException("Can not find owner from table.");
    }

    /**
     * 通过列名 和  关联关系查找表
     * @param columnName
     * @param relationMetas
     * @return
     */
    private Optional<String> findTableNameFromMetaData(final String columnName, final RelationMetas relationMetas) {
        for (SimpleTableSegment each : tables) {
            // 如果某个表包含了该列 那么返回该表名
            if (relationMetas.containsColumn(each.getTableName().getIdentifier().getValue(), columnName)) {
                return Optional.of(each.getTableName().getIdentifier().getValue());
            }
        }
        return Optional.empty();
    }
}
