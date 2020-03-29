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

package org.apache.shardingsphere.sql.parser.sql.segment.generic.table;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.apache.shardingsphere.sql.parser.sql.segment.generic.AliasAvailable;
import org.apache.shardingsphere.sql.parser.sql.segment.generic.AliasSegment;
import org.apache.shardingsphere.sql.parser.sql.segment.generic.OwnerAvailable;
import org.apache.shardingsphere.sql.parser.sql.segment.generic.OwnerSegment;
import org.apache.shardingsphere.sql.parser.sql.value.identifier.IdentifierValue;

import java.util.Optional;

/**
 * Simple table segment.
 * 表相关的段信息   每个段都是AST 上的一个节点
 */
@RequiredArgsConstructor
@Getter
@ToString
public final class SimpleTableSegment implements TableSegment, OwnerAvailable, AliasAvailable {

    /**
     * 该段携带的是 表名
     */
    private final TableNameSegment tableName;

    /**
     * 这个 owner 应该是指 这个表被哪个用户所持有吧
     */
    @Setter
    private OwnerSegment owner;

    /**
     * 该段内部包含的是别名信息
     */
    @Setter
    private AliasSegment alias;
    
    public SimpleTableSegment(final int startIndex, final int stopIndex, final IdentifierValue identifierValue) {
        tableName = new TableNameSegment(startIndex, stopIndex, identifierValue);
    }
    
    @Override
    public int getStartIndex() {
        return null == owner ? tableName.getStartIndex() : owner.getStartIndex(); 
    }
    
    @Override
    public int getStopIndex() {
        return tableName.getStopIndex();
        //FIXME: Rewriter need to handle alias as well
//        return null == alias ? tableName.getStopIndex() : alias.getStopIndex();
    }
    
    @Override
    public Optional<OwnerSegment> getOwner() {
        return Optional.ofNullable(owner);
    }
    
    @Override
    public Optional<String> getAlias() {
        return null == alias ? Optional.empty() : Optional.ofNullable(alias.getIdentifier().getValue());
    }
}
