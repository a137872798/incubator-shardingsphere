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

package org.apache.shardingsphere.sql.parser.relation.statement;

import lombok.Getter;
import lombok.ToString;
import org.apache.shardingsphere.sql.parser.relation.segment.table.TablesContext;
import org.apache.shardingsphere.sql.parser.sql.statement.SQLStatement;

import java.util.Collections;

/**
 * Common SQL statement context.
 * 
 * @param <T> type of SQL statement
 *           这里抽取了 会话上下文公共的部分
 */
@Getter
@ToString
public class CommonSQLStatementContext<T extends SQLStatement> implements SQLStatementContext<T> {

    /**
     * 本次会话实体   是通过visitor 观察 ast语法树后生成的
     */
    private final T sqlStatement;
    
    private final TablesContext tablesContext;
    
    public CommonSQLStatementContext(final T sqlStatement) {
        this.sqlStatement = sqlStatement;
        tablesContext = new TablesContext(Collections.emptyList());
    }
}