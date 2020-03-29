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

package org.apache.shardingsphere.underlying.rewrite.sql.token.generator.generic;

import com.google.common.base.Preconditions;
import org.apache.shardingsphere.sql.parser.relation.statement.SQLStatementContext;
import org.apache.shardingsphere.sql.parser.sql.segment.generic.RemoveAvailable;
import org.apache.shardingsphere.sql.parser.sql.statement.dal.dialect.mysql.ShowColumnsStatement;
import org.apache.shardingsphere.sql.parser.sql.statement.dal.dialect.mysql.ShowTableStatusStatement;
import org.apache.shardingsphere.sql.parser.sql.statement.dal.dialect.mysql.ShowTablesStatement;
import org.apache.shardingsphere.underlying.rewrite.sql.token.generator.CollectionSQLTokenGenerator;
import org.apache.shardingsphere.underlying.rewrite.sql.token.pojo.generic.RemoveToken;

import java.util.Collection;
import java.util.Collections;

/**
 * Remove token generator.
 * token生成器的默认实现 应该就是从sql 中找到token 同时从sql中移除
 */
public final class RemoveTokenGenerator implements CollectionSQLTokenGenerator {
    
    @Override
    public boolean isGenerateSQLToken(final SQLStatementContext sqlStatementContext) {
        // 如果本次会话是查询表 相关信息的 那么根据 fromSchema 的结果
        if (sqlStatementContext.getSqlStatement() instanceof ShowTablesStatement) {
            return ((ShowTablesStatement) sqlStatementContext.getSqlStatement()).getFromSchema().isPresent();
        }
        if (sqlStatementContext.getSqlStatement() instanceof ShowTableStatusStatement) {
            return ((ShowTableStatusStatement) sqlStatementContext.getSqlStatement()).getFromSchema().isPresent();
        }
        if (sqlStatementContext.getSqlStatement() instanceof ShowColumnsStatement) {
            return ((ShowColumnsStatement) sqlStatementContext.getSqlStatement()).getFromSchema().isPresent();
        }
        return false;
    }

    /**
     * 从当前会话中生成一组 removeToken
     * @param sqlStatementContext SQL statement context
     * @return
     */
    @Override
    public Collection<RemoveToken> generateSQLTokens(final SQLStatementContext sqlStatementContext) {
        if (sqlStatementContext.getSqlStatement() instanceof ShowTablesStatement) {
            // 首先要确保 存在 fromSchema 之后获取该值 并包装成 RemoveToken
            Preconditions.checkState(((ShowTablesStatement) sqlStatementContext.getSqlStatement()).getFromSchema().isPresent());
            RemoveAvailable removeAvailable = ((ShowTablesStatement) sqlStatementContext.getSqlStatement()).getFromSchema().get();
            return Collections.singletonList(new RemoveToken(removeAvailable.getStartIndex(), removeAvailable.getStopIndex()));
        }
        if (sqlStatementContext.getSqlStatement() instanceof ShowTableStatusStatement) {
            Preconditions.checkState(((ShowTableStatusStatement) sqlStatementContext.getSqlStatement()).getFromSchema().isPresent());
            RemoveAvailable removeAvailable = ((ShowTableStatusStatement) sqlStatementContext.getSqlStatement()).getFromSchema().get();
            return Collections.singletonList(new RemoveToken(removeAvailable.getStartIndex(), removeAvailable.getStopIndex()));
        }
        if (sqlStatementContext.getSqlStatement() instanceof ShowColumnsStatement) {
            Preconditions.checkState(((ShowColumnsStatement) sqlStatementContext.getSqlStatement()).getFromSchema().isPresent());
            RemoveAvailable removeAvailable = ((ShowColumnsStatement) sqlStatementContext.getSqlStatement()).getFromSchema().get();
            return Collections.singletonList(new RemoveToken(removeAvailable.getStartIndex(), removeAvailable.getStopIndex()));
        }
        return Collections.emptyList();
    }
}
