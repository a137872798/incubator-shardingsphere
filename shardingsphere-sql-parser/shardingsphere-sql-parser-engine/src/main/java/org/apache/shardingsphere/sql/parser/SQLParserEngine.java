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

package org.apache.shardingsphere.sql.parser;

import lombok.RequiredArgsConstructor;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.shardingsphere.sql.parser.cache.SQLParseResultCache;
import org.apache.shardingsphere.sql.parser.core.constant.RuleName;
import org.apache.shardingsphere.sql.parser.core.parser.SQLParserExecutor;
import org.apache.shardingsphere.sql.parser.core.visitor.ParseTreeVisitorFactory;
import org.apache.shardingsphere.sql.parser.hook.ParsingHook;
import org.apache.shardingsphere.sql.parser.hook.SPIParsingHook;
import org.apache.shardingsphere.sql.parser.sql.statement.SQLStatement;

import java.util.Optional;

/**
 * SQL parser engine.
 * 解析sql 的引擎对象   具体的解析逻辑委托给了第三方的框架
 */
@RequiredArgsConstructor
public final class SQLParserEngine {

    /**
     * 本次 dataSource 类型 比如 mysql
     */
    private final String databaseTypeName;

    /**
     * 保存解析结果的缓存对象
     * TODO 因为解析本身是一个非常耗时的工作 那么如果执行的sql过多 不是会对内存造成压力吗 还是说只涉及到分表的sql才会需要解析???
     */
    private final SQLParseResultCache cache = new SQLParseResultCache();
    
    /**
     * Parse SQL.
     *
     * @param sql SQL
     * @param useCache use cache or not
     * @return SQL statement
     */
    public SQLStatement parse(final String sql, final boolean useCache) {
        // 统一管理所有钩子
        ParsingHook parsingHook = new SPIParsingHook();
        parsingHook.start(sql);
        try {
            // 解析sql 并生成会话对象
            SQLStatement result = parse0(sql, useCache);
            parsingHook.finishSuccess(result);
            return result;
            // CHECKSTYLE:OFF
        } catch (final Exception ex) {
            // CHECKSTYLE:ON
            parsingHook.finishFailure(ex);
            throw ex;
        }
    }
    
    private SQLStatement parse0(final String sql, final boolean useCache) {
        // 首先尝试从缓存获取
        if (useCache) {
            Optional<SQLStatement> cachedSQLStatement = cache.getSQLStatement(sql);
            if (cachedSQLStatement.isPresent()) {
                return cachedSQLStatement.get();
            }
        }
        // 生成一个树形对象 
        ParseTree parseTree = new SQLParserExecutor(databaseTypeName, sql).execute();
        // visitor 会检测生成的语法树节点是否满足观察条件 如果满足的话则会触发visitor的逻辑
        // 这里将信息抽取出来生成了 会话对象
        SQLStatement result = (SQLStatement) ParseTreeVisitorFactory.newInstance(databaseTypeName, RuleName.valueOf(parseTree.getClass())).visit(parseTree);
        if (useCache) {
            cache.put(sql, result);
        }
        return result;
    }
}
