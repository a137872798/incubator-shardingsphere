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

package org.apache.shardingsphere.underlying.rewrite.sql.token.generator;

import org.apache.shardingsphere.sql.parser.relation.metadata.RelationMetas;
import org.apache.shardingsphere.sql.parser.relation.statement.SQLStatementContext;
import org.apache.shardingsphere.underlying.rewrite.sql.token.generator.aware.ParametersAware;
import org.apache.shardingsphere.underlying.rewrite.sql.token.generator.aware.PreviousSQLTokensAware;
import org.apache.shardingsphere.underlying.rewrite.sql.token.generator.aware.RelationMetasAware;
import org.apache.shardingsphere.underlying.rewrite.sql.token.pojo.SQLToken;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * SQL token generators.
 * token生成器
 */
public final class SQLTokenGenerators {

    /**
     * 包含一组对象 每个 SQLTokenGenerator 能从sql 中解析出某种Token
     */
    private final Collection<SQLTokenGenerator> sqlTokenGenerators = new LinkedList<>();
    
    /**
     * Add all SQL token generators.
     * 
     * @param sqlTokenGenerators SQL token generators
     */
    public void addAll(final Collection<SQLTokenGenerator> sqlTokenGenerators) {
        for (SQLTokenGenerator each : sqlTokenGenerators) {
            if (!containsClass(each)) {
                this.sqlTokenGenerators.add(each);
            }
        }
    }
    
    private boolean containsClass(final SQLTokenGenerator sqlTokenGenerator) {
        for (SQLTokenGenerator each : sqlTokenGenerators) {
            if (each.getClass() == sqlTokenGenerator.getClass()) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * Generate SQL tokens.
     *
     * @param sqlStatementContext SQL statement context   本次会话相关信息
     * @param parameters SQL parameters  本次使用的所有参数 (已进行改写)
     * @param relationMetas relation metas  所有逻辑表有哪些列信息
     * @return SQL tokens
     * 传入会话对象并经过所有 tokenGenerator 处理后获取到一组token    token 就代表本次sql的一些关键字信息
     */
    public List<SQLToken> generateSQLTokens(final SQLStatementContext sqlStatementContext, final List<Object> parameters, final RelationMetas relationMetas) {
        List<SQLToken> result = new LinkedList<>();
        for (SQLTokenGenerator each : sqlTokenGenerators) {
            // 设置相关参数
            setUpSQLTokenGenerator(each, parameters, relationMetas, result);
            // 代表不满足生成token的条件
            if (!each.isGenerateSQLToken(sqlStatementContext)) {
                continue;
            }
            // 解析出token 后 保存到list 中
            if (each instanceof OptionalSQLTokenGenerator) {
                SQLToken sqlToken = ((OptionalSQLTokenGenerator) each).generateSQLToken(sqlStatementContext);
                if (!result.contains(sqlToken)) {
                    result.add(sqlToken);
                }
            } else if (each instanceof CollectionSQLTokenGenerator) {
                result.addAll(((CollectionSQLTokenGenerator) each).generateSQLTokens(sqlStatementContext));
            }
        }
        return result;
    }

    /**
     * 为 tokenGenerator 设置相关参数
     * @param sqlTokenGenerator
     * @param parameters
     * @param relationMetas
     * @param previousSQLTokens
     */
    private void setUpSQLTokenGenerator(final SQLTokenGenerator sqlTokenGenerator, final List<Object> parameters, final RelationMetas relationMetas, final List<SQLToken> previousSQLTokens) {
        if (sqlTokenGenerator instanceof ParametersAware) {
            ((ParametersAware) sqlTokenGenerator).setParameters(parameters);
        }
        if (sqlTokenGenerator instanceof RelationMetasAware) {
            ((RelationMetasAware) sqlTokenGenerator).setRelationMetas(relationMetas);
        }
        if (sqlTokenGenerator instanceof PreviousSQLTokensAware) {
            ((PreviousSQLTokensAware) sqlTokenGenerator).setPreviousSQLTokens(previousSQLTokens);
        }
    }
}
