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

package org.apache.shardingsphere.underlying.rewrite.context;

import lombok.AccessLevel;
import lombok.Getter;
import org.apache.shardingsphere.sql.parser.relation.metadata.RelationMetas;
import org.apache.shardingsphere.sql.parser.relation.statement.SQLStatementContext;
import org.apache.shardingsphere.sql.parser.relation.statement.dml.InsertStatementContext;
import org.apache.shardingsphere.underlying.rewrite.parameter.builder.ParameterBuilder;
import org.apache.shardingsphere.underlying.rewrite.parameter.builder.impl.GroupedParameterBuilder;
import org.apache.shardingsphere.underlying.rewrite.parameter.builder.impl.StandardParameterBuilder;
import org.apache.shardingsphere.underlying.rewrite.sql.token.generator.SQLTokenGenerator;
import org.apache.shardingsphere.underlying.rewrite.sql.token.generator.SQLTokenGenerators;
import org.apache.shardingsphere.underlying.rewrite.sql.token.generator.builder.DefaultTokenGeneratorBuilder;
import org.apache.shardingsphere.underlying.rewrite.sql.token.pojo.SQLToken;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * SQL rewrite context.
 * 改写sql的上下文
 */
@Getter
public final class SQLRewriteContext {

    /**
     * 所有逻辑表下所有列
     */
    private final RelationMetas relationMetas;

    /**
     * 本次关联的 会话上下文
     */
    private final SQLStatementContext sqlStatementContext;

    /**
     * 原始sql
     */
    private final String sql;

    /**
     * 执行本次sql 使用的一组参数
     */
    private final List<Object> parameters;

    /**
     * token 在shardingSphere 中代表关键字  比如 in select delete 等 这里是详细信息 比如第几行第几列
     */
    private final List<SQLToken> sqlTokens = new LinkedList<>();

    /**
     * 参数构建器 使用原始参数进行初始化  可以调用api 对内部参数进行修改
     */
    private final ParameterBuilder parameterBuilder;

    /**
     * 该对象用于从 sql 中解析出token
     */
    @Getter(AccessLevel.NONE)
    private final SQLTokenGenerators sqlTokenGenerators = new SQLTokenGenerators();
    
    public SQLRewriteContext(final RelationMetas relationMetas, final SQLStatementContext sqlStatementContext, final String sql, final List<Object> parameters) {
        this.relationMetas = relationMetas;
        this.sqlStatementContext = sqlStatementContext;
        this.sql = sql;
        this.parameters = parameters;
        // 添加一组token 生成器
        addSQLTokenGenerators(new DefaultTokenGeneratorBuilder().getSQLTokenGenerators());
        // 如果是插入语句 就获取 groupedParameters
        parameterBuilder = sqlStatementContext instanceof InsertStatementContext
                // 多个子插入语句
                ? new GroupedParameterBuilder(((InsertStatementContext) sqlStatementContext).getGroupedParameters()) : new StandardParameterBuilder(parameters);
    }
    
    /**
     * Add SQL token generators.
     * 
     * @param sqlTokenGenerators SQL token generators
     */
    public void addSQLTokenGenerators(final Collection<SQLTokenGenerator> sqlTokenGenerators) {
        this.sqlTokenGenerators.addAll(sqlTokenGenerators);
    }
    
    /**
     * Generate SQL tokens.
     */
    public void generateSQLTokens() {
        // 通过 token 生成器 将结果保存到容器中
        sqlTokens.addAll(sqlTokenGenerators.generateSQLTokens(sqlStatementContext, parameters, relationMetas));
    }
}
