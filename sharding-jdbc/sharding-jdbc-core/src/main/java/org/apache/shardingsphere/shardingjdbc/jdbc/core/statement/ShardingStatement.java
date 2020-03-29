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

package org.apache.shardingsphere.shardingjdbc.jdbc.core.statement;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import org.apache.shardingsphere.core.shard.BaseShardingEngine;
import org.apache.shardingsphere.core.shard.SimpleQueryShardingEngine;
import org.apache.shardingsphere.encrypt.rule.EncryptRule;
import org.apache.shardingsphere.sharding.execute.context.ShardingExecutionContext;
import org.apache.shardingsphere.sharding.execute.sql.execute.result.StreamQueryResult;
import org.apache.shardingsphere.sharding.merge.ShardingResultMergerEngine;
import org.apache.shardingsphere.sharding.route.engine.keygen.GeneratedKey;
import org.apache.shardingsphere.shardingjdbc.executor.StatementExecutor;
import org.apache.shardingsphere.shardingjdbc.jdbc.adapter.AbstractStatementAdapter;
import org.apache.shardingsphere.shardingjdbc.jdbc.core.connection.ShardingConnection;
import org.apache.shardingsphere.shardingjdbc.jdbc.core.constant.SQLExceptionConstant;
import org.apache.shardingsphere.shardingjdbc.jdbc.core.context.ShardingRuntimeContext;
import org.apache.shardingsphere.shardingjdbc.jdbc.core.resultset.GeneratedKeysResultSet;
import org.apache.shardingsphere.shardingjdbc.jdbc.core.resultset.ShardingResultSet;
import org.apache.shardingsphere.shardingjdbc.merge.JDBCEncryptResultDecoratorEngine;
import org.apache.shardingsphere.sql.parser.relation.statement.dml.InsertStatementContext;
import org.apache.shardingsphere.sql.parser.relation.statement.dml.SelectStatementContext;
import org.apache.shardingsphere.sql.parser.sql.statement.dal.DALStatement;
import org.apache.shardingsphere.underlying.common.rule.BaseRule;
import org.apache.shardingsphere.underlying.executor.QueryResult;
import org.apache.shardingsphere.underlying.merge.MergeEntry;
import org.apache.shardingsphere.underlying.merge.engine.ResultProcessEngine;
import org.apache.shardingsphere.underlying.merge.result.MergedResult;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Statement that support sharding.
 * shardingSphere级别的会话 内部包含了 如果处理数据源 解析sql 等动作
 * 通过适配 jdbc的Statement 对外保持一致
 */
public final class ShardingStatement extends AbstractStatementAdapter {

    /**
     * 该连接对象是一个包装对象 内部包含了一组 dataSource
     */
    @Getter
    private final ShardingConnection connection;

    /**
     * 会话执行器  内部包含了 怎么分配connection 来执行不同的statement 以及生成resultSet
     */
    private final StatementExecutor statementExecutor;

    /**
     * 本次结果是否要携带自动生成的主键
     */
    private boolean returnGeneratedKeys;
    
    private ShardingExecutionContext shardingExecutionContext;

    /**
     * 本次执行结果集
     */
    private ResultSet currentResultSet;
    
    public ShardingStatement(final ShardingConnection connection) {
        // TYPE_FORWARD_ONLY 代表结果集只能向下滚动   CONCUR_READ_ONLY  代表不能用结果集更新数据库
        this(connection, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }
    
    public ShardingStatement(final ShardingConnection connection, final int resultSetType, final int resultSetConcurrency) {
        // HOLD_CURSORS_OVER_COMMIT  表示不关闭结果集
        // 默认情况下执行完一个 statement 后执行下一个 那么上一个的resultSet 就会关闭,调用Connection.commit时 也会关闭
        this(connection, resultSetType, resultSetConcurrency, ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }
    
    public ShardingStatement(final ShardingConnection connection, final int resultSetType, final int resultSetConcurrency, final int resultSetHoldability) {
        super(Statement.class);
        this.connection = connection;
        // 执行会话的对象
        statementExecutor = new StatementExecutor(resultSetType, resultSetConcurrency, resultSetHoldability, connection);
    }

    /**
     * 根据 sql 语句生成查询结果集
     * @param sql
     * @return
     * @throws SQLException
     */
    @Override
    public ResultSet executeQuery(final String sql) throws SQLException {
        if (Strings.isNullOrEmpty(sql)) {
            throw new SQLException(SQLExceptionConstant.SQL_STRING_NULL_OR_EMPTY);
        }
        ResultSet result;
        try {
            clearPrevious();
            // 拆解sql 实现分表功能
            shard(sql);
            // 根据datasource 为connection 进行分组
            initStatementExecutor();
            // 执行sql 并包装查询结果 这里会合并返回的结果
            result = getResultSet(statementExecutor.executeQuery());
        } finally {
            currentResultSet = null;
        }
        currentResultSet = result;
        return result;
    }
    
    @Override
    public ResultSet getResultSet() throws SQLException {
        if (null != currentResultSet) {
            return currentResultSet;
        }
        List<ResultSet> resultSets = new ArrayList<>(statementExecutor.getStatements().size());
        List<QueryResult> queryResults = new ArrayList<>(statementExecutor.getStatements().size());
        for (Statement each : statementExecutor.getStatements()) {
            ResultSet resultSet = each.getResultSet();
            resultSets.add(resultSet);
            if (resultSet != null) {
                queryResults.add(new StreamQueryResult(resultSet));
            }
        }
        if (shardingExecutionContext.getSqlStatementContext() instanceof SelectStatementContext || shardingExecutionContext.getSqlStatementContext().getSqlStatement() instanceof DALStatement) {
            currentResultSet = new ShardingResultSet(resultSets, createMergedResult(resultSets, queryResults), this, shardingExecutionContext);
        }
        return currentResultSet;
    }

    /**
     * 将一组查询结果包装成 shardingResultSet  不考虑 group by  order  by 那么就只是把多个迭代器整合在一起
     * @param queryResults
     * @return
     * @throws SQLException
     */
    private ShardingResultSet getResultSet(final List<QueryResult> queryResults) throws SQLException {
        List<ResultSet> resultSets = statementExecutor.getResultSets();
        // 返回的结果还需要进行合并
        return new ShardingResultSet(resultSets, createMergedResult(resultSets, queryResults), this, shardingExecutionContext);
    }

    /**
     *
     * @param resultSets   jdbc返回的普通结果集
     * @param queryResults  根据限制模式 (比如内存限制 或者连接限制 生成的包装对象)
     * @return
     * @throws SQLException
     */
    private MergedResult createMergedResult(final List<ResultSet> resultSets, final List<QueryResult> queryResults) throws SQLException {
        Map<BaseRule, ResultProcessEngine> engines = new HashMap<>(2, 1);
        engines.put(connection.getRuntimeContext().getRule(), new ShardingResultMergerEngine());
        // 加密的先忽略
        EncryptRule encryptRule = connection.getRuntimeContext().getRule().getEncryptRule();
        if (!encryptRule.getEncryptTableNames().isEmpty()) {
            engines.put(encryptRule, new JDBCEncryptResultDecoratorEngine(resultSets.get(0).getMetaData()));
        }
        MergeEntry mergeEntry = new MergeEntry(connection.getRuntimeContext().getDatabaseType(), 
                connection.getRuntimeContext().getMetaData().getRelationMetas(), connection.getRuntimeContext().getProperties(), engines);
        return mergeEntry.process(queryResults, shardingExecutionContext.getSqlStatementContext());
    }
    
    @Override
    public int executeUpdate(final String sql) throws SQLException {
        try {
            clearPrevious();
            shard(sql);
            initStatementExecutor();
            return statementExecutor.executeUpdate();
        } finally {
            currentResultSet = null;
        }
    }
    
    @Override
    public int executeUpdate(final String sql, final int autoGeneratedKeys) throws SQLException {
        if (RETURN_GENERATED_KEYS == autoGeneratedKeys) {
            returnGeneratedKeys = true;
        }
        try {
            clearPrevious();
            shard(sql);
            initStatementExecutor();
            return statementExecutor.executeUpdate(autoGeneratedKeys);
        } finally {
            currentResultSet = null;
        }
    }
    
    @Override
    public int executeUpdate(final String sql, final int[] columnIndexes) throws SQLException {
        returnGeneratedKeys = true;
        try {
            clearPrevious();
            shard(sql);
            initStatementExecutor();
            return statementExecutor.executeUpdate(columnIndexes);
        } finally {
            currentResultSet = null;
        }
    }
    
    @Override
    public int executeUpdate(final String sql, final String[] columnNames) throws SQLException {
        returnGeneratedKeys = true;
        try {
            clearPrevious();
            shard(sql);
            initStatementExecutor();
            return statementExecutor.executeUpdate(columnNames);
        } finally {
            currentResultSet = null;
        }
    }
    
    @Override
    public boolean execute(final String sql) throws SQLException {
        try {
            clearPrevious();
            shard(sql);
            initStatementExecutor();
            return statementExecutor.execute();
        } finally {
            currentResultSet = null;
        }
    }
    
    @Override
    public boolean execute(final String sql, final int autoGeneratedKeys) throws SQLException {
        if (RETURN_GENERATED_KEYS == autoGeneratedKeys) {
            returnGeneratedKeys = true;
        }
        try {
            clearPrevious();
            shard(sql);
            initStatementExecutor();
            return statementExecutor.execute(autoGeneratedKeys);
        } finally {
            currentResultSet = null;
        }
    }
    
    @Override
    public boolean execute(final String sql, final int[] columnIndexes) throws SQLException {
        returnGeneratedKeys = true;
        try {
            clearPrevious();
            shard(sql);
            initStatementExecutor();
            return statementExecutor.execute(columnIndexes);
        } finally {
            currentResultSet = null;
        }
    }
    
    @Override
    public boolean execute(final String sql, final String[] columnNames) throws SQLException {
        returnGeneratedKeys = true;
        try {
            clearPrevious();
            shard(sql);
            // 此时shardingExecutionContext 内部已经存放了解析好的sql 这里开始初始化 statement
            initStatementExecutor();
            // 因为该对象此时内部已经维护了 会话对象了 所以只要传入列名就可以执行
            return statementExecutor.execute(columnNames);
        } finally {
            currentResultSet = null;
        }
    }

    /**
     * 初始化 执行引擎 之前已经确认好本次如何路由
     * @throws SQLException
     */
    private void initStatementExecutor() throws SQLException {
        // 将 ExecutionUnit 分组 并且创建会话对象
        statementExecutor.init(shardingExecutionContext);
        // 执行一些前置动作
        replayMethodForStatements();
    }
    
    private void replayMethodForStatements() {
        for (Statement each : statementExecutor.getStatements()) {
            replayMethodsInvocation(each);
        }
    }

    /**
     * 应该就是这里对sql 进行拆解变成多个sql
     * @param sql
     */
    private void shard(final String sql) {
        ShardingRuntimeContext runtimeContext = connection.getRuntimeContext();
        // 包装成简单的引擎对象
        BaseShardingEngine shardingEngine = new SimpleQueryShardingEngine(runtimeContext.getRule(), runtimeContext.getProperties(), runtimeContext.getMetaData(), runtimeContext.getSqlParserEngine());
        // 开始执行分表逻辑 第二个参数代表 params  并且生成的包含执行单元 (也就是实际的语句)
        shardingExecutionContext = (ShardingExecutionContext) shardingEngine.shard(sql, Collections.emptyList());
    }

    /**
     * 清除上一次的结果集 connection 等
     * @throws SQLException
     */
    private void clearPrevious() throws SQLException {
        statementExecutor.clear();
    }
    
    @SuppressWarnings("MagicConstant")
    @Override
    public int getResultSetType() {
        return statementExecutor.getResultSetType();
    }
    
    @SuppressWarnings("MagicConstant")
    @Override
    public int getResultSetConcurrency() {
        return statementExecutor.getResultSetConcurrency();
    }
    
    @Override
    public int getResultSetHoldability() {
        return statementExecutor.getResultSetHoldability();
    }
    
    @Override
    public boolean isAccumulate() {
        return !connection.getRuntimeContext().getRule().isAllBroadcastTables(shardingExecutionContext.getSqlStatementContext().getTablesContext().getTableNames());
    }
    
    @Override
    public Collection<Statement> getRoutedStatements() {
        return statementExecutor.getStatements();
    }
    
    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        Optional<GeneratedKey> generatedKey = getGeneratedKey();
        if (returnGeneratedKeys && generatedKey.isPresent()) {
            Preconditions.checkState(shardingExecutionContext.getGeneratedKey().isPresent());
            return new GeneratedKeysResultSet(shardingExecutionContext.getGeneratedKey().get().getGeneratedValues().iterator(), generatedKey.get().getColumnName(), this);
        }
        if (1 == getRoutedStatements().size()) {
            return getRoutedStatements().iterator().next().getGeneratedKeys();
        }
        return new GeneratedKeysResultSet();
    }
    
    private Optional<GeneratedKey> getGeneratedKey() {
        return null != shardingExecutionContext && shardingExecutionContext.getSqlStatementContext() instanceof InsertStatementContext
                ? shardingExecutionContext.getGeneratedKey() : Optional.empty();
    }
}
