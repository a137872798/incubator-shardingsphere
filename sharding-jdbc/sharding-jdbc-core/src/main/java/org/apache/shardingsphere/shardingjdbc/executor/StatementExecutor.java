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

package org.apache.shardingsphere.shardingjdbc.executor;

import org.apache.shardingsphere.sharding.execute.context.ShardingExecutionContext;
import org.apache.shardingsphere.sharding.execute.sql.StatementExecuteUnit;
import org.apache.shardingsphere.sharding.execute.sql.execute.SQLExecuteCallback;
import org.apache.shardingsphere.sharding.execute.sql.execute.result.MemoryQueryResult;
import org.apache.shardingsphere.sharding.execute.sql.execute.result.StreamQueryResult;
import org.apache.shardingsphere.sharding.execute.sql.execute.threadlocal.ExecutorExceptionHandler;
import org.apache.shardingsphere.sharding.execute.sql.prepare.SQLExecutePrepareCallback;
import org.apache.shardingsphere.shardingjdbc.jdbc.core.connection.ShardingConnection;
import org.apache.shardingsphere.underlying.executor.QueryResult;
import org.apache.shardingsphere.underlying.executor.constant.ConnectionMode;
import org.apache.shardingsphere.underlying.executor.engine.InputGroup;
import org.apache.shardingsphere.underlying.executor.context.ExecutionUnit;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.List;

/**
 * Statement executor.
 * 该对象用于执行会话
 */
public final class StatementExecutor extends AbstractStatementExecutor {
    
    public StatementExecutor(final int resultSetType, final int resultSetConcurrency, final int resultSetHoldability, final ShardingConnection shardingConnection) {
        super(resultSetType, resultSetConcurrency, resultSetHoldability, shardingConnection);
    }
    
    /**
     * Initialize executor.
     *
     * @param shardingExecutionContext sharding execution context  该上下文对象内部维护了所有的会话单元
     * @throws SQLException SQL exception
     * 使用一个上下文对象来初始化
     */
    public void init(final ShardingExecutionContext shardingExecutionContext) throws SQLException {
        setSqlStatementContext(shardingExecutionContext.getSqlStatementContext());
        getInputGroups().addAll(obtainExecuteGroups(shardingExecutionContext.getExecutionUnits()));
        // 参数和会话对象有必要保留么 如果下次执行不一样的语句这些都废弃了
        cacheStatements();
    }

    /**
     * 这里将结果分组 使得更好的利用多个 connection
     * @param executionUnits
     * @return
     * @throws SQLException
     */
    private Collection<InputGroup<StatementExecuteUnit>> obtainExecuteGroups(final Collection<ExecutionUnit> executionUnits) throws SQLException {
        // 将一组普通执行单元包装成会话执行单元
        return getSqlExecutePrepareTemplate().getExecuteUnitGroups(executionUnits, new SQLExecutePrepareCallback() {

            /**
             * 根据数据源以及想要的连接数量 获取对应连接
             * @param connectionMode connection mode   连接模式    如果maxConnectionsSizePerQuery < sqlUnits.size() 就代表连接限制模式 也就是连接数容易达到瓶颈
             * @param dataSourceName data source name
             * @param connectionSize connection size   要使用多少条连接
             * @return
             * @throws SQLException
             */
            @Override
            public List<Connection> getConnections(final ConnectionMode connectionMode, final String dataSourceName, final int connectionSize) throws SQLException {
                return StatementExecutor.super.getConnection().getConnections(connectionMode, dataSourceName, connectionSize);
            }

            /**
             * 将 ExecutionUnit 包装成 StatementExecuteUnit
             * @param connection connection
             * @param executionUnit execution unit
             * @param connectionMode connection mode
             * @return
             * @throws SQLException
             */
            @SuppressWarnings("MagicConstant")
            @Override
            public StatementExecuteUnit createStatementExecuteUnit(final Connection connection, final ExecutionUnit executionUnit, final ConnectionMode connectionMode) throws SQLException {
                return new StatementExecuteUnit(executionUnit, connection.createStatement(getResultSetType(), getResultSetConcurrency(), getResultSetHoldability()), connectionMode);
            }
        });
    }
    
    /**
     * Execute query.
     * 
     * @return result set list
     * @throws SQLException SQL exception
     * 执行会话并返回一组查询结果
     */
    public List<QueryResult> executeQuery() throws SQLException {
        final boolean isExceptionThrown = ExecutorExceptionHandler.isExceptionThrown();
        SQLExecuteCallback<QueryResult> executeCallback = new SQLExecuteCallback<QueryResult>(getDatabaseType(), isExceptionThrown) {
            
            @Override
            protected QueryResult executeSQL(final String sql, final Statement statement, final ConnectionMode connectionMode) throws SQLException {
                return getQueryResult(sql, statement, connectionMode);
            }
        };
        return executeCallback(executeCallback);
    }

    /**
     * 执行sql 并返回结果
     * @param sql
     * @param statement
     * @param connectionMode
     * @return
     * @throws SQLException
     */
    private QueryResult getQueryResult(final String sql, final Statement statement, final ConnectionMode connectionMode) throws SQLException {
        ResultSet resultSet = statement.executeQuery(sql);
        // 首先结果会存在于父对象的 ResultSets
        getResultSets().add(resultSet);
        // 如果执行会话时使用的connection 比较多 就是连接数限制 那么查询很可能会因为获取不到连接数而被当下 这时必然内存中不会有太多数据 可以考虑将数据全部加载到内存
        // 如果连接数不会限制 那么还选择将数据全部加载到内存就会引发oom 所以必须按需获取
        return ConnectionMode.MEMORY_STRICTLY == connectionMode ? new StreamQueryResult(resultSet) : new MemoryQueryResult(resultSet);
    }
    
    /**
     * Execute update.
     * 
     * @return effected records count
     * @throws SQLException SQL exception
     * 执行更新动作
     */
    public int executeUpdate() throws SQLException {
        return executeUpdate(Statement::executeUpdate);
    }
    
    /**
     * Execute update with auto generated keys.
     * 
     * @param autoGeneratedKeys auto generated keys' flag  代表本次是否要自动生成主键
     * @return effected records count
     * @throws SQLException SQL exception
     */
    public int executeUpdate(final int autoGeneratedKeys) throws SQLException {
        return executeUpdate((statement, sql) -> statement.executeUpdate(sql, autoGeneratedKeys));
    }
    
    /**
     * Execute update with column indexes.
     *
     * @param columnIndexes column indexes
     * @return effected records count
     * @throws SQLException SQL exception
     */
    public int executeUpdate(final int[] columnIndexes) throws SQLException {
        return executeUpdate((statement, sql) -> statement.executeUpdate(sql, columnIndexes));
    }
    
    /**
     * Execute update with column names.
     *
     * @param columnNames column names
     * @return effected records count
     * @throws SQLException SQL exception
     */
    public int executeUpdate(final String[] columnNames) throws SQLException {
        return executeUpdate((statement, sql) -> statement.executeUpdate(sql, columnNames));
    }

    /**
     * 执行更新动作
     * @param updater
     * @return
     * @throws SQLException
     */
    private int executeUpdate(final Updater updater) throws SQLException {
        final boolean isExceptionThrown = ExecutorExceptionHandler.isExceptionThrown();
        SQLExecuteCallback<Integer> executeCallback = new SQLExecuteCallback<Integer>(getDatabaseType(), isExceptionThrown) {
            
            @Override
            protected Integer executeSQL(final String sql, final Statement statement, final ConnectionMode connectionMode) throws SQLException {
                return updater.executeUpdate(statement, sql);
            }
        };
        // 使用 executorEngine 执行回调
        List<Integer> results = executeCallback(executeCallback);
        // 累加每个分表更新的结果数
        if (isAccumulate()) {
            return accumulate(results);
        } else {
            return null == results.get(0) ? 0 : results.get(0);
        }
    }
    
    private int accumulate(final List<Integer> results) {
        int result = 0;
        for (Integer each : results) {
            result += null == each ? 0 : each;
        }
        return result;
    }
    
    /**
     * Execute SQL.
     *
     * @return return true if is DQL, false if is DML
     * @throws SQLException SQL exception
     */
    public boolean execute() throws SQLException {
        return execute(Statement::execute);
    }
    
    /**
     * Execute SQL with auto generated keys.
     *
     * @param autoGeneratedKeys auto generated keys' flag
     * @return return true if is DQL, false if is DML
     * @throws SQLException SQL exception
     */
    public boolean execute(final int autoGeneratedKeys) throws SQLException {
        return execute((statement, sql) -> statement.execute(sql, autoGeneratedKeys));
    }
    
    /**
     * Execute SQL with column indexes.
     *
     * @param columnIndexes column indexes
     * @return return true if is DQL, false if is DML
     * @throws SQLException SQL exception
     */
    public boolean execute(final int[] columnIndexes) throws SQLException {
        return execute((statement, sql) -> statement.execute(sql, columnIndexes));
    }
    
    /**
     * Execute SQL with column names.
     *
     * @param columnNames column names
     * @return return true if is DQL, false if is DML
     * @throws SQLException SQL exception
     */
    public boolean execute(final String[] columnNames) throws SQLException {
        return execute((statement, sql) -> statement.execute(sql, columnNames));
    }
    
    private boolean execute(final Executor executor) throws SQLException {
        final boolean isExceptionThrown = ExecutorExceptionHandler.isExceptionThrown();
        SQLExecuteCallback<Boolean> executeCallback = new SQLExecuteCallback<Boolean>(getDatabaseType(), isExceptionThrown) {
            
            @Override
            protected Boolean executeSQL(final String sql, final Statement statement, final ConnectionMode connectionMode) throws SQLException {
                return executor.execute(statement, sql);
            }
        };
        List<Boolean> result = executeCallback(executeCallback);
        if (null == result || result.isEmpty() || null == result.get(0)) {
            return false;
        }
        return result.get(0);
    }
    
    private interface Updater {
        
        int executeUpdate(Statement statement, String sql) throws SQLException;
    }
    
    private interface Executor {
        
        boolean execute(Statement statement, String sql) throws SQLException;
    }
}

