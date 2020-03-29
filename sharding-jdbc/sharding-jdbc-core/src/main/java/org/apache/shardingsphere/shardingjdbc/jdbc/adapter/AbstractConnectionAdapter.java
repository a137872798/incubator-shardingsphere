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

package org.apache.shardingsphere.shardingjdbc.jdbc.adapter;

import com.google.common.base.Preconditions;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;
import lombok.Getter;
import org.apache.shardingsphere.masterslave.route.engine.impl.MasterVisitedManager;
import org.apache.shardingsphere.shardingjdbc.jdbc.adapter.executor.ForceExecuteTemplate;
import org.apache.shardingsphere.shardingjdbc.jdbc.unsupported.AbstractUnsupportedOperationConnection;
import org.apache.shardingsphere.transaction.core.TransactionTypeHolder;
import org.apache.shardingsphere.underlying.common.hook.RootInvokeHook;
import org.apache.shardingsphere.underlying.common.hook.SPIRootInvokeHook;
import org.apache.shardingsphere.underlying.executor.constant.ConnectionMode;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Adapter for {@code Connection}.
 * 该对象代表 原生Connection 与 shardingSphere 的包装层
 */
public abstract class AbstractConnectionAdapter extends AbstractUnsupportedOperationConnection {

    /**
     * 每个dataSource 都对应了一个 Connection
     */
    @Getter
    private final Multimap<String, Connection> cachedConnections = LinkedHashMultimap.create();
    
    @Getter
    private final ForceExecuteTemplate<Connection> forceExecuteTemplate = new ForceExecuteTemplate<>();
    
    private final ForceExecuteTemplate<Entry<String, Connection>> forceExecuteTemplateForClose = new ForceExecuteTemplate<>();

    /**
     * 执行过程中触发的钩子
     */
    private final RootInvokeHook rootInvokeHook = new SPIRootInvokeHook();

    /**
     * 默认情况下自动提交事务
     */
    private boolean autoCommit = true;

    /**
     * 是否开启只读模式
     */
    private boolean readOnly;
    
    private volatile boolean closed;
    
    private int transactionIsolation = TRANSACTION_READ_UNCOMMITTED;
    
    protected AbstractConnectionAdapter() {
        // 该对象被创建时 先启动所有钩子
        rootInvokeHook.start();
    }
    
    /**
     * Get database connection.
     *
     * @param dataSourceName data source name
     * @return database connection
     * @throws SQLException SQL exception
     * 默认情况只获取一个连接
     */
    public final Connection getConnection(final String dataSourceName) throws SQLException {
        return getConnections(ConnectionMode.MEMORY_STRICTLY, dataSourceName, 1).get(0);
    }
    
    /**
     * Get database connections.
     * 当路由完sql 后 经过改写生成了一组可执行单元 那么这里要获取对应的连接去执行任务
     * @param connectionMode connection mode
     * @param dataSourceName data source name
     * @param connectionSize size of connection list to be get
     * @return database connections
     * @throws SQLException SQL exception
     */
    public final List<Connection> getConnections(final ConnectionMode connectionMode, final String dataSourceName, final int connectionSize) throws SQLException {
        // 获取某个数据源
        DataSource dataSource = getDataSourceMap().get(dataSourceName);
        Preconditions.checkState(null != dataSource, "Missing the data source name: '%s'", dataSourceName);
        Collection<Connection> connections;
        // 如果有缓存的连接 就直接使用
        synchronized (cachedConnections) {
            connections = cachedConnections.get(dataSourceName);
        }
        List<Connection> result;
        // 代表此时数据源对应缓存的连接数 足够多  那么截取掉多余的部分
        if (connections.size() >= connectionSize) {
            result = new ArrayList<>(connections).subList(0, connectionSize);
            // 代表连接数不足 那么要继续创建连接来填满list
        } else if (!connections.isEmpty()) {
            result = new ArrayList<>(connectionSize);
            result.addAll(connections);
            List<Connection> newConnections = createConnections(dataSourceName, connectionMode, dataSource, connectionSize - connections.size());
            result.addAll(newConnections);
            // 将连接设置到缓存中
            synchronized (cachedConnections) {
                cachedConnections.putAll(dataSourceName, newConnections);
            }
        } else {
            result = new ArrayList<>(createConnections(dataSourceName, connectionMode, dataSource, connectionSize));
            synchronized (cachedConnections) {
                cachedConnections.putAll(dataSourceName, result);
            }
        }
        return result;
    }

    /**
     * 创建需要的连接数
     * @param dataSourceName
     * @param connectionMode
     * @param dataSource  从哪个数据源获取连接
     * @param connectionSize  获取多少条连接
     * @return
     * @throws SQLException
     */
    @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
    private List<Connection> createConnections(final String dataSourceName, final ConnectionMode connectionMode, final DataSource dataSource, final int connectionSize) throws SQLException {
        if (1 == connectionSize) {
            // 只需要创建一个连接  就是通过 DataSource 获取一个物理连接  (.getConnection())
            Connection connection = createConnection(dataSourceName, dataSource);
            // 调用所有已经提前保存好的方法(反射调用)
            replayMethodsInvocation(connection);
            return Collections.singletonList(connection);
        }

        // 创建多条连接
        if (ConnectionMode.CONNECTION_STRICTLY == connectionMode) {
            return createConnections(dataSourceName, dataSource, connectionSize);
        }
        // 都没有锁定数据源的  地方 啥意思
        synchronized (dataSource) {
            return createConnections(dataSourceName, dataSource, connectionSize);
        }
    }

    /**
     * 根据某个数据源 创建一组连接对象
     * @param dataSourceName
     * @param dataSource
     * @param connectionSize
     * @return
     * @throws SQLException
     */
    private List<Connection> createConnections(final String dataSourceName, final DataSource dataSource, final int connectionSize) throws SQLException {
        List<Connection> result = new ArrayList<>(connectionSize);
        for (int i = 0; i < connectionSize; i++) {
            try {
                // 挨个创建连接 并调用一些预备方法 失败时关闭所有连接并 抛出异常
                Connection connection = createConnection(dataSourceName, dataSource);
                replayMethodsInvocation(connection);
                result.add(connection);
            } catch (final SQLException ex) {
                for (Connection each : result) {
                    each.close();
                }
                throw new SQLException(String.format("Could't get %d connections one time, partition succeed connection(%d) have released!", connectionSize, result.size()), ex);
            }
        }
        return result;
    }
    
    protected abstract Connection createConnection(String dataSourceName, DataSource dataSource) throws SQLException;
    
    protected abstract Map<String, DataSource> getDataSourceMap();
    
    @Override
    public final boolean getAutoCommit() {
        return autoCommit;
    }
    
    @Override
    public void setAutoCommit(final boolean autoCommit) throws SQLException {
        this.autoCommit = autoCommit;
        setAutoCommitForLocalTransaction(autoCommit);
    }
    
    private void setAutoCommitForLocalTransaction(final boolean autoCommit) throws SQLException {
        // 新创建的连接都会自动执行该方法
        recordMethodInvocation(Connection.class, "setAutoCommit", new Class[]{boolean.class}, new Object[]{autoCommit});
        // 强制当前所有连接执行该方法
        forceExecuteTemplate.execute(cachedConnections.values(), connection -> connection.setAutoCommit(autoCommit));
    }
    
    @Override
    public void commit() throws SQLException {
        forceExecuteTemplate.execute(cachedConnections.values(), Connection::commit);
    }
    
    @Override
    public void rollback() throws SQLException {
        forceExecuteTemplate.execute(cachedConnections.values(), Connection::rollback);
    }

    /**
     * 当关闭这个总的连接对象时 (外部只能看到这个connection 实际上该对象内部有很多连接)
     * @throws SQLException
     */
    @Override
    public final void close() throws SQLException {
        closed = true;
        // 清理 2个 ThreadLocal
        MasterVisitedManager.clear();
        TransactionTypeHolder.clear();
        int connectionSize = cachedConnections.size();
        try {
            forceExecuteTemplateForClose.execute(cachedConnections.entries(), cachedConnections -> cachedConnections.getValue().close());
        } finally {
            cachedConnections.clear();
            rootInvokeHook.finish(connectionSize);
        }
    }
    
    @Override
    public final boolean isClosed() {
        return closed;
    }
    
    @Override
    public final boolean isReadOnly() {
        return readOnly;
    }
    
    @Override
    public final void setReadOnly(final boolean readOnly) throws SQLException {
        this.readOnly = readOnly;
        recordMethodInvocation(Connection.class, "setReadOnly", new Class[]{boolean.class}, new Object[]{readOnly});
        forceExecuteTemplate.execute(cachedConnections.values(), connection -> connection.setReadOnly(readOnly));
    }
    
    @Override
    public final int getTransactionIsolation() throws SQLException {
        if (cachedConnections.values().isEmpty()) {
            return transactionIsolation;
        }
        return cachedConnections.values().iterator().next().getTransactionIsolation();
    }
    
    @Override
    public final void setTransactionIsolation(final int level) throws SQLException {
        transactionIsolation = level;
        recordMethodInvocation(Connection.class, "setTransactionIsolation", new Class[]{int.class}, new Object[]{level});
        forceExecuteTemplate.execute(cachedConnections.values(), connection -> connection.setTransactionIsolation(level));
    }
    
    // ------- Consist with MySQL driver implementation -------
    
    @Override
    public final SQLWarning getWarnings() {
        return null;
    }
    
    @Override
    public void clearWarnings() {
    }
    
    @Override
    public final int getHoldability() {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }
    
    @Override
    public final void setHoldability(final int holdability) {
    }
}
