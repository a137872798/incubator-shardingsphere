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

import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import org.apache.shardingsphere.core.rule.ShardingRule;
import org.apache.shardingsphere.encrypt.metadata.decorator.EncryptTableMetaDataDecorator;
import org.apache.shardingsphere.sharding.execute.metadata.loader.ShardingTableMetaDataLoader;
import org.apache.shardingsphere.sharding.execute.sql.StatementExecuteUnit;
import org.apache.shardingsphere.sharding.execute.sql.execute.SQLExecuteCallback;
import org.apache.shardingsphere.sharding.execute.sql.execute.SQLExecuteTemplate;
import org.apache.shardingsphere.sharding.execute.sql.prepare.SQLExecutePrepareTemplate;
import org.apache.shardingsphere.shardingjdbc.jdbc.core.connection.ShardingConnection;
import org.apache.shardingsphere.shardingjdbc.jdbc.core.context.ShardingRuntimeContext;
import org.apache.shardingsphere.shardingjdbc.jdbc.metadata.JDBCDataSourceMapConnectionManager;
import org.apache.shardingsphere.spi.database.type.DatabaseType;
import org.apache.shardingsphere.sql.parser.relation.statement.SQLStatementContext;
import org.apache.shardingsphere.sql.parser.relation.statement.ddl.AlterTableStatementContext;
import org.apache.shardingsphere.sql.parser.relation.statement.ddl.CreateIndexStatementContext;
import org.apache.shardingsphere.sql.parser.relation.statement.ddl.CreateTableStatementContext;
import org.apache.shardingsphere.sql.parser.relation.statement.ddl.DropIndexStatementContext;
import org.apache.shardingsphere.sql.parser.relation.statement.ddl.DropTableStatementContext;
import org.apache.shardingsphere.sql.parser.sql.segment.ddl.index.IndexSegment;
import org.apache.shardingsphere.sql.parser.sql.segment.generic.table.SimpleTableSegment;
import org.apache.shardingsphere.sql.parser.sql.statement.ddl.AlterTableStatement;
import org.apache.shardingsphere.sql.parser.sql.statement.ddl.CreateIndexStatement;
import org.apache.shardingsphere.sql.parser.sql.statement.ddl.CreateTableStatement;
import org.apache.shardingsphere.sql.parser.sql.statement.ddl.DropIndexStatement;
import org.apache.shardingsphere.sql.parser.sql.statement.ddl.DropTableStatement;
import org.apache.shardingsphere.underlying.common.constant.properties.PropertiesConstant;
import org.apache.shardingsphere.underlying.common.constant.properties.ShardingSphereProperties;
import org.apache.shardingsphere.underlying.common.metadata.table.TableMetaData;
import org.apache.shardingsphere.underlying.common.metadata.table.TableMetas;
import org.apache.shardingsphere.underlying.common.metadata.table.init.TableMetaDataInitializer;
import org.apache.shardingsphere.underlying.common.metadata.table.init.TableMetaDataInitializerEntry;
import org.apache.shardingsphere.underlying.common.rule.BaseRule;
import org.apache.shardingsphere.underlying.executor.engine.ExecutorEngine;
import org.apache.shardingsphere.underlying.executor.engine.InputGroup;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

/**
 * Abstract statement executor.
 * 会话执行器
 */
@Getter(AccessLevel.PROTECTED)
public abstract class AbstractStatementExecutor {

    /**
     * 该会话发往的数据库类型 (比如mysql)
     */
    private final DatabaseType databaseType;
    
    @Getter
    private final int resultSetType;
    
    @Getter
    private final int resultSetConcurrency;
    
    @Getter
    private final int resultSetHoldability;

    /**
     * 该连接对象内部实际维护了一组连接
     */
    private final ShardingConnection connection;

    /**
     * 该对象可以为sql 分组 (一定数量的sql 由一个connection 来处理)
     */
    private final SQLExecutePrepareTemplate sqlExecutePrepareTemplate;

    /**
     * 使用 ExecutorEngine 来执行statement 并行的方式提高效率
     */
    private final SQLExecuteTemplate sqlExecuteTemplate;
    
    private final Collection<Connection> connections = new LinkedList<>();

    /**
     * 本次执行会话相关的上下文  即使分表 它们执行的语句类型是一致的
     */
    @Getter
    @Setter
    private SQLStatementContext sqlStatementContext;
    
    @Getter
    private final List<List<Object>> parameterSets = new LinkedList<>();

    /**
     * 执行某次sql 发生分表并生成的多个会话
     */
    @Getter
    private final List<Statement> statements = new LinkedList<>();

    /**
     * 执行后的结果集
     */
    @Getter
    private final List<ResultSet> resultSets = new CopyOnWriteArrayList<>();

    /**
     * 内部每个对象都代表一组会话 并且使用同一个连接
     */
    private final Collection<InputGroup<StatementExecuteUnit>> inputGroups = new LinkedList<>();

    /**
     * 通过 Connection 创建 statement 或者 prepareStatement 时  就会生成一个对应的 执行器对象
     * @param resultSetType
     * @param resultSetConcurrency
     * @param resultSetHoldability
     * @param shardingConnection
     */
    public AbstractStatementExecutor(final int resultSetType, final int resultSetConcurrency, final int resultSetHoldability, final ShardingConnection shardingConnection) {
        this.databaseType = shardingConnection.getRuntimeContext().getDatabaseType();
        this.resultSetType = resultSetType;
        this.resultSetConcurrency = resultSetConcurrency;
        this.resultSetHoldability = resultSetHoldability;
        this.connection = shardingConnection;
        // 尽可能通过多个connection 来提高性能  那么netty的连接池很可能也是这个思路 在达到一定的连接数前
        // 那么连接数肯定是越多越好
        int maxConnectionsSizePerQuery = connection.getRuntimeContext().getProperties().<Integer>getValue(PropertiesConstant.MAX_CONNECTIONS_SIZE_PER_QUERY);
        // 执行引擎 就是线程池
        ExecutorEngine executorEngine = connection.getRuntimeContext().getExecutorEngine();
        // 预备模板
        sqlExecutePrepareTemplate = new SQLExecutePrepareTemplate(maxConnectionsSizePerQuery);
        // 执行模板
        sqlExecuteTemplate = new SQLExecuteTemplate(executorEngine, connection.isHoldTransaction());
    }

    /**
     * 缓存会话对象
     */
    protected final void cacheStatements() {
        // InputGroup 中每个元素 又是一个List 代表每个连接要处理的sql
        for (InputGroup<StatementExecuteUnit> each : inputGroups) {
            // 获取会话对象并保存
            statements.addAll(each.getInputs().stream().map(StatementExecuteUnit::getStatement).collect(Collectors.toList()));
            parameterSets.addAll(each.getInputs().stream().map(input -> input.getExecutionUnit().getSqlUnit().getParameters()).collect(Collectors.toList()));
        }
    }
    
    /**
     * To make sure SkyWalking will be available at the next release of ShardingSphere,
     * a new plugin should be provided to SkyWalking project if this API changed.
     * 
     * @see <a href="https://github.com/apache/skywalking/blob/master/docs/en/guides/Java-Plugin-Development-Guide.md#user-content-plugin-development-guide">Plugin Development Guide</a>
     * 
     * @param executeCallback execute callback
     * @param <T> class type of return value 
     * @return result
     * @throws SQLException SQL exception
     * 使用指定回调处理会话 并生成结果集
     */
    @SuppressWarnings("unchecked")
    protected final <T> List<T> executeCallback(final SQLExecuteCallback<T> executeCallback) throws SQLException {
        List<T> result = sqlExecuteTemplate.execute((Collection) inputGroups, executeCallback);
        refreshMetaDataIfNeeded(connection.getRuntimeContext(), sqlStatementContext);
        return result;
    }
    
    /**
     * is accumulate.
     * 非广播模式下就是 累加
     * @return accumulate or not
     */
    public final boolean isAccumulate() {
        return !connection.getRuntimeContext().getRule().isAllBroadcastTables(sqlStatementContext.getTablesContext().getTableNames());
    }
    
    /**
     * Clear data.
     * 每当使用statement 执行一个新的语句就会清除内部残留的数据
     * @throws SQLException SQL exception
     */
    public void clear() throws SQLException {
        clearStatements();
        statements.clear();
        parameterSets.clear();
        connections.clear();
        resultSets.clear();
        inputGroups.clear();
    }

    /**
     * 每次执行一个新的sql 前都要将之前开启的多个会话关闭  因为执行sql时触发了分表 所以生成了多个 statement
     * @throws SQLException
     */
    private void clearStatements() throws SQLException {
        for (Statement each : getStatements()) {
            each.close();
        }
    }

    /**
     * 判断是否需要更新元数据  当执行的会话是修改表相关的那么就需要重新拉取该表的元数据信息了
     * @param runtimeContext
     * @param sqlStatementContext
     * @throws SQLException
     */
    private void refreshMetaDataIfNeeded(final ShardingRuntimeContext runtimeContext, final SQLStatementContext sqlStatementContext) throws SQLException {
        if (null == sqlStatementContext) {
            return;
        }
        if (sqlStatementContext instanceof CreateTableStatementContext) {
            refreshTableMetaData(runtimeContext, ((CreateTableStatementContext) sqlStatementContext).getSqlStatement());
        } else if (sqlStatementContext instanceof AlterTableStatementContext) {
            refreshTableMetaData(runtimeContext, ((AlterTableStatementContext) sqlStatementContext).getSqlStatement());
        } else if (sqlStatementContext instanceof DropTableStatementContext) {
            refreshTableMetaData(runtimeContext, ((DropTableStatementContext) sqlStatementContext).getSqlStatement());
        } else if (sqlStatementContext instanceof CreateIndexStatementContext) {
            refreshTableMetaData(runtimeContext, ((CreateIndexStatementContext) sqlStatementContext).getSqlStatement());
        } else if (sqlStatementContext instanceof DropIndexStatementContext) {
            refreshTableMetaData(runtimeContext, ((DropIndexStatementContext) sqlStatementContext).getSqlStatement());
        }
    }
    
    private void refreshTableMetaData(final ShardingRuntimeContext runtimeContext, final CreateTableStatement createTableStatement) throws SQLException {
        String tableName = createTableStatement.getTable().getTableName().getIdentifier().getValue();
        runtimeContext.getMetaData().getTables().put(tableName, createTableMetaDataInitializerEntry().init(tableName));
    }
    
    private void refreshTableMetaData(final ShardingRuntimeContext runtimeContext, final AlterTableStatement alterTableStatement) throws SQLException {
        String tableName = alterTableStatement.getTable().getTableName().getIdentifier().getValue();
        runtimeContext.getMetaData().getTables().put(tableName, createTableMetaDataInitializerEntry().init(tableName));
    }
    
    private void refreshTableMetaData(final ShardingRuntimeContext runtimeContext, final DropTableStatement dropTableStatement) {
        for (SimpleTableSegment each : dropTableStatement.getTables()) {
            runtimeContext.getMetaData().getTables().remove(each.getTableName().getIdentifier().getValue());
        }
    }
    
    private void refreshTableMetaData(final ShardingRuntimeContext runtimeContext, final CreateIndexStatement createIndexStatement) {
        if (null == createIndexStatement.getIndex()) {
            return;
        }
        runtimeContext.getMetaData().getTables().get(
                createIndexStatement.getTable().getTableName().getIdentifier().getValue()).getIndexes().add(createIndexStatement.getIndex().getIdentifier().getValue());
    }
    
    private void refreshTableMetaData(final ShardingRuntimeContext runtimeContext, final DropIndexStatement dropIndexStatement) {
        Collection<String> indexNames = getIndexNames(dropIndexStatement);
        TableMetaData tableMetaData = runtimeContext.getMetaData().getTables().get(dropIndexStatement.getTable().getTableName().getIdentifier().getValue());
        if (null != dropIndexStatement.getTable()) {
            tableMetaData.getIndexes().removeAll(indexNames);
        }
        for (String each : indexNames) {
            if (findLogicTableName(runtimeContext.getMetaData().getTables(), each).isPresent()) {
                tableMetaData.getIndexes().remove(each);
            }
        }
    }
    
    private Collection<String> getIndexNames(final DropIndexStatement dropIndexStatement) {
        Collection<String> result = new LinkedList<>();
        for (IndexSegment each : dropIndexStatement.getIndexes()) {
            result.add(each.getIdentifier().getValue());
        }
        return result;
    }
    
    private Optional<String> findLogicTableName(final TableMetas tableMetas, final String logicIndexName) {
        for (String each : tableMetas.getAllTableNames()) {
            if (tableMetas.get(each).containsIndex(logicIndexName)) {
                return Optional.of(each);
            }
        }
        return Optional.empty();
    }
    
    private TableMetaDataInitializerEntry createTableMetaDataInitializerEntry() {
        ShardingRule shardingRule = connection.getRuntimeContext().getRule();
        ShardingSphereProperties properties = connection.getRuntimeContext().getProperties();
        Map<BaseRule, TableMetaDataInitializer> tableMetaDataInitializes = new HashMap<>(2, 1);
        tableMetaDataInitializes.put(shardingRule, new ShardingTableMetaDataLoader(connection.getRuntimeContext().getMetaData().getDataSources(), connection.getRuntimeContext().getExecutorEngine(), 
                new JDBCDataSourceMapConnectionManager(connection.getDataSourceMap()),
                properties.<Integer>getValue(PropertiesConstant.MAX_CONNECTIONS_SIZE_PER_QUERY), properties.<Boolean>getValue(PropertiesConstant.CHECK_TABLE_METADATA_ENABLED)));
        if (!shardingRule.getEncryptRule().getEncryptTableNames().isEmpty()) {
            tableMetaDataInitializes.put(shardingRule.getEncryptRule(), new EncryptTableMetaDataDecorator());
        }
        return new TableMetaDataInitializerEntry(tableMetaDataInitializes);
    }
}
