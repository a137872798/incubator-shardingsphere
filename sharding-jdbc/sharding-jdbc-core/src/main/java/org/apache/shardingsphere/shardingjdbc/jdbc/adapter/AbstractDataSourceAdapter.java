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
import lombok.Getter;
import lombok.Setter;
import org.apache.shardingsphere.underlying.common.database.type.DatabaseTypes;
import org.apache.shardingsphere.shardingjdbc.jdbc.core.context.RuntimeContext;
import org.apache.shardingsphere.shardingjdbc.jdbc.unsupported.AbstractUnsupportedOperationDataSource;
import org.apache.shardingsphere.spi.database.type.DatabaseType;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Adapter for {@code Datasource}.
 * 骨架类
 */
@Getter
public abstract class AbstractDataSourceAdapter extends AbstractUnsupportedOperationDataSource implements AutoCloseable {
    
    private final Map<String, DataSource> dataSourceMap;
    
    private final DatabaseType databaseType;
    
    @Setter
    private PrintWriter logWriter = new PrintWriter(System.out);

    /**
     * 通过一组数据源来进行初始化
     * @param dataSourceMap
     * @throws SQLException
     */
    public AbstractDataSourceAdapter(final Map<String, DataSource> dataSourceMap) throws SQLException {
        this.dataSourceMap = dataSourceMap;
        databaseType = createDatabaseType();
    }
    
    public AbstractDataSourceAdapter(final DataSource dataSource) throws SQLException {
        dataSourceMap = new HashMap<>(1, 1);
        dataSourceMap.put("unique", dataSource);
        databaseType = createDatabaseType();
    }

    /**
     * 读取数据源的类型
     * @return
     * @throws SQLException
     */
    private DatabaseType createDatabaseType() throws SQLException {
        DatabaseType result = null;
        for (DataSource each : dataSourceMap.values()) {
            DatabaseType databaseType = createDatabaseType(each);
            // 要求被配置的所有数据源 type 必须一致 否则抛出异常
            Preconditions.checkState(null == result || result == databaseType, String.format("Database type inconsistent with '%s' and '%s'", result, databaseType));
            result = databaseType;
        }
        return result;
    }
    
    private DatabaseType createDatabaseType(final DataSource dataSource) throws SQLException {
        // 如果该数据源已经被代理过了  那么直接取设置的数据源
        if (dataSource instanceof AbstractDataSourceAdapter) {
            return ((AbstractDataSourceAdapter) dataSource).databaseType;
        }
        // 否则根据 url 信息获取数据源类型  看来是按数据库来区分  DatabaseType
        // 调用 getConnection 时 会间接连接到远端 也就是失败会直接抛出异常
        try (Connection connection = dataSource.getConnection()) {
            // 能进入到这里 url必然是有效的
            return DatabaseTypes.getDatabaseTypeByURL(connection.getMetaData().getURL());
        }
    }
    
    @Override
    public final Logger getParentLogger() {
        return Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
    }
    
    @Override
    public final Connection getConnection(final String username, final String password) throws SQLException {
        return getConnection();
    }
    
    @Override
    public final void close() throws Exception {
        close(dataSourceMap.keySet());
    }
    
    /**
     * Close dataSources.
     * 
     * @param dataSourceNames data source names
     * @throws Exception exception
     * 关闭一组数据源
     */
    public void close(final Collection<String> dataSourceNames) throws Exception {
        for (String each : dataSourceNames) {
            close(dataSourceMap.get(each));
        }
        // 关闭运行时上下文
        getRuntimeContext().close();
    }

    /**
     * 反射调用关闭方法
     * @param dataSource
     */
    private void close(final DataSource dataSource) {
        try {
            Method method = dataSource.getClass().getDeclaredMethod("close");
            method.setAccessible(true);
            method.invoke(dataSource);
        } catch (final ReflectiveOperationException ignored) {
        }
    }
    
    protected abstract RuntimeContext getRuntimeContext();
}
