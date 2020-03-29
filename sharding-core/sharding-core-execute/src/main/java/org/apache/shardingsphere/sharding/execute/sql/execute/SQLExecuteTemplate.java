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

package org.apache.shardingsphere.sharding.execute.sql.execute;

import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.sharding.execute.sql.StatementExecuteUnit;
import org.apache.shardingsphere.sharding.execute.sql.execute.threadlocal.ExecutorExceptionHandler;
import org.apache.shardingsphere.underlying.executor.engine.ExecutorEngine;
import org.apache.shardingsphere.underlying.executor.engine.InputGroup;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * SQL execute template.
 * 执行sql 的模板对象
 */
@RequiredArgsConstructor
public final class SQLExecuteTemplate {

    /**
     * 可以简单的理解为线程池
     */
    private final ExecutorEngine executorEngine;

    /**
     * 是否串行执行  (也就是在一个线程中循环执行)   如果当前在一个事务中就不能并发执行了
     */
    private final boolean serial;
    
    /**
     * Execute.
     *
     * @param inputGroups input groups  包含一组会话执行最小单元
     * @param callback SQL execute callback  回调中包含了处理statement的逻辑
     * @param <T> class type of return value
     * @return execute result
     * @throws SQLException SQL exception
     */
    public <T> List<T> execute(final Collection<InputGroup<? extends StatementExecuteUnit>> inputGroups, final SQLExecuteCallback<T> callback) throws SQLException {
        return execute(inputGroups, null, callback);
    }
    
    /**
     * Execute.
     *
     * @param inputGroups input groups
     * @param firstCallback first SQL execute callback
     * @param callback SQL execute callback
     * @param <T> class type of return value
     * @return execute result
     * @throws SQLException SQL exception
     */
    @SuppressWarnings("unchecked")
    public <T> List<T> execute(final Collection<InputGroup<? extends StatementExecuteUnit>> inputGroups,
                               final SQLExecuteCallback<T> firstCallback, final SQLExecuteCallback<T> callback) throws SQLException {
        try {
            return executorEngine.execute((Collection) inputGroups, firstCallback, callback, serial);
        } catch (final SQLException ex) {
            ExecutorExceptionHandler.handleException(ex);
            return Collections.emptyList();
        }
    }
}
