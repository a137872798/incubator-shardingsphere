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

package org.apache.shardingsphere.underlying.executor.engine;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.shardingsphere.underlying.common.exception.ShardingSphereException;
import org.apache.shardingsphere.underlying.executor.engine.impl.ShardingSphereExecutorService;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Executor engine.
 * 执行引擎 对应着线程池
 */
public final class ExecutorEngine implements AutoCloseable {

    /**
     * 该线程池就是增强了  JDK 线程池 额外增加了监听器功能
     */
    private final ShardingSphereExecutorService executorService;

    /**
     * 指定线程数量
     * @param executorSize
     */
    public ExecutorEngine(final int executorSize) {
        executorService = new ShardingSphereExecutorService(executorSize);
    }
    
    /**
     * Execute.
     *
     * @param inputGroups input groups 一组待执行任务 每个会尽可能对应一个 connection 对象
     * @param callback grouped callback  该回调对象接收一组参数 并返回一组参数
     * @param <I> type of input value
     * @param <O> type of return value
     * @return execute result
     * @throws SQLException throw if execute failure
     */
    public <I, O> List<O> execute(final Collection<InputGroup<I>> inputGroups, final GroupedCallback<I, O> callback) throws SQLException {
        return execute(inputGroups, null, callback, false);
    }
    
    /**
     * Execute.
     *
     * @param inputGroups input groups
     * @param firstCallback first grouped callback
     * @param callback other grouped callback
     * @param serial whether using multi thread execute or not
     * @param <I> type of input value
     * @param <O> type of return value
     * @return execute result
     * @throws SQLException throw if execute failure
     */
    public <I, O> List<O> execute(final Collection<InputGroup<I>> inputGroups, 
                                  final GroupedCallback<I, O> firstCallback, final GroupedCallback<I, O> callback, final boolean serial) throws SQLException {
        // 没有传入参数直接返回空结果
        if (inputGroups.isEmpty()) {
            return Collections.emptyList();
        }
        return serial ? serialExecute(inputGroups, firstCallback, callback) : parallelExecute(inputGroups, firstCallback, callback);
    }

    /**
     * 使用当前线程 使用每个输入数据去触发回调
     * @param inputGroups   每个InputGroup 本身就是一组数据
     * @param firstCallback
     * @param callback
     * @param <I>
     * @param <O>
     * @return
     * @throws SQLException
     */
    private <I, O> List<O> serialExecute(final Collection<InputGroup<I>> inputGroups, final GroupedCallback<I, O> firstCallback, final GroupedCallback<I, O> callback) throws SQLException {
        Iterator<InputGroup<I>> inputGroupsIterator = inputGroups.iterator();
        InputGroup<I> firstInputs = inputGroupsIterator.next();
        List<O> result = new LinkedList<>(syncExecute(firstInputs, null == firstCallback ? callback : firstCallback));
        for (InputGroup<I> each : Lists.newArrayList(inputGroupsIterator)) {
            // 在当前线程依次执行每个回调
            result.addAll(syncExecute(each, callback));
        }
        return result;
    }

    /**
     * 将任务提交给线程池来执行
     * @param inputGroups
     * @param firstCallback
     * @param callback
     * @param <I>
     * @param <O>
     * @return
     * @throws SQLException
     */
    private <I, O> List<O> parallelExecute(final Collection<InputGroup<I>> inputGroups, final GroupedCallback<I, O> firstCallback, final GroupedCallback<I, O> callback) throws SQLException {
        Iterator<InputGroup<I>> inputGroupsIterator = inputGroups.iterator();
        InputGroup<I> firstInputs = inputGroupsIterator.next();
        Collection<ListenableFuture<Collection<O>>> restResultFutures = asyncExecute(Lists.newArrayList(inputGroupsIterator), callback);
        // 在同步模式下处理第一次的结果 并与 future 列表整合结果
        return getGroupResults(syncExecute(firstInputs, null == firstCallback ? callback : firstCallback), restResultFutures);
    }

    /**
     * 这里先是在主线程中直接触发回调
     * @param inputGroup
     * @param callback
     * @param <I>
     * @param <O>
     * @return
     * @throws SQLException
     */
    private <I, O> Collection<O> syncExecute(final InputGroup<I> inputGroup, final GroupedCallback<I, O> callback) throws SQLException {
        return callback.execute(inputGroup.getInputs(), true, ExecutorDataMap.getValue());
    }

    /**
     *
     * @param inputGroups
     * @param callback
     * @param <I>
     * @param <O>
     * @return
     */
    private <I, O> Collection<ListenableFuture<Collection<O>>> asyncExecute(final List<InputGroup<I>> inputGroups, final GroupedCallback<I, O> callback) {
        Collection<ListenableFuture<Collection<O>>> result = new LinkedList<>();
        for (InputGroup<I> each : inputGroups) {
            // 异步执行任务并返回一组future对象
            result.add(asyncExecute(each, callback));
        }
        return result;
    }

    /**
     * 将任务提交给线程池
     * @param inputGroup
     * @param callback
     * @param <I>
     * @param <O>
     * @return
     */
    private <I, O> ListenableFuture<Collection<O>> asyncExecute(final InputGroup<I> inputGroup, final GroupedCallback<I, O> callback) {
        // 线程池里的线程会对这个map 并发修改
        final Map<String, Object> dataMap = ExecutorDataMap.getValue();
        return executorService.getExecutorService().submit(() -> callback.execute(inputGroup.getInputs(), false, dataMap));
    }
    
    private <O> List<O> getGroupResults(final Collection<O> firstResults, final Collection<ListenableFuture<Collection<O>>> restFutures) throws SQLException {
        List<O> result = new LinkedList<>(firstResults);
        for (ListenableFuture<Collection<O>> each : restFutures) {
            try {
                // 这里会阻塞直到得到结果
                result.addAll(each.get());
            } catch (final InterruptedException | ExecutionException ex) {
                return throwException(ex);
            }
        }
        return result;
    }
    
    private <O> List<O> throwException(final Exception exception) throws SQLException {
        if (exception.getCause() instanceof SQLException) {
            throw (SQLException) exception.getCause();
        }
        throw new ShardingSphereException(exception);
    }
    
    @Override
    public void close() {
        executorService.close();
    }
}
