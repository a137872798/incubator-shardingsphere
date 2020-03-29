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

package org.apache.shardingsphere.underlying.executor.engine.impl;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import lombok.Getter;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * ShardingSphere executor service.
 * shardingSphere 内部定义的一组线程池
 */
@Getter
public final class ShardingSphereExecutorService {
    
    private static final String DEFAULT_NAME_FORMAT = "%d";

    /**
     * 这里单线程是专门用来关闭的
     */
    private static final ExecutorService SHUTDOWN_EXECUTOR = Executors.newSingleThreadExecutor(ShardingSphereThreadFactoryBuilder.build("Executor-Engine-Closer"));

    /**
     * 该对象就是在普通线程池基础上增加了监听器   当提交任务后会返回一个 ListenFuture  为该对象添加了监听器后 完成时会触发回调
     */
    private ListeningExecutorService executorService;
    
    public ShardingSphereExecutorService(final int executorSize) {
        this(executorSize, DEFAULT_NAME_FORMAT);
    }
    
    public ShardingSphereExecutorService(final int executorSize, final String nameFormat) {
        // 监听器Executor 只是一个装饰器 内部还是JDK 的线程池
        executorService = MoreExecutors.listeningDecorator(getExecutorService(executorSize, nameFormat));
        // 为Runtime 增加终结钩子 当程序终止时 会关闭线程池
        MoreExecutors.addDelayedShutdownHook(executorService, 60, TimeUnit.SECONDS);
    }
    
    private ExecutorService getExecutorService(final int executorSize, final String nameFormat) {
        ThreadFactory threadFactory = ShardingSphereThreadFactoryBuilder.build(nameFormat);
        return 0 == executorSize ? Executors.newCachedThreadPool(threadFactory) : Executors.newFixedThreadPool(executorSize, threadFactory);
    }
    
    /**
     * Close executor service.
     * 这里是异步关闭 不会阻塞主线程
     */
    public void close() {
        SHUTDOWN_EXECUTOR.execute(() -> {
            try {
                executorService.shutdown();
                while (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                    executorService.shutdownNow();
                }
            } catch (final InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        });
    }
}
