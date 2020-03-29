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

package org.apache.shardingsphere.sharding.execute.sql;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.underlying.executor.constant.ConnectionMode;
import org.apache.shardingsphere.underlying.executor.context.ExecutionUnit;

import java.sql.Statement;

/**
 * Execute unit for JDBC statement.
 * 会话层面的执行单元
 */
@RequiredArgsConstructor
@Getter
public final class StatementExecuteUnit {

    /**
     * 包含一个 dataSource 一个 sql 一组param[]
     */
    private final ExecutionUnit executionUnit;

    /**
     * 本次实际的会话对象  意味着绑定在某个具体的connection了  ExecuteUnit还不确定由哪个connection来执行
     */
    private final Statement statement;

    /**
     * 采用的连接模式 是 内存限制 还是连接数限制
     */
    private final ConnectionMode connectionMode;
}
