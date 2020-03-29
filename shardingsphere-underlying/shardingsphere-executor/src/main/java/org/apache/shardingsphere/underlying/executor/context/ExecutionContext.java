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

package org.apache.shardingsphere.underlying.executor.context;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.sql.parser.relation.statement.SQLStatementContext;

import java.util.Collection;
import java.util.LinkedHashSet;

/**
 * Execution context.
 * 执行sql时的上下文信息
 */
@RequiredArgsConstructor
@Getter
public class ExecutionContext {

    /**
     * 对应会话期的上下文对象
     */
    private final SQLStatementContext sqlStatementContext;

    /**
     * 包含 dataSource sql args   数据源 sql语句 以及使用的参数
     */
    private final Collection<ExecutionUnit> executionUnits = new LinkedHashSet<>();
}
