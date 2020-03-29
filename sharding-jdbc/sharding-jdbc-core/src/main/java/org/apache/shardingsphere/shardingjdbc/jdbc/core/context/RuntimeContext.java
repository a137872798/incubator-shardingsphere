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

package org.apache.shardingsphere.shardingjdbc.jdbc.core.context;

import org.apache.shardingsphere.underlying.common.constant.properties.ShardingSphereProperties;
import org.apache.shardingsphere.underlying.executor.engine.ExecutorEngine;
import org.apache.shardingsphere.sql.parser.SQLParserEngine;
import org.apache.shardingsphere.underlying.common.rule.BaseRule;
import org.apache.shardingsphere.spi.database.type.DatabaseType;

/**
 * Runtime context.
 *
 * @param <T> type of rule
 *           当shardingSphere 启动时 包含运行时信息的上下文
 */
public interface RuntimeContext<T extends BaseRule> extends AutoCloseable {
    
    /**
     * Get rule.
     * 
     * @return rule
     * 获取本次设置的全局rule 信息
     */
    T getRule();
    
    /**
     * Get properties.
     *
     * @return properties
     * 获取本次所有额外属性
     */
    ShardingSphereProperties getProperties();
    
    /**
     * Get database type.
     * 
     * @return database type
     * 本次数据源类型 (无论配置了多少数据源 它们的类型必须是一致的)
     */
    DatabaseType getDatabaseType();
    
    /**
     * Get execute engine.
     * 
     * @return execute engine
     */
    ExecutorEngine getExecutorEngine();
    
    /**
     * Get SQL parser engine.
     * 
     * @return SQL parser engine
     */
    SQLParserEngine getSqlParserEngine();
}
