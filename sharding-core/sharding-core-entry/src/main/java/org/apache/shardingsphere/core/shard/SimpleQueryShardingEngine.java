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

package org.apache.shardingsphere.core.shard;

import org.apache.shardingsphere.sharding.route.engine.context.ShardingRouteContext;
import org.apache.shardingsphere.core.rule.ShardingRule;
import org.apache.shardingsphere.sql.parser.SQLParserEngine;
import org.apache.shardingsphere.underlying.common.constant.properties.ShardingSphereProperties;
import org.apache.shardingsphere.underlying.common.metadata.ShardingSphereMetaData;

import java.util.Collections;
import java.util.List;

/**
 * Sharding engine for simple query.
 * 
 * <pre>
 *     Simple query:  
 *       for JDBC is Statement; 
 *       for MyQL is COM_QUERY; 
 *       for PostgreSQL is Simple Query;
 * </pre>
 *  该对象是分表的核心类
 */
public final class SimpleQueryShardingEngine extends BaseShardingEngine {

    /**
     * 初始化该对象时携带了 引擎对象
     * @param shardingRule
     * @param properties
     * @param metaData
     * @param sqlParserEngine
     */
    public SimpleQueryShardingEngine(final ShardingRule shardingRule, final ShardingSphereProperties properties, final ShardingSphereMetaData metaData, final SQLParserEngine sqlParserEngine) {
        super(shardingRule, properties, metaData, sqlParserEngine);
    }
    
    @Override
    protected List<Object> cloneParameters(final List<Object> parameters) {
        return Collections.emptyList();
    }

    /**
     * 通过路由器对象 解析sql 并将路由结果 包装成一个 RouteContext 对象
     * @param sql
     * @param parameters
     * @return
     */
    @Override
    protected ShardingRouteContext route(final String sql, final List<Object> parameters) {
        return getShardingRouter().route(sql, Collections.emptyList(), false);
    }
}
