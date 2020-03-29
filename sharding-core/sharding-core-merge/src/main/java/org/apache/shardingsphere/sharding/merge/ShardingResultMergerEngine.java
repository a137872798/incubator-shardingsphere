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

package org.apache.shardingsphere.sharding.merge;

import org.apache.shardingsphere.core.rule.ShardingRule;
import org.apache.shardingsphere.sharding.merge.dal.ShardingDALResultMerger;
import org.apache.shardingsphere.sharding.merge.dql.ShardingDQLResultMerger;
import org.apache.shardingsphere.spi.database.type.DatabaseType;
import org.apache.shardingsphere.sql.parser.relation.statement.SQLStatementContext;
import org.apache.shardingsphere.sql.parser.relation.statement.dml.SelectStatementContext;
import org.apache.shardingsphere.sql.parser.sql.statement.dal.DALStatement;
import org.apache.shardingsphere.underlying.common.constant.properties.ShardingSphereProperties;
import org.apache.shardingsphere.underlying.merge.engine.merger.ResultMerger;
import org.apache.shardingsphere.underlying.merge.engine.merger.impl.TransparentResultMerger;
import org.apache.shardingsphere.underlying.merge.engine.merger.ResultMergerEngine;

/**
 * Result merger engine for sharding.
 * 用于整合ResultSet的引擎对象
 */
public final class ShardingResultMergerEngine implements ResultMergerEngine<ShardingRule> {
    
    @Override
    public ResultMerger newInstance(final DatabaseType databaseType, final ShardingRule shardingRule, final ShardingSphereProperties properties, final SQLStatementContext sqlStatementContext) {

        // 结合select 语句结果
        if (sqlStatementContext instanceof SelectStatementContext) {
            return new ShardingDQLResultMerger(databaseType);
        } 
        if (sqlStatementContext.getSqlStatement() instanceof DALStatement) {
            return new ShardingDALResultMerger(shardingRule);
        }
        // 返回一个整合对象  应该是 DELETE/UPDATE/INSERT 不用结合 结果 所以直接返回第一个结果
        return new TransparentResultMerger();
    }
}
