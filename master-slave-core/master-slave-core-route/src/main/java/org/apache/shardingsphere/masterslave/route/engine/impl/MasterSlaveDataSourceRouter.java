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

package org.apache.shardingsphere.masterslave.route.engine.impl;

import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.api.hint.HintManager;
import org.apache.shardingsphere.core.rule.MasterSlaveRule;
import org.apache.shardingsphere.sql.parser.sql.statement.SQLStatement;
import org.apache.shardingsphere.sql.parser.sql.statement.dml.SelectStatement;

import java.util.ArrayList;

/**
 * Data source router for master-slave.
 * 从主机 路由到某个从机上
 */
@RequiredArgsConstructor
public final class MasterSlaveDataSourceRouter {

    /**
     * 包含了 主机的 dataSource  以及一组 从机的 dataSource
     */
    private final MasterSlaveRule masterSlaveRule;
    
    /**
     * Route.
     * 
     * @param sqlStatement SQL statement  这里传入的是 会话对象
     * @return data source name
     */
    public String route(final SQLStatement sqlStatement) {
        if (isMasterRoute(sqlStatement)) {
            // 将ThreadLocal 标记成true
            MasterVisitedManager.setMasterVisited();
            // 返回主节点的 dataSourceName
            return masterSlaveRule.getMasterDataSourceName();
        }

        // 从一组dataSource 中随机返回一个
        return masterSlaveRule.getLoadBalanceAlgorithm().getDataSource(
                masterSlaveRule.getName(), masterSlaveRule.getMasterDataSourceName(), new ArrayList<>(masterSlaveRule.getSlaveDataSourceNames()));
    }

    /**
     * 该会话对象是否会强制路由至 master节点
     * @param sqlStatement
     * @return
     */
    private boolean isMasterRoute(final SQLStatement sqlStatement) {
        return containsLockSegment(sqlStatement) || !(sqlStatement instanceof SelectStatement) || MasterVisitedManager.isMasterVisited() || HintManager.isMasterRouteOnly();
    }
    
    private boolean containsLockSegment(final SQLStatement sqlStatement) {
        return sqlStatement instanceof SelectStatement && ((SelectStatement) sqlStatement).getLock().isPresent();
    }
}
