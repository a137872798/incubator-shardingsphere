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

package org.apache.shardingsphere.masterslave.route.engine;

import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.masterslave.route.engine.impl.MasterSlaveDataSourceRouter;
import org.apache.shardingsphere.underlying.route.context.RouteContext;
import org.apache.shardingsphere.underlying.route.DateNodeRouteDecorator;
import org.apache.shardingsphere.underlying.route.context.RouteUnit;
import org.apache.shardingsphere.core.rule.MasterSlaveRule;

import java.util.Collection;
import java.util.LinkedList;

/**
 * Route decorator for master-slave.
 * 主从机包装对象
 */
@RequiredArgsConstructor
public final class MasterSlaveRouteDecorator implements DateNodeRouteDecorator {
    
    private final MasterSlaveRule masterSlaveRule;
    
    @Override
    public RouteContext decorate(final RouteContext routeContext) {
        Collection<RouteUnit> toBeRemoved = new LinkedList<>();
        Collection<RouteUnit> toBeAdded = new LinkedList<>();
        // 遍历路由的所有结果
        for (RouteUnit each : routeContext.getRouteResult().getRouteUnits()) {
            // 匹配规则时 就要将本对象移除 并且 通过MSRule 处理后获取了真正引向的 dataSource
            if (masterSlaveRule.getName().equalsIgnoreCase(each.getActualDataSourceName())) {
                toBeRemoved.add(each);
                toBeAdded.add(createNewRouteUnit(new MasterSlaveDataSourceRouter(masterSlaveRule).route(routeContext.getSqlStatementContext().getSqlStatement()), each));
            }
        }
        routeContext.getRouteResult().getRouteUnits().removeAll(toBeRemoved);
        routeContext.getRouteResult().getRouteUnits().addAll(toBeAdded);
        return routeContext;
    }

    /**
     * 创建路由对象
     * @param actualDataSourceName  本次真正指向的 数据源 (已经经过读写分离处理)
     * @param originalTableUnit
     * @return
     */
    private RouteUnit createNewRouteUnit(final String actualDataSourceName, final RouteUnit originalTableUnit) {
        RouteUnit result = new RouteUnit(originalTableUnit.getLogicDataSourceName(), actualDataSourceName);
        // 将所有 table 信息也移动过去
        result.getTableUnits().addAll(originalTableUnit.getTableUnits());
        return result;
    }
}
