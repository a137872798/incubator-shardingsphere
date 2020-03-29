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

package org.apache.shardingsphere.sharding.route.engine.type;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.shardingsphere.core.rule.ShardingRule;
import org.apache.shardingsphere.sharding.route.engine.condition.ShardingConditions;
import org.apache.shardingsphere.sharding.route.engine.type.broadcast.ShardingDataSourceGroupBroadcastRoutingEngine;
import org.apache.shardingsphere.sharding.route.engine.type.broadcast.ShardingDatabaseBroadcastRoutingEngine;
import org.apache.shardingsphere.sharding.route.engine.type.broadcast.ShardingMasterInstanceBroadcastRoutingEngine;
import org.apache.shardingsphere.sharding.route.engine.type.broadcast.ShardingTableBroadcastRoutingEngine;
import org.apache.shardingsphere.sharding.route.engine.type.complex.ShardingComplexRoutingEngine;
import org.apache.shardingsphere.sharding.route.engine.type.defaultdb.ShardingDefaultDatabaseRoutingEngine;
import org.apache.shardingsphere.sharding.route.engine.type.ignore.ShardingIgnoreRoutingEngine;
import org.apache.shardingsphere.sharding.route.engine.type.standard.ShardingStandardRoutingEngine;
import org.apache.shardingsphere.sharding.route.engine.type.unicast.ShardingUnicastRoutingEngine;
import org.apache.shardingsphere.sql.parser.relation.statement.SQLStatementContext;
import org.apache.shardingsphere.sql.parser.sql.statement.SQLStatement;
import org.apache.shardingsphere.sql.parser.sql.statement.dal.DALStatement;
import org.apache.shardingsphere.sql.parser.sql.statement.dal.SetStatement;
import org.apache.shardingsphere.sql.parser.sql.statement.dal.dialect.mysql.ShowDatabasesStatement;
import org.apache.shardingsphere.sql.parser.sql.statement.dal.dialect.mysql.UseStatement;
import org.apache.shardingsphere.sql.parser.sql.statement.dal.dialect.postgresql.ResetParameterStatement;
import org.apache.shardingsphere.sql.parser.sql.statement.dcl.DCLStatement;
import org.apache.shardingsphere.sql.parser.sql.statement.ddl.DDLStatement;
import org.apache.shardingsphere.sql.parser.sql.statement.dml.DMLStatement;
import org.apache.shardingsphere.sql.parser.sql.statement.dml.SelectStatement;
import org.apache.shardingsphere.sql.parser.relation.type.TableAvailable;
import org.apache.shardingsphere.sql.parser.sql.statement.tcl.TCLStatement;
import org.apache.shardingsphere.underlying.common.constant.properties.ShardingSphereProperties;
import org.apache.shardingsphere.underlying.common.metadata.ShardingSphereMetaData;

import java.util.Collection;

/**
 * Sharding routing engine factory.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class ShardingRouteEngineFactory {
    
    /**
     * Create new instance of routing engine.
     * 
     * @param shardingRule sharding rule   本次分表规则
     * @param metaData meta data of ShardingSphere    相关元数据
     * @param sqlStatementContext SQL statement context
     * @param shardingConditions shardingConditions  影响分表的条件对象
     * @param properties sharding sphere properties
     * @return new instance of routing engine
     * 生成路由引擎对象
     */
    public static ShardingRouteEngine newInstance(final ShardingRule shardingRule,
                                                  final ShardingSphereMetaData metaData, final SQLStatementContext sqlStatementContext,
                                                  final ShardingConditions shardingConditions, final ShardingSphereProperties properties) {
        // 获取会话对象
        SQLStatement sqlStatement = sqlStatementContext.getSqlStatement();
        // 获取本次所有的表名
        Collection<String> tableNames = sqlStatementContext.getTablesContext().getTableNames();
        if (sqlStatement instanceof TCLStatement) {
            return new ShardingDatabaseBroadcastRoutingEngine();
        }
        if (sqlStatement instanceof DDLStatement) {
            return new ShardingTableBroadcastRoutingEngine(metaData.getTables(), sqlStatementContext);
        }
        if (sqlStatement instanceof DALStatement) {
            return getDALRoutingEngine(shardingRule, sqlStatement, tableNames);
        }
        if (sqlStatement instanceof DCLStatement) {
            return getDCLRoutingEngine(sqlStatementContext, metaData);
        }
        if (shardingRule.isAllInDefaultDataSource(tableNames)) {
            return new ShardingDefaultDatabaseRoutingEngine(tableNames);
        }
        if (shardingRule.isAllBroadcastTables(tableNames)) {
            return sqlStatement instanceof SelectStatement ? new ShardingUnicastRoutingEngine(tableNames) : new ShardingDatabaseBroadcastRoutingEngine();
        }
        // 只看 DML 相关的路由引擎


        if (sqlStatementContext.getSqlStatement() instanceof DMLStatement && tableNames.isEmpty() && shardingRule.hasDefaultDataSourceName()) {
            return new ShardingDefaultDatabaseRoutingEngine(tableNames);
        }
        // 代表没有分表键相关的条件 那么就看作是单表处理
        if (sqlStatementContext.getSqlStatement() instanceof DMLStatement && shardingConditions.isAlwaysFalse() || tableNames.isEmpty() || !shardingRule.tableRuleExists(tableNames)) {
            return new ShardingUnicastRoutingEngine(tableNames);
        }
        // 一般就是这种情况
        return getShardingRoutingEngine(shardingRule, sqlStatementContext, shardingConditions, tableNames, properties);
    }
    
    private static ShardingRouteEngine getDALRoutingEngine(final ShardingRule shardingRule, final SQLStatement sqlStatement, final Collection<String> tableNames) {
        if (sqlStatement instanceof UseStatement) {
            return new ShardingIgnoreRoutingEngine();
        }
        if (sqlStatement instanceof SetStatement || sqlStatement instanceof ResetParameterStatement || sqlStatement instanceof ShowDatabasesStatement) {
            return new ShardingDatabaseBroadcastRoutingEngine();
        }
        if (!tableNames.isEmpty() && !shardingRule.tableRuleExists(tableNames) && shardingRule.hasDefaultDataSourceName()) {
            return new ShardingDefaultDatabaseRoutingEngine(tableNames);
        }
        if (!tableNames.isEmpty()) {
            return new ShardingUnicastRoutingEngine(tableNames);
        }
        return new ShardingDataSourceGroupBroadcastRoutingEngine();
    }
    
    private static ShardingRouteEngine getDCLRoutingEngine(final SQLStatementContext sqlStatementContext, final ShardingSphereMetaData metaData) {
        return isDCLForSingleTable(sqlStatementContext) 
                ? new ShardingTableBroadcastRoutingEngine(metaData.getTables(), sqlStatementContext) : new ShardingMasterInstanceBroadcastRoutingEngine(metaData.getDataSources());
    }
    
    private static boolean isDCLForSingleTable(final SQLStatementContext sqlStatementContext) {
        if (sqlStatementContext instanceof TableAvailable) {
            TableAvailable tableSegmentsAvailable = (TableAvailable) sqlStatementContext;
            return 1 == tableSegmentsAvailable.getAllTables().size() && !"*".equals(tableSegmentsAvailable.getAllTables().iterator().next().getTableName().getIdentifier().getValue());
        }
        return false;
    }

    /**
     *
     * @param shardingRule
     * @param sqlStatementContext
     * @param shardingConditions
     * @param tableNames
     * @param properties
     * @return
     */
    private static ShardingRouteEngine getShardingRoutingEngine(final ShardingRule shardingRule, final SQLStatementContext sqlStatementContext,
                                                                final ShardingConditions shardingConditions, final Collection<String> tableNames, final ShardingSphereProperties properties) {
        // 这里过滤掉了不包含 TableRule 的逻辑表
        Collection<String> shardingTableNames = shardingRule.getShardingLogicTableNames(tableNames);
        // 如果只有一个逻辑表 就是简单操作  如果本次所有逻辑表 绑定了同一个tableRule 那么返回的也是标准对象 (也就是绑定的数据每次都是发往相同的物理节点)
        if (1 == shardingTableNames.size() || shardingRule.isAllBindingTables(shardingTableNames)) {
            // 这样只要传入一个逻辑表名就可以
            return new ShardingStandardRoutingEngine(shardingTableNames.iterator().next(), sqlStatementContext, shardingConditions, properties);
        }
        // TODO config for cartesian set
        // 每个表规则对象都有一个 strategy 那么如果每个逻辑表都有各自的  分表策略 就会使用 complexRoutingEngine 进行路由
        return new ShardingComplexRoutingEngine(tableNames, sqlStatementContext, shardingConditions, properties);
    }
}
