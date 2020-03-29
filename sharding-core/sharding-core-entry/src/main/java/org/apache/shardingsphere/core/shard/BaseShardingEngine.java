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

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.api.hint.HintManager;
import org.apache.shardingsphere.sharding.route.engine.context.ShardingRouteContext;
import org.apache.shardingsphere.sharding.route.hook.SPIRoutingHook;
import org.apache.shardingsphere.masterslave.route.engine.MasterSlaveRouteDecorator;
import org.apache.shardingsphere.sharding.route.engine.ShardingRouter;
import org.apache.shardingsphere.core.rule.MasterSlaveRule;
import org.apache.shardingsphere.core.rule.ShardingRule;
import org.apache.shardingsphere.core.shard.log.ShardingSQLLogger;
import org.apache.shardingsphere.encrypt.rewrite.context.EncryptSQLRewriteContextDecorator;
import org.apache.shardingsphere.sharding.execute.context.ShardingExecutionContext;
import org.apache.shardingsphere.sharding.rewrite.context.ShardingSQLRewriteContextDecorator;
import org.apache.shardingsphere.sharding.rewrite.engine.ShardingSQLRewriteEngine;
import org.apache.shardingsphere.sql.parser.SQLParserEngine;
import org.apache.shardingsphere.underlying.common.constant.properties.PropertiesConstant;
import org.apache.shardingsphere.underlying.common.constant.properties.ShardingSphereProperties;
import org.apache.shardingsphere.underlying.common.metadata.ShardingSphereMetaData;
import org.apache.shardingsphere.underlying.common.rule.BaseRule;
import org.apache.shardingsphere.underlying.executor.context.ExecutionContext;
import org.apache.shardingsphere.underlying.executor.context.ExecutionUnit;
import org.apache.shardingsphere.underlying.executor.context.SQLUnit;
import org.apache.shardingsphere.underlying.rewrite.SQLRewriteEntry;
import org.apache.shardingsphere.underlying.rewrite.context.SQLRewriteContext;
import org.apache.shardingsphere.underlying.rewrite.context.SQLRewriteContextDecorator;
import org.apache.shardingsphere.underlying.rewrite.engine.SQLRewriteResult;
import org.apache.shardingsphere.underlying.route.context.RouteUnit;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

/**
 * Base sharding engine.
 * 引擎对象  内部维护了各个关键的class
 */
@RequiredArgsConstructor
public abstract class BaseShardingEngine {

    /**
     * 本次使用的分库分表规则
     */
    private final ShardingRule shardingRule;

    /**
     * 内部包含有关的配置
     */
    private final ShardingSphereProperties properties;

    /**
     * 所有相关的元数据
     */
    private final ShardingSphereMetaData metaData;

    /**
     * 包含一个路由对象
     */
    @Getter
    private final ShardingRouter shardingRouter;
    
    private final SPIRoutingHook routingHook;
    
    public BaseShardingEngine(final ShardingRule shardingRule, final ShardingSphereProperties properties, final ShardingSphereMetaData metaData, final SQLParserEngine sqlParserEngine) {
        this.shardingRule = shardingRule;
        this.properties = properties;
        this.metaData = metaData;
        // 通过引擎对象生成路由对象
        shardingRouter = new ShardingRouter(shardingRule, properties, metaData, sqlParserEngine);
        routingHook = new SPIRoutingHook();
    }
    
    /**
     * Shard.
     *
     * @param sql SQL  本次待解析的sql
     * @param parameters SQL parameters  本次携带的参数
     * @return execution context
     */
    public ExecutionContext shard(final String sql, final List<Object> parameters) {
        // 为了保护入参 还特地使用了深拷贝
        List<Object> clonedParameters = cloneParameters(parameters);
        // 这里已经得到分表的结果了
        ShardingRouteContext shardingRouteContext = executeRoute(sql, clonedParameters);
        // 将语句的会话信息和主键 包装成 执行时上下文
        ShardingExecutionContext result = new ShardingExecutionContext(shardingRouteContext.getSqlStatementContext(), shardingRouteContext.getGeneratedKey().orElse(null));
        // 改写实际执行的sql 并添加到 result 中
        result.getExecutionUnits().addAll(HintManager.isDatabaseShardingOnly() ? convert(sql, clonedParameters, shardingRouteContext) : rewriteAndConvert(sql, clonedParameters, shardingRouteContext));
        boolean showSQL = properties.getValue(PropertiesConstant.SQL_SHOW);
        if (showSQL) {
            boolean showSimple = properties.getValue(PropertiesConstant.SQL_SIMPLE);
            ShardingSQLLogger.logSQL(sql, showSimple, result.getSqlStatementContext(), result.getExecutionUnits());
        }
        return result;
    }

    /**
     * 拷贝一份该参数的副本
     * @param parameters
     * @return
     */
    protected abstract List<Object> cloneParameters(List<Object> parameters);

    /**
     * 通过sql 以及参数信息 进行路由
     * @param sql
     * @param parameters
     * @return
     */
    protected abstract ShardingRouteContext route(String sql, List<Object> parameters);

    /**
     * 开始路由
     * @param sql
     * @param clonedParameters
     * @return
     */
    private ShardingRouteContext executeRoute(final String sql, final List<Object> clonedParameters) {
        routingHook.start(sql);
        try {
            ShardingRouteContext result = decorate(route(sql, clonedParameters));
            // 在处理完读写分离后 触发后置钩子
            routingHook.finishSuccess(result, metaData.getTables());
            return result;
            // CHECKSTYLE:OFF
        } catch (final Exception ex) {
            // CHECKSTYLE:ON
            routingHook.finishFailure(ex);
            throw ex;
        }
    }

    /**
     * 对返回的context 进行包装
     * @param shardingRouteContext
     * @return
     */
    private ShardingRouteContext decorate(final ShardingRouteContext shardingRouteContext) {
        ShardingRouteContext result = shardingRouteContext;
        // 获取当前所有主从机规则   这里就是实现读写分离的地方
        for (MasterSlaveRule each : shardingRule.getMasterSlaveRules()) {
            result = (ShardingRouteContext) new MasterSlaveRouteDecorator(each).decorate(result);
        }
        return result;
    }

    /**
     * 将相关参数转换成执行单元  之前只是一个 路由到物理表的信息 现在ExecutionUnit 中还包含了本次待执行的sql
     * @param sql
     * @param parameters
     * @param shardingRouteContext  包含路由结果
     * @return
     */
    private Collection<ExecutionUnit> convert(final String sql, final List<Object> parameters, final ShardingRouteContext shardingRouteContext) {
        Collection<ExecutionUnit> result = new LinkedHashSet<>();
        // 获取本次所有路由单元
        for (RouteUnit each : shardingRouteContext.getRouteResult().getRouteUnits()) {
            // 根据实际路由到的数据源信息 以及 sql parameters 来生成执行单元
            result.add(new ExecutionUnit(each.getActualDataSourceName(), new SQLUnit(sql, parameters)));
        }
        return result;
    }

    /**
     * 改写sql 语句 以及添加/修改一些必备的参数后 返回可以执行的实际单元 (内部的sql 已经被改写)
     * @param sql
     * @param parameters
     * @param shardingRouteContext
     * @return
     */
    private Collection<ExecutionUnit> rewriteAndConvert(final String sql, final List<Object> parameters, final ShardingRouteContext shardingRouteContext) {
        Collection<ExecutionUnit> result = new LinkedHashSet<>();
        SQLRewriteContext sqlRewriteContext = new SQLRewriteEntry(
                metaData, properties).createSQLRewriteContext(sql, parameters, shardingRouteContext.getSqlStatementContext(), createSQLRewriteContextDecorator(shardingRouteContext));
        // 一个 routeUnit 代表一个路由结果 内部包含了 本次sql对应的物理数据源 以及本次sql 对应的物理表 (哪些逻辑表需要被改写)
        for (RouteUnit each : shardingRouteContext.getRouteResult().getRouteUnits()) {
            // 创建重写引擎
            ShardingSQLRewriteEngine sqlRewriteEngine = new ShardingSQLRewriteEngine(shardingRule, shardingRouteContext.getShardingConditions(), each);
            // 创建重写结果 这里已经更改了sql   TODO  具体的改写流程就不看了  之后还是debug调试比较清楚  大体先了解整个运作流程 在具体使用时再配合debug理解改写流程
            SQLRewriteResult sqlRewriteResult = sqlRewriteEngine.rewrite(sqlRewriteContext);
            // 这里改写后的参数也设置进去了
            result.add(new ExecutionUnit(each.getActualDataSourceName(), new SQLUnit(sqlRewriteResult.getSql(), sqlRewriteResult.getParameters())));
        }
        return result;
    }

    /**
     * 创建装饰对象
     * @param shardingRouteContext
     * @return
     */
    private Map<BaseRule, SQLRewriteContextDecorator> createSQLRewriteContextDecorator(final ShardingRouteContext shardingRouteContext) {
        Map<BaseRule, SQLRewriteContextDecorator> result = new LinkedHashMap<>(2, 1);
        result.put(shardingRule, new ShardingSQLRewriteContextDecorator(shardingRouteContext));
        if (!shardingRule.getEncryptRule().getEncryptTableNames().isEmpty()) {
            result.put(shardingRule.getEncryptRule(), new EncryptSQLRewriteContextDecorator());
        }
        return result;
    }
}
