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

import lombok.Getter;
import org.apache.shardingsphere.core.log.ConfigurationLogger;
import org.apache.shardingsphere.spi.database.type.DatabaseType;
import org.apache.shardingsphere.sql.parser.SQLParserEngine;
import org.apache.shardingsphere.sql.parser.SQLParserEngineFactory;
import org.apache.shardingsphere.underlying.common.constant.properties.PropertiesConstant;
import org.apache.shardingsphere.underlying.common.constant.properties.ShardingSphereProperties;
import org.apache.shardingsphere.underlying.common.database.type.DatabaseTypes;
import org.apache.shardingsphere.underlying.common.metadata.ShardingSphereMetaData;
import org.apache.shardingsphere.underlying.common.rule.BaseRule;
import org.apache.shardingsphere.underlying.executor.engine.ExecutorEngine;

import java.util.Properties;

/**
 * Abstract runtime context.
 *
 * @param <T> type of rule
 *           运行时上下文基类 就是包含必要属性
 */
@Getter
public abstract class AbstractRuntimeContext<T extends BaseRule> implements RuntimeContext<T> {
    
    private final T rule;
    
    private final ShardingSphereProperties properties;

    /**
     * 代表使用的数据源类型 比如 mysql
     */
    private final DatabaseType databaseType;

    /**
     * 使用线程池来执行批任务 (也可以在同步模式下执行)
     */
    private final ExecutorEngine executorEngine;

    /**
     * sql 解析引擎
     */
    private final SQLParserEngine sqlParserEngine;

    /**
     * 该对象与 shardingDataSource 一一对应 也就是全局只有一个
     * @param rule
     * @param props
     * @param databaseType
     */
    protected AbstractRuntimeContext(final T rule, final Properties props, final DatabaseType databaseType) {
        this.rule = rule;
        // 把prop 封装成 shardingSphereProp 内部包含了校验逻辑 以及指定了一些属性
        this.properties = new ShardingSphereProperties(null == props ? new Properties() : props);
        this.databaseType = databaseType;
        // 通过线程池数量来初始化线程池  该线程池是全局范围使用  那么还要考虑线程数与连接数的数量对应关系 否则会资源耗尽 白白等待
        executorEngine = new ExecutorEngine(properties.<Integer>getValue(PropertiesConstant.EXECUTOR_SIZE));
        // 根据数据源类型来生成不同的 解析引擎  该对象也是全局唯一  因为在启动的时候已经强制要求只使用一个 databaseType了 否则会报错
        // 该对象负责解析sql
        sqlParserEngine = SQLParserEngineFactory.getSQLParserEngine(DatabaseTypes.getTrunkDatabaseTypeName(databaseType));
        ConfigurationLogger.log(rule.getRuleConfiguration());
        ConfigurationLogger.log(props);
    }
    
    protected abstract ShardingSphereMetaData getMetaData();

    /**
     * 关闭上下文代表对引擎的关闭
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        executorEngine.close();
    }
}
