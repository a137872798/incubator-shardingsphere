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

package org.apache.shardingsphere.underlying.common.metadata.datasource;

import org.apache.shardingsphere.spi.database.metadata.DataSourceMetaData;
import org.apache.shardingsphere.spi.database.metadata.MemorizedDataSourceMetaData;
import org.apache.shardingsphere.spi.database.type.DatabaseType;
import org.apache.shardingsphere.underlying.common.config.DatabaseAccessConfiguration;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

/**
 * Data source metas.
 */
public final class DataSourceMetas {

    /**
     * 通过 DatabaseAccessConfiguration 变相得到的更多信息
     */
    private final Map<String, DataSourceMetaData> dataSourceMetaDataMap;

    /**
     *
     * @param databaseType  本批数据源的类型 (mysql)  shardingSphere 要求设置的数据源类型必须相同
     * @param databaseAccessConfigurationMap   每个数据源对应的连接信息
     */
    public DataSourceMetas(final DatabaseType databaseType, final Map<String, DatabaseAccessConfiguration> databaseAccessConfigurationMap) {
        dataSourceMetaDataMap = getDataSourceMetaDataMap(databaseType, databaseAccessConfigurationMap);
    }
    
    private Map<String, DataSourceMetaData> getDataSourceMetaDataMap(final DatabaseType databaseType, final Map<String, DatabaseAccessConfiguration> databaseAccessConfigurationMap) {
        Map<String, DataSourceMetaData> result = new HashMap<>(databaseAccessConfigurationMap.size(), 1);
        for (Entry<String, DatabaseAccessConfiguration> entry : databaseAccessConfigurationMap.entrySet()) {
            // 只看 mysql 实际上这里的 DataSourceMetaData 就是解析url 并获取更多信息
            result.put(entry.getKey(), databaseType.getDataSourceMetaData(entry.getValue().getUrl(), entry.getValue().getUsername()));
        }
        return result;
    }
    
    /**
     * Get all instance data source names.
     *
     * @return instance data source names
     */
    public Collection<String> getAllInstanceDataSourceNames() {
        Collection<String> result = new LinkedList<>();
        for (Entry<String, DataSourceMetaData> entry : dataSourceMetaDataMap.entrySet()) {
            if (!isExisted(entry.getKey(), result)) {
                result.add(entry.getKey());
            }
        }
        return result;
    }

    /**
     * 这里就是去重
     * @param dataSourceName
     * @param existedDataSourceNames
     * @return
     */
    private boolean isExisted(final String dataSourceName, final Collection<String> existedDataSourceNames) {
        DataSourceMetaData sample = dataSourceMetaDataMap.get(dataSourceName);
        for (String each : existedDataSourceNames) {
            if (isInSameDatabaseInstance(sample, dataSourceMetaDataMap.get(each))) {
                return true;
            }
        }
        return false;
    }

    /**
     * 重复的基准就是 dataSource 的 schema host port 全部相同
     * @param sample
     * @param target
     * @return
     */
    private boolean isInSameDatabaseInstance(final DataSourceMetaData sample, final DataSourceMetaData target) {
        return sample instanceof MemorizedDataSourceMetaData
                ? Objects.equals(target.getSchema(), sample.getSchema()) : target.getHostName().equals(sample.getHostName()) && target.getPort() == sample.getPort();
    }
    
    /**
     * Get data source meta data.
     * 
     * @param dataSourceName data source name
     * @return data source meta data
     */
    public DataSourceMetaData getDataSourceMetaData(final String dataSourceName) {
        return dataSourceMetaDataMap.get(dataSourceName);
    }
}
