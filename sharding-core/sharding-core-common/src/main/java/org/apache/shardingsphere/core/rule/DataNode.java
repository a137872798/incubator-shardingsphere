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

package org.apache.shardingsphere.core.rule;

import com.google.common.base.Objects;
import com.google.common.base.Splitter;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.apache.shardingsphere.underlying.common.config.exception.ShardingSphereConfigurationException;

import java.util.List;

/**
 * Sharding data unit node.
 * 物理表名 + 物理数据源 定位到唯一个 dataNode
 */
@RequiredArgsConstructor
@Getter
@ToString
public final class DataNode {
    
    private static final String DELIMITER = ".";

    private final String dataSourceName;

    /**
     * 如果通过设置 dataSourceName 和 tableName 来初始化 那么此时传入的是逻辑表名   (这种情况实际上也无法进行分表 逻辑表就是物理表)
     * 如果通过 dataNode 表达式进行初始化 那么是物理表名
     */
    private final String tableName;
    
    /**
     * Constructs a data node with well-formatted string.
     *
     * @param dataNode string of data node. use {@code .} to split data source name and table name.
     *                 2种初始化方式 如果在配置文件中直接指定了某个逻辑表的 dataNode 那么用表达式设置
     */
    public DataNode(final String dataNode) {
        if (!isValidDataNode(dataNode)) {
            throw new ShardingSphereConfigurationException("Invalid format for actual data nodes: '%s'", dataNode);
        }
        // 根据分隔符 拆分后进行赋值
        List<String> segments = Splitter.on(DELIMITER).splitToList(dataNode);
        dataSourceName = segments.get(0);
        tableName = segments.get(1);
    }

    /**
     * 是否满足指定的格式
     * @param dataNodeStr
     * @return
     */
    private static boolean isValidDataNode(final String dataNodeStr) {
        return dataNodeStr.contains(DELIMITER) && 2 == Splitter.on(DELIMITER).splitToList(dataNodeStr).size();
    }
    
    @Override
    public boolean equals(final Object object) {
        if (this == object) {
            return true;
        }
        if (null == object || getClass() != object.getClass()) {
            return false;
        }
        DataNode dataNode = (DataNode) object;
        return Objects.equal(this.dataSourceName.toUpperCase(), dataNode.dataSourceName.toUpperCase())
            && Objects.equal(this.tableName.toUpperCase(), dataNode.tableName.toUpperCase());
    }
    
    @Override
    public int hashCode() {
        return Objects.hashCode(dataSourceName.toUpperCase(), tableName.toUpperCase());
    }
}
