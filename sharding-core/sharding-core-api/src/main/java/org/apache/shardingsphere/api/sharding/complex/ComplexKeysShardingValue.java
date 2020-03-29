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

package org.apache.shardingsphere.api.sharding.complex;

import com.google.common.collect.Range;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.apache.shardingsphere.api.sharding.ShardingValue;

import java.util.Collection;
import java.util.Map;

/**
 * Sharding value for complex keys.
 * 通过多列来决定代理的结果
 */
@RequiredArgsConstructor
@Getter
@ToString
public final class ComplexKeysShardingValue<T extends Comparable<?>> implements ShardingValue {

    /**
     * 逻辑表名 应该就是还未进行代理前的要查询的表名
     */
    private final String logicTableName;

    // 下面2个容器 代表列名 以及该列的范围 比如 a = 3 , a in (4,5) , a between 6 and 7

    private final Map<String, Collection<T>> columnNameAndShardingValuesMap;

    private final Map<String, Range<T>> columnNameAndRangeValuesMap;
}
