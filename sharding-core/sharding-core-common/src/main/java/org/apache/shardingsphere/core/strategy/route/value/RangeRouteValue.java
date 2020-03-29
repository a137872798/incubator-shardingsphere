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

package org.apache.shardingsphere.core.strategy.route.value;

import com.google.common.collect.Range;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

/**
 * Route value for range.
 * 代表定位到了某个表某个列的一系列值
 */
@RequiredArgsConstructor
@Getter
@ToString
public final class RangeRouteValue<T extends Comparable<?>> implements RouteValue {
    
    private final String columnName;
    
    private final String tableName;

    /**
     * 存在多个 范围实现 内部包含一个值 以及一些判定是否满足范围的表达式
     */
    private final Range<T> valueRange;
}
