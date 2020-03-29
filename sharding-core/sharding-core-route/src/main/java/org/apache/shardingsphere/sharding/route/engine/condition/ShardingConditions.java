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

package org.apache.shardingsphere.sharding.route.engine.condition;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import java.util.List;

/**
 * Sharding conditions.
 * 进行代理的 条件
 */
@RequiredArgsConstructor
@Getter
@ToString
public final class ShardingConditions {

    /**
     * 这些条件会影响到代理sql的结果   对应 select/update/delete  就是 哪些涉及到分表的字段值必须是多少 或者必须满足啥条件
     * 对应 insert 就是某列必须是什么值
     */
    private final List<ShardingCondition> conditions;
    
    /**
     * Judge sharding conditions is always false or not.
     *
     * @return sharding conditions is always false or not
     * 当内部 conditions 为空 或者全都是 AlwaysFalseShardingCondition 的子类
     */
    public boolean isAlwaysFalse() {
        if (conditions.isEmpty()) {
            return false;
        }
        for (ShardingCondition each : conditions) {
            if (!(each instanceof AlwaysFalseShardingCondition)) {
                return false;
            }
        }
        return true;
    }
}
