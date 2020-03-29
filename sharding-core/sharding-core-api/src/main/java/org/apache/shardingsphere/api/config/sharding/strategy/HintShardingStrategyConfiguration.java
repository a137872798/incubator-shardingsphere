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

package org.apache.shardingsphere.api.config.sharding.strategy;

import com.google.common.base.Preconditions;
import lombok.Getter;
import org.apache.shardingsphere.api.sharding.hint.HintShardingAlgorithm;

/**
 * Hint sharding strategy configuration.
 * 由用户传入 hint 来确定sql拆分规则
 */
@Getter
public final class HintShardingStrategyConfiguration implements ShardingStrategyConfiguration {

    /**
     * 通过外部传入算法来初始化
     */
    private final HintShardingAlgorithm shardingAlgorithm;
    
    public HintShardingStrategyConfiguration(final HintShardingAlgorithm shardingAlgorithm) {
        Preconditions.checkNotNull(shardingAlgorithm, "ShardingAlgorithm is required.");
        this.shardingAlgorithm = shardingAlgorithm;
    }
}
