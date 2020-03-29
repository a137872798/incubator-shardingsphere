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

package org.apache.shardingsphere.sql.parser.relation.segment.insert;

import lombok.Getter;
import lombok.ToString;
import org.apache.shardingsphere.sql.parser.sql.segment.dml.expr.ExpressionSegment;
import org.apache.shardingsphere.sql.parser.sql.segment.dml.expr.simple.LiteralExpressionSegment;
import org.apache.shardingsphere.sql.parser.sql.segment.dml.expr.simple.ParameterMarkerExpressionSegment;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Insert value context.
 * 如果是 批量插入 那么对应一个子句
 */
@Getter
@ToString
public final class InsertValueContext {

    /**
     * 本次插入的参数数量
     */
    private final int parametersCount;

    /**
     * 插入的值 在 ASTNode 中被看作是一个 expression
     */
    private final List<ExpressionSegment> valueExpressions;
    
    private final List<Object> parameters;
    
    public InsertValueContext(final Collection<ExpressionSegment> assignments, final List<Object> parameters, final int parametersOffset) {
        parametersCount = calculateParametersCount(assignments);
        valueExpressions = getValueExpressions(assignments);
        this.parameters = getParameters(parameters, parametersOffset);
    }
    
    private int calculateParametersCount(final Collection<ExpressionSegment> assignments) {
        int result = 0;
        for (ExpressionSegment each : assignments) {
            // 如果是被标记的值 那么就需要进行改写
            if (each instanceof ParameterMarkerExpressionSegment) {
                result++;
            }
        }
        return result;
    }
    
    private List<ExpressionSegment> getValueExpressions(final Collection<ExpressionSegment> assignments) {
        List<ExpressionSegment> result = new ArrayList<>(assignments.size());
        result.addAll(assignments);
        return result;
    }

    /**
     * 从传入的整个参数列表中 截取部分参数
     * @param parameters
     * @param parametersOffset
     * @return
     */
    private List<Object> getParameters(final List<Object> parameters, final int parametersOffset) {
        if (0 == parametersCount) {
            return Collections.emptyList();
        }
        List<Object> result = new ArrayList<>(parametersCount);
        result.addAll(parameters.subList(parametersOffset, parametersOffset + parametersCount));
        return result;
    }
    
    /**
     * Get value.
     *
     * @param index index
     * @return value
     * index 对应下标 在 valueExpression能定位到第几个 那么就从参数的第几个开始找
     */
    public Object getValue(final int index) {
        ExpressionSegment valueExpression = valueExpressions.get(index);
        return valueExpression instanceof ParameterMarkerExpressionSegment ? parameters.get(getParameterIndex(valueExpression)) : ((LiteralExpressionSegment) valueExpression).getLiterals();
    }
    
    private int getParameterIndex(final ExpressionSegment valueExpression) {
        int result = 0;
        for (ExpressionSegment each : valueExpressions) {
            if (valueExpression == each) {
                return result;
            }
            if (each instanceof ParameterMarkerExpressionSegment) {
                result++;
            }
        }
        throw new IllegalArgumentException("Can not get parameter index.");
    }
}
