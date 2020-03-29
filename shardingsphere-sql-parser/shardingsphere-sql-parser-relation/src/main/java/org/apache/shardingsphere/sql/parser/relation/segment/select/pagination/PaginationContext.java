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

package org.apache.shardingsphere.sql.parser.relation.segment.select.pagination;

import lombok.Getter;
import org.apache.shardingsphere.sql.parser.relation.statement.dml.SelectStatementContext;
import org.apache.shardingsphere.sql.parser.sql.segment.dml.pagination.NumberLiteralPaginationValueSegment;
import org.apache.shardingsphere.sql.parser.sql.segment.dml.pagination.PaginationValueSegment;
import org.apache.shardingsphere.sql.parser.sql.segment.dml.pagination.ParameterMarkerPaginationValueSegment;
import org.apache.shardingsphere.sql.parser.sql.segment.dml.pagination.limit.LimitValueSegment;

import java.util.List;
import java.util.Optional;

/**
 * Pagination context.
 */
public final class PaginationContext {
    
    @Getter
    private final boolean hasPagination;
    
    private final PaginationValueSegment offsetSegment;
    
    private final PaginationValueSegment rowCountSegment;
    
    private final long actualOffset;
    
    private final Long actualRowCount;
    
    public PaginationContext(final PaginationValueSegment offsetSegment, final PaginationValueSegment rowCountSegment, final List<Object> parameters) {
        hasPagination = null != offsetSegment || null != rowCountSegment;
        this.offsetSegment = offsetSegment;
        this.rowCountSegment = rowCountSegment;
        actualOffset = null == offsetSegment ? 0 : getValue(offsetSegment, parameters);
        actualRowCount = null == rowCountSegment ? null : getValue(rowCountSegment, parameters); 
    }
    
    private long getValue(final PaginationValueSegment paginationValueSegment, final List<Object> parameters) {
        if (paginationValueSegment instanceof ParameterMarkerPaginationValueSegment) {
            Object obj = parameters.get(((ParameterMarkerPaginationValueSegment) paginationValueSegment).getParameterIndex());
            return obj instanceof Long ? (long) obj : (int) obj;
        } else {
            return ((NumberLiteralPaginationValueSegment) paginationValueSegment).getValue();
        }
    }
    
    /**
     * Get offset segment.
     * 
     * @return offset segment
     */
    public Optional<PaginationValueSegment> getOffsetSegment() {
        return Optional.ofNullable(offsetSegment);
    }
    
    /**
     * Get row count segment.
     *
     * @return row count segment
     */
    public Optional<PaginationValueSegment> getRowCountSegment() {
        return Optional.ofNullable(rowCountSegment);
    }
    
    /**
     * Get actual offset.
     * 
     * @return actual offset
     */
    public long getActualOffset() {
        if (null == offsetSegment) {
            return 0L;
        }
        return offsetSegment.isBoundOpened() ? actualOffset - 1 : actualOffset;
    }
    
    /**
     * Get actual row count.
     *
     * @return actual row count
     */
    public Optional<Long> getActualRowCount() {
        if (null == rowCountSegment) {
            return Optional.empty();
        }
        return Optional.of(rowCountSegment.isBoundOpened() ? actualRowCount + 1 : actualRowCount);
    }
    
    /**
     * Get offset parameter index.
     *
     * @return offset parameter index
     */
    public Optional<Integer> getOffsetParameterIndex() {
        return offsetSegment instanceof ParameterMarkerPaginationValueSegment ? Optional.of(((ParameterMarkerPaginationValueSegment) offsetSegment).getParameterIndex()) : Optional.empty();
    }
    
    /**
     * Get row count parameter index.
     *
     * @return row count parameter index
     */
    public Optional<Integer> getRowCountParameterIndex() {
        return rowCountSegment instanceof ParameterMarkerPaginationValueSegment
                ? Optional.of(((ParameterMarkerPaginationValueSegment) rowCountSegment).getParameterIndex()) : Optional.empty();
    }
    
    /**
     * Get revised offset.
     *
     * @return revised offset
     */
    public long getRevisedOffset() {
        return 0L;
    }
    
    /**
     * Get revised row count.
     * 
     * @param shardingStatement sharding optimized statement
     * @return revised row count
     */
    public long getRevisedRowCount(final SelectStatementContext shardingStatement) {
        if (isMaxRowCount(shardingStatement)) {
            return Integer.MAX_VALUE;
        }
        // 否则正常情况下 分别获取每个分表的  rowCount 就够了 最差情况 就是数据全部落在一张表 那么记录数也够
        return rowCountSegment instanceof LimitValueSegment ? actualOffset + actualRowCount : actualRowCount;
    }

    /**
     * 出现分页和分组的情况 就会读取全部的数据
     * @param shardingStatement
     * @return
     */
    private boolean isMaxRowCount(final SelectStatementContext shardingStatement) {
        return (!shardingStatement.getGroupByContext().getItems().isEmpty()
                // 存在聚合函数的场景
                || !shardingStatement.getProjectionsContext().getAggregationProjections().isEmpty()) && !shardingStatement.isSameGroupByAndOrderByItems();
    }
}
