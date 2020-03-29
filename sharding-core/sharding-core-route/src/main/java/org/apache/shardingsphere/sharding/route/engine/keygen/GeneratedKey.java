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

package org.apache.shardingsphere.sharding.route.engine.keygen;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.apache.shardingsphere.core.rule.ShardingRule;
import org.apache.shardingsphere.sql.parser.sql.segment.dml.expr.ExpressionSegment;
import org.apache.shardingsphere.sql.parser.sql.segment.dml.expr.simple.LiteralExpressionSegment;
import org.apache.shardingsphere.sql.parser.sql.segment.dml.expr.simple.ParameterMarkerExpressionSegment;
import org.apache.shardingsphere.sql.parser.sql.statement.dml.InsertStatement;
import org.apache.shardingsphere.underlying.common.metadata.table.TableMetas;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

/**
 * Generated key.
 */
@RequiredArgsConstructor
@Getter
@ToString
public final class GeneratedKey {

    /**
     * 代表哪个列是自增列
     */
    private final String columnName;

    /**
     * 是否是生成的  (反之就是用户自己添加的)
     */
    private final boolean generated;

    /**
     * 本次执行的insert 中 插入的generatedKey 集合 因为insert 可以是批插入
     */
    private final LinkedList<Comparable<?>> generatedValues = new LinkedList<>();
    
    /**
     * Get generate key.
     *
     * @param shardingRule sharding rule
     * @param tableMetas table metas
     * @param parameters SQL parameters
     * @param insertStatement insert statement
     * @return generate key
     * 通过本次传入的 InsertStatement 以及相关的逻辑表名 判断是否有自动插入的语句
     */
    public static Optional<GeneratedKey> getGenerateKey(final ShardingRule shardingRule, final TableMetas tableMetas, final List<Object> parameters, final InsertStatement insertStatement) {
        // 获取该表标记了需要自增id 的列
        Optional<String> generateKeyColumnNameOptional = shardingRule.findGenerateKeyColumnName(insertStatement.getTable().getTableName().getIdentifier().getValue());
        // Optional 如果为空 那么 .map 会直接返回一个 Optional.empty()
        return generateKeyColumnNameOptional.map(generateKeyColumnName -> containsGenerateKey(tableMetas, insertStatement, generateKeyColumnName)
                // 代表本次insert 语句中本来就包含了主键 那么就不需要额外生成了
                ? findGeneratedKey(tableMetas, parameters, insertStatement, generateKeyColumnName)
                // 通过额外的算法获取generateKey
                : createGeneratedKey(shardingRule, insertStatement, generateKeyColumnName));
    }

    /**
     *
     * @param tableMetas  所有表信息
     * @param insertStatement  ast解析出来的插入对象
     * @param generateKeyColumnName  本次可能会自动生成的id (在一开始的rule对象中选择表时通过Connection 可以获取TableMeta
     *                               这样就可以知道这个表的generateKey是哪列了 不过还要跟本次ast的解析结果做匹配 )
     * @return
     */
    private static boolean containsGenerateKey(final TableMetas tableMetas, final InsertStatement insertStatement, final String generateKeyColumnName) {
        return insertStatement.getColumnNames().isEmpty()
                // TODO 这里不太明确 先忽略  能够确定的是 如果本次插入的字段 包含了 自增字段 那么返回true
                ? tableMetas.getAllColumnNames(insertStatement.getTable().getTableName().getIdentifier().getValue()).size() == insertStatement.getValueCountForPerGroup()
                : insertStatement.getColumnNames().contains(generateKeyColumnName);
    }

    /**
     * 生成一个不需要通过 generateKey算法生成主键的 key
     * @param tableMetas
     * @param parameters
     * @param insertStatement
     * @param generateKeyColumnName
     * @return
     */
    private static GeneratedKey findGeneratedKey(final TableMetas tableMetas, final List<Object> parameters, final InsertStatement insertStatement, final String generateKeyColumnName) {
        GeneratedKey result = new GeneratedKey(generateKeyColumnName, false);
        for (ExpressionSegment each : findGenerateKeyExpressions(tableMetas, insertStatement, generateKeyColumnName)) {
            // 应该是代表可以直接按照参数列表以及下标来获取吧
            if (each instanceof ParameterMarkerExpressionSegment) {
                result.getGeneratedValues().add((Comparable<?>) parameters.get(((ParameterMarkerExpressionSegment) each).getParameterMarkerIndex()));
            } else if (each instanceof LiteralExpressionSegment) {
                result.getGeneratedValues().add((Comparable<?>) ((LiteralExpressionSegment) each).getLiterals());
            }
        }
        return result;
    }
    
    private static Collection<ExpressionSegment> findGenerateKeyExpressions(final TableMetas tableMetas, final InsertStatement insertStatement, final String generateKeyColumnName) {
        Collection<ExpressionSegment> result = new LinkedList<>();
        // getAllValueExpressions 应该是对应批插入的 每个子句的 所有列
        for (List<ExpressionSegment> each : insertStatement.getAllValueExpressions()) {
            result.add(each.get(findGenerateKeyIndex(tableMetas, insertStatement, generateKeyColumnName.toLowerCase())));
        }
        return result;
    }

    /**
     * 找到自增列的下标
     * @param tableMetas
     * @param insertStatement
     * @param generateKeyColumnName
     * @return
     */
    private static int findGenerateKeyIndex(final TableMetas tableMetas, final InsertStatement insertStatement, final String generateKeyColumnName) {
        return insertStatement.getColumnNames().isEmpty() ? tableMetas.getAllColumnNames(insertStatement.getTable().getTableName().getIdentifier().getValue()).indexOf(generateKeyColumnName) 
                : insertStatement.getColumnNames().indexOf(generateKeyColumnName);
    }

    /**
     * 当本次insert 语句插入的字段不包含自增列 这里应该是要通过 某种策略来生成 而不是通过数据库生成(因为要确保id在多个物理表的基础上自增)
     * @param shardingRule
     * @param insertStatement
     * @param generateKeyColumnName
     * @return
     */
    private static GeneratedKey createGeneratedKey(final ShardingRule shardingRule, final InsertStatement insertStatement, final String generateKeyColumnName) {
        GeneratedKey result = new GeneratedKey(generateKeyColumnName, true);
        // 每个子语句 要生成一个 自增值
        for (int i = 0; i < insertStatement.getValueListCount(); i++) {
            result.getGeneratedValues().add(shardingRule.generateKey(insertStatement.getTable().getTableName().getIdentifier().getValue()));
        }
        return result;
    }
}
