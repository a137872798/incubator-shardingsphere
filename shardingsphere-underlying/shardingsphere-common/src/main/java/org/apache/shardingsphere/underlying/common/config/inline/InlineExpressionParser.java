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

package org.apache.shardingsphere.underlying.common.config.inline;

import com.google.common.base.Joiner;
import com.google.common.collect.Collections2;
import com.google.common.collect.Sets;
import groovy.lang.Closure;
import groovy.lang.GString;
import groovy.lang.GroovyShell;
import groovy.lang.Script;
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Inline expression parser.
 * 内联表达式解析器  这个表达式的解析 依赖于第三方库
 */
@RequiredArgsConstructor
public final class InlineExpressionParser {

    /**
     * 表达式所用的分隔符
     */
    private static final char SPLITTER = ',';

    /**
     * 应该是拆分后的结果
     */
    private static final Map<String, Script> SCRIPTS = new HashMap<>();
    
    private static final GroovyShell SHELL = new GroovyShell();

    /**
     * 表达式本体   比如 ds,ds_${0..1}
     */
    private final String inlineExpression;
    
    /**
     * Replace all inline expression placeholders.
     * 
     * @param inlineExpression inline expression with {@code $->}
     * @return result inline expression with {@code $}
     * 将所有 "$->{" 替换成 "${"
     */
    public static String handlePlaceHolder(final String inlineExpression) {
        return inlineExpression.contains("$->{") ? inlineExpression.replaceAll("\\$->\\{", "\\$\\{") : inlineExpression;
    }
    
    /**
     * Split and evaluate inline expression.
     *
     * @return result list
     */
    public List<String> splitAndEvaluate() {
        if (null == inlineExpression) {
            return Collections.emptyList();
        }
        return flatten(evaluate(split()));
    }
    
    /**
     * Evaluate closure.
     *
     * @return closure
     */
    public Closure<?> evaluateClosure() {
        return (Closure) evaluate(Joiner.on("").join("{it -> \"", inlineExpression, "\"}"));
    }

    /**
     * list中每个元素都是一段表达式
     * @param inlineExpressions
     * @return
     */
    private List<Object> evaluate(final List<String> inlineExpressions) {
        List<Object> result = new ArrayList<>(inlineExpressions.size());
        for (String each : inlineExpressions) {
            StringBuilder expression = new StringBuilder(handlePlaceHolder(each));
            // 这里使用 \\ 包装字符串
            if (!each.startsWith("\"")) {
                expression.insert(0, "\"");
            }
            if (!each.endsWith("\"")) {
                expression.append("\"");
            }
            result.add(evaluate(expression.toString()));
        }
        return result;
    }

    /**
     * 使用groovy 解析
     * @param expression
     * @return
     */
    private Object evaluate(final String expression) {
        Script script;
        if (SCRIPTS.containsKey(expression)) {
            script = SCRIPTS.get(expression);
        } else {
            // 解析后并缓存结果
            script = SHELL.parse(expression);
            SCRIPTS.put(expression, script);
        }
        return script.run();
    }

    /**
     * 拆分内部的表达式
     * @return
     */
    private List<String> split() {
        List<String> result = new ArrayList<>();
        StringBuilder segment = new StringBuilder();
        // 对应嵌套层数
        int bracketsDepth = 0;
        for (int i = 0; i < inlineExpression.length(); i++) {
            char each = inlineExpression.charAt(i);
            switch (each) {
                // 对应 ${x1,x2}  x1 x2 会被看作是2个值
                case SPLITTER:
                    // 代表该 , 属于某个 ${ 内 那么算作有效
                    if (bracketsDepth > 0) {
                        segment.append(each);
                    } else {
                        // 以括号外的 , 为分界线保存 表达式信息
                        result.add(segment.toString().trim());
                        segment.setLength(0);
                    }
                    break;
                // 找到了表达式的起始符
                case '$':
                    if ('{' == inlineExpression.charAt(i + 1)) {
                        bracketsDepth++;
                    }
                    if ("->{".equals(inlineExpression.substring(i + 1, i + 4))) {
                        bracketsDepth++;
                    }
                    segment.append(each);
                    break;
                case '}':
                    if (bracketsDepth > 0) {
                        bracketsDepth--;
                    }
                    segment.append(each);
                    break;
                // 其余字符正常添加
                default:
                    segment.append(each);
                    break;
            }
        }
        if (segment.length() > 0) {
            result.add(segment.toString().trim());
        }
        return result;
    }

    /**
     * 处理加工完的结果
     * @param segments
     * @return
     */
    private List<String> flatten(final List<Object> segments) {
        List<String> result = new ArrayList<>();
        for (Object each : segments) {
            if (each instanceof GString) {
                result.addAll(assemblyCartesianSegments((GString) each));
            } else {
                result.add(each.toString());
            }
        }
        return result;
    }

    /**
     * 将一个 GS 对象变成一组string
     * @param segment
     * @return
     */
    private List<String> assemblyCartesianSegments(final GString segment) {
        Set<List<String>> cartesianValues = getCartesianValues(segment);
        List<String> result = new ArrayList<>(cartesianValues.size());
        for (List<String> each : cartesianValues) {
            result.add(assemblySegment(each, segment));
        }
        return result;
    }
    
    @SuppressWarnings("unchecked")
    private Set<List<String>> getCartesianValues(final GString segment) {
        List<Set<String>> result = new ArrayList<>(segment.getValues().length);
        for (Object each : segment.getValues()) {
            if (null == each) {
                continue;
            }
            if (each instanceof Collection) {
                result.add(Sets.newLinkedHashSet(Collections2.transform((Collection<Object>) each, Object::toString)));
            } else {
                result.add(Sets.newHashSet(each.toString()));
            }
        }
        return Sets.cartesianProduct(result);
    }
    
    private String assemblySegment(final List<String> cartesianValue, final GString segment) {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < segment.getStrings().length; i++) {
            result.append(segment.getStrings()[i]);
            if (i < cartesianValue.size()) {
                result.append(cartesianValue.get(i));
            }
        }
        return result.toString();
    }
}
