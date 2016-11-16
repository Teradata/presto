/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.sql.planner;

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GroupingOperation;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.type.ListLiteralType;

import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.resolveFunction;
import static java.util.Objects.requireNonNull;

public class GroupingOperationRewriter
        extends ExpressionRewriter<Void>
{
    private final PlanBuilder subPlan;
    private final QuerySpecification queryNode;
    private final Analysis analysis;
    private final Metadata metadata;
    private final boolean isGroupingOperationTopLevel;

    public GroupingOperationRewriter(PlanBuilder subPlan, QuerySpecification queryNode, Analysis analysis, Metadata metadata, boolean isGroupingOperationTopLevel)
    {
        this.subPlan = requireNonNull(subPlan, "subPlan is null");
        this.queryNode = requireNonNull(queryNode, "node is null");
        this.analysis = requireNonNull(analysis, "analysis is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.isGroupingOperationTopLevel = isGroupingOperationTopLevel;
    }

    @Override
    public Expression rewriteExpression(Expression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
    {
        return treeRewriter.defaultRewrite(node, context);
    }

    @Override
    public Expression rewriteGroupingOperation(GroupingOperation node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
    {
        FunctionCall rewrittenExpression = subPlan.rewriteGroupingOperationToFunctionCall(node, queryNode);
        if (isGroupingOperationTopLevel) {
            analysis.setOutputExpressions(
                    node,
                    analysis.getOutputExpressions(queryNode).stream()
                            .map(expression -> (expression instanceof GroupingOperation) ? rewrittenExpression : expression)
                            .collect(Collectors.toList()));
        }
        IdentityHashMap<Expression, Type> expressionTypes = new IdentityHashMap<>();
        IdentityHashMap<FunctionCall, Signature> functionSignatures = new IdentityHashMap<>();
        List<TypeSignature> functionTypes = Arrays.asList(
                BIGINT.getTypeSignature(),
                ListLiteralType.LIST_LITERAL.getTypeSignature(),
                ListLiteralType.LIST_LITERAL.getTypeSignature()
        );
        Signature functionSignature = resolveFunction(rewrittenExpression, functionTypes, metadata.getFunctionRegistry());

        expressionTypes.put(rewrittenExpression, INTEGER);
        functionSignatures.put(rewrittenExpression, functionSignature);

        analysis.addTypes(expressionTypes);
        analysis.addFunctionSignatures(functionSignatures);

        return rewrittenExpression;
    }
}
