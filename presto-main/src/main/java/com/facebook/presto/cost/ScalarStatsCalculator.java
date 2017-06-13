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
package com.facebook.presto.cost;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.operator.scalar.ScalarFunctionImplementation;
import com.facebook.presto.spi.statistics.ColumnStatistics;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.sql.analyzer.ExpressionAnalyzer;
import com.facebook.presto.sql.analyzer.Scope;
import com.facebook.presto.sql.planner.ExpressionInterpreter;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.Map;

import static com.facebook.presto.sql.planner.LiteralInterpreter.evaluate;
import static java.util.Objects.requireNonNull;

public class ScalarStatsCalculator
{
    private final Metadata metadata;

    @Inject
    public ScalarStatsCalculator(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata can not be null");
    }

    public ColumnStatistics calculate(Expression scalarExpression, PlanNodeStatsEstimate inputStatistics, Session session, Map<Symbol, Type> types)
    {
        return new Visitor(inputStatistics, session, types).process(scalarExpression);
    }

    private class Visitor
            extends AstVisitor<ColumnStatistics, Void>
    {
        private final PlanNodeStatsEstimate input;
        private final Session session;
        private final Map<Symbol, Type> types;

        Visitor(PlanNodeStatsEstimate input, Session session, Map<Symbol, Type> types)
        {
            this.input = input;
            this.session = session;
            this.types = types;
        }

        @Override
        protected ColumnStatistics visitNode(Node node, Void context)
        {
            return ColumnStatistics.UNKNOWN_COLUMN_STATISTICS;
        }

        @Override
        protected ColumnStatistics visitSymbolReference(SymbolReference node, Void context)
        {
            return input.getSymbolStatistics().getOrDefault(Symbol.from(node), ColumnStatistics.UNKNOWN_COLUMN_STATISTICS);
        }

        @Override
        protected ColumnStatistics visitNullLiteral(NullLiteral node, Void context)
        {
            ColumnStatistics.Builder builder = ColumnStatistics.builder();
            builder.addRange(rb -> rb
                    .setFraction(Estimate.of(0.0))
                    .setDistinctValuesCount(Estimate.zeroValue()));
            builder.setNullsFraction(Estimate.of(1.0));
            return builder.build();
        }

        @Override
        protected ColumnStatistics visitLiteral(Literal node, Void context)
        {
            Object literalValue = evaluate(metadata, session.toConnectorSession(), node);
            ColumnStatistics.Builder builder = ColumnStatistics.builder();
            builder.addRange(literalValue, literalValue, rb -> rb
                    .setFraction(Estimate.of(1.0))
                    .setDistinctValuesCount(Estimate.of(1.0)));
            builder.setNullsFraction(Estimate.zeroValue());
            return builder.build();
        }

        protected ColumnStatistics visitCast(Cast node, Void context)
        {
            // todo - are there cases that lowValue/highValue does not map to lowValue/highValue after cast?
            // todo - compute ndv more cleverly for cases when cast may flatten domain (e.g. double -> bigint)
            ColumnStatistics sourceStats = process(node.getExpression());
            TypeSignature sourceType = getExpressionType(node.getExpression()).getTypeSignature();
            TypeSignature targetType = TypeSignature.parseTypeSignature(node.getType());

            return ColumnStatistics.builder()
                    .setNullsFraction(sourceStats.getNullsFraction())
                    .addRange(rangeStatistics -> rangeStatistics
                            .setLowValue(sourceStats.getOnlyRangeColumnStatistics().getLowValue().map(value -> castLiteral(value, sourceType, targetType)))
                            .setHighValue(sourceStats.getOnlyRangeColumnStatistics().getHighValue().map(value -> castLiteral(value, sourceType, targetType)))
                            .setDistinctValuesCount(sourceStats.getOnlyRangeColumnStatistics().getDistinctValuesCount())
                            .setFraction(sourceStats.getOnlyRangeColumnStatistics().getFraction()))
                    .build();
        }

        private Object castLiteral(Object value, TypeSignature sourceType, TypeSignature targetType)
        {
            Signature signature = metadata.getFunctionRegistry().getCoercion(sourceType, targetType);
            ScalarFunctionImplementation castFunction = metadata.getFunctionRegistry().getScalarFunctionImplementation(signature);
            return ExpressionInterpreter.invoke(session.toConnectorSession(), castFunction, ImmutableList.of(value));
        }

        private Type getExpressionType(Expression expression)
        {
            ExpressionAnalyzer expressionAnalyzer = ExpressionAnalyzer.createWithoutSubqueries(metadata.getFunctionRegistry(), metadata.getTypeManager(), session, types);
            expressionAnalyzer.analyze(expression, Scope.create());
            return expressionAnalyzer.getExpressionTypes().get(NodeRef.of(expression));
        }
    }
}
