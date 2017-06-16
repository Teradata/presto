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
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.LiteralInterpreter;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.ComparisonExpressionType;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.SymbolReference;

import javax.inject.Inject;

import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;

import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.POSITIVE_INFINITY;
import static java.lang.Double.isNaN;
import static java.lang.Math.max;
import static java.lang.Math.min;

public class FilterStatsCalculator
{
    private final Metadata metadata;

    @Inject
    public FilterStatsCalculator(Metadata metadata)
    {
        this.metadata = metadata;
    }

    public PlanNodeStatsEstimate filterStats(
            PlanNodeStatsEstimate statsEstimate,
            Expression predicate,
            Session session,
            Map<Symbol, Type> types)
    {
        return new FilterExpressionStatsCalculatingVisitor(statsEstimate, session, types).process(predicate);
    }

    private class FilterExpressionStatsCalculatingVisitor
            extends AstVisitor<PlanNodeStatsEstimate, Void>
    {
        private final PlanNodeStatsEstimate input;
        private final Session session;
        private final Map<Symbol, Type> types;

        FilterExpressionStatsCalculatingVisitor(PlanNodeStatsEstimate input, Session session, Map<Symbol, Type> types)
        {
            this.input = input;
            this.session = session;
            this.types = types;
        }

        @Override
        protected PlanNodeStatsEstimate visitExpression(Expression node, Void context)
        {
            return filterForUnknownExpression();
        }

        private PlanNodeStatsEstimate filterForUnknownExpression()
        {
            return filterStatsByFactor(0.5);
        }

        @Override
        protected PlanNodeStatsEstimate visitComparisonExpression(ComparisonExpression node, Void context)
        {
            // FIXME left and right might not be exactly SymbolReference and Literal
            if (node.getLeft() instanceof SymbolReference && node.getRight() instanceof SymbolReference) {
                return comparisonSymbolToSymbolStats(
                        Symbol.from(node.getLeft()),
                        Symbol.from(node.getRight()),
                        node.getType()
                );
            }
            else if (node.getLeft() instanceof SymbolReference && node.getRight() instanceof Literal) {
                return comparisonSymbolToLiteralStats(
                        Symbol.from(node.getLeft()),
                        (Literal) node.getRight(),
                        node.getType()
                );
            }
            else if (node.getLeft() instanceof Literal && node.getRight() instanceof SymbolReference) {
                return comparisonSymbolToLiteralStats(
                        Symbol.from(node.getRight()),
                        (Literal) node.getLeft(),
                        node.getType().flip()
                );
            }
            else {
                return filterForUnknownExpression();
            }
        }

        @Override
        protected PlanNodeStatsEstimate visitBooleanLiteral(BooleanLiteral node, Void context)
        {
            if (node.getValue()) {
                return input;
            }
            else {
                return filterStatsByFactor(0.0);
            }
        }

        private PlanNodeStatsEstimate comparisonSymbolToLiteralStats(Symbol symbol, Literal literal, ComparisonExpressionType type)
        {
            Object literalValue = LiteralInterpreter.evaluate(metadata, session.toConnectorSession(), literal);
            TypeStatOperatorCaller operatorCaller = new TypeStatOperatorCaller(types.get(symbol), metadata.getFunctionRegistry(), session.toConnectorSession());
            OptionalDouble doubleLiteral = operatorCaller.translateToDouble(literalValue);
            if (doubleLiteral.isPresent()) {
                switch (type) {
                    case EQUAL:
                        return symbolToLiteralEquality(symbol, doubleLiteral.getAsDouble());
                    case NOT_EQUAL:
                        return symbolToLiteralNonEquality(symbol, doubleLiteral.getAsDouble());
                    case LESS_THAN:
                    case LESS_THAN_OR_EQUAL:
                        return symbolToLiteralLessThan(symbol, doubleLiteral.getAsDouble());
                    case GREATER_THAN:
                    case GREATER_THAN_OR_EQUAL:
                        return symbolToLiteralGreaterThan(symbol, doubleLiteral.getAsDouble());
                    case IS_DISTINCT_FROM:
                        break;
                }
            }
            return filterForUnknownExpression();
        }

        private PlanNodeStatsEstimate symbolToLiteralGreaterThan(Symbol symbol, double literal)
        {
            SymbolStatsEstimate symbolStats = input.getSymbolStatistics().get(symbol);

            SimplifiedHistogramStats histogram = SimplifiedHistogramStats.of(symbolStats);
            SimplifiedHistogramStats newStats = histogram.intersect(new StatsHistogramRange(literal, POSITIVE_INFINITY, Optional.empty()));
            double filtered = newStats.getFilteredPercent();
            return filterStatsByFactor(filtered).mapSymbolColumnStatistics(symbol, x -> newStats.toSymbolsStatistics());
        }

        private PlanNodeStatsEstimate symbolToLiteralLessThan(Symbol symbol, double literal)
        {
            SymbolStatsEstimate symbolStats = input.getSymbolStatistics().get(symbol);

            SimplifiedHistogramStats histogram = SimplifiedHistogramStats.of(symbolStats);
            SimplifiedHistogramStats newStats = histogram.intersect(new StatsHistogramRange(NEGATIVE_INFINITY, literal, Optional.empty()));
            double filtered = newStats.getFilteredPercent();
            return filterStatsByFactor(filtered).mapSymbolColumnStatistics(symbol, x -> newStats.toSymbolsStatistics());
        }

        private PlanNodeStatsEstimate symbolToLiteralNonEquality(Symbol symbol, double literal)
        {
            SymbolStatsEstimate symbolStats = input.getSymbolStatistics().get(symbol);

            SimplifiedHistogramStats histogram = SimplifiedHistogramStats.of(symbolStats);
            SimplifiedHistogramStats intersectStats = histogram.intersect(new StatsHistogramRange(literal, literal, Optional.empty()));
            double filtered = 1.0 - intersectStats.getFilteredPercent();
            return filterStatsByFactor(filtered)
                    .mapSymbolColumnStatistics(symbol,
                            stats -> SymbolStatsEstimate.buildFrom(stats)
                                    .setNullsFraction(0)
                                    .setDataSize(stats.getDataSize() * filtered)
                                    .setDistinctValuesCount(stats.getDistinctValuesCount() - 1)
                                    .build());
        }

        private PlanNodeStatsEstimate symbolToLiteralEquality(Symbol symbol, double literal)
        {
            SymbolStatsEstimate symbolStats = input.getSymbolStatistics().get(symbol);

            SimplifiedHistogramStats histogram = SimplifiedHistogramStats.of(symbolStats);
            SimplifiedHistogramStats newStats = histogram.intersect(new StatsHistogramRange(literal, literal, Optional.empty()));
            double filtered = newStats.getFilteredPercent();
            return filterStatsByFactor(filtered).mapSymbolColumnStatistics(symbol, x -> newStats.toSymbolsStatistics());
        }

        private PlanNodeStatsEstimate filterStatsByFactor(double filterRate)
        {
            return input
                    .mapOutputRowCount(size -> size * filterRate);
        }

        private PlanNodeStatsEstimate comparisonSymbolToSymbolStats(Symbol left, Symbol right, ComparisonExpressionType type)
        {
            switch (type) {
                case EQUAL:
                    return symbolToSymbolEquality(left, right);
                case NOT_EQUAL:
                case LESS_THAN:
                case LESS_THAN_OR_EQUAL:
                case GREATER_THAN:
                case GREATER_THAN_OR_EQUAL:
                case IS_DISTINCT_FROM:
            }
            return filterStatsByFactor(0.5); //fixme
        }

        private PlanNodeStatsEstimate symbolToSymbolEquality(Symbol left, Symbol right)
        {
            SymbolStatsEstimate leftStats = input.getSymbolStatistics().get(left);
            SymbolStatsEstimate rightStats = input.getSymbolStatistics().get(right);

            if (leftStats == null || rightStats == null ||
                    isNaN(leftStats.getDistinctValuesCount()) || isNaN(rightStats.getDistinctValuesCount())) {
                return filterStatsByFactor(0.5); //fixme
            }

            double maxDistinctValues = max(leftStats.getDistinctValuesCount(), rightStats.getDistinctValuesCount());
            double minDistinctValues = min(leftStats.getDistinctValuesCount(), rightStats.getDistinctValuesCount());

            double filterRate = 1 / maxDistinctValues * (1 - leftStats.getNullsFraction()) * (1 - rightStats.getNullsFraction());

            SymbolStatsEstimate newRightStats = SymbolStatsEstimate.buildFrom(rightStats)
                    .setNullsFraction(0)
                    .setDistinctValuesCount(minDistinctValues)
                    .build();
            SymbolStatsEstimate newLeftStats = SymbolStatsEstimate.buildFrom(leftStats)
                    .setNullsFraction(0)
                    .setDistinctValuesCount(minDistinctValues)
                    .build();

            return filterStatsByFactor(filterRate)
                    .mapSymbolColumnStatistics(left, x -> newLeftStats)
                    .mapSymbolColumnStatistics(right, x -> newRightStats);
        }
    }
}
