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
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.SymbolReference;

import javax.inject.Inject;

import java.util.Map;
import java.util.stream.Stream;

import static java.lang.Double.NaN;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.String.format;
import static org.weakref.jmx.internal.guava.base.Preconditions.checkState;

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

    public static PlanNodeStatsEstimate filterStatsForUnknownExpression(PlanNodeStatsEstimate inputStatistics)
    {
        return inputStatistics.mapOutputRowCount(size -> size * 0.5);
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
            return filterStatsForUnknownExpression(input);
        }

        protected PlanNodeStatsEstimate visitNotExpression(NotExpression node, Void context)
        {
            PlanNodeStatsEstimate innerStats = process(node.getValue());

            PlanNodeStatsEstimate.Builder statsBuilder = PlanNodeStatsEstimate.builder();
            double newRowCount = input.getOutputRowCount() - innerStats.getOutputRowCount();

            Stream.concat(innerStats.getSymbolsWithKnownStatistics().stream(), input.getSymbolsWithKnownStatistics().stream())
                    .forEach(symbol -> {
                        statsBuilder.addSymbolStatistics(symbol,
                                subtractStats(input.getSymbolStatistics(symbol),
                                        input.getOutputRowCount(),
                                        innerStats.getSymbolStatistics(symbol),
                                        innerStats.getOutputRowCount(),
                                        newRowCount));
                    });

            return statsBuilder.setOutputRowCount(newRowCount).build();
        }

        private SymbolStatsEstimate subtractStats(SymbolStatsEstimate leftStats, double leftRowCount, SymbolStatsEstimate rightStats, double rightRowCount, double newRowCount)
        {
            StatisticRange leftRange = new StatisticRange(leftStats.getLowValue(), leftStats.getHighValue(), leftStats.getDistinctValuesCount());
            StatisticRange rightRange = new StatisticRange(rightStats.getLowValue(), rightStats.getHighValue(), rightStats.getDistinctValuesCount());

            StatisticRange union = leftRange.subtract(rightRange);
            double nullsCountLeft = leftStats.getNullsFraction() * leftRowCount;
            double nullsCountRight = rightStats.getNullsFraction() * rightRowCount;
            double innerFilterFactor = nullsCountRight / nullsCountLeft;

            return SymbolStatsEstimate.builder()
                    .setDistinctValuesCount(union.getDistinctValuesCount())
                    .setHighValue(union.getHigh())
                    .setLowValue(union.getLow())
                    .setAverageRowSize(leftStats.getAverageRowSize() * innerFilterFactor - (rightStats.getAverageRowSize() - leftStats.getAverageRowSize()) * (1 - innerFilterFactor)) //FIXME? // left and right should be equal in most cases anyway
                    .setNullsFraction((nullsCountLeft - nullsCountRight) / newRowCount)
                    .build();
        }

        @Override
        protected PlanNodeStatsEstimate visitLogicalBinaryExpression(LogicalBinaryExpression node, Void context)
        {
            PlanNodeStatsEstimate left = process(node.getLeft());
            PlanNodeStatsEstimate right = process(node.getRight());
            switch (node.getType()) {
                case AND:
                    return intersectStats(left, right);
                case OR:
                    return unionStats(left, right);
                default:
                    checkState(false, format("Unimplemented logical binary operator expression %s", node.getType()));
                    return PlanNodeStatsEstimate.UNKNOWN_STATS;
            }
        }

        private PlanNodeStatsEstimate unionStats(PlanNodeStatsEstimate left, PlanNodeStatsEstimate right)
        {
            PlanNodeStatsEstimate.Builder statsBuilder = PlanNodeStatsEstimate.builder();

            double leftFilterFactor = left.getOutputRowCount() / input.getOutputRowCount();
            double rightFilterFactor = right.getOutputRowCount() / input.getOutputRowCount();
            double totalRowsWithOverlaps = (leftFilterFactor + rightFilterFactor) * input.getOutputRowCount();
            double intersectingRows = intersectStats(left, right).getOutputRowCount();
            double newRowCount = totalRowsWithOverlaps - intersectingRows;

            Stream.concat(left.getSymbolsWithKnownStatistics().stream(), right.getSymbolsWithKnownStatistics().stream())
                    .forEach(symbol -> {
                        statsBuilder.addSymbolStatistics(symbol,
                                unionColumnStats(left.getSymbolStatistics(symbol),
                                        left.getOutputRowCount(),
                                        right.getSymbolStatistics(symbol),
                                        right.getOutputRowCount(), newRowCount));
                    });

            return statsBuilder.setOutputRowCount(newRowCount).build();
        }

        private SymbolStatsEstimate unionColumnStats(SymbolStatsEstimate leftStats, double leftRows, SymbolStatsEstimate rightStats, double rightRows, double newRowCount)
        {
            StatisticRange leftRange = new StatisticRange(leftStats.getLowValue(), leftStats.getHighValue(), leftStats.getDistinctValuesCount());
            StatisticRange rightRange = new StatisticRange(rightStats.getLowValue(), rightStats.getHighValue(), rightStats.getDistinctValuesCount());

            StatisticRange union = leftRange.union(rightRange);
            double nullsCountLeft = leftStats.getNullsFraction() * rightRows;
            double nullsCountRight = rightStats.getNullsFraction() * leftRows;

            return SymbolStatsEstimate.builder()
                    .setDistinctValuesCount(union.getDistinctValuesCount())
                    .setHighValue(union.getHigh())
                    .setLowValue(union.getLow())
                    .setAverageRowSize((leftStats.getAverageRowSize() + rightStats.getAverageRowSize()) / 2) // left and right should be equal in most cases anyway
                    .setNullsFraction(max(nullsCountLeft, nullsCountRight) / newRowCount)
                    .build();
        }

        private PlanNodeStatsEstimate intersectStats(PlanNodeStatsEstimate left, PlanNodeStatsEstimate right)
        {
            PlanNodeStatsEstimate.Builder statsBuilder = PlanNodeStatsEstimate.builder();

            double leftFilterFactor = left.getOutputRowCount() / input.getOutputRowCount();
            double rightFilterFactor = right.getOutputRowCount() / input.getOutputRowCount();
            double newRowCount = leftFilterFactor * rightFilterFactor * input.getOutputRowCount();

            Stream.concat(left.getSymbolsWithKnownStatistics().stream(), right.getSymbolsWithKnownStatistics().stream())
                    .forEach(symbol -> {
                        statsBuilder.addSymbolStatistics(symbol,
                                intersectColumnStats(left.getSymbolStatistics(symbol),
                                        left.getOutputRowCount(),
                                        right.getSymbolStatistics(symbol),
                                        right.getOutputRowCount(), newRowCount));
                    });

            return statsBuilder.setOutputRowCount(newRowCount).build();
        }

        private SymbolStatsEstimate intersectColumnStats(SymbolStatsEstimate leftStats, double leftRows, SymbolStatsEstimate rightStats, double rightRows, double newRowCount)
        {
            StatisticRange leftRange = new StatisticRange(leftStats.getLowValue(), leftStats.getHighValue(), leftStats.getDistinctValuesCount());
            StatisticRange rightRange = new StatisticRange(rightStats.getLowValue(), rightStats.getHighValue(), rightStats.getDistinctValuesCount());

            StatisticRange intersect = leftRange.intersect(rightRange);
            double nullsCountLeft = leftStats.getNullsFraction() * rightRows;
            double nullsCountRight = rightStats.getNullsFraction() * leftRows;

            return SymbolStatsEstimate.builder()
                    .setDistinctValuesCount(intersect.getDistinctValuesCount())
                    .setHighValue(intersect.getHigh())
                    .setLowValue(intersect.getLow())
                    .setAverageRowSize((leftStats.getAverageRowSize() + rightStats.getAverageRowSize()) / 2) // left and right should be equal in most cases anyway
                    .setNullsFraction(min(nullsCountLeft, nullsCountRight) / newRowCount)
                    .build();
        }

        @Override
        protected PlanNodeStatsEstimate visitComparisonExpression(ComparisonExpression node, Void context)
        {
            ComparisonStatsCalculator comparisonStatsCalculator = new ComparisonStatsCalculator(input);

            // FIXME left and right might not be exactly SymbolReference and Literal
            if (node.getLeft() instanceof SymbolReference && node.getRight() instanceof SymbolReference) {
                return comparisonStatsCalculator.comparisonSymbolToSymbolStats(
                        Symbol.from(node.getLeft()),
                        Symbol.from(node.getRight()),
                        node.getType()
                );
            }
            else if (node.getLeft() instanceof SymbolReference && node.getRight() instanceof Literal) {
                Symbol symbol = Symbol.from(node.getLeft());
                return comparisonStatsCalculator.comparisonSymbolToLiteralStats(
                        symbol,
                        doubleValueFromLiteral(types.get(symbol), (Literal) node.getRight()),
                        node.getType()
                );
            }
            else if (node.getLeft() instanceof Literal && node.getRight() instanceof SymbolReference) {
                Symbol symbol = Symbol.from(node.getRight());
                return comparisonStatsCalculator.comparisonSymbolToLiteralStats(
                        symbol,
                        doubleValueFromLiteral(types.get(symbol), (Literal) node.getLeft()),
                        node.getType().flip()
                );
            }
            else {
                return filterStatsForUnknownExpression(input);
            }
        }

        private double doubleValueFromLiteral(Type type, Literal literal)
        {
            Object literalValue = LiteralInterpreter.evaluate(metadata, session.toConnectorSession(), literal);
            TypeStatOperatorCaller operatorCaller = new TypeStatOperatorCaller(type, metadata.getFunctionRegistry(), session.toConnectorSession());
            return operatorCaller.translateToDouble(literalValue).orElse(NaN);
        }

        @Override
        protected PlanNodeStatsEstimate visitBooleanLiteral(BooleanLiteral node, Void context)
        {
            if (node.getValue()) {
                return input;
            }
            else {
                return input.mapOutputRowCount(size -> size * 0.0);
            }
        }
    }
}
