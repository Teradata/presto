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
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.SymbolReference;

import javax.inject.Inject;

import java.util.Map;
import java.util.OptionalDouble;
import java.util.stream.Stream;

import static com.facebook.presto.cost.SymbolStatsEstimate.buildFrom;
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

        protected PlanNodeStatsEstimate visitNotExpression(NotExpression node, Void context)
        {
            return input;
        }

        @Override
        protected PlanNodeStatsEstimate visitLogicalBinaryExpression(LogicalBinaryExpression node, Void context)
        {
            PlanNodeStatsEstimate left = process(node.getLeft());
            PlanNodeStatsEstimate right = process(node.getRight());
            switch (node.getType()) {
                case AND:
                    return intersectStats(left, right);
                break;
                case OR:
                    return unionStats(left, right);
                break;
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

            return statsBuilder.setOutputRowCount(totalRowsWithOverlaps - intersectingRows).build();
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
            SymbolStatsEstimate symbolStats = input.getSymbolStatistics(symbol);

            StatisticRange range = new StatisticRange(symbolStats.getLowValue(), symbolStats.getHighValue(), symbolStats.getDistinctValuesCount());
            StatisticRange intersectRange = range.intersect(new StatisticRange(literal, POSITIVE_INFINITY, POSITIVE_INFINITY));

            double filterFactor = range.overlapPercentWith(intersectRange);
            SymbolStatsEstimate symbolNewEstimate =
                    SymbolStatsEstimate.builder()
                            .setAverageRowSize(symbolStats.getAverageRowSize())
                            .setDistinctValuesCount(filterFactor * intersectRange.getDistinctValuesCount())
                            .setHighValue(intersectRange.getHigh())
                            .setLowValue(intersectRange.getLow())
                            .setNullsFraction(0.0).build();

            return input.mapOutputRowCount(x -> filterFactor * x)
                    .mapSymbolColumnStatistics(symbol, x -> symbolNewEstimate);
        }

        private PlanNodeStatsEstimate symbolToLiteralLessThan(Symbol symbol, double literal)
        {
            SymbolStatsEstimate symbolStats = input.getSymbolStatistics(symbol);

            StatisticRange range = new StatisticRange(symbolStats.getLowValue(), symbolStats.getHighValue(), symbolStats.getDistinctValuesCount());
            StatisticRange intersectRange = range.intersect(new StatisticRange(NEGATIVE_INFINITY, literal, POSITIVE_INFINITY));

            double filterFactor = range.overlapPercentWith(intersectRange);
            SymbolStatsEstimate symbolNewEstimate =
                    SymbolStatsEstimate.builder()
                            .setAverageRowSize(symbolStats.getAverageRowSize())
                            .setDistinctValuesCount(filterFactor * intersectRange.getDistinctValuesCount())
                            .setHighValue(intersectRange.getHigh())
                            .setLowValue(intersectRange.getLow())
                            .setNullsFraction(0.0).build();

            return input.mapOutputRowCount(x -> filterFactor * x)
                    .mapSymbolColumnStatistics(symbol, x -> symbolNewEstimate);
        }

        private PlanNodeStatsEstimate symbolToLiteralNonEquality(Symbol symbol, double literal)
        {
            SymbolStatsEstimate symbolStats = input.getSymbolStatistics(symbol);

            StatisticRange range = new StatisticRange(symbolStats.getLowValue(), symbolStats.getHighValue(), symbolStats.getDistinctValuesCount());
            StatisticRange intersectRange = range.intersect(new StatisticRange(literal, literal, 1));

            double filterFactor = range.overlapPercentWith(intersectRange);

            return input.mapOutputRowCount(x -> filterFactor * x)
                    .mapSymbolColumnStatistics(symbol, x -> buildFrom(x)
                            .setNullsFraction(0.0)
                            .setDistinctValuesCount(x.getDistinctValuesCount() - 1)
                            .setAverageRowSize(x.getAverageRowSize())
                            .build());
        }

        private PlanNodeStatsEstimate symbolToLiteralEquality(Symbol symbol, double literal)
        {
            SymbolStatsEstimate symbolStats = input.getSymbolStatistics(symbol);

            StatisticRange range = new StatisticRange(symbolStats.getLowValue(), symbolStats.getHighValue(), symbolStats.getDistinctValuesCount());
            StatisticRange intersectRange = range.intersect(new StatisticRange(literal, literal, 1));

            double filterFactor = range.overlapPercentWith(intersectRange);
            SymbolStatsEstimate symbolNewEstimate =
                    SymbolStatsEstimate.builder()
                            .setAverageRowSize(symbolStats.getAverageRowSize())
                            .setDistinctValuesCount(1)
                            .setHighValue(intersectRange.getHigh())
                            .setLowValue(intersectRange.getLow())
                            .setNullsFraction(0.0).build();

            return input.mapOutputRowCount(x -> filterFactor * x)
                        .mapSymbolColumnStatistics(symbol, x -> symbolNewEstimate);
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
            SymbolStatsEstimate leftStats = input.getSymbolStatistics(left);
            SymbolStatsEstimate rightStats = input.getSymbolStatistics(right);

            if (isNaN(leftStats.getDistinctValuesCount()) || isNaN(rightStats.getDistinctValuesCount())) {
                return filterStatsByFactor(0.5); //fixme
            }

            double maxDistinctValues = max(leftStats.getDistinctValuesCount(), rightStats.getDistinctValuesCount());
            double minDistinctValues = min(leftStats.getDistinctValuesCount(), rightStats.getDistinctValuesCount());

            double filterRate = 1 / maxDistinctValues * (1 - leftStats.getNullsFraction()) * (1 - rightStats.getNullsFraction());

            SymbolStatsEstimate newRightStats = buildFrom(rightStats)
                    .setNullsFraction(0)
                    .setDistinctValuesCount(minDistinctValues)
                    .build();
            SymbolStatsEstimate newLeftStats = buildFrom(leftStats)
                    .setNullsFraction(0)
                    .setDistinctValuesCount(minDistinctValues)
                    .build();

            return filterStatsByFactor(filterRate)
                    .mapSymbolColumnStatistics(left, x -> newLeftStats)
                    .mapSymbolColumnStatistics(right, x -> newRightStats);
        }
    }
}
