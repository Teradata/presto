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

import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.tree.ComparisonExpressionType;

import static com.facebook.presto.cost.FilterStatsCalculator.filterStatsForUnknownExpression;
import static com.facebook.presto.cost.SymbolStatsEstimate.buildFrom;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.POSITIVE_INFINITY;
import static java.lang.Double.isNaN;
import static java.lang.Math.max;
import static java.lang.Math.min;

public class ComparisonStatsCalculator
{
    private final PlanNodeStatsEstimate inputStatistics;

    ComparisonStatsCalculator(PlanNodeStatsEstimate inputStatistics)
    {
        this.inputStatistics = inputStatistics;
    }

    public static double nullsFilterFactor(SymbolStatsEstimate symbolStats)
    {
        StatisticRange statisticRange = new StatisticRange(symbolStats.getLowValue(), symbolStats.getHighValue(), symbolStats.getDistinctValuesCount());

        if (statisticRange.isEmpty()) {
            return 1.0; // If there are no values at all, we can avoid making output row count NaN as we know 1.0 is best value we can serve
        }
        return symbolStats.getNullsFraction();
    }

    public PlanNodeStatsEstimate comparisonSymbolToLiteralStats(Symbol symbol, double doubleLiteral, ComparisonExpressionType type)
    {
        switch (type) {
            case EQUAL:
                return symbolToLiteralEquality(symbol, doubleLiteral);
            case NOT_EQUAL:
                return symbolToLiteralNonEquality(symbol, doubleLiteral);
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
                return symbolToLiteralLessThan(symbol, doubleLiteral);
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
                return symbolToLiteralGreaterThan(symbol, doubleLiteral);
            case IS_DISTINCT_FROM:
                break;
        }

        return filterStatsForUnknownExpression(inputStatistics);
    }

    private PlanNodeStatsEstimate symbolToLiteralGreaterThan(Symbol symbol, double literal)
    {
        SymbolStatsEstimate symbolStats = inputStatistics.getSymbolStatistics(symbol);

        StatisticRange range = new StatisticRange(symbolStats.getLowValue(), symbolStats.getHighValue(), symbolStats.getDistinctValuesCount());
        StatisticRange intersectRange = range.intersect(new StatisticRange(literal, POSITIVE_INFINITY, POSITIVE_INFINITY));

        double filterFactor = range.overlapPercentWith(intersectRange);
        SymbolStatsEstimate symbolNewEstimate =
                SymbolStatsEstimate.builder()
                        .setAverageRowSize(symbolStats.getAverageRowSize())
                        .setDistinctValuesCount(intersectRange.getDistinctValuesCount())
                        .setHighValue(intersectRange.getHigh())
                        .setLowValue(intersectRange.getLow())
                        .setNullsFraction(0.0).build();

        return inputStatistics.mapOutputRowCount(rowCount -> filterFactor * (1 - nullsFilterFactor(symbolStats)) * rowCount)
                .mapSymbolColumnStatistics(symbol, oldStats -> symbolNewEstimate);
    }

    private PlanNodeStatsEstimate symbolToLiteralLessThan(Symbol symbol, double literal)
    {
        SymbolStatsEstimate symbolStats = inputStatistics.getSymbolStatistics(symbol);

        StatisticRange range = new StatisticRange(symbolStats.getLowValue(), symbolStats.getHighValue(), symbolStats.getDistinctValuesCount());
        StatisticRange intersectRange = range.intersect(new StatisticRange(NEGATIVE_INFINITY, literal, POSITIVE_INFINITY));

        double filterFactor = range.overlapPercentWith(intersectRange);
        SymbolStatsEstimate symbolNewEstimate =
                SymbolStatsEstimate.builder()
                        .setAverageRowSize(symbolStats.getAverageRowSize())
                        .setDistinctValuesCount(intersectRange.getDistinctValuesCount())
                        .setHighValue(intersectRange.getHigh())
                        .setLowValue(intersectRange.getLow())
                        .setNullsFraction(0.0).build();

        return inputStatistics.mapOutputRowCount(rowCount -> filterFactor * (1 - nullsFilterFactor(symbolStats)) * rowCount)
                .mapSymbolColumnStatistics(symbol, oldStats -> symbolNewEstimate);
    }

    private PlanNodeStatsEstimate symbolToLiteralNonEquality(Symbol symbol, double literal)
    {
        SymbolStatsEstimate symbolStats = inputStatistics.getSymbolStatistics(symbol);

        StatisticRange range = new StatisticRange(symbolStats.getLowValue(), symbolStats.getHighValue(), symbolStats.getDistinctValuesCount());
        StatisticRange intersectRange = range.intersect(new StatisticRange(literal, literal, 1));

        double filterFactor = 1 - range.overlapPercentWith(intersectRange);

        return inputStatistics.mapOutputRowCount(rowCount -> filterFactor * (1 - nullsFilterFactor(symbolStats)) * rowCount)
                .mapSymbolColumnStatistics(symbol, oldStats -> buildFrom(oldStats)
                        .setNullsFraction(0.0)
                        .setDistinctValuesCount(max(oldStats.getDistinctValuesCount() - 1, 0))
                        .setAverageRowSize(oldStats.getAverageRowSize())
                        .build());
    }

    private PlanNodeStatsEstimate symbolToLiteralEquality(Symbol symbol, double literal)
    {
        SymbolStatsEstimate symbolStats = inputStatistics.getSymbolStatistics(symbol);

        StatisticRange range = new StatisticRange(symbolStats.getLowValue(), symbolStats.getHighValue(), symbolStats.getDistinctValuesCount());
        StatisticRange intersectRange = range.intersect(new StatisticRange(literal, literal, 1));

        double filterFactor = range.overlapPercentWith(intersectRange);
        SymbolStatsEstimate symbolNewEstimate =
                SymbolStatsEstimate.builder()
                        .setAverageRowSize(symbolStats.getAverageRowSize())
                        .setDistinctValuesCount(intersectRange.getDistinctValuesCount())
                        .setHighValue(intersectRange.getHigh())
                        .setLowValue(intersectRange.getLow())
                        .setNullsFraction(0.0).build();

        return inputStatistics.mapOutputRowCount(rowCount -> filterFactor * (1 - nullsFilterFactor(symbolStats)) * rowCount)
                .mapSymbolColumnStatistics(symbol, oldStats -> symbolNewEstimate);
    }

    public PlanNodeStatsEstimate comparisonSymbolToSymbolStats(Symbol left, Symbol right, ComparisonExpressionType type)
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
        return inputStatistics.mapOutputRowCount(size -> size * 0.5);
    }

    private PlanNodeStatsEstimate symbolToSymbolEquality(Symbol left, Symbol right)
    {
        SymbolStatsEstimate leftStats = inputStatistics.getSymbolStatistics(left);
        SymbolStatsEstimate rightStats = inputStatistics.getSymbolStatistics(right);

        if (isNaN(leftStats.getDistinctValuesCount()) || isNaN(rightStats.getDistinctValuesCount())) {
            return inputStatistics.mapOutputRowCount(size -> size * 0.5);
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

        return inputStatistics.mapOutputRowCount(size -> size * filterRate)
                .mapSymbolColumnStatistics(left, oldLeftStats -> newLeftStats)
                .mapSymbolColumnStatistics(right, oldRightStats -> newRightStats);
    }
}
