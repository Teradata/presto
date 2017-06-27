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

import java.util.stream.Stream;

import static com.facebook.presto.cost.ComparisonStatsCalculator.nullsFilterFactor;
import static java.lang.Double.max;
import static java.lang.Double.min;

public class LogicalExpressionStatsCalculator
{
    private final PlanNodeStatsEstimate inputStatistics;

    public LogicalExpressionStatsCalculator(PlanNodeStatsEstimate inputStatistics)
    {
        this.inputStatistics = inputStatistics;
    }

    public PlanNodeStatsEstimate negateStats(PlanNodeStatsEstimate innerStats)
    {
        return subtractStats(inputStatistics, innerStats);
    }

    public static PlanNodeStatsEstimate subtractStats(PlanNodeStatsEstimate left, PlanNodeStatsEstimate right)
    {
        PlanNodeStatsEstimate.Builder statsBuilder = PlanNodeStatsEstimate.builder();
        double newRowCount = left.getOutputRowCount() - right.getOutputRowCount();

        Stream.concat(left.getSymbolsWithKnownStatistics().stream(), right.getSymbolsWithKnownStatistics().stream())
                .forEach(symbol -> {
                    statsBuilder.addSymbolStatistics(symbol,
                            subtractColumnStats(left.getSymbolStatistics(symbol),
                                    left.getOutputRowCount(),
                                    right.getSymbolStatistics(symbol),
                                    right.getOutputRowCount(),
                                    newRowCount));
                });

        return statsBuilder.setOutputRowCount(newRowCount).build();
    }

    private static SymbolStatsEstimate subtractColumnStats(SymbolStatsEstimate leftStats, double leftRowCount, SymbolStatsEstimate rightStats, double rightRowCount, double newRowCount)
    {
        if (leftStats.equals(rightStats)) {
            return leftStats; // HACK - there should be other way to determine if symbol was touched at all. This way not (symbol1 = symbol1) will not work.
        }
        StatisticRange leftRange = new StatisticRange(leftStats.getLowValue(), leftStats.getHighValue(), leftStats.getDistinctValuesCount());
        StatisticRange rightRange = new StatisticRange(rightStats.getLowValue(), rightStats.getHighValue(), rightStats.getDistinctValuesCount());

        StatisticRange subtracted = leftRange.subtract(rightRange);
        double nullsCountLeft = leftStats.getNullsFraction() * leftRowCount;
        double nullsCountRight = rightStats.getNullsFraction() * rightRowCount;
        double totalSizeLeft = leftRowCount * leftStats.getAverageRowSize();
        double totalSizeRight = rightRowCount * rightStats.getAverageRowSize();

        return SymbolStatsEstimate.builder()
                .setDistinctValuesCount(leftStats.getDistinctValuesCount() - rightStats.getDistinctValuesCount())
                .setHighValue(subtracted.getHigh())
                .setLowValue(subtracted.getLow())
                .setAverageRowSize((totalSizeLeft - totalSizeRight) / newRowCount)
                .setNullsFraction((nullsCountLeft - nullsCountRight) / newRowCount)
                .build();
    }

    public PlanNodeStatsEstimate unionStats(PlanNodeStatsEstimate left, PlanNodeStatsEstimate right)
    {
        PlanNodeStatsEstimate.Builder statsBuilder = PlanNodeStatsEstimate.builder();

        double totalRowsWithOverlaps = left.getOutputRowCount() + right.getOutputRowCount();
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
        if (leftStats.equals(rightStats)) {
            return leftStats;
        }
        StatisticRange leftRange = new StatisticRange(leftStats.getLowValue(), leftStats.getHighValue(), leftStats.getDistinctValuesCount());
        StatisticRange rightRange = new StatisticRange(rightStats.getLowValue(), rightStats.getHighValue(), rightStats.getDistinctValuesCount());

        StatisticRange union = leftRange.union(rightRange);
        double nullsCountLeft = leftStats.getNullsFraction() * rightRows;
        double nullsCountRight = rightStats.getNullsFraction() * leftRows;

        return SymbolStatsEstimate.builder()
                .setDistinctValuesCount(union.getDistinctValuesCount())
                .setHighValue(union.getHigh())
                .setLowValue(union.getLow())
                .setAverageRowSize((leftStats.getAverageRowSize() + rightStats.getAverageRowSize()) / 2) // FIXME, weights to average. left and right should be equal in most cases anyway
                .setNullsFraction(max(nullsCountLeft, nullsCountRight) / newRowCount)
                .build();
    }

    public PlanNodeStatsEstimate intersectStats(PlanNodeStatsEstimate left, PlanNodeStatsEstimate right)
    {
        PlanNodeStatsEstimate.Builder statsBuilder = PlanNodeStatsEstimate.builder();

        double newRowCount = Stream.concat(left.getSymbolsWithKnownStatistics().stream(), right.getSymbolsWithKnownStatistics().stream())
                .mapToDouble(symbol ->
                        rowsOfIntersect(left.getSymbolStatistics(symbol),
                                left.getOutputRowCount(),
                                right.getSymbolStatistics(symbol),
                                right.getOutputRowCount()))
                .min().orElse(0.0);

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

    private double rowsOfIntersect(SymbolStatsEstimate leftStats, double leftRows, SymbolStatsEstimate rightStats, double rightRows)
    {
        double nullsCountLeft = nullsFilterFactor(leftStats) * leftRows;
        double nullsCountRight = nullsFilterFactor(rightStats) * rightRows;
        double nonNullRowCount = nonNullRowsOfIntersect(leftStats, leftRows, rightStats, rightRows);

        return nonNullRowCount + min(nullsCountLeft, nullsCountRight);
    }

    private double nonNullRowsOfIntersect(SymbolStatsEstimate leftStats, double leftRows, SymbolStatsEstimate rightStats, double rightRows)
    {
        StatisticRange leftRange = new StatisticRange(leftStats.getLowValue(), leftStats.getHighValue(), leftStats.getDistinctValuesCount());
        StatisticRange rightRange = new StatisticRange(rightStats.getLowValue(), rightStats.getHighValue(), rightStats.getDistinctValuesCount());
        double notNullLeftRows = leftRows * (1 - nullsFilterFactor(leftStats));
        double notNullRightRows = rightRows * (1 - nullsFilterFactor(rightStats));

        return min(max(leftRange.overlapPercentWith(rightRange) * notNullLeftRows, rightRange.overlapPercentWith(leftRange) * notNullRightRows),
                   min(notNullLeftRows, notNullRightRows));
    }

    private SymbolStatsEstimate intersectColumnStats(SymbolStatsEstimate leftStats, double leftRows, SymbolStatsEstimate rightStats, double rightRows, double newRowCount)
    {
        StatisticRange leftRange = new StatisticRange(leftStats.getLowValue(), leftStats.getHighValue(), leftStats.getDistinctValuesCount());
        StatisticRange rightRange = new StatisticRange(rightStats.getLowValue(), rightStats.getHighValue(), rightStats.getDistinctValuesCount());

        StatisticRange intersect = leftRange.intersect(rightRange);
        double nullsCountLeft = nullsFilterFactor(leftStats) * leftRows;
        double nullsCountRight = nullsFilterFactor(rightStats) * rightRows;

        return SymbolStatsEstimate.builder()
                .setDistinctValuesCount(intersect.getDistinctValuesCount())
                .setHighValue(intersect.getHigh())
                .setLowValue(intersect.getLow())
                .setAverageRowSize((leftStats.getAverageRowSize() + rightStats.getAverageRowSize()) / 2) // FIXME (weights to average) left and right should be equal in most cases anyway
                .setNullsFraction(min(nullsCountLeft, nullsCountRight) / newRowCount)
                .build();
    }
}
