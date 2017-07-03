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

import com.google.common.collect.Range;

import static com.facebook.presto.util.MoreMath.sum;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Double.isFinite;
import static java.lang.Math.max;
import static java.lang.Math.min;

public class PlanNodeStatsEstimates
{
    private PlanNodeStatsEstimates()
    {
    }

    public static PlanNodeStatsEstimate union(PlanNodeStatsEstimate first, PlanNodeStatsEstimate second)
    {
        checkArgument(first.getSymbolsWithKnownStatistics().equals(second.getSymbolsWithKnownStatistics()));

        PlanNodeStatsEstimate.Builder estimate = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(first.getOutputRowCount() + second.getOutputRowCount());

        first.getSymbolsWithKnownStatistics().stream()
                .forEach(symbol -> estimate.addSymbolStatistics(symbol, union(
                        first.getSymbolStatistics(symbol),
                        first.getOutputRowCount(),
                        second.getSymbolStatistics(symbol),
                        second.getOutputRowCount())));
        return estimate.build();
    }

    private static SymbolStatsEstimate union(SymbolStatsEstimate first, double firstRowCount, SymbolStatsEstimate second, double secondRowCount)
    {
        final double low;
        final double high;
        if (first.getNullsFraction() == 1) {
            low = second.getLowValue();
            high = second.getHighValue();
        }
        else if (second.getNullsFraction() == 1) {
            low = first.getLowValue();
            high = first.getHighValue();
        }
        else {
            low = min(first.getLowValue(), second.getLowValue());
            high = max(first.getHighValue(), second.getHighValue());
        }

        SymbolStatsEstimate.Builder estimate = SymbolStatsEstimate.builder()
                .setLowValue(low)
                .setHighValue(high)
                .setNullsFraction(
                        sum(first.getNullsFraction() * firstRowCount, second.getNullsFraction() * secondRowCount)
                                / sum(firstRowCount, secondRowCount));

        if (isFinite(low) && isFinite(high)) {
            double distinctValuesCount;
            if (first.getNullsFraction() == 1) {
                distinctValuesCount = second.getDistinctValuesCount();
            }
            else if (second.getNullsFraction() == 1) {
                distinctValuesCount = first.getDistinctValuesCount();
            }
            else {
                distinctValuesCount = sum(first.getDistinctValuesCount(), second.getDistinctValuesCount());
            }
            double commonDomainLength = commonDomainLength(first, second);
            double firstDomainLength = first.getDomainLength();
            double secondDomainLength = second.getDomainLength();
            if (commonDomainLength > 0 && firstDomainLength > 0 && secondDomainLength > 0) {
                double firstDistinctValuesDensity = first.getDistinctValuesCount() / firstDomainLength;
                double secondDistinctValuesDensity = second.getDistinctValuesCount() / secondDomainLength;
                // remove duplicate values
                distinctValuesCount -= min(firstDistinctValuesDensity, secondDistinctValuesDensity) * commonDomainLength;
            }
            estimate.setDistinctValuesCount(distinctValuesCount);
        }

        return estimate.build();
    }

    private static double commonDomainLength(SymbolStatsEstimate first, SymbolStatsEstimate second)
    {
        Range<Double> firstRange = asRange(first);
        Range<Double> secondRange = asRange(second);
        if (!firstRange.isConnected(secondRange)) {
            return 0;
        }
        Range<Double> intersection = firstRange.intersection(secondRange);
        if (!intersection.hasLowerBound() || !intersection.hasUpperBound()) {
            return 0;
        }
        return intersection.upperEndpoint() - intersection.lowerEndpoint();
    }

    private static Range<Double> asRange(SymbolStatsEstimate estimate)
    {
        return Range.closed(estimate.getLowValue(), estimate.getHighValue());
    }
}
