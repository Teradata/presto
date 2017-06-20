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

import java.util.Optional;

import static com.facebook.presto.cost.SimplifiedHistogramStats.SimplifedHistogramStatsBuilder.aSimplifedHistogramStats;

public class SimplifiedHistogramStats
{
    private final StatsHistogramRange range;

    private final double dataSize;
    private final double nullsFraction;
    private final double distinctValuesCount;
    private final double filteredPercent; // HACK!

    private SimplifiedHistogramStats(StatsHistogramRange range, double dataSize, double nullsFraction, double distinctValuesCount, double filteredPercent)
    {
        this.range = range;
        this.dataSize = dataSize;
        this.nullsFraction = nullsFraction;
        this.distinctValuesCount = distinctValuesCount;
        this.filteredPercent = filteredPercent;
    }

    public SymbolStatsEstimate toSymbolsStatistics()
    {
        return SymbolStatsEstimate.builder()
                .setNullsFraction(nullsFraction)
                .setLowValue(range.getLow())
                .setHighValue(range.getHigh())
                .setDistinctValuesCount(distinctValuesCount)
                .setDataSize(dataSize)
                .build();
    }

    public static SimplifiedHistogramStats of(SymbolStatsEstimate symbolStats)
    {
        SimplifiedHistogramStats newStats = new SimplifiedHistogramStats(
                new StatsHistogramRange(symbolStats.getLowValue(), symbolStats.getHighValue(), Optional.empty()),
                symbolStats.getDataSize(),
                symbolStats.getNullsFraction(),
                symbolStats.getDistinctValuesCount(),
                Double.NaN);
        newStats.range.setStatsParent(Optional.of(newStats));
        return newStats;
    }

    public double getLow()
    {
        return range.getLow();
    }

    public double getHigh()
    {
        return range.getHigh();
    }

    public SimplifiedHistogramStats intersect(StatsHistogramRange other)
    {
        StatsHistogramRange newRange = range.intersect(other);
        double percentOfDataLeft = range.overlapPartOf(newRange);

        return aSimplifedHistogramStats().withRange(newRange)
                .withDataSize(dataSize * percentOfDataLeft)
                .withNullsFraction(0)
                .withDistinctValuesCount(distinctValuesCount * percentOfDataLeft) // FIXME nulls not counted!
                .withFilteredPercent(percentOfDataLeft)
                .build();
    }

    public double getDistinctValuesCount()
    {
        return distinctValuesCount;
    }

    public double getNullsFraction()
    {
        return nullsFraction;
    }

    public double getFilteredPercent()
    {
        return filteredPercent;
    }

    public StatsHistogramRange getRange()
    {
        return range;
    }

    public static final class SimplifedHistogramStatsBuilder
    {
        private StatsHistogramRange range;
        private double dataSize;
        private double nullsFraction;
        private double distinctValuesCount;
        private double filteredPercent; //FIXME HACK!

        private SimplifedHistogramStatsBuilder() {}

        public static SimplifedHistogramStatsBuilder aSimplifedHistogramStats()
        {
            return new SimplifedHistogramStatsBuilder();
        }

        public SimplifedHistogramStatsBuilder withRange(StatsHistogramRange range)
        {
            this.range = range;
            return this;
        }

        public SimplifedHistogramStatsBuilder withDataSize(double dataSize)
        {
            this.dataSize = dataSize;
            return this;
        }

        public SimplifedHistogramStatsBuilder withNullsFraction(double nullsFraction)
        {
            this.nullsFraction = nullsFraction;
            return this;
        }

        public SimplifedHistogramStatsBuilder withDistinctValuesCount(double distinctValuesCount)
        {
            this.distinctValuesCount = distinctValuesCount;
            return this;
        }

        public SimplifedHistogramStatsBuilder withFilteredPercent(double filtered) // HACK!
        {
            this.filteredPercent = filtered;
            return this;
        }

        public SimplifiedHistogramStats build()
        {
            SimplifiedHistogramStats simplifiedHistogramStats = new SimplifiedHistogramStats(
                    range, dataSize, nullsFraction, distinctValuesCount, filteredPercent);
            simplifiedHistogramStats.range.setStatsParent(Optional.of(simplifiedHistogramStats));
            return simplifiedHistogramStats;
        }
    }
}
