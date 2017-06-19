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

import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Double.NaN;
import static java.lang.Double.isNaN;

public class SymbolStatsEstimate
{
    public static final SymbolStatsEstimate UNKNOWN_STATS = SymbolStatsEstimate.builder().build();

    // for now we support only types which map to real domain naturally and keep low/high value as doulbe in stats.
    private final double lowValue;
    private final double highValue;
    private final double nullsFraction;
    private final double dataSize;
    private final double distinctValuesCount;

    public SymbolStatsEstimate(double lowValue, double highValue, double nullsFraction, double dataSize, double distinctValuesCount)
    {
        checkArgument(lowValue <= highValue || (isNaN(lowValue) && isNaN(highValue)), "lowValue must be less than or equal highValue");
        this.lowValue = lowValue;
        this.highValue = highValue;
        this.nullsFraction = nullsFraction;
        this.dataSize = dataSize;
        this.distinctValuesCount = distinctValuesCount;
    }

    public double getLowValue()
    {
        return lowValue;
    }

    public double getHighValue()
    {
        return highValue;
    }

    public boolean hasNonNullValues()
    {
        return distinctValuesCount != 0 || (!isNaN(lowValue) && !isNaN(highValue));
    }

    public double getNullsFraction()
    {
        return nullsFraction;
    }

    public double getDataSize()
    {
        return dataSize;
    }

    public double getDistinctValuesCount()
    {
        return distinctValuesCount;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SymbolStatsEstimate that = (SymbolStatsEstimate) o;
        return Double.compare(that.nullsFraction, nullsFraction) == 0 &&
                Double.compare(that.dataSize, dataSize) == 0 &&
                Double.compare(that.distinctValuesCount, distinctValuesCount) == 0 &&
                Objects.equals(lowValue, that.lowValue) &&
                Objects.equals(highValue, that.highValue);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(lowValue, highValue, nullsFraction, dataSize, distinctValuesCount);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static Builder buildFrom(SymbolStatsEstimate other)
    {
        return builder()
                .setLowValue(other.getLowValue())
                .setHighValue(other.getHighValue())
                .setNullsFraction(other.getNullsFraction())
                .setDataSize(other.getDataSize())
                .setDistinctValuesCount(other.getDistinctValuesCount());
    }

    public static final class Builder
    {
        private double lowValue = Double.NEGATIVE_INFINITY;
        private double highValue = Double.POSITIVE_INFINITY;
        private double nullsFraction = NaN;
        private double dataSize = NaN;
        private double distinctValuesCount = NaN;

        public Builder setLowValue(double lowValue)
        {
            this.lowValue = lowValue;
            return this;
        }

        public Builder setHighValue(double highValue)
        {
            this.highValue = highValue;
            return this;
        }

        public Builder setNullsFraction(double nullsFraction)
        {
            this.nullsFraction = nullsFraction;
            return this;
        }

        public Builder setDataSize(double dataSize)
        {
            this.dataSize = dataSize;
            return this;
        }

        public Builder setDistinctValuesCount(double distinctValuesCount)
        {
            this.distinctValuesCount = distinctValuesCount;
            return this;
        }

        public SymbolStatsEstimate build()
        {
            return new SymbolStatsEstimate(lowValue, highValue, nullsFraction, dataSize, distinctValuesCount);
        }
    }
}
