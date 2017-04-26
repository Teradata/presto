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
package com.facebook.presto.spi.statistics;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spi.statistics.Estimate.unknownValue;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;

public class RangeColumnStatistics
{
    private static final String DATA_SIZE_STATISTIC_KEY = "data_size";
    private static final String NULLS_FRACTION_STATISTIC_KEY = "nulls_fraction";
    private static final String DISTINCT_VALUES_STATITIC_KEY = "distinct_values_count";

    private final Optional<Object> lowValue;
    private final Optional<Object> highValue;
    private final Map<String, Estimate> statistics;

    public RangeColumnStatistics(
            Optional<Object> lowValue,
            Optional<Object> highValue,
            Estimate dataSize,
            Estimate nullsFraction,
            Estimate distinctValuesCount)
    {
        this.lowValue = requireNonNull(lowValue, "lowValue can not be null");
        this.highValue = requireNonNull(highValue, "highValue can not be null");
        requireNonNull(dataSize, "dataSize can not be null");
        requireNonNull(nullsFraction, "nullsFraction can not be null");
        requireNonNull(distinctValuesCount, "distinctValuesCount can not be null");
        this.statistics = createStatisticsMap(dataSize, nullsFraction, distinctValuesCount);
    }

    private static Map<String, Estimate> createStatisticsMap(
            Estimate dataSize,
            Estimate nullsFraction,
            Estimate distinctValuesCount)
    {
        Map<String, Estimate> statistics = new HashMap<>();
        statistics.put(DATA_SIZE_STATISTIC_KEY, dataSize);
        statistics.put(NULLS_FRACTION_STATISTIC_KEY, nullsFraction);
        statistics.put(DISTINCT_VALUES_STATITIC_KEY, distinctValuesCount);
        return unmodifiableMap(statistics);
    }

    public Optional<Object> getLowValue()
    {
        return lowValue;
    }

    public Optional<Object> getHighValue()
    {
        return highValue;
    }

    public Estimate getDataSize()
    {
        return statistics.get(DATA_SIZE_STATISTIC_KEY);
    }

    public Estimate getNullsFraction()
    {
        return statistics.get(NULLS_FRACTION_STATISTIC_KEY);
    }

    public Estimate getDistinctValuesCount()
    {
        return statistics.get(DISTINCT_VALUES_STATITIC_KEY);
    }

    public Map<String, Estimate> getStatistics()
    {
        return statistics;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static final class Builder
    {
        private Optional<Object> lowValue = Optional.empty();
        private Optional<Object> highValue = Optional.empty();
        private Estimate dataSize = unknownValue();
        private Estimate nullsFraction = unknownValue();
        private Estimate distinctValuesCount = unknownValue();

        public Builder setLowValue(Optional<Object> lowValue)
        {
            this.lowValue = lowValue;
            return this;
        }

        public Builder setHighValue(Optional<Object> highValue)
        {
            this.highValue = highValue;
            return this;
        }

        public Builder setDataSize(Estimate dataSize)
        {
            this.dataSize = requireNonNull(dataSize, "dataSize can not be null");
            return this;
        }

        public Builder setNullsFraction(Estimate nullsFraction)
        {
            this.nullsFraction = nullsFraction;
            return this;
        }

        public Builder setDistinctValuesCount(Estimate distinctValuesCount)
        {
            this.distinctValuesCount = distinctValuesCount;
            return this;
        }

        public RangeColumnStatistics build()
        {
            return new RangeColumnStatistics(lowValue, highValue, dataSize, nullsFraction, distinctValuesCount);
        }
    }
}
