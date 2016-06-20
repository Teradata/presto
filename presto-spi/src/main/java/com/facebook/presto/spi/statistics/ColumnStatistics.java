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

import java.util.Optional;

public class ColumnStatistics
{
    public static final ColumnStatistics EMPTY_STATISTICS = ColumnStatistics.builder().build();

    private final Optional<StatisticsValue<Long>> dataSize;
    private final Optional<StatisticsValue<Long>> rowsCount;
    private final Optional<StatisticsValue<Object>> min;
    private final Optional<StatisticsValue<Object>> max;

    public ColumnStatistics(Optional<StatisticsValue<Long>> dataSize, Optional<StatisticsValue<Long>> rowsCount, Optional<StatisticsValue<Object>> min, Optional<StatisticsValue<Object>> max)
    {
        this.dataSize = dataSize;
        this.rowsCount = rowsCount;
        this.min = min;
        this.max = max;
    }

    public Optional<StatisticsValue<Long>> getDataSize()
    {
        return dataSize;
    }

    public Optional<StatisticsValue<Long>> getRowsCount()
    {
        return rowsCount;
    }

    public Optional<StatisticsValue<Object>> getMin()
    {
        return min;
    }

    public Optional<StatisticsValue<Object>> getMax()
    {
        return max;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private Optional<StatisticsValue<Long>> dataSize = Optional.empty();
        private Optional<StatisticsValue<Long>> rowsCount = Optional.empty();
        private Optional<StatisticsValue<Object>> min = Optional.empty();
        private Optional<StatisticsValue<Object>> max = Optional.empty();

        public Builder setDataSize(StatisticsValue<Long> dataSize)
        {
            this.dataSize = Optional.of(dataSize);
            return this;
        }

        public Builder setRowsCount(StatisticsValue<Long> rowsCount)
        {
            this.rowsCount = Optional.of(rowsCount);
            return this;
        }

        public Builder setMin(StatisticsValue<Object> min)
        {
            this.min = Optional.of(min);
            return this;
        }

        public Builder setMax(StatisticsValue<Object> max)
        {
            this.max = Optional.of(max);
            return this;
        }

        public ColumnStatistics build()
        {
            return new ColumnStatistics(dataSize, rowsCount, min, max);
        }
    }
}
