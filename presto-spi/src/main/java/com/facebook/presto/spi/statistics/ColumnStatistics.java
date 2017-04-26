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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

public final class ColumnStatistics
{
    private static final List<RangeColumnStatistics> SINGLE_UNKNOWN_RANGE_STATISTICS = singletonList(RangeColumnStatistics.builder().build());

    private final List<RangeColumnStatistics> rangeColumnStatistics;

    private ColumnStatistics(List<RangeColumnStatistics> rangeColumnStatistics)
    {
        requireNonNull(rangeColumnStatistics, "rangeColumnStatistics can not be null");
        if (rangeColumnStatistics.size() > 1) {
            // todo add support for multiple ranges.
            throw new IllegalArgumentException("Statistics for multiple ranges are not supported");
        }
        if (rangeColumnStatistics.isEmpty()) {
            rangeColumnStatistics = SINGLE_UNKNOWN_RANGE_STATISTICS;
        }
        this.rangeColumnStatistics = unmodifiableList(rangeColumnStatistics);
    }

    public RangeColumnStatistics getOnlyRangeColumnStatistics()
    {
        return rangeColumnStatistics.get(0);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static final class Builder
    {
        private List<RangeColumnStatistics> rangeColumnStatistics = new ArrayList<>();

        public Builder addRange(Consumer<RangeColumnStatistics.Builder> rangeBuilderConsumer)
        {
            RangeColumnStatistics.Builder rangeBuilder = RangeColumnStatistics.builder();
            rangeBuilderConsumer.accept(rangeBuilder);
            addRange(rangeBuilder.build());
            return this;
        }

        public Builder addRange(Object lowValue, Object highValue, Consumer<RangeColumnStatistics.Builder> rangeBuilderConsumer)
        {
            RangeColumnStatistics.Builder rangeBuilder = RangeColumnStatistics.builder();
            rangeBuilder.setLowValue(Optional.of(lowValue));
            rangeBuilder.setHighValue(Optional.of(highValue));
            rangeBuilderConsumer.accept(rangeBuilder);
            addRange(rangeBuilder.build());
            return this;
        }

        public Builder addRange(RangeColumnStatistics rangeColumnStatistics)
        {
            this.rangeColumnStatistics.add(rangeColumnStatistics);
            return this;
        }

        public ColumnStatistics build()
        {
            return new ColumnStatistics(rangeColumnStatistics);
        }
    }
}
