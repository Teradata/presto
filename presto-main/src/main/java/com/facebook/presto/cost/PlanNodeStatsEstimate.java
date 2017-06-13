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

import com.facebook.presto.spi.statistics.ColumnStatistics;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.spi.statistics.RangeColumnStatistics;
import com.facebook.presto.sql.planner.Symbol;
import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.statistics.Estimate.unknownValue;
import static java.util.Objects.requireNonNull;

public class PlanNodeStatsEstimate
{
    public static final PlanNodeStatsEstimate UNKNOWN_STATS = PlanNodeStatsEstimate.builder().build();
    public static final double DEFAULT_DATA_SIZE_PER_COLUMN = 10;

    private final Estimate outputRowCount;
    private final Map<Symbol, ColumnStatistics> symbolStatistics;

    private PlanNodeStatsEstimate(Estimate outputRowCount, Map<Symbol, ColumnStatistics> symbolStatistics)
    {
        this.outputRowCount = requireNonNull(outputRowCount, "outputRowCount can not be null");
        this.symbolStatistics = symbolStatistics;
    }

    public Estimate getOutputRowCount()
    {
        return outputRowCount;
    }

    public Estimate getOutputSizeInBytes()
    {
        Estimate outputSizeInBytes = Estimate.zeroValue();
        for (Map.Entry<Symbol, ColumnStatistics> entry : symbolStatistics.entrySet()) {
            outputSizeInBytes = outputSizeInBytes.add(getOutputSizeForColumn(entry.getKey(), entry.getValue()));
        }
        return outputSizeInBytes;
    }

    private Estimate getOutputSizeForColumn(Symbol key, ColumnStatistics value)
    {
        Estimate dataSizeForColumn = value.getOnlyRangeColumnStatistics().getDataSize();
        if (dataSizeForColumn.isValueUnknown()) {
            // TODO take into consderation data type of column
            dataSizeForColumn = outputRowCount.map(count -> count * DEFAULT_DATA_SIZE_PER_COLUMN);
        }
        return dataSizeForColumn;
    }

    public PlanNodeStatsEstimate mapOutputRowCount(Function<Double, Double> mappingFunction)
    {
        return buildFrom(this).setOutputRowCount(outputRowCount.map(mappingFunction)).build();
    }

    public PlanNodeStatsEstimate mapSymbolColumnStatistics(Symbol symbol, Function<ColumnStatistics, ColumnStatistics> mappingFunction)
    {
        return buildFrom(this)
                .setSymbolStatistics(symbolStatistics.entrySet().stream()
                        .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                e -> {
                                    if (e.getKey().equals(symbol)) {
                                        return mappingFunction.apply(e.getValue());
                                    }
                                    return e.getValue();
                                })))
                .build();
    }

    public PlanNodeStatsEstimate add(PlanNodeStatsEstimate other)
    {
        ImmutableMap.Builder<Symbol, ColumnStatistics> symbolsStatsBuilder = ImmutableMap.builder();
        symbolsStatsBuilder.putAll(getSymbolStatistics()).putAll(other.getSymbolStatistics()); // This may not count all information

        PlanNodeStatsEstimate.Builder statsBuilder = PlanNodeStatsEstimate.builder();
        return statsBuilder.setSymbolStatistics(symbolsStatsBuilder.build())
                .setOutputRowCount(getOutputRowCount().add(other.getOutputRowCount()))
                .build();
    }

    public RangeColumnStatistics getOnlyRangeStats(Symbol symbol)
    {
        return symbolStatistics.get(symbol).getOnlyRangeColumnStatistics();
    }

    public boolean containsSymbolStats(Symbol symbol)
    {
        return symbolStatistics.containsKey(symbol);
    }

    @Override
    public String toString()
    {
        return "PlanNodeStatsEstimate{outputRowCount=" + outputRowCount + '}';
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
        PlanNodeStatsEstimate that = (PlanNodeStatsEstimate) o;
        return Objects.equals(outputRowCount, that.outputRowCount);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(outputRowCount);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static Builder buildFrom(PlanNodeStatsEstimate other)
    {
        return builder().setOutputRowCount(other.getOutputRowCount())
                .setSymbolStatistics(other.getSymbolStatistics());
    }

    public Map<Symbol, ColumnStatistics> getSymbolStatistics()
    {
        return symbolStatistics;
    }

    public ColumnStatistics getSymbolStatistics(Symbol symbol)
    {
        return symbolStatistics.getOrDefault(symbol, ColumnStatistics.UNKNOWN_COLUMN_STATISTICS);
    }

    public static final class Builder
    {
        private Estimate outputRowCount = unknownValue();
        private Map<Symbol, ColumnStatistics> symbolStatistics = new HashMap<>();

        public Builder setOutputRowCount(Estimate outputRowCount)
        {
            this.outputRowCount = outputRowCount;
            return this;
        }

        public Builder setSymbolStatistics(Map<Symbol, ColumnStatistics> symbolStatistics)
        {
            this.symbolStatistics = new HashMap<>(symbolStatistics);
            return this;
        }

        public Builder addSymbolStatistics(Symbol symbol, Consumer<ColumnStatistics.Builder> statisticsBuilderConsumer)
        {
            ColumnStatistics.Builder statisticsBuilder = ColumnStatistics.builder();
            statisticsBuilderConsumer.accept(statisticsBuilder);
            return addSymbolStatistics(symbol, statisticsBuilder.build());
        }

        public Builder addSymbolStatistics(Symbol symbol, ColumnStatistics statistics)
        {
            this.symbolStatistics.put(symbol, statistics);
            return this;
        }

        public PlanNodeStatsEstimate build()
        {
            return new PlanNodeStatsEstimate(outputRowCount, symbolStatistics);
        }
    }
}
