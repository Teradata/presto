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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class TableStatistics
{
    public static final TableStatistics EMPTY_STATISTICS = TableStatistics.builder().build();

    private final Optional<StatisticsValue<Long>> rowsCount;
    private final Map<String, ColumnStatistics> columnStatisticsMap;

    public TableStatistics(Optional<StatisticsValue<Long>> rowsCount, Map<String, ColumnStatistics> columnStatisticsMap)
    {
        this.rowsCount = requireNonNull(rowsCount, "rowsCount can not be null");
        this.columnStatisticsMap = Collections.unmodifiableMap(new HashMap<>(requireNonNull(columnStatisticsMap, "columnStatisticsMap can not be null")));
    }

    public Optional<StatisticsValue<Long>> getRowsCount()
    {
        return rowsCount;
    }

    public Map<String, ColumnStatistics> getColumnStatistics()
    {
        return columnStatisticsMap;
    }

    public ColumnStatistics getColumnStatistics(String columnName)
    {
        if (columnStatisticsMap.containsKey(columnName)) {
            return columnStatisticsMap.get(columnName);
        }
        return ColumnStatistics.EMPTY_STATISTICS;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private Optional<StatisticsValue<Long>> rowsCount = Optional.empty();
        private Map<String, ColumnStatistics> columnStatisticsMap = new HashMap<>();

        public Builder setRowsCount(StatisticsValue<Long> rowsCount)
        {
            this.rowsCount = Optional.of(rowsCount);
            return this;
        }

        public Builder setColumnStatistics(String columnName, ColumnStatistics columnStatistics)
        {
            this.columnStatisticsMap.put(columnName, columnStatistics);
            return this;
        }

        public TableStatistics build()
        {
            return new TableStatistics(rowsCount, columnStatisticsMap);
        }
    }
}
