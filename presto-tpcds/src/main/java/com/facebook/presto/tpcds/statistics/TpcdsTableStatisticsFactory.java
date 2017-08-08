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

package com.facebook.presto.tpcds.statistics;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.statistics.ColumnStatistics;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.teradata.tpcds.Table;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.Optional;

public class TpcdsTableStatisticsFactory
{
    private final TableStatisticsDataRepository statisticsDataRepository = new TableStatisticsDataRepository();

    public TableStatistics create(String schemaName, Table table, Map<String, ColumnHandle> columnHandles)
    {
        Optional<TableStatisticsData> statisticsDataOptional = statisticsDataRepository.load(schemaName, table);
        return statisticsDataOptional.map(statisticsData -> toTableStatistics(columnHandles, statisticsData))
                .orElseGet(() -> unknownStatistics(columnHandles));
    }

    private TableStatistics toTableStatistics(Map<String, ColumnHandle> columnHandles, TableStatisticsData statisticsData)
    {
        TableStatistics.Builder tableStatistics = TableStatistics.builder()
                .setRowCount(new Estimate(statisticsData.getRowCount()));

        Map<String, ColumnStatisticsData> columnsData = statisticsData.getColumns();
        for (Map.Entry<String, ColumnHandle> entry : columnHandles.entrySet()) {
            tableStatistics.setColumnStatistics(entry.getValue(), toColumnStatistics(columnsData.get(entry.getKey()), statisticsData.getRowCount()));
        }

        return tableStatistics.build();
    }

    private ColumnStatistics toColumnStatistics(@Nullable ColumnStatisticsData columnStatisticsData, long rowCount)
    {
        if (columnStatisticsData == null || rowCount == 0) {
            return ColumnStatistics.UNKNOWN_COLUMN_STATISTICS;
        }

        ColumnStatistics.Builder columnStatistics = ColumnStatistics.builder();
        long nullCount = columnStatisticsData.getNullsCount();
        columnStatistics.setNullsFraction(new Estimate(nullCount / rowCount));
        columnStatistics.addRange(builder -> builder
                .setLowValue(columnStatisticsData.getMin())
                .setHighValue(columnStatisticsData.getMax())
                .setDistinctValuesCount(new Estimate(columnStatisticsData.getDistinctValuesCount()))
                .setFraction(new Estimate((rowCount - nullCount) / rowCount))
                .build());

        return columnStatistics.build();
    }

    private TableStatistics unknownStatistics(Map<String, ColumnHandle> columnHandles)
    {
        TableStatistics.Builder tableStatistics = TableStatistics.builder();
        columnHandles.values().stream()
                .forEach(columnHandle -> tableStatistics.setColumnStatistics(columnHandle, ColumnStatistics.UNKNOWN_COLUMN_STATISTICS));
        return tableStatistics.build();
    }
}
