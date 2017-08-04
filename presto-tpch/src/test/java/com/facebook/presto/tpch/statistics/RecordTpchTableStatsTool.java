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
package com.facebook.presto.tpch.statistics;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.collect.ImmutableList;
import io.airlift.tpch.TpchColumn;
import io.airlift.tpch.TpchEntity;
import io.airlift.tpch.TpchTable;

import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.facebook.presto.tpch.TpchMetadata.schemaNameToScaleFactor;
import static io.airlift.tpch.OrderColumn.ORDER_STATUS;
import static io.airlift.tpch.TpchTable.ORDERS;
import static java.lang.String.format;
import static java.util.Optional.empty;

/**
 * This is a tool used to record statistics for TPCH tables.
 * <p>
 * The results are output to {@code presto-tpch/src/main/resources/tpch/statistics/${schemaName}} directory.
 * <p>
 * The tool is run by invoking its {@code main} method.
 */
public class RecordTpchTableStatsTool
{
    private final TableStatisticsRecorder tableStatisticsRecorder;
    private final TableStatisticsDataRepository tableStatisticsDataRepository;

    public RecordTpchTableStatsTool(TableStatisticsRecorder tableStatisticsRecorder, TableStatisticsDataRepository tableStatisticsDataRepository)
    {
        this.tableStatisticsRecorder = tableStatisticsRecorder;
        this.tableStatisticsDataRepository = tableStatisticsDataRepository;
    }

    public static void main(String[] args)
    {
        RecordTpchTableStatsTool tool = new RecordTpchTableStatsTool(new TableStatisticsRecorder(), new TableStatisticsDataRepository(createObjectMapper()));

        ImmutableList.of("tiny", "sf1").forEach(schemaName -> {
            TpchTable.getTables()
                    .forEach(table -> tool.computeAndOutputStatsFor(schemaName, table));

            Stream.of("F", "O", "P").forEach(partitionValue -> {
                tool.computeAndOutputStatsFor(schemaName, ORDERS, ORDER_STATUS, partitionValue);
            });
        });
    }

    private static ObjectMapper createObjectMapper()
    {
        return new ObjectMapper()
                .registerModule(new Jdk8Module());
    }

    private <E extends TpchEntity> void computeAndOutputStatsFor(String schemaName, TpchTable<E> table)
    {
        computeAndOutputStatsFor(schemaName, table, row -> true, empty(), empty());
    }

    private <E extends TpchEntity> void computeAndOutputStatsFor(String schemaName, TpchTable<E> table, TpchColumn<E> partitionColumn, String partitionValue)
    {
        Predicate<E> predicate = row -> partitionColumn.getString(row).equals(partitionValue);
        computeAndOutputStatsFor(schemaName, table, predicate, Optional.of(partitionColumn), Optional.of(partitionValue));
    }

    private <E extends TpchEntity> void computeAndOutputStatsFor(String schemaName, TpchTable<E> table, Predicate<E> predicate, Optional<TpchColumn<?>> partitionColumn, Optional<String> partitionValue)
    {
        double scaleFactor = schemaNameToScaleFactor(schemaName);

        long start = System.nanoTime();

        TableStatisticsData statisticsData = tableStatisticsRecorder.recordStatistics(table, predicate, scaleFactor);

        long duration = (System.nanoTime() - start) / 1_000_000;
        System.out.println(format("Finished stats recording for %s[%s] sf %s, took %s ms", table.getTableName(), partitionValue.orElse(""), scaleFactor, duration));

        tableStatisticsDataRepository.save(schemaName, table, partitionColumn, partitionValue, statisticsData);
    }
}
