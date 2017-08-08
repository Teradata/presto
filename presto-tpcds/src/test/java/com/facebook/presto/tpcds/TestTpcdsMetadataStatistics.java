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

package com.facebook.presto.tpcds;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.statistics.ColumnStatistics;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.spi.statistics.RangeColumnStatistics;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.teradata.tpcds.Table;
import com.teradata.tpcds.column.CallCenterColumn;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spi.Constraint.alwaysTrue;
import static com.facebook.presto.spi.statistics.ColumnStatistics.UNKNOWN_COLUMN_STATISTICS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

public class TestTpcdsMetadataStatistics
{
    private static final EstimateAssertion estimateAssertion = new EstimateAssertion(0.01);
    private static final ConnectorSession session = null;
    private final TpcdsMetadata metadata = new TpcdsMetadata();

    @Test
    public void testNoTableStatsForNotSupportedSchema()
    {
        Table.getBaseTables().forEach(
                table -> {
                    SchemaTableName schemaTableName = new SchemaTableName("sf10", table.getName());
                    ConnectorTableHandle tableHandle = metadata.getTableHandle(session, schemaTableName);
                    TableStatistics tableStatistics = metadata.getTableStatistics(session, tableHandle, alwaysTrue());
                    assertTrue(tableStatistics.getRowCount().isValueUnknown());
                    for (ColumnHandle column : metadata.getColumnHandles(session, tableHandle).values()) {
                        assertTrue(tableStatistics.getColumnStatistics().containsKey(column));
                        assertEquals(tableStatistics.getColumnStatistics().get(column), UNKNOWN_COLUMN_STATISTICS);
                    }
                });
    }

    @Test
    public void testTableStatsExistenceSupportedSchema()
    {
        Table.getBaseTables().forEach(
                table -> {
                    SchemaTableName schemaTableName = new SchemaTableName("sf1", table.getName());
                    ConnectorTableHandle tableHandle = metadata.getTableHandle(session, schemaTableName);
                    TableStatistics tableStatistics = metadata.getTableStatistics(session, tableHandle, alwaysTrue());
                    assertFalse(tableStatistics.getRowCount().isValueUnknown());
                    for (ColumnHandle column : metadata.getColumnHandles(session, tableHandle).values()) {
                        assertTrue(tableStatistics.getColumnStatistics().containsKey(column));
                        assertNotEquals(tableStatistics.getColumnStatistics().get(column), UNKNOWN_COLUMN_STATISTICS);
                    }
                });
    }

    @Test
    public void testTableStatsDetails()
    {
        SchemaTableName schemaTableName = new SchemaTableName("sf1", Table.CALL_CENTER.getName());
        ConnectorTableHandle tableHandle = metadata.getTableHandle(session, schemaTableName);
        TableStatistics tableStatistics = metadata.getTableStatistics(session, tableHandle, alwaysTrue());

        estimateAssertion.assertClose(tableStatistics.getRowCount(), new Estimate(6), "Row count does not match");

        // all columns have stats
        Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, tableHandle);
        for (ColumnHandle column : columnHandles.values()) {
            assertTrue(tableStatistics.getColumnStatistics().containsKey(column));
            assertNotEquals(tableStatistics.getColumnStatistics().get(column), UNKNOWN_COLUMN_STATISTICS);
        }

        // identifier
        assertColumnStatistics(
                tableStatistics.getColumnStatistics().get(columnHandles.get(CallCenterColumn.CC_CALL_CENTER_SK.getName())),
                ColumnStatistics.builder()
                        .setNullsFraction(new Estimate(0))
                        .addRange(range -> range
                                .setFraction(new Estimate(1.0))
                                .setDistinctValuesCount(new Estimate(6))
                                .setLowValue(Optional.of(1))
                                .setHighValue(Optional.of(6))
                                .build())
                        .build());

        // varchar
        assertColumnStatistics(
                tableStatistics.getColumnStatistics().get(columnHandles.get(CallCenterColumn.CC_CALL_CENTER_ID.getName())),
                ColumnStatistics.builder()
                        .setNullsFraction(new Estimate(0))
                        .addRange(range -> range
                                .setFraction(new Estimate(1.0))
                                .setDistinctValuesCount(new Estimate(3))
                                .setLowValue(Optional.of("AAAAAAAABAAAAAAA"))
                                .setHighValue(Optional.of("AAAAAAAAEAAAAAAA"))
                                .build())
                        .build());

        // char
        assertColumnStatistics(
                tableStatistics.getColumnStatistics().get(columnHandles.get(CallCenterColumn.CC_ZIP.getName())),
                ColumnStatistics.builder()
                        .setNullsFraction(new Estimate(0))
                        .addRange(range -> range
                                .setFraction(new Estimate(1.0))
                                .setDistinctValuesCount(new Estimate(1))
                                .setLowValue(Optional.of("31904"))
                                .setHighValue(Optional.of("31904"))
                                .build())
                        .build());

        // decimal
        assertColumnStatistics(
                tableStatistics.getColumnStatistics().get(columnHandles.get(CallCenterColumn.CC_GMT_OFFSET.getName())),
                ColumnStatistics.builder()
                        .setNullsFraction(new Estimate(0))
                        .addRange(range -> range
                                .setFraction(new Estimate(1.0))
                                .setDistinctValuesCount(new Estimate(1))
                                .setLowValue(Optional.of(-500))
                                .setHighValue(Optional.of(-500))
                                .build())
                        .build());

        // date
        assertColumnStatistics(
                tableStatistics.getColumnStatistics().get(columnHandles.get(CallCenterColumn.CC_REC_START_DATE.getName())),
                ColumnStatistics.builder()
                        .setNullsFraction(new Estimate(0))
                        .addRange(range -> range
                                .setFraction(new Estimate(1))
                                .setDistinctValuesCount(new Estimate(4))
                                .setLowValue(Optional.of(10227))
                                .setHighValue(Optional.of(11688))
                                .build())
                        .build());

        // only null values
        assertColumnStatistics(
                tableStatistics.getColumnStatistics().get(columnHandles.get(CallCenterColumn.CC_CLOSED_DATE_SK.getName())),
                ColumnStatistics.builder()
                        .setNullsFraction(new Estimate(1))
                        .addRange(range -> range
                                .setFraction(new Estimate(0))
                                .setDistinctValuesCount(new Estimate(0))
                                .setLowValue(Optional.empty())
                                .setHighValue(Optional.empty())
                                .build())
                        .build());
    }

    private void assertColumnStatistics(ColumnStatistics actual, ColumnStatistics expected)
    {
        estimateAssertion.assertClose(actual.getNullsFraction(), expected.getNullsFraction(), "Null fraction does not match");

        RangeColumnStatistics actualRange = actual.getOnlyRangeColumnStatistics();
        RangeColumnStatistics expectedRange = expected.getOnlyRangeColumnStatistics();
        if (expectedRange.getFraction().isValueUnknown()) {
            assertTrue(actualRange.getFraction().isValueUnknown());
        }
        else {
            estimateAssertion.assertClose(actualRange.getFraction(), expectedRange.getFraction(), "Fraction does not match");
        }
        if (expectedRange.getDataSize().isValueUnknown()) {
            assertTrue(actualRange.getDataSize().isValueUnknown());
        }
        else {
            estimateAssertion.assertClose(actualRange.getDataSize(), expectedRange.getDataSize(), "Data size does not match");
        }
        if (expectedRange.getDistinctValuesCount().isValueUnknown()) {
            assertTrue(actualRange.getDistinctValuesCount().isValueUnknown());
        }
        else {
            estimateAssertion.assertClose(actualRange.getDistinctValuesCount(), expectedRange.getDistinctValuesCount(), "Distinct values count does not match");
        }
        assertEquals(actualRange.getLowValue(), expectedRange.getLowValue());
        assertEquals(actualRange.getHighValue(), expectedRange.getHighValue());
    }
}
