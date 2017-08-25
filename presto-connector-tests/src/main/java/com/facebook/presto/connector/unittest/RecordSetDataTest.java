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
package com.facebook.presto.connector.unittest;

import com.facebook.presto.connector.meta.RequiredFeatures;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.testing.TestingConnectorSession;
import com.facebook.presto.testing.TestingSplit;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.facebook.presto.connector.meta.ConnectorFeature.CREATE_TABLE_AS;
import static com.facebook.presto.connector.meta.ConnectorFeature.DROP_TABLE;
import static com.facebook.presto.connector.meta.ConnectorFeature.RECORD_SET_PROVIDER;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

@RequiredFeatures(RECORD_SET_PROVIDER)
public interface RecordSetDataTest
        extends SPITest
{
    @Test
    @RequiredFeatures({CREATE_TABLE_AS, DROP_TABLE})
    default void ReadEmptyTableTest()
            throws Exception
    {
        final ConnectorSession session = new TestingConnectorSession(ImmutableList.of());
        final Connector connector = getConnector();

        final List<ColumnMetadata> tableColumns = ImmutableList.of(
                new ColumnMetadata("bigint_column", BIGINT),
                new ColumnMetadata("double_column", DOUBLE));

        final List<String> desiredColumns = tableColumns.stream()
                .map(ColumnMetadata::getName)
                .collect(toImmutableList());

        final SchemaTableName tableName = schemaTableName("emptytable");

        ConnectorTableMetadata emptyTable = new ConnectorTableMetadata(tableName, tableColumns, getTableProperties());

        withTables(session, ImmutableList.of(emptyTable),
                () -> {
                    ConnectorRecordSetProvider recordSetProvider = connector.getRecordSetProvider();
                    ConnectorTransactionHandle transaction = connector.beginTransaction(IsolationLevel.READ_UNCOMMITTED, true);

                    ConnectorMetadata metadata = connector.getMetadata(transaction);

                    ConnectorTableHandle table = metadata.getTableHandle(session, tableName);

                    Map<String, ColumnHandle> columns = metadata.getColumnHandles(session, table);

                    List<ColumnHandle> columnHandles = desiredColumns.stream()
                            .map(columns::get)
                            .collect(toImmutableList());

                    List<ConnectorTableLayoutResult> tableLayouts =
                            metadata.getTableLayouts(session, table, Constraint.alwaysTrue(), Optional.of(ImmutableSet.copyOf(columnHandles)));

                    ConnectorTableLayout tableLayout = Iterables.getOnlyElement(tableLayouts).getTableLayout();

                    ConnectorSplitSource splitSource = connector.getSplitManager().getSplits(transaction, session, tableLayout.getHandle());

                    ConnectorSplit firstSplit = Iterables.getOnlyElement(getFutureValue(splitSource.getNextBatch(1)));

                    RecordSet recordSet = recordSetProvider.getRecordSet(transaction, session, firstSplit, columnHandles);

                    List<Type> expectedTypes = expectedTypes(tableColumns, desiredColumns);
                    RecordCursor cursor = recordSet.cursor();

                    assertEquals(expectedTypes, recordSet.getColumnTypes());
                    assertFalse(cursor.advanceNextPosition());

                    for (int i = 0; i < expectedTypes.size(); ++i) {
                        assertEquals(expectedTypes.get(i), cursor.getType(i));
                    }

                    connector.commit(transaction);
                    return null;
                });
    }

    default List<Type> expectedTypes(List<ColumnMetadata> tableColumns, List<String> desiredColumns)
    {
        Map<String, ColumnMetadata> lookup = tableColumns.stream()
                .collect(toImmutableMap(ColumnMetadata::getName, Function.identity()));

        return desiredColumns.stream()
                .map(lookup::get)
                .map(ColumnMetadata::getType)
                .collect(toImmutableList());
    }
}
