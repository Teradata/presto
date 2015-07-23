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

package com.facebook.presto.plugin.blackhole;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.facebook.presto.plugin.blackhole.BlackHoleInsertTableHandle.BLACK_HOLE_INSERT_TABLE_HANDLE;
import static com.facebook.presto.plugin.blackhole.Types.checkType;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class BlackHoleMetadata
        implements ConnectorMetadata
{
    public static final String SCHEMA_NAME = "default";
    public static final String SPLITS_COUNT_PROPERTY = "splits_count";
    public static final String PAGES_PER_SPLIT_PROPERTY = "pages_per_split";
    public static final String ROWS_PER_PAGE_PROPERTY = "rows_per_page";

    private final Map<String, BlackHoleTableHandle> tables = new ConcurrentHashMap<>();

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.of(SCHEMA_NAME);
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        return tables.get(tableName.getTableName());
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorTableHandle tableHandle)
    {
        BlackHoleTableHandle blackHoleTableHandle = checkType(tableHandle, BlackHoleTableHandle.class, "tableHandle");
        return blackHoleTableHandle.toTableMetadata();
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        checkArgument(schemaNameOrNull == null || schemaNameOrNull.equals(SCHEMA_NAME),
                "Only '%s' schema is supported", SCHEMA_NAME);
        return tables.values().stream()
                .map(BlackHoleTableHandle::toSchemaTableName)
                .collect(toList());
    }

    @Override
    public ColumnHandle getSampleWeightColumnHandle(ConnectorTableHandle tableHandle)
    {
        //  returns null as the table does not contain sampled data
        // (see {@link com.facebook.presto.spi.ConnectorMetadata.getSampleWeightColumnHandle()}
        return null;
    }

    @Override
    public boolean canCreateSampledTables(ConnectorSession session)
    {
        return false;
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorTableHandle tableHandle)
    {
        BlackHoleTableHandle blackHoleTableHandle = checkType(tableHandle, BlackHoleTableHandle.class, "tableHandle");
        return blackHoleTableHandle.getColumnHandles().stream()
                .collect(toMap(BlackHoleColumnHandle::getName, column -> column));
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        BlackHoleColumnHandle blackHoleColumnHandle = checkType(columnHandle, BlackHoleColumnHandle.class, "columnHandle");
        return blackHoleColumnHandle.toColumnMetadata();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        return tables.values().stream()
                .filter(table -> prefix.matches(table.toSchemaTableName()))
                .collect(toMap(BlackHoleTableHandle::toSchemaTableName, handle -> handle.toTableMetadata().getColumns()));
    }

    @Override
    public void dropTable(ConnectorTableHandle tableHandle)
    {
        BlackHoleTableHandle blackHoleTableHandle = checkType(tableHandle, BlackHoleTableHandle.class, "tableHandle");
        tables.remove(blackHoleTableHandle.getTableName());
    }

    @Override
    public void renameTable(ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        BlackHoleTableHandle oldTableHandle = checkType(tableHandle, BlackHoleTableHandle.class, "tableHandle");
        BlackHoleTableHandle newTableHandle = new BlackHoleTableHandle(
                oldTableHandle.getSchemaName(),
                newTableName.getTableName(),
                oldTableHandle.getColumnHandles(),
                oldTableHandle.getSplitsCount(),
                oldTableHandle.getPagesPerSplit(),
                oldTableHandle.getRowsPerPage()
        );
        synchronized (tables) {
            tables.remove(oldTableHandle.getTableName());
            tables.put(newTableName.getTableName(), newTableHandle);
        }
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        ConnectorOutputTableHandle outputTableHandle = beginCreateTable(session, tableMetadata);
        commitCreateTable(outputTableHandle, ImmutableList.of());
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        int splitsCount = getIntProperty(session, SPLITS_COUNT_PROPERTY);
        int pagesPerSplit = getIntProperty(session, PAGES_PER_SPLIT_PROPERTY);
        int rowsPerPage = getIntProperty(session, ROWS_PER_PAGE_PROPERTY);

        if (splitsCount > 0 || pagesPerSplit > 0 || rowsPerPage > 0) {
            checkArgument(
                    splitsCount * pagesPerSplit * rowsPerPage > 0,
                    format("You have to set all of the properties [%s, %s and %s] or none",
                            SPLITS_COUNT_PROPERTY, PAGES_PER_SPLIT_PROPERTY, ROWS_PER_PAGE_PROPERTY));
        }

        return new BlackHoleOutputTableHandle(new BlackHoleTableHandle(
                tableMetadata,
                splitsCount,
                pagesPerSplit,
                rowsPerPage));
    }

    private int getIntProperty(ConnectorSession session, String propertyName)
    {
        String property = session.getProperties().get(propertyName);
        if (property == null) {
            return 0;
        }
        return Integer.valueOf(property);
    }

    @Override
    public void commitCreateTable(ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments)
    {
        BlackHoleOutputTableHandle blackHoleOutputTableHandle = checkType(tableHandle, BlackHoleOutputTableHandle.class, "tableHandle");
        BlackHoleTableHandle table = blackHoleOutputTableHandle.getTable();
        tables.put(table.getTableName(), table);
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return BLACK_HOLE_INSERT_TABLE_HANDLE;
    }

    @Override
    public void commitInsert(ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments)
    {
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName viewName, String viewData, boolean replace)
    {
        throw viewsAreNotSupportedException();
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        throw viewsAreNotSupportedException();
    }

    private PrestoException viewsAreNotSupportedException()
    {
        return new PrestoException(NOT_SUPPORTED, "This connector does not support views");
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, String schemaNameOrNull)
    {
        return ImmutableList.of();
    }

    @Override
    public Map<SchemaTableName, String> getViews(ConnectorSession session, SchemaTablePrefix prefix)
    {
        return ImmutableMap.of();
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorTableHandle handle, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        checkNotNull(handle);
        checkArgument(handle instanceof BlackHoleTableHandle);
        BlackHoleTableHandle blackHoleHandle = (BlackHoleTableHandle) handle;

        return ImmutableList.of(
                new ConnectorTableLayoutResult(
                        getTableLayout(new BlackHoleTableLayoutHandle(
                                blackHoleHandle.getSplitsCount(),
                                blackHoleHandle.getPagesPerSplit(),
                                blackHoleHandle.getRowsPerPage())),
                        TupleDomain.all()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorTableLayoutHandle handle)
    {
        return new ConnectorTableLayout(handle, Optional.empty(), TupleDomain.none(), Optional.empty(), Optional.empty(), ImmutableList.of());
    }
}
