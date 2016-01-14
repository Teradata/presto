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

package com.facebook.presto.hive;

import com.facebook.presto.hadoop.shaded.com.google.common.base.Throwables;
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
import com.facebook.presto.spi.ConnectorViewDefinition;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.google.inject.Inject;
import io.airlift.slice.Slice;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

public class HiveAuthenticatingMetadata
        implements ConnectorMetadata
{
    private final HdfsEnvironment hdfsEnvironment;
    private final ConnectorMetadata targetMetadata;

    @Inject
    public HiveAuthenticatingMetadata(HdfsEnvironment hdfsEnvironment, ConnectorMetadata targetMetadata)
    {
        this.hdfsEnvironment = hdfsEnvironment;
        this.targetMetadata = targetMetadata;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        try {
            return hdfsEnvironment.doAs(() -> targetMetadata.listSchemaNames(session), session.getUser());
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        try {
            return hdfsEnvironment.doAs(() -> targetMetadata.getTableHandle(session, tableName), session.getUser());
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        try {
            return hdfsEnvironment.doAs(() -> targetMetadata.getTableLayouts(session, table, constraint, desiredColumns), session.getUser());
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        try {
            return hdfsEnvironment.doAs(() -> targetMetadata.getTableLayout(session, handle), session.getUser());
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        try {
            return hdfsEnvironment.doAs(() -> targetMetadata.getTableMetadata(session, table), session.getUser());
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        try {
            return hdfsEnvironment.doAs(() -> targetMetadata.listTables(session, schemaNameOrNull), session.getUser());
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public ColumnHandle getSampleWeightColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        try {
            return hdfsEnvironment.doAs(() -> targetMetadata.getSampleWeightColumnHandle(session, tableHandle), session.getUser());
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public boolean canCreateSampledTables(ConnectorSession session)
    {
        try {
            return hdfsEnvironment.doAs(() -> targetMetadata.canCreateSampledTables(session), session.getUser());
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        try {
            return hdfsEnvironment.doAs(() -> targetMetadata.getColumnHandles(session, tableHandle), session.getUser());
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        try {
            return hdfsEnvironment.doAs(() -> targetMetadata.getColumnMetadata(session, tableHandle, columnHandle), session.getUser());
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        try {
            return hdfsEnvironment.doAs(() -> targetMetadata.listTableColumns(session, prefix), session.getUser());
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        try {
            hdfsEnvironment.doAs(() -> {
                targetMetadata.createTable(session, tableMetadata);
                return null;
            }, session.getUser());
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        try {
            hdfsEnvironment.doAs(() -> {
                targetMetadata.dropTable(session, tableHandle);
                return null;
            }, session.getUser());
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        try {
            hdfsEnvironment.doAs(() -> {
                targetMetadata.renameTable(session, tableHandle, newTableName);
                return null;
            }, session.getUser());
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column)
    {
        try {
            hdfsEnvironment.doAs(() -> {
                targetMetadata.addColumn(session, tableHandle, column);
                return null;
            }, session.getUser());
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void renameColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle source, String target)
    {
        try {
            hdfsEnvironment.doAs(() -> {
                targetMetadata.renameColumn(session, tableHandle, source, target);
                return null;
            }, session.getUser());
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        try {
            return hdfsEnvironment.doAs(() -> targetMetadata.beginCreateTable(session, tableMetadata), session.getUser());
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void commitCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments)
    {
        try {
            hdfsEnvironment.doAs(() -> {
                targetMetadata.commitCreateTable(session, tableHandle, fragments);
                return null;
            }, session.getUser());
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void rollbackCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle)
    {
        try {
            hdfsEnvironment.doAs(() -> {
                targetMetadata.rollbackCreateTable(session, tableHandle);
                return null;
            }, session.getUser());
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        try {
            return hdfsEnvironment.doAs(() -> targetMetadata.beginInsert(session, tableHandle), session.getUser());
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void commitInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments)
    {
        try {
            hdfsEnvironment.doAs(() -> {
                targetMetadata.commitInsert(session, insertHandle, fragments);
                return null;
            }, session.getUser());
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void rollbackInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle)
    {
        try {
            hdfsEnvironment.doAs(() -> {
                targetMetadata.rollbackInsert(session, insertHandle);
                return null;
            }, session.getUser());
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public ColumnHandle getUpdateRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        try {
            return hdfsEnvironment.doAs(() -> targetMetadata.getUpdateRowIdColumnHandle(session, tableHandle), session.getUser());
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public ConnectorTableHandle beginDelete(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        try {
            return hdfsEnvironment.doAs(() -> targetMetadata.beginDelete(session, tableHandle), session.getUser());
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void commitDelete(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<Slice> fragments)
    {
        try {
            hdfsEnvironment.doAs(() -> {
                targetMetadata.commitDelete(session, tableHandle, fragments);
                return null;
            }, session.getUser());
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void rollbackDelete(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        try {
            hdfsEnvironment.doAs(() -> {
                targetMetadata.rollbackDelete(session, tableHandle);
                return null;
            }, session.getUser());
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName viewName, String viewData, boolean replace)
    {
        try {
            hdfsEnvironment.doAs(() -> {
                targetMetadata.createView(session, viewName, viewData, replace);
                return null;
            }, session.getUser());
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        try {
            hdfsEnvironment.doAs(() -> {
                targetMetadata.dropView(session, viewName);
                return null;
            }, session.getUser());
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, String schemaNameOrNull)
    {
        try {
            return hdfsEnvironment.doAs(() -> targetMetadata.listViews(session, schemaNameOrNull), session.getUser());
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, SchemaTablePrefix prefix)
    {
        try {
            return hdfsEnvironment.doAs(() -> targetMetadata.getViews(session, prefix), session.getUser());
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public boolean supportsMetadataDelete(ConnectorSession session, ConnectorTableHandle tableHandle, ConnectorTableLayoutHandle tableLayoutHandle)
    {
        try {
            return hdfsEnvironment.doAs(() -> targetMetadata.supportsMetadataDelete(session, tableHandle, tableLayoutHandle), session.getUser());
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public OptionalLong metadataDelete(ConnectorSession session, ConnectorTableHandle tableHandle, ConnectorTableLayoutHandle tableLayoutHandle)
    {
        try {
            return hdfsEnvironment.doAs(() -> targetMetadata.metadataDelete(session, tableHandle, tableLayoutHandle), session.getUser());
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
