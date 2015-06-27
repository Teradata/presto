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

import com.facebook.presto.hive.metastore.HiveMetastore;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.ViewNotFoundException;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.airlift.concurrent.MoreFutures;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.joda.time.DateTimeZone;

import javax.inject.Inject;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.facebook.presto.hive.HiveColumnHandle.SAMPLE_WEIGHT_COLUMN_NAME;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_FILESYSTEM_ERROR;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_PATH_ALREADY_EXISTS;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_TIMEZONE_MISMATCH;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_UNSUPPORTED_FORMAT;
import static com.facebook.presto.hive.HiveTableProperties.PARTITIONED_BY_PROPERTY;
import static com.facebook.presto.hive.HiveTableProperties.STORAGE_FORMAT_PROPERTY;
import static com.facebook.presto.hive.HiveTableProperties.getHiveStorageFormat;
import static com.facebook.presto.hive.HiveTableProperties.getPartitionedBy;
import static com.facebook.presto.hive.HiveType.toHiveType;
import static com.facebook.presto.hive.HiveUtil.PRESTO_VIEW_FLAG;
import static com.facebook.presto.hive.HiveUtil.decodeViewData;
import static com.facebook.presto.hive.HiveUtil.encodeViewData;
import static com.facebook.presto.hive.HiveUtil.hiveColumnHandles;
import static com.facebook.presto.hive.HiveUtil.schemaTableName;
import static com.facebook.presto.hive.HiveWriteUtils.checkTableIsWritable;
import static com.facebook.presto.hive.HiveWriteUtils.createDirectory;
import static com.facebook.presto.hive.HiveWriteUtils.getTableDefaultLocation;
import static com.facebook.presto.hive.util.Types.checkType;
import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.facebook.presto.spi.StandardErrorCode.PERMISSION_DENIED;
import static com.facebook.presto.spi.StandardErrorCode.USER_ERROR;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.transform;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hive.serde.serdeConstants.STRING_TYPE_NAME;

public class HiveMetadata
        implements ConnectorMetadata
{
    private static final Logger log = Logger.get(HiveMetadata.class);
    private static final int PARTITION_COMMIT_BATCH_SIZE = 8;

    private final String connectorId;
    private final boolean allowDropTable;
    private final boolean allowRenameTable;
    private final boolean allowCorruptWritesForTesting;
    private final HiveMetastore metastore;
    private final HdfsEnvironment hdfsEnvironment;
    private final DateTimeZone timeZone;
    private final TypeManager typeManager;
    private final JsonCodec<PartitionUpdate> partitionUpdateCodec;
    private final ExecutorService renameExecutionService;

    @Inject
    @SuppressWarnings("deprecation")
    public HiveMetadata(
            HiveConnectorId connectorId,
            HiveClientConfig hiveClientConfig,
            HiveMetastore metastore,
            HdfsEnvironment hdfsEnvironment,
            @ForHiveClient ExecutorService executorService,
            TypeManager typeManager,
            JsonCodec<PartitionUpdate> partitionUpdateCodec)
    {
        this(connectorId,
                metastore,
                hdfsEnvironment,
                hiveClientConfig.getDateTimeZone(),
                hiveClientConfig.getAllowDropTable(),
                hiveClientConfig.getAllowRenameTable(),
                hiveClientConfig.getAllowCorruptWritesForTesting(),
                typeManager,
                partitionUpdateCodec);
    }

    public HiveMetadata(
            HiveConnectorId connectorId,
            HiveMetastore metastore,
            HdfsEnvironment hdfsEnvironment,
            DateTimeZone timeZone,
            boolean allowDropTable,
            boolean allowRenameTable,
            boolean allowCorruptWritesForTesting,
            TypeManager typeManager,
            JsonCodec<PartitionUpdate> partitionUpdateCodec)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null").toString();

        this.allowDropTable = allowDropTable;
        this.allowRenameTable = allowRenameTable;
        this.allowCorruptWritesForTesting = allowCorruptWritesForTesting;

        this.metastore = checkNotNull(metastore, "metastore is null");
        this.hdfsEnvironment = checkNotNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.timeZone = checkNotNull(timeZone, "timeZone is null");
        this.typeManager = checkNotNull(typeManager, "typeManager is null");
        this.partitionUpdateCodec = checkNotNull(partitionUpdateCodec, "partitionUpdateCodec is null");

        if (!allowCorruptWritesForTesting && !timeZone.equals(DateTimeZone.getDefault())) {
            log.warn("Hive writes are disabled. " +
                            "To write data to Hive, your JVM timezone must match the Hive storage timezone. " +
                            "Add -Duser.timezone=%s to your JVM arguments",
                    timeZone.getID());
        }

        renameExecutionService = Executors.newWorkStealingPool(); // todo how many concurrent renames should we do?
    }

    public HiveMetastore getMetastore()
    {
        return metastore;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return metastore.getAllDatabases();
    }

    @Override
    public HiveTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        checkNotNull(tableName, "tableName is null");
        if (!metastore.getTable(tableName.getSchemaName(), tableName.getTableName()).isPresent()) {
            return null;
        }
        return new HiveTableHandle(connectorId, tableName.getSchemaName(), tableName.getTableName());
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        SchemaTableName tableName = schemaTableName(tableHandle);
        return getTableMetadata(tableName);
    }

    private ConnectorTableMetadata getTableMetadata(SchemaTableName tableName)
    {
        Optional<Table> table = metastore.getTable(tableName.getSchemaName(), tableName.getTableName());
        if (!table.isPresent() || table.get().getTableType().equals(TableType.VIRTUAL_VIEW.name())) {
            throw new TableNotFoundException(tableName);
        }
        List<HiveColumnHandle> handles = hiveColumnHandles(typeManager, connectorId, table.get(), false);
        List<ColumnMetadata> columns = ImmutableList.copyOf(transform(handles, columnMetadataGetter(table.get(), typeManager)));

        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
        try {
            HiveStorageFormat format = extractHiveStorageFormat(table.get());
            properties.put(STORAGE_FORMAT_PROPERTY, format);
        }
        catch (PrestoException ignored) {
            // todo fail if format is not known
        }
        List<String> partitionedBy = table.get().getPartitionKeys().stream()
                .map(FieldSchema::getName)
                .collect(toList());
        if (!partitionedBy.isEmpty()) {
            properties.put(PARTITIONED_BY_PROPERTY, partitionedBy);
        }

        return new ConnectorTableMetadata(tableName, columns, properties.build(), table.get().getOwner());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        ImmutableList.Builder<SchemaTableName> tableNames = ImmutableList.builder();
        for (String schemaName : listSchemas(session, schemaNameOrNull)) {
            for (String tableName : metastore.getAllTables(schemaName).orElse(emptyList())) {
                tableNames.add(new SchemaTableName(schemaName, tableName));
            }
        }
        return tableNames.build();
    }

    private List<String> listSchemas(ConnectorSession session, String schemaNameOrNull)
    {
        if (schemaNameOrNull == null) {
            return listSchemaNames(session);
        }
        return ImmutableList.of(schemaNameOrNull);
    }

    @Override
    public ColumnHandle getSampleWeightColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        SchemaTableName tableName = schemaTableName(tableHandle);
        Optional<Table> table = metastore.getTable(tableName.getSchemaName(), tableName.getTableName());
        if (!table.isPresent()) {
            throw new TableNotFoundException(tableName);
        }
        for (HiveColumnHandle columnHandle : hiveColumnHandles(typeManager, connectorId, table.get(), true)) {
            if (columnHandle.getName().equals(SAMPLE_WEIGHT_COLUMN_NAME)) {
                return columnHandle;
            }
        }
        return null;
    }

    @Override
    public boolean canCreateSampledTables(ConnectorSession session)
    {
        return true;
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        SchemaTableName tableName = schemaTableName(tableHandle);
        Optional<Table> table = metastore.getTable(tableName.getSchemaName(), tableName.getTableName());
        if (!table.isPresent()) {
            throw new TableNotFoundException(tableName);
        }
        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        for (HiveColumnHandle columnHandle : hiveColumnHandles(typeManager, connectorId, table.get(), false)) {
            columnHandles.put(columnHandle.getName(), columnHandle);
        }
        return columnHandles.build();
    }

    @SuppressWarnings("TryWithIdenticalCatches")
    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        checkNotNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix)) {
            try {
                columns.put(tableName, getTableMetadata(tableName).getColumns());
            }
            catch (HiveViewNotSupportedException e) {
                // view is not supported
            }
            catch (TableNotFoundException e) {
                // table disappeared during listing operation
            }
        }
        return columns.build();
    }

    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix)
    {
        if (prefix.getSchemaName() == null || prefix.getTableName() == null) {
            return listTables(session, prefix.getSchemaName());
        }
        return ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
    }

    /**
     * NOTE: This method does not return column comment
     */
    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        checkType(tableHandle, HiveTableHandle.class, "tableHandle");
        return checkType(columnHandle, HiveColumnHandle.class, "columnHandle").getColumnMetadata(typeManager);
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        checkArgument(!isNullOrEmpty(tableMetadata.getOwner()), "Table owner is null or empty");

        SchemaTableName schemaTableName = tableMetadata.getTable();
        String schemaName = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();
        List<String> partitionedBy = getPartitionedBy(tableMetadata.getProperties());
        List<HiveColumnHandle> columnHandles = getColumnHandles(connectorId, tableMetadata, ImmutableSet.copyOf(partitionedBy));
        HiveStorageFormat hiveStorageFormat = getHiveStorageFormat(tableMetadata.getProperties());
        createTable(schemaName, tableName, tableMetadata.getOwner(), columnHandles, hiveStorageFormat, partitionedBy);
    }

    public void createTable(String schemaName,
            String tableName,
            String tableOwner,
            List<HiveColumnHandle> columnHandles,
            HiveStorageFormat hiveStorageFormat,
            List<String> partitionedBy)
    {
        Path targetPath = getTableDefaultLocation(metastore, hdfsEnvironment, schemaName, tableName);

        // verify the target directory for the table
        if (HiveWriteUtils.pathExists(hdfsEnvironment, targetPath)) {
            throw new PrestoException(HIVE_PATH_ALREADY_EXISTS, format("Target directory for table '%s.%s' already exists: %s", schemaName, tableName, targetPath));
        }

        createDirectory(hdfsEnvironment, targetPath);
        createTable(schemaName, tableName, tableOwner, columnHandles, hiveStorageFormat, partitionedBy, targetPath);
    }

    private Table createTable(String schemaName,
            String tableName,
            String tableOwner,
            List<HiveColumnHandle> columnHandles,
            HiveStorageFormat hiveStorageFormat,
            List<String> partitionedBy,
            Path targetPath)
    {
        Map<String, HiveColumnHandle> columnHandlesByName = Maps.uniqueIndex(columnHandles, HiveColumnHandle::getName);
        List<FieldSchema> partitionColumns = partitionedBy.stream()
                .map(columnHandlesByName::get)
                .map(column -> new FieldSchema(column.getName(), column.getHiveType().getHiveTypeName(), null))
                .collect(toList());

        Set<String> partitionColumnNames = ImmutableSet.copyOf(partitionedBy);

        boolean sampled = false;
        ImmutableList.Builder<FieldSchema> columns = ImmutableList.builder();
        for (HiveColumnHandle columnHandle : columnHandles) {
            String name = columnHandle.getName();
            String type = columnHandle.getHiveType().getHiveTypeName();
            if (name.equals(SAMPLE_WEIGHT_COLUMN_NAME)) {
                columns.add(new FieldSchema(name, type, "Presto sample weight column"));
                sampled = true;
            }
            else if (!partitionColumnNames.contains(name)) {
                verify(!columnHandle.isPartitionKey(), "Column handles are not consistent with partitioned by property");
                columns.add(new FieldSchema(name, type, null));
            }
            else {
                verify(columnHandle.isPartitionKey(), "Column handles are not consistent with partitioned by property");
            }
        }

        SerDeInfo serdeInfo = new SerDeInfo();
        serdeInfo.setName(tableName);
        serdeInfo.setSerializationLib(hiveStorageFormat.getSerDe());
        serdeInfo.setParameters(ImmutableMap.of());

        StorageDescriptor sd = new StorageDescriptor();
        sd.setLocation(targetPath.toString());
        sd.setCols(columns.build());
        sd.setSerdeInfo(serdeInfo);
        sd.setInputFormat(hiveStorageFormat.getInputFormat());
        sd.setOutputFormat(hiveStorageFormat.getOutputFormat());
        sd.setParameters(ImmutableMap.of());

        Table table = new Table();
        table.setDbName(schemaName);
        table.setTableName(tableName);
        table.setOwner(tableOwner);
        table.setTableType(TableType.MANAGED_TABLE.toString());
        String tableComment = "Created by Presto";
        if (sampled) {
            tableComment = "Sampled table created by Presto. Only query this table from Hive if you understand how Presto implements sampling.";
        }
        table.setParameters(ImmutableMap.of("comment", tableComment));
        table.setPartitionKeys(partitionColumns);
        table.setSd(sd);

        metastore.createTable(table);
        return table;
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        if (!allowRenameTable) {
            throw new PrestoException(PERMISSION_DENIED, "Renaming tables is disabled in this Hive catalog");
        }

        HiveTableHandle handle = checkType(tableHandle, HiveTableHandle.class, "tableHandle");
        metastore.renameTable(handle.getSchemaName(), handle.getTableName(), newTableName.getSchemaName(), newTableName.getTableName());
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        HiveTableHandle handle = checkType(tableHandle, HiveTableHandle.class, "tableHandle");
        SchemaTableName tableName = schemaTableName(tableHandle);

        if (!allowDropTable) {
            throw new PrestoException(PERMISSION_DENIED, "DROP TABLE is disabled in this Hive catalog");
        }

        Optional<Table> target = metastore.getTable(handle.getSchemaName(), handle.getTableName());
        if (!target.isPresent()) {
            throw new TableNotFoundException(tableName);
        }
        Table table = target.get();

        if (!session.getUser().equals(table.getOwner())) {
            throw new PrestoException(PERMISSION_DENIED, format("Unable to drop table '%s': owner of the table is different from session user", table));
        }
        metastore.dropTable(handle.getSchemaName(), handle.getTableName());
    }

    @Override
    public HiveOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        verifyJvmTimeZone();

        checkArgument(!isNullOrEmpty(tableMetadata.getOwner()), "Table owner is null or empty");

        HiveStorageFormat hiveStorageFormat = getHiveStorageFormat(tableMetadata.getProperties());
        List<String> partitionedBy = getPartitionedBy(tableMetadata.getProperties());

        // get the root directory for the database
        SchemaTableName schemaTableName = tableMetadata.getTable();
        String schemaName = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();

        List<HiveColumnHandle> columnHandles = getColumnHandles(connectorId, tableMetadata, ImmutableSet.copyOf(partitionedBy));

        Path targetPath = getTableDefaultLocation(metastore, hdfsEnvironment, schemaName, tableName);

        // verify the target directory for the table
        if (HiveWriteUtils.pathExists(hdfsEnvironment, targetPath)) {
            throw new PrestoException(HIVE_PATH_ALREADY_EXISTS, format("Target directory for table '%s.%s' already exists: %s", schemaName, tableName, targetPath));
        }

        String writePath;
        if (!useTemporaryDirectory(targetPath)) {
            writePath = targetPath.toString();
        }
        else {
            writePath = HiveWriteUtils.createTemporaryPath(hdfsEnvironment, targetPath);
        }

        return new HiveOutputTableHandle(
                connectorId,
                schemaName,
                tableName,
                columnHandles,
                randomUUID().toString(), // todo this should really be the queryId
                writePath,
                hiveStorageFormat,
                partitionedBy,
                tableMetadata.getOwner());
    }

    @Override
    public void commitCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments)
    {
        HiveOutputTableHandle handle = checkType(tableHandle, HiveOutputTableHandle.class, "tableHandle");

        List<PartitionUpdate> partitionUpdates = fragments.stream()
                .map(Slice::getBytes)
                .map(partitionUpdateCodec::fromJson)
                .collect(toList());

        Path targetPath = getTableDefaultLocation(metastore, hdfsEnvironment, handle.getSchemaName(), handle.getTableName());
        Path writePath = new Path(handle.getWritePath().get());

        // rename if using a temporary directory
        if (!targetPath.equals(writePath)) {
            // verify no one raced us to create the target directory
            if (HiveWriteUtils.pathExists(hdfsEnvironment, targetPath)) {
                throw new PrestoException(HIVE_PATH_ALREADY_EXISTS, format("Target directory for table '%s.%s' already exists: %s",
                        handle.getSchemaName(),
                        handle.getTableName(),
                        targetPath));
            }
            // rename the temporary directory to the target
            HiveWriteUtils.renameDirectory(hdfsEnvironment, handle.getSchemaName(), handle.getTableName(), writePath, targetPath);
        }

        PartitionCommitter partitionCommitter = new PartitionCommitter(handle.getSchemaName(), handle.getTableName(), metastore, PARTITION_COMMIT_BATCH_SIZE);
        try {
            partitionUpdates = PartitionUpdate.mergePartitionUpdates(partitionUpdates);

            Table table = createTable(
                    handle.getSchemaName(),
                    handle.getTableName(),
                    handle.getTableOwner(),
                    handle.getInputColumns(),
                    handle.getHiveStorageFormat(),
                    handle.getPartitionedBy(),
                    targetPath);

            if (!handle.getPartitionedBy().isEmpty()) {
                partitionUpdates.stream()
                        .map(partitionUpdate -> createPartition(table, partitionUpdate))
                        .forEach(partitionCommitter::addPartition);
            }
            partitionCommitter.close();
        }
        catch (Throwable throwable) {
            // drop created partitions
            for (Partition createdPartition : partitionCommitter.getCreatedPartitions()) {
                try {
                    metastore.dropPartition(handle.getSchemaName(), handle.getTableName(), createdPartition.getValues());
                }
                catch (Exception e) {
                    log.error(e, "Error rolling back new partition '%s' in table '%s.%s", createdPartition.getValues(), handle.getSchemaName(), handle.getTableName());
                }
            }
            // clean up the target directory
            try {
                FileSystem fileSystem = hdfsEnvironment.getFileSystem(targetPath);
                fileSystem.delete(targetPath, true);
            }
            catch (IOException e) {
                log.debug(e, "Error rolling back table creation. Can not table directory %s", targetPath);
            }

            throw throwable;
        }
    }

    @Override
    public void rollbackCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle)
    {
        HiveOutputTableHandle handle = checkType(tableHandle, HiveOutputTableHandle.class, "tableHandle");

        // create table only writes to the writePath which is a new temp dir
        rollbackDataFiles(handle.getWritePath().get(), handle.getFilePrefix());

        try {
            Path writePath = new Path(handle.getWritePath().get());
            // todo is this correct
            hdfsEnvironment.getFileSystem(writePath).delete(writePath, false);
        }
        catch (IOException ignored) {
            // this is temp data so an error isn't a big problem
        }
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        verifyJvmTimeZone();

        SchemaTableName tableName = schemaTableName(tableHandle);
        Optional<Table> table = metastore.getTable(tableName.getSchemaName(), tableName.getTableName());
        if (!table.isPresent()) {
            throw new TableNotFoundException(tableName);
        }

        checkTableIsWritable(table.get());

        HiveStorageFormat hiveStorageFormat = extractHiveStorageFormat(table.get());

        List<HiveColumnHandle> handles = hiveColumnHandles(typeManager, connectorId, table.get(), false);

        Path targetPath = new Path(table.get().getSd().getLocation());
        Optional<String> writePath;
        if (!useTemporaryDirectory(targetPath)) {
            writePath = Optional.empty();
        }
        else {
            writePath = Optional.of(HiveWriteUtils.createTemporaryPath(hdfsEnvironment, targetPath));
        }

        return new HiveInsertTableHandle(
                connectorId,
                tableName.getSchemaName(),
                tableName.getTableName(),
                handles,
                randomUUID().toString(), // todo this should really be the queryId
                writePath,
                hiveStorageFormat);
    }

    private static class PartitionCommitter
            implements Closeable
    {
        private final String schemaName;
        private final String tableName;
        private final HiveMetastore metastore;
        private final int batchSize;
        private final List<Partition> batch;
        private final List<Partition> createdPartitions = new ArrayList<>();

        public PartitionCommitter(String schemaName, String tableName, HiveMetastore metastore, int batchSize)
        {
            this.schemaName = schemaName;
            this.tableName = tableName;
            this.metastore = metastore;
            this.batchSize = batchSize;
            this.batch = new ArrayList<>(batchSize);
        }

        public List<Partition> getCreatedPartitions()
        {
            return ImmutableList.copyOf(createdPartitions);
        }

        public void addPartition(Partition partition)
        {
            batch.add(partition);
            if (batch.size() >= batchSize) {
                createdPartitions.addAll(metastore.addPartitions(schemaName, tableName, batch));
                batch.clear();
            }
        }

        @Override
        public void close()
        {
            if (!batch.isEmpty()) {
                createdPartitions.addAll(metastore.addPartitions(schemaName, tableName, batch));
                batch.clear();
            }
        }
    }

    @Override
    public void commitInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments)
    {
        HiveInsertTableHandle handle = checkType(insertHandle, HiveInsertTableHandle.class, "invalid insertHandle");

        List<PartitionUpdate> partitionUpdates = fragments.stream()
                .map(Slice::getBytes)
                .map(partitionUpdateCodec::fromJson)
                .collect(toList());

        PartitionCommitter partitionCommitter = new PartitionCommitter(handle.getSchemaName(), handle.getTableName(), metastore, PARTITION_COMMIT_BATCH_SIZE);
        try {
            partitionUpdates = PartitionUpdate.mergePartitionUpdates(partitionUpdates);

            Optional<Table> table = metastore.getTable(handle.getSchemaName(), handle.getTableName());
            if (!table.isPresent()) {
                throw new TableNotFoundException(new SchemaTableName(handle.getSchemaName(), handle.getTableName()));
            }

            List<CompletableFuture<?>> fileRenameFutures = new ArrayList<>();
            for (PartitionUpdate partitionUpdate : partitionUpdates) {
                if (!partitionUpdate.getName().isEmpty() && partitionUpdate.isNew()) {
                    // move data to final location
                    if (!partitionUpdate.getWritePath().equals(partitionUpdate.getTargetPath())) {
                        HiveWriteUtils.renameDirectory(hdfsEnvironment,
                                table.get().getDbName(),
                                table.get().getTableName(),
                                new Path(partitionUpdate.getWritePath()),
                                new Path(partitionUpdate.getTargetPath()));
                    }
                    // add new partition
                    Partition partition = createPartition(table.get(), partitionUpdate);
                    partitionCommitter.addPartition(partition);
                }
                else {
                    // move data to final location
                    if (!partitionUpdate.getWritePath().equals(partitionUpdate.getTargetPath())) {
                        Path writeDir = new Path(partitionUpdate.getWritePath());
                        Path targetDir = new Path(partitionUpdate.getTargetPath());

                        FileSystem fileSystem;
                        try {
                            fileSystem = hdfsEnvironment.getFileSystem(targetDir);
                        }
                        catch (IOException e) {
                            throw new PrestoException(HIVE_FILESYSTEM_ERROR, e);
                        }

                        for (String fileName : partitionUpdate.getFilesNames()) {
                            fileRenameFutures.add(CompletableFuture.runAsync(() -> {
                                Path source = new Path(writeDir, fileName);
                                Path target = new Path(targetDir, fileName);
                                try {
                                    fileSystem.rename(source, target);
                                }
                                catch (IOException e) {
                                    throw new PrestoException(HIVE_FILESYSTEM_ERROR, format("Error moving INSERT data from %s to final location %s", source, target), e);
                                }
                            }, renameExecutionService));
                        }
                    }
                }
            }
            partitionCommitter.close();
            for (CompletableFuture<?> fileRenameFuture : fileRenameFutures) {
                MoreFutures.getFutureValue(fileRenameFuture, PrestoException.class);
            }
        }
        catch (Throwable t) {
            rollbackInsert(handle.getSchemaName(), handle.getTableName(), partitionUpdates, partitionCommitter.getCreatedPartitions());
            throw t;
        }
    }

    private static Partition createPartition(Table table, PartitionUpdate partitionUpdate)
    {
        List<String> values = HiveSplitManager.extractPartitionKeyValues(partitionUpdate.getName());
        Partition partition = new Partition();
        partition.setDbName(table.getDbName());
        partition.setTableName(table.getTableName());
        partition.setValues(values);
        partition.setSd(table.getSd().deepCopy());
        partition.getSd().setLocation(partitionUpdate.getTargetPath());
        return partition;
    }

    private void rollbackInsert(String schemaName, String tableName, List<PartitionUpdate> partitionUpdates, List<Partition> createdPartitions)
    {
        // drop created partitions
        for (Partition createdPartition : createdPartitions) {
            try {
                metastore.dropPartition(schemaName, tableName, createdPartition.getValues());
            }
            catch (Exception e) {
                log.error(e, "Error rolling back new partition '%s' in table '%s.%s", createdPartition.getValues(), schemaName, tableName);
            }
        }

        for (PartitionUpdate partitionUpdate : partitionUpdates) {
            // delete temp data if we used a temp dir
            if (!partitionUpdate.getWritePath().equals(partitionUpdate.getTargetPath())) {
                Path writePath = new Path(partitionUpdate.getWritePath());
                try {
                    FileSystem fileSystem = hdfsEnvironment.getFileSystem(writePath);
                    fileSystem.delete(writePath, true);
                }
                catch (IOException e) {
                    // this is temp data so an error isn't a big problem
                    log.debug(e, "Error deleting temp data in %s", writePath);
                }
            }

            // delete data from target directory
            Path targetPath = new Path(partitionUpdate.getTargetPath());
            FileSystem fileSystem;
            try {
                fileSystem = hdfsEnvironment.getFileSystem(targetPath);
            }
            catch (IOException e) {
                log.error(e, "Error rolling back INSERT data files %s", partitionUpdate.getFilesNames().stream()
                        .map(name -> new Path(targetPath, name).toString())
                        .collect(joining(", ")));
                continue;
            }
            for (String fileName : partitionUpdate.getFilesNames()) {
                Path file = new Path(targetPath, fileName);
                try {
                    fileSystem.delete(file, true);
                }
                catch (IOException e) {
                    log.error(e, "Error rolling back INSERT data file %s", file);
                }
            }

            // todo is this correct
            try {
                fileSystem.delete(targetPath, false);
            }
            catch (IOException ignore) {
                // this is just cleanup
            }
        }
    }

    @Override
    public void rollbackInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle)
    {
        HiveInsertTableHandle handle = checkType(insertHandle, HiveInsertTableHandle.class, "invalid insertHandle");
        String filePrefix = handle.getFilePrefix();

        Optional<Table> table = metastore.getTable(handle.getSchemaName(), handle.getTableName());
        if (!table.isPresent()) {
            throw new TableNotFoundException(new SchemaTableName(handle.getSchemaName(), handle.getTableName()));
        }
        String tableLocation = table.get().getSd().getLocation();
        rollbackDataFiles(tableLocation, filePrefix);

        // This is cleanup, so no table or partitions is ok
        List<String> partitionNames = metastore.getPartitionNames(handle.getSchemaName(), handle.getTableName())
                .orElse(ImmutableList.of());
        for (List<String> partitionNameBatch : Iterables.partition(partitionNames, 10)) {
            Map<String, Partition> partitions = metastore.getPartitionsByNames(handle.getSchemaName(), handle.getTableName(), partitionNameBatch).orElse(ImmutableMap.of());
            for (Partition partition : partitions.values()) {
                String location = partition.getSd().getLocation();
                rollbackDataFiles(location, filePrefix);
            }
        }
    }

    private void rollbackDataFiles(String location, String filePrefix)
    {
        // delete data from target directory
        Path targetPath = new Path(location);
        try {
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(targetPath);
            for (FileStatus fileStatus : fileSystem.listStatus(targetPath)) {
                if (fileStatus.isFile() && fileStatus.getPath().getName().startsWith(filePrefix)) {
                    try {
                        fileSystem.delete(fileStatus.getPath(), true);
                    }
                    catch (IOException e) {
                        log.error(e, "Error rolling back data file %s", fileStatus.getPath());
                    }
                }
            }
        }
        catch (IOException e) {
            log.error(e, "Error rolling back data files %s/%s*", location, filePrefix);
        }
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName viewName, String viewData, boolean replace)
    {
        if (replace) {
            try {
                dropView(session, viewName);
            }
            catch (ViewNotFoundException ignored) {
            }
        }

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("comment", "Presto View")
                .put(PRESTO_VIEW_FLAG, "true")
                .build();

        FieldSchema dummyColumn = new FieldSchema("dummy", STRING_TYPE_NAME, null);

        StorageDescriptor sd = new StorageDescriptor();
        sd.setCols(ImmutableList.of(dummyColumn));
        sd.setSerdeInfo(new SerDeInfo());

        Table table = new Table();
        table.setDbName(viewName.getSchemaName());
        table.setTableName(viewName.getTableName());
        table.setOwner(session.getUser());
        table.setTableType(TableType.VIRTUAL_VIEW.name());
        table.setParameters(properties);
        table.setViewOriginalText(encodeViewData(viewData));
        table.setViewExpandedText("/* Presto View */");
        table.setSd(sd);

        try {
            metastore.createTable(table);
        }
        catch (TableAlreadyExistsException e) {
            throw new ViewAlreadyExistsException(e.getTableName());
        }
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        String view = getViews(session, viewName.toSchemaTablePrefix()).get(viewName);
        if (view == null) {
            throw new ViewNotFoundException(viewName);
        }

        try {
            metastore.dropTable(viewName.getSchemaName(), viewName.getTableName());
        }
        catch (TableNotFoundException e) {
            throw new ViewNotFoundException(e.getTableName());
        }
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, String schemaNameOrNull)
    {
        ImmutableList.Builder<SchemaTableName> tableNames = ImmutableList.builder();
        for (String schemaName : listSchemas(session, schemaNameOrNull)) {
            for (String tableName : metastore.getAllViews(schemaName).orElse(emptyList())) {
                tableNames.add(new SchemaTableName(schemaName, tableName));
            }
        }
        return tableNames.build();
    }

    @Override
    public Map<SchemaTableName, String> getViews(ConnectorSession session, SchemaTablePrefix prefix)
    {
        ImmutableMap.Builder<SchemaTableName, String> views = ImmutableMap.builder();
        List<SchemaTableName> tableNames;
        if (prefix.getTableName() != null) {
            tableNames = ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
        }
        else {
            tableNames = listViews(session, prefix.getSchemaName());
        }

        for (SchemaTableName schemaTableName : tableNames) {
            Optional<Table> table = metastore.getTable(schemaTableName.getSchemaName(), schemaTableName.getTableName());
            if (table.isPresent() && HiveUtil.isPrestoView(table.get())) {
                views.put(schemaTableName, decodeViewData(table.get().getViewOriginalText()));
            }
        }

        return views.build();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("clientId", connectorId)
                .toString();
    }

    private void verifyJvmTimeZone()
    {
        if (!allowCorruptWritesForTesting && !timeZone.equals(DateTimeZone.getDefault())) {
            throw new PrestoException(HIVE_TIMEZONE_MISMATCH, format(
                    "To write Hive data, your JVM timezone must match the Hive storage timezone. Add -Duser.timezone=%s to your JVM arguments.",
                    timeZone.getID()));
        }
    }

    private boolean useTemporaryDirectory(Path path)
    {
        try {
            // skip using temporary directory for S3
            return !(hdfsEnvironment.getFileSystem(path) instanceof PrestoS3FileSystem);
        }
        catch (IOException e) {
            throw new PrestoException(HIVE_FILESYSTEM_ERROR, "Failed checking path: " + path, e);
        }
    }

    private static HiveStorageFormat extractHiveStorageFormat(Table table)
    {
        StorageDescriptor descriptor = table.getSd();
        if (descriptor == null) {
            throw new PrestoException(HIVE_INVALID_METADATA, "Table is missing storage descriptor");
        }
        SerDeInfo serdeInfo = descriptor.getSerdeInfo();
        if (serdeInfo == null) {
            throw new PrestoException(HIVE_INVALID_METADATA, "Table storage descriptor is missing SerDe info");
        }
        String outputFormat = descriptor.getOutputFormat();
        String serializationLib = serdeInfo.getSerializationLib();

        for (HiveStorageFormat format : HiveStorageFormat.values()) {
            if (format.getOutputFormat().equals(outputFormat) && format.getSerDe().equals(serializationLib)) {
                return format;
            }
        }
        throw new PrestoException(HIVE_UNSUPPORTED_FORMAT, format("Output format %s with SerDe %s is not supported", outputFormat, serializationLib));
    }

    private static List<HiveColumnHandle> getColumnHandles(String connectorId, ConnectorTableMetadata tableMetadata, Set<String> partitionColumnNames)
    {
        ImmutableList.Builder<HiveColumnHandle> columnHandles = ImmutableList.builder();
        Set<String> foundPartitionColumns = new HashSet<>();
        int ordinal = 0;
        for (ColumnMetadata column : tableMetadata.getColumns()) {
            boolean partitionKey = partitionColumnNames.contains(column.getName());
            if (partitionKey) {
                foundPartitionColumns.add(column.getName());
            }

            columnHandles.add(new HiveColumnHandle(
                    connectorId,
                    column.getName(),
                    ordinal,
                    toHiveType(column.getType()),
                    column.getType().getTypeSignature(),
                    ordinal,
                    partitionKey));
            ordinal++;
        }
        if (tableMetadata.isSampled()) {
            columnHandles.add(new HiveColumnHandle(
                    connectorId,
                    SAMPLE_WEIGHT_COLUMN_NAME,
                    ordinal,
                    toHiveType(BIGINT),
                    BIGINT.getTypeSignature(),
                    ordinal,
                    false));
            // ordinal++;
        }

        if (!partitionColumnNames.equals(foundPartitionColumns)) {
            throw new PrestoException(NOT_FOUND, format("Partition columns %s were not found", Sets.difference(partitionColumnNames, foundPartitionColumns)));
        }
        if (columnHandles.build().isEmpty()) {
            throw new PrestoException(USER_ERROR, "Table contains only partition columns");
        }

        return columnHandles.build();
    }

    private static Function<HiveColumnHandle, ColumnMetadata> columnMetadataGetter(Table table, TypeManager typeManager)
    {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        for (FieldSchema field : concat(table.getSd().getCols(), table.getPartitionKeys())) {
            if (field.getComment() != null) {
                builder.put(field.getName(), field.getComment());
            }
        }
        Map<String, String> columnComment = builder.build();

        return input -> new ColumnMetadata(
                input.getName(),
                typeManager.getType(input.getTypeSignature()),
                input.isPartitionKey(),
                columnComment.get(input.getName()),
                false);
    }
}
