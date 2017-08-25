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

import com.facebook.presto.connector.meta.SupportedTestCondition;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closer;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

@ExtendWith(SupportedTestCondition.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public interface SPITest
{
    Connector getConnector();

    Map<String, Object> getTableProperties();

    /*
     * Returns a SchemaTableName to be used when the name of the schema is NOT
     * important.
     *
     * Tests that require tables in one schema should use this method. An
     * example would be a test that renames a table within a single schema.
     *
     * Tests written using this can be run against connectors that support the
     * creation of schemas and those that do not, but come with a usable schema
     * out of the box. PostgreSQL, for example has a "public" schema.
     *
     * A class implementing this interface for e.g. PostgreSQL should return a
     * SchemaTableName with the schemaName set to "public".
     *
     * Tests invoking this method do NOT generally need to be annotated with
     * @RequiredFeatures({CREATE_SCHEMA, DROP_SCHEMA}).
     *
     * A class overriding this method should also override withSchemas, as
     * described below.
     */
    default SchemaTableName schemaTableName(String tableName)
    {
        return new SchemaTableName("default_schema", tableName);
    }

    default void withMetadata(Consumer<ConnectorMetadata> consumer)
    {
        Connector connector = getConnector();
        ConnectorTransactionHandle transaction = connector.beginTransaction(IsolationLevel.READ_UNCOMMITTED, true);
        ConnectorMetadata metadata = connector.getMetadata(transaction);
        consumer.accept(metadata);
        connector.commit(transaction);
    }

    /*
     * Create and drop the schemas specified by schemaNames, running callable
     * in between creating and dropping the schemas.
     *
     * A class implementing this interface for a connector that does NOT
     * support creating and/or dropping schemas, but DOES have a usable schema
     * (e.g. PostgreSQL with the "public" schema) should override this method
     * and invoke callable without trying to create or destroy any of the
     * schemas specified.
     *
     * Overriding this method in such a way allows tests that explicitly
     * require creating and dropping tables to be written such that they are
     * independent of whether or not the connector supports creating and
     * dropping schemas.
     */
    default void withSchemas(ConnectorSession session, List<String> schemaNames, Callable<Void> callable)
            throws Exception
    {
        try (Closer cleanup = closerOf(schemaNames.stream()
                .distinct()
                .map(schemaName -> new Schema(this, session, schemaName))
                .collect(toImmutableList()))) {
            callable.call();
        }
    }

    default void withTables(ConnectorSession session, List<ConnectorTableMetadata> tables, Callable<Void> callable)
            throws Exception
    {
        withSchemas(session, schemaNamesOf(tables), () -> {
            try (Closer cleanup = closerOf(tables.stream()
                    .map(table -> new Table(this, session, table))
                    .collect(toImmutableList()))) {
                callable.call();
            }
            return null;
        });
    }

    class Schema
            implements Closeable
    {
        private final SPITest test;
        private final ConnectorSession session;
        private final String name;

        public Schema(SPITest test, ConnectorSession session, String name)
        {
            this.test = requireNonNull(test, "test is null");
            this.session = requireNonNull(session, "session is null");
            this.name = requireNonNull(name, "name is null");

            test.withMetadata(metadata -> metadata.createSchema(session, name, ImmutableMap.of()));
        }

        @Override
        public void close()
        {
            test.withMetadata(metadata -> metadata.dropSchema(session, name));
        }
    }

    class Table
            implements Closeable
    {
        private final SPITest test;
        private final ConnectorSession session;
        private final ConnectorTableMetadata tableMetadata;

        public Table(SPITest test, ConnectorSession session, ConnectorTableMetadata tableMetadata)
        {
            this.test = requireNonNull(test, "test is null");
            this.session = requireNonNull(session, "session is null");
            this.tableMetadata = requireNonNull(tableMetadata, "tableMetadata is null");

            test.withMetadata(metadata -> {
                ConnectorOutputTableHandle handle = metadata.beginCreateTable(session, tableMetadata, Optional.empty());
                metadata.finishCreateTable(session, handle, ImmutableList.of());
            });
        }

        @Override
        public void close()
        {
            test.withMetadata(metadata -> {
                ConnectorTableHandle handle = metadata.getTableHandle(session, tableMetadata.getTable());
                metadata.dropTable(session, handle);
            });
        }
    }

    default List<String> schemaNamesOf(List<ConnectorTableMetadata> tables)
    {
        return tables.stream()
                .map(ConnectorTableMetadata::getTable)
                .map(SchemaTableName::getSchemaName)
                .distinct()
                .collect(toImmutableList());
    }

    default Closer closerOf(List<Closeable> closables)
    {
        Closer result = Closer.create();
        closables.forEach(result::register);
        return result;
    }
}
