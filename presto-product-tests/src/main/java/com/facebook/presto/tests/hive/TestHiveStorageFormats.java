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
package com.facebook.presto.tests.hive;

import com.facebook.presto.tests.ImmutableTpchTablesRequirements.ImmutableLineItemTable;
import com.google.common.base.MoreObjects;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.teradata.tempto.ProductTest;
import com.teradata.tempto.Requires;
import com.teradata.tempto.assertions.QueryAssert.Row;
import com.teradata.tempto.query.QueryResult;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.tests.TestGroups.BIG_QUERY;
import static com.facebook.presto.tests.TestGroups.STORAGE_FORMATS;
import static com.facebook.presto.tests.utils.JdbcDriverUtils.setSessionProperty;
import static com.facebook.presto.tests.utils.QueryExecutors.onHive;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.teradata.tempto.assertions.QueryAssert.Row.row;
import static com.teradata.tempto.assertions.QueryAssert.assertThat;
import static com.teradata.tempto.query.QueryExecutor.defaultQueryExecutor;
import static com.teradata.tempto.query.QueryExecutor.query;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class TestHiveStorageFormats
        extends ProductTest
{
    private static final String TPCH_SCHEMA = "tiny";
    private static final String TEST_TPCH_LINIETEM = "tpch." + TPCH_SCHEMA + ".lineitem";

    @DataProvider(name = "storage_formats")
    public static Object[][] storageFormats()
    {
        return new StorageFormat[][] {
                {storageFormat("ORC")},
                {storageFormat("DWRF")},
                {storageFormat("PARQUET")},
                {storageFormat("PARQUET", ImmutableMap.of("hive.parquet_optimized_reader_enabled", "true"))},
                {storageFormat("RCBINARY")},
                {storageFormat("RCTEXT")},
                {storageFormat("SEQUENCEFILE")},
                {storageFormat("TEXTFILE")}
        };
    }

    @Test(dataProvider = "storage_formats", groups = {STORAGE_FORMATS})
    public void testInsertIntoTable(StorageFormat storageFormat)
    {
        setSessionProperties(storageFormat);

        String tableName = "storage_formats_test_insert_into_" + storageFormat.getName().toLowerCase();

        // DROP TABLE
        query(format("DROP TABLE IF EXISTS %s", tableName));

        // CREATE TABLE
        String createTable = format(
                "CREATE TABLE %s(" +
                        "   orderkey      BIGINT," +
                        "   partkey       BIGINT," +
                        "   suppkey       BIGINT," +
                        "   linenumber    INTEGER," +
                        "   quantity      DOUBLE," +
                        "   extendedprice DOUBLE," +
                        "   discount      DOUBLE," +
                        "   tax           DOUBLE," +
                        "   linestatus    VARCHAR," +
                        "   shipinstruct  VARCHAR," +
                        "   shipmode      VARCHAR," +
                        "   comment       VARCHAR," +
                        "   returnflag    VARCHAR" +
                        ") WITH (format='%s')",
                tableName,
                storageFormat.getName());
        query(createTable);

        // INSERT INTO TABLE
        String insertInto = format("INSERT INTO %s " +
                "SELECT " +
                "orderkey, partkey, suppkey, linenumber, quantity, extendedprice, discount, tax, " +
                "linestatus, shipinstruct, shipmode, comment, returnflag " +
                "FROM tpch.%s.lineitem", tableName, TPCH_SCHEMA);
        query(insertInto);

        // SELECT FROM TABLE
        assertSelect("select sum(tax), sum(discount), sum(linenumber) from %s", tableName, TEST_TPCH_LINIETEM);

        // DROP TABLE
        query(format("DROP TABLE %s", tableName));
    }

    @Test(dataProvider = "storage_formats", groups = {STORAGE_FORMATS})
    public void testCreateTableAs(StorageFormat storageFormat)
    {
        setSessionProperties(storageFormat);

        String tableName = "storage_formats_test_create_table_as_select_" + storageFormat.getName().toLowerCase();

        // DROP TABLE
        query(format("DROP TABLE IF EXISTS %s", tableName));

        // CREATE TABLE AS SELECT
        String createTableAsSelect = format(
                "CREATE TABLE %s WITH (format='%s') AS " +
                        "SELECT " +
                        "partkey, suppkey, extendedprice " +
                        "FROM tpch.%s.lineitem",
                tableName,
                storageFormat.getName(),
                TPCH_SCHEMA);
        query(createTableAsSelect);

        // SELECT FROM TABLE
        assertSelect("select sum(extendedprice), sum(suppkey), count(partkey) from %s", tableName, TEST_TPCH_LINIETEM);

        // DROP TABLE
        query(format("DROP TABLE %s", tableName));
    }

    @Test(dataProvider = "storage_formats", groups = {STORAGE_FORMATS})
    public void testInsertIntoPartitionedTable(StorageFormat storageFormat)
    {
        setSessionProperties(storageFormat);

        String tableName = "storage_formats_test_insert_into_partitioned_" + storageFormat.getName().toLowerCase();

        // DROP TABLE
        query(format("DROP TABLE IF EXISTS %s", tableName));

        // CREATE TABLE
        String createTable = format(
                "CREATE TABLE %s(" +
                        "   orderkey      BIGINT," +
                        "   partkey       BIGINT," +
                        "   suppkey       BIGINT," +
                        "   linenumber    INTEGER," +
                        "   quantity      DOUBLE," +
                        "   extendedprice DOUBLE," +
                        "   discount      DOUBLE," +
                        "   tax           DOUBLE," +
                        "   linestatus    VARCHAR," +
                        "   shipinstruct  VARCHAR," +
                        "   shipmode      VARCHAR," +
                        "   comment       VARCHAR," +
                        "   returnflag    VARCHAR" +
                        ") WITH (format='%s', partitioned_by = ARRAY['returnflag'])",
                tableName,
                storageFormat.getName());
        query(createTable);

        // INSERT INTO TABLE
        String insertInto = format("INSERT INTO %s " +
                "SELECT " +
                "orderkey, partkey, suppkey, linenumber, quantity, extendedprice, discount, tax, " +
                "linestatus, shipinstruct, shipmode, comment, returnflag " +
                "FROM tpch.%s.lineitem", tableName, TPCH_SCHEMA);
        query(insertInto);

        // SELECT FROM TABLE
        assertSelect("select sum(tax), sum(discount), sum(length(returnflag)) from %s", tableName, TEST_TPCH_LINIETEM);

        // DROP TABLE
        query(format("DROP TABLE %s", tableName));
    }

    @Test(dataProvider = "storage_formats", groups = {STORAGE_FORMATS})
    public void testCreatePartitionedTableAs(StorageFormat storageFormat)
    {
        setSessionProperties(storageFormat);

        String tableName = "storage_formats_test_create_table_as_select_partitioned_" + storageFormat.getName().toLowerCase();

        // DROP TABLE
        query(format("DROP TABLE IF EXISTS %s", tableName));

        // CREATE TABLE AS SELECT
        String createTableAsSelect = format(
                "CREATE TABLE %s WITH (format='%s', partitioned_by = ARRAY['returnflag']) AS " +
                        "SELECT " +
                        "tax, discount, returnflag " +
                        "FROM tpch.%s.lineitem",
                tableName,
                storageFormat.getName(),
                TPCH_SCHEMA);
        query(createTableAsSelect);

        // SELECT FROM TABLE
        assertSelect("select sum(tax), sum(discount), sum(length(returnflag)) from %s", tableName, TEST_TPCH_LINIETEM);

        // DROP TABLE
        query(format("DROP TABLE %s", tableName));
    }

    @Requires(ImmutableLineItemTable.class)
    @Test(groups = {STORAGE_FORMATS, BIG_QUERY})
    public void testSelectFromPartitionedBzipTable() throws Exception
    {
        // This test is marked as "big_query" because INSERT OVERWRITE TABLE is very slow, but that
        // is the only way to get bzip tables in Hive.

        String tableName = "storage_formats_test_select_partitioned_bzip";
        query(format("DROP TABLE IF EXISTS %s", tableName));

        // The BZIP part of the table comes from the configs that are set during insert
        String createTable = format(
                "CREATE TABLE %s(" +
                        "   l_orderkey      BIGINT," +
                        "   l_partkey       BIGINT," +
                        "   l_suppkey       BIGINT," +
                        "   l_linenumber    INT," +
                        "   l_quantity      DOUBLE," +
                        "   l_extendedprice DOUBLE," +
                        "   l_discount      DOUBLE," +
                        "   l_tax           DOUBLE," +
                        "   l_linestatus    VARCHAR(1)," +
                        "   l_shipinstruct  VARCHAR(25)," +
                        "   l_shipmode      VARCHAR(10)," +
                        "   l_comment       VARCHAR(44)" +
                        ") PARTITIONED BY (l_returnflag VARCHAR(1)) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE",
                tableName);
        onHive().executeQuery(createTable);

        try {
            String insertInto = format(
                    "INSERT OVERWRITE TABLE %s PARTITION(l_returnflag) " +
                            "SELECT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, " +
                            "l_linestatus, l_shipinstruct, l_shipmode, l_comment, l_returnflag " +
                            "FROM default.lineitem", tableName);
            Statement statement = onHive().getConnection().createStatement();
            setHiveConfigsForBzipInsert(statement);
            statement.execute(insertInto);
            statement.close();

            assertSelect("select sum(l_tax), sum(l_discount), sum(length(l_returnflag)) from %s", tableName, "hive.default.lineitem");
        }
        finally {
            query(format("DROP TABLE %s", tableName));
        }
    }

    private void setHiveConfigsForBzipInsert(Statement statement)
            throws SQLException
    {
        statement.execute("SET hive.exec.compress.output=true;");
        statement.execute("SET mapreduce.output.fileoutputformat.compress=true;");
        statement.execute("SET mapred.output.compress=true");
        statement.execute("SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.BZip2Codec");
        statement.execute("SET hive.exec.dynamic.partition.mode=nonstrict;");
    }

    private static void assertSelect(String query, String tableName, String expectedTable)
    {
        QueryResult expected = query(format(query, expectedTable));
        List<Row> expectedRows = expected.rows().stream()
                .map((columns) -> row(columns.toArray()))
                .collect(toImmutableList());
        QueryResult actual = query(format(query, tableName));
        assertThat(actual)
                .hasColumns(expected.getColumnTypes())
                .containsExactly(expectedRows);
    }

    private static void setSessionProperties(StorageFormat storageFormat)
    {
        setSessionProperties(storageFormat.getSessionProperties());
    }

    private static void setSessionProperties(Map<String, String> sessionProperties)
    {
        Connection connection = defaultQueryExecutor().getConnection();
        try {
            // create more than one split
            setSessionProperty(connection, "task_writer_count", "4");
            setSessionProperty(connection, "redistribute_writes", "false");
            for (Map.Entry<String, String> sessionProperty : sessionProperties.entrySet()) {
                setSessionProperty(connection, sessionProperty.getKey(), sessionProperty.getValue());
            }
        }
        catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    private static StorageFormat storageFormat(String name)
    {
        return storageFormat(name, ImmutableMap.of());
    }

    private static StorageFormat storageFormat(String name, Map<String, String> sessionProperties)
    {
        return new StorageFormat(name, sessionProperties);
    }

    private static class StorageFormat
    {
        private final String name;
        private final Map<String, String> sessionProperties;

        private StorageFormat(String name, Map<String, String> sessionProperties)
        {
            this.name = requireNonNull(name, "name is null");
            this.sessionProperties = requireNonNull(sessionProperties, "sessionProperties is null");
        }

        public String getName()
        {
            return name;
        }

        public Map<String, String> getSessionProperties()
        {
            return sessionProperties;
        }

        @Override
        public String toString()
        {
            return MoreObjects.toStringHelper(this)
                    .add("name", name)
                    .add("sessionProperties", sessionProperties)
                    .toString();
        }
    }
}
