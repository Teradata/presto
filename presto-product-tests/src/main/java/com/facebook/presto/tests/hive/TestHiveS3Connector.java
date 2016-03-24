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

import com.teradata.tempto.ProductTest;
import com.teradata.tempto.query.QueryResult;
import com.teradata.tempto.query.QueryType;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;

import static com.facebook.presto.tests.TestGroups.QUARANTINE;
import static com.facebook.presto.tests.TestGroups.S3_CONNECTOR;
import static com.teradata.tempto.assertions.QueryAssert.Row.row;
import static com.teradata.tempto.assertions.QueryAssert.assertThat;
import static com.teradata.tempto.query.QueryExecutor.query;
import static com.teradata.tempto.util.DateTimeUtils.parseTimestampInUTC;
import static java.lang.String.format;

// Tests in this class are all quarantined so that they only run as part of the s3 connector job
// These tests require access to an s3 cluster as well as some predefined tables with data loaded in s3.
public class TestHiveS3Connector
        extends ProductTest
{
    @Test(groups = {S3_CONNECTOR, QUARANTINE})
    public void testSelectFromTextFile()
            throws SQLException
    {
        // This test uses a standard tpch nation table.  It assumes that table is already created in the hive catalog
        // and has data loaded.
        QueryResult queryResult = query("SELECT n_name FROM hive.default.nation where n_nationkey = 7");
        assertThat(queryResult).containsOnly(row("GERMANY"));
    }

    @Test(groups = {S3_CONNECTOR, QUARANTINE})
    public void testAllDataTypesTextFile()
            throws SQLException
    {
        // This test uses the all_types table as defined in the AllSimpleTypesTableDefinition class and
        // data from hive/data/all_types/data.textfile. The table is create with the name 'alltypes_text'.
        // The test assumes that table is already created in the hive catalog and has data loaded.

        assertProperAllDatatypesSchema("alltypes_text");
        QueryResult queryResult = query("SELECT * FROM alltypes_text");

        assertThat(queryResult).containsOnly(
                row(
                        127,
                        32767,
                        2147483647,
                        9223372036854775807L,
                        123.345f,
                        234.567,
                        new BigDecimal("346"),
                        new BigDecimal("345.67800"),
                        parseTimestampInUTC("2015-05-10 12:15:35.123"),
                        Date.valueOf("2015-05-10"),
                        "ala ma kota",
                        "ala ma kot",
                        "ala ma    ",
                        true,
                        "kot binarny".getBytes()
                )
        );
    }

    @Test(groups = {S3_CONNECTOR, QUARANTINE})
    public void testAllDataTypesRcFile()
            throws SQLException
    {
        // This test uses the all_types table as defined in the AllSimpleTypesTableDefinition class with data
        // from hive/data/all_types/data.rcfile.  the table is created with the name alltypes_rcfile.
        // The test assumes that table is already created in the hive catalog and has data loaded.

        assertProperAllDatatypesSchema("alltypes_rcfile");
        QueryResult queryResult = query("SELECT * FROM alltypes_rcfile");

        assertThat(queryResult).containsOnly(
                row(
                        127,
                        32767,
                        2147483647,
                        9223372036854775807L,
                        123.345f,
                        234.567,
                        new BigDecimal("346"),
                        new BigDecimal("345.67800"),
                        parseTimestampInUTC("2015-05-10 12:15:35.123"),
                        Date.valueOf("2015-05-10"),
                        "ala ma kota",
                        "ala ma kot",
                        "ala ma    ",
                        true,
                        "kot binarny".getBytes()
                )
        );
    }

    @Test(groups = {S3_CONNECTOR, QUARANTINE})
    public void testAllDataTypesOrc()
            throws SQLException
    {
        // This test uses the all_types table as defined in the AllSimpleTypesTableDefinition class and
        // with data from hive/data/all_types/data.orc.  The table is created with the name alltypes_orc.
        // The test assumes that table is already created in the hive catalog and has data loaded.

        assertProperAllDatatypesSchema("alltypes_orc");
        QueryResult queryResult = query("SELECT * FROM alltypes_orc");

        assertThat(queryResult).containsOnly(
                row(
                        127,
                        32767,
                        2147483647,
                        9223372036854775807L,
                        123.345f,
                        234.567,
                        new BigDecimal("346"),
                        new BigDecimal("345.67800"),
                        parseTimestampInUTC("2015-05-10 12:15:35.123"),
                        Date.valueOf("2015-05-10"),
                        "ala ma kota",
                        "ala ma kot",
                        "ala ma    ",
                        true,
                        "kot binarny".getBytes()
                )
        );
    }

    @Test(groups = {S3_CONNECTOR, QUARANTINE})
    public void testAllDataTypesParquet()
            throws SQLException
    {
        // This test uses all_types table for parquet as defined in the AllSimpleTypesTableDefinition class.  The data
        // was generated by inserting and relevant columns into a parquet table using the data in
        // hive/data/all_types/data.textfile. The table is created with the name alltypes_parquet.
        // The test assumes that table is already created in the hive catalog and has data loaded.

        assertProperParquetDatatypesSchema("alltypes_parquet");
        QueryResult queryResult = query("SELECT * FROM alltypes_parquet");

        assertThat(queryResult).containsOnly(
                row(
                        127,
                        32767,
                        2147483647,
                        9223372036854775807L,
                        123.345f,
                        234.567,
                        parseTimestampInUTC("2015-05-10 12:15:35.123"),
                        "ala ma kota",
                        "ala ma kot",
                        "ala ma    ",
                        true
                )
        );
    }

    @Test(groups = {S3_CONNECTOR, QUARANTINE})
    public void shouldCreateTableAsSelect()
            throws Exception
    {
        String tableName = "create_table_as_select";
        query(format("DROP TABLE IF EXISTS %s", tableName));
        query(format("CREATE TABLE %s AS SELECT * FROM nation", tableName));
        assertThat(query(format("SELECT * FROM %s", tableName))).hasRowsCount(25);
    }

    private void assertProperAllDatatypesSchema(String tableName)
    {
        assertThat(query("SHOW COLUMNS FROM " + tableName, QueryType.SELECT).project(1, 2)).containsExactly(
                row("c_tinyint", "tinyint"),
                row("c_smallint", "smallint"),
                row("c_int", "integer"),
                row("c_bigint", "bigint"),
                row("c_float", "real"),
                row("c_double", "double"),
                row("c_decimal", "decimal(10,0)"),
                row("c_decimal_w_params", "decimal(10,5)"),
                row("c_timestamp", "timestamp"),
                row("c_date", "date"),
                row("c_string", "varchar"),
                row("c_varchar", "varchar(10)"),
                row("c_char", "char(10)"),
                row("c_boolean", "boolean"),
                row("c_binary", "varbinary")
        );
    }

    private void assertProperParquetDatatypesSchema(String tableName)
    {
        assertThat(query("SHOW COLUMNS FROM " + tableName, QueryType.SELECT).project(1, 2)).containsExactly(
                row("c_tinyint", "tinyint"),
                row("c_smallint", "smallint"),
                row("c_int", "integer"),
                row("c_bigint", "bigint"),
                row("c_float", "real"),
                row("c_double", "double"),
                row("c_timestamp", "timestamp"),
                row("c_string", "varchar"),
                row("c_varchar", "varchar(10)"),
                row("c_char", "char(10)"),
                row("c_boolean", "boolean")
        );
    }
}
