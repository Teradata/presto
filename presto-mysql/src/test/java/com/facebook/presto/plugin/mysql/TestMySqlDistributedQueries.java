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
package com.facebook.presto.plugin.mysql;

import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tests.AbstractTestQueries;
import com.facebook.presto.tests.datatype.CreateAndInsertDataSetup;
import com.facebook.presto.tests.datatype.CreateAsSelectDataSetup;
import com.facebook.presto.tests.datatype.DataTypeTest;
import com.facebook.presto.tests.sql.JdbcSqlExecutor;
import com.facebook.presto.tests.sql.PrestoSqlExecutor;
import com.facebook.presto.tests.sql.SqlExecutor;
import io.airlift.testing.mysql.TestingMySqlServer;
import io.airlift.tpch.TpchTable;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static com.facebook.presto.plugin.mysql.MySqlQueryRunner.createMySqlQueryRunner;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.tests.datatype.DataType.stringDataType;
import static com.facebook.presto.tests.datatype.DataType.varcharDataType;
import static io.airlift.testing.Closeables.closeAllRuntimeException;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test
public class TestMySqlDistributedQueries
        extends AbstractTestQueries
{
    private final TestingMySqlServer mysqlServer;

    public TestMySqlDistributedQueries()
            throws Exception
    {
        this(new TestingMySqlServer("testuser", "testpass", "tpch"));
    }

    public TestMySqlDistributedQueries(TestingMySqlServer mysqlServer)
            throws Exception
    {
        super(createMySqlQueryRunner(mysqlServer, TpchTable.getTables()));
        this.mysqlServer = mysqlServer;
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        closeAllRuntimeException(mysqlServer);
    }

    @Test
    public void testDropTable()
            throws Exception
    {
        assertUpdate("CREATE TABLE test_drop AS SELECT 123 x", 1);
        assertTrue(queryRunner.tableExists(getSession(), "test_drop"));

        assertUpdate("DROP TABLE test_drop");
        assertFalse(queryRunner.tableExists(getSession(), "test_drop"));
    }

    @Test
    public void testViews()
            throws Exception
    {
        execute("CREATE OR REPLACE VIEW tpch.test_view AS SELECT * FROM tpch.orders");

        assertQuery("SELECT orderkey FROM test_view", "SELECT orderkey FROM orders");

        execute("DROP VIEW IF EXISTS tpch.test_view");
    }

    @Test
    public void testPrestoCreatedParametrizedVarchar()
            throws Exception
    {
        PrestoSqlExecutor presto = new PrestoSqlExecutor(queryRunner);
        DataTypeTest.create()
                .addRoundtrip(stringDataType("varchar(10)", createVarcharType(255)), "text_a")
                .addRoundtrip(stringDataType("varchar(255)", createVarcharType(255)), "text_b")
                .addRoundtrip(stringDataType("varchar(256)", createVarcharType(65535)), "text_c")
                .addRoundtrip(stringDataType("varchar(65535)", createVarcharType(65535)), "text_d")
                .addRoundtrip(stringDataType("varchar(65536)", createVarcharType(16777215)), "text_e")
                .addRoundtrip(stringDataType("varchar(16777215)", createVarcharType(16777215)), "text_f")
                .addRoundtrip(stringDataType("varchar(16777216)", createUnboundedVarcharType()), "text_g")
                .addRoundtrip(stringDataType("varchar(" + VarcharType.MAX_LENGTH + ")", createUnboundedVarcharType()), "text_h")
                .addRoundtrip(stringDataType("varchar", createUnboundedVarcharType()), "unbounded")
                .execute(queryRunner, new CreateAsSelectDataSetup(presto, "presto_test_parameterized_varchar"));
    }

    @Test
    public void testMySqlCreatedParametrizedVarchar()
            throws Exception
    {
        SqlExecutor mysql = new JdbcSqlExecutor(mysqlServer.getJdbcUrl());
        DataTypeTest.create()
                .addRoundtrip(stringDataType("tinytext", createVarcharType(255)), "a")
                .addRoundtrip(stringDataType("text", createVarcharType(65535)), "b")
                .addRoundtrip(stringDataType("mediumtext", createVarcharType(16777215)), "c")
                .addRoundtrip(stringDataType("longtext", createUnboundedVarcharType()), "d")
                .addRoundtrip(varcharDataType(32), "e")
                .addRoundtrip(varcharDataType(20000), "f")
                .execute(queryRunner, new CreateAndInsertDataSetup(mysql, "tpch.mysql_test_parameterized_varchar"));
    }

    @Override
    public void testShowColumns()
            throws Exception
    {
        MaterializedResult actual = computeActual("SHOW COLUMNS FROM orders");

        MaterializedResult expectedParametrizedVarchar = resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "bigint", "")
                .row("custkey", "bigint", "")
                .row("orderstatus", "varchar(255)", "")
                .row("totalprice", "double", "")
                .row("orderdate", "date", "")
                .row("orderpriority", "varchar(255)", "")
                .row("clerk", "varchar(255)", "")
                .row("shippriority", "integer", "")
                .row("comment", "varchar(255)", "")
                .build();

        assertEquals(actual, expectedParametrizedVarchar);
    }

    private void execute(String sql)
            throws SQLException
    {
        try (Connection connection = DriverManager.getConnection(mysqlServer.getJdbcUrl());
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }
}
