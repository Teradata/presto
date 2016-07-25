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

package com.facebook.presto.execution;

import com.facebook.presto.Session;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.ArrayConstructor;
import com.facebook.presto.sql.tree.Explain;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.SetSession;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.StringLiteral;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.execution.SqlQueryManager.unwrapExecuteStatement;
import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.facebook.presto.sql.QueryUtil.equal;
import static com.facebook.presto.sql.QueryUtil.logicalAnd;
import static com.facebook.presto.sql.QueryUtil.nameReference;
import static com.facebook.presto.sql.QueryUtil.selectList;
import static com.facebook.presto.sql.QueryUtil.simpleQuery;
import static com.facebook.presto.sql.QueryUtil.table;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.EXPRESSION_NOT_CONSTANT;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_PARAMETER_USAGE;
import static java.util.Collections.emptyList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestUnwrapExecute
{
    private static final SqlParser SQL_PARSER = new SqlParser();

    @Test
    public void testSelectStatement()
            throws Exception
    {
        Statement statement = SQL_PARSER.createStatement("SELECT * FROM foo");
        assertEquals(unwrapExecuteStatement(statement, SQL_PARSER, TEST_SESSION),
                simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("foo"))));
    }

    @Test
    public void testExecuteStatement()
            throws Exception
    {
        Session session = TEST_SESSION.withPreparedStatement("my_query", "SELECT * FROM foo");
        Statement statement = SQL_PARSER.createStatement("EXECUTE my_query");
        assertEquals(unwrapExecuteStatement(statement, SQL_PARSER, session),
                simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("foo"))));
    }

    @Test
    public void testExecuteSelectWithParameters()
            throws Exception
    {
        Session session = TEST_SESSION.withPreparedStatement("my_query", "SELECT * FROM foo WHERE bar = ? AND baz = ?");
        Statement statement = SQL_PARSER.createStatement("EXECUTE my_query USING 123, ARRAY ['abc']");
        assertEquals(unwrapExecuteStatement(statement, SQL_PARSER, session),
                simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("foo")),
                        logicalAnd(equal(nameReference("bar"), new LongLiteral("123")),
                                equal(nameReference("baz"), new ArrayConstructor(ImmutableList.of(new StringLiteral("abc")))))));
    }

    @Test
    public void testExecuteDataDefinitionTaskWithParameters()
            throws Exception
    {
        Session session = TEST_SESSION.withPreparedStatement("my_query", "SET SESSION foo = ?");
        Statement statement = SQL_PARSER.createStatement("EXECUTE my_query USING 123");
        assertEquals(unwrapExecuteStatement(statement, SQL_PARSER, session),
                new SetSession(QualifiedName.of("foo"), new LongLiteral("123")));
    }

    @Test
    public void testExecuteExplainWithParameters()
            throws Exception
    {
        Session session = TEST_SESSION.withPreparedStatement("my_query", "EXPLAIN SELECT ? from foo");
        Statement statement = SQL_PARSER.createStatement("EXECUTE my_query USING 123");
        assertEquals(unwrapExecuteStatement(statement, SQL_PARSER, session),
                new Explain(simpleQuery(selectList(new LongLiteral("123")), table(QualifiedName.of("foo"))), false, emptyList()));
    }

    @Test
    public void testTooManyParameters()
            throws Exception
    {
        try {
            Session session = TEST_SESSION.withPreparedStatement("my_query", "SELECT * FROM foo");
            Statement statement = SQL_PARSER.createStatement("EXECUTE my_query USING 123");
            unwrapExecuteStatement(statement, SQL_PARSER, session);
            fail("expected exception");
        }
        catch (SemanticException e) {
            assertEquals(e.getCode(), INVALID_PARAMETER_USAGE);
            assertEquals(e.getMessage(), "line 1:1: Incorrect number of parameters: expected 0 but found 1");
        }
    }

    @Test
    public void testTooFewParameters()
            throws Exception
    {
        try {
            Session session = TEST_SESSION.withPreparedStatement("my_query", "SELECT * FROM foo WHERE bar = ? AND baz = ?");
            Statement statement = SQL_PARSER.createStatement("EXECUTE my_query USING 123");
            unwrapExecuteStatement(statement, SQL_PARSER, session);
            fail("expected exception");
        }
        catch (SemanticException e) {
            assertEquals(e.getCode(), INVALID_PARAMETER_USAGE);
            assertEquals(e.getMessage(), "line 1:1: Incorrect number of parameters: expected 2 but found 1");
        }
    }

    @Test
    public void testNonConstantParameter()
            throws Exception
    {
        try {
            Session session = TEST_SESSION.withPreparedStatement("my_query", "SELECT * FROM foo WHERE bar = ?");
            Statement statement = SQL_PARSER.createStatement("EXECUTE my_query USING col_reference");
            unwrapExecuteStatement(statement, SQL_PARSER, session);
            fail("expected exception");
        }
        catch (SemanticException e) {
            assertEquals(e.getCode(), EXPRESSION_NOT_CONSTANT);
            assertEquals(e.getMessage(), "line 1:24: Constant expression cannot contain column references");
        }
    }

    @Test
    public void testExecuteStatementDoesNotExist()
            throws Exception
    {
        try {
            Statement statement = SQL_PARSER.createStatement("execute my_query");
            unwrapExecuteStatement(statement, SQL_PARSER, TEST_SESSION);
            fail("expected exception");
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), NOT_FOUND.toErrorCode());
        }
    }
}
