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
package com.facebook.presto.tests;

import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.testing.TestingSession.TESTING_CATALOG;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;

public class TestLocalBinarySpilledQueries
        extends AbstractTestQueries
{
    public TestLocalBinarySpilledQueries()
    {
        super(createLocalQueryRunner());
    }

    private static LocalQueryRunner createLocalQueryRunner()
    {
        Session defaultSession = testSessionBuilder()
                .setCatalog("local")
                .setSchema(TINY_SCHEMA_NAME)
                .setSystemProperty(SystemSessionProperties.SPILL_ENABLED, "true")
                .setSystemProperty(SystemSessionProperties.OPERATOR_MEMORY_LIMIT_BEFORE_SPILL, "1MB") //spill constantly
                .build();

        LocalQueryRunner localQueryRunner = new LocalQueryRunner(defaultSession);

        // add the tpch catalog
        // local queries run directly against the generator
        localQueryRunner.createCatalog(
                defaultSession.getCatalog().get(),
                new TpchConnectorFactory(1),
                ImmutableMap.of());

        localQueryRunner.getMetadata().addFunctions(CUSTOM_FUNCTIONS);

        SessionPropertyManager sessionPropertyManager = localQueryRunner.getMetadata().getSessionPropertyManager();
        sessionPropertyManager.addSystemSessionProperties(TEST_SYSTEM_PROPERTIES);
        sessionPropertyManager.addConnectorSessionProperties(new ConnectorId(TESTING_CATALOG), TEST_CATALOG_PROPERTIES);

        return localQueryRunner;
    }

    @Test
    public void testSimpleRightAndFullJoinFail()
            throws Exception
    {
        assertQueryFails("SELECT COUNT(*) FROM lineitem RIGHT JOIN orders ON lineitem.orderkey = orders.orderkey",
                "Spilling for RIGHT and FULL outer joins is unsupported");

        assertQueryFails("SELECT COUNT(*) FROM lineitem FULL JOIN orders ON lineitem.orderkey = orders.orderkey",
                "Spilling for RIGHT and FULL outer joins is unsupported");
    }

    @Test(enabled = false)
    @Override
    public void testJoinWithStatefulFilterFunction()
            throws Exception
    {
        super.testJoinWithStatefulFilterFunction();
    }

    @Test(enabled = false)
    @Override
    public void testAggregationOverRigthJoinOverSingleStreamProbe()
    {
        super.testAggregationOverRigthJoinOverSingleStreamProbe();
    }

    @Test(enabled = false)
    @Override
    public void testSimpleRightJoin()
    {
        super.testSimpleRightJoin();
    }

    @Test(enabled = false)
    @Override
    public void testBuildFilteredRightJoin()
    {
        super.testBuildFilteredRightJoin();
    }

    @Test(enabled = false)
    @Override
    public void testDoubleFilteredRightJoinWithLeftConstantEquality()
    {
        super.testDoubleFilteredRightJoinWithLeftConstantEquality();
    }

    @Test(enabled = false)
    @Override
    public void testDoubleFilteredRightJoinWithRightConstantEquality()
    {
        super.testDoubleFilteredRightJoinWithRightConstantEquality();
    }

    @Test(enabled = false)
    @Override
    public void testFullJoinNormalizedToRight()
    {
        super.testFullJoinNormalizedToRight();
    }

    @Test(enabled = false)
    @Override
    public void testFullJoinWithLeftConstantEquality()
    {
        super.testFullJoinWithLeftConstantEquality();
    }

    @Test(enabled = false)
    @Override
    public void testFullJoinWithRightConstantEquality()
    {
        super.testFullJoinWithRightConstantEquality();
    }

    @Test(enabled = false)
    @Override
    public void testJoinUsingSymbolsFromJustOneSideOfJoin()
            throws Exception
    {
        super.testJoinUsingSymbolsFromJustOneSideOfJoin();
    }

    @Test(enabled = false)
    @Override
    public void testNonEqualityFullJoin()
    {
        super.testNonEqualityFullJoin();
    }

    @Test(enabled = false)
    @Override
    public void testRightJoinEqualityInference()
    {
        super.testRightJoinEqualityInference();
    }

    @Test(enabled = false)
    @Override
    public void testRightJoinDoubleClauseWithRightOverlap()
    {
        super.testRightJoinDoubleClauseWithRightOverlap();
    }

    @Test(enabled = false)
    @Override
    public void testProbeFilteredRightJoin()
    {
        super.testProbeFilteredRightJoin();
    }

    @Test(enabled = false)
    @Override
    public void testPlainRightJoinPredicatePushdown()
    {
        super.testPlainRightJoinPredicatePushdown();
    }

    @Test(enabled = false)
    @Override
    public void testOuterJoinWithNullsOnProbe()
    {
        super.testOuterJoinWithNullsOnProbe();
    }

    @Test(enabled = false)
    @Override
    public void testNonEqualityRightJoin()
    {
        super.testNonEqualityRightJoin();
    }

    @Test(enabled = false)
    @Override
    public void testRightJoinWithRightConstantEquality()
    {
        super.testRightJoinWithRightConstantEquality();
    }

    @Test(enabled = false)
    @Override
    public void testRightJoinWithLeftConstantEquality()
    {
        super.testRightJoinWithLeftConstantEquality();
    }

    @Test(enabled = false)
    @Override
    public void testRightJoinWithEmptyInnerTable()
    {
        super.testRightJoinWithEmptyInnerTable();
    }

    @Test(enabled = false)
    @Override
    public void testSimpleRightJoinWithRightConstantEquality()
    {
        super.testSimpleRightJoinWithRightConstantEquality();
    }

    @Test(enabled = false)
    @Override
    public void testSimpleRightJoinWithLeftConstantEquality()
    {
        super.testSimpleRightJoinWithLeftConstantEquality();
    }

    @Test(enabled = false)
    @Override
    public void testSimpleFullJoinWithRightConstantEquality()
    {
        super.testSimpleFullJoinWithRightConstantEquality();
    }

    @Test(enabled = false)
    @Override
    public void testSimpleFullJoinWithLeftConstantEquality()
    {
        super.testSimpleFullJoinWithLeftConstantEquality();
    }

    @Test(enabled = false)
    @Override
    public void testSimpleFullJoin()
    {
        super.testSimpleFullJoin();
    }

    @Test(enabled = false)
    @Override
    public void testRightJoinDoubleClauseWithLeftOverlap()
    {
        super.testRightJoinDoubleClauseWithLeftOverlap();
    }

    @Test(enabled = false)
    @Override
    public void testJoinsWithTrueJoinCondition()
            throws Exception
    {
        super.testJoinsWithTrueJoinCondition();
    }

    @Test(enabled = false)
    @Override
    public void testExistsSubquery()
    {
        super.testExistsSubquery();
    }

    @Test(enabled = false)
    @Override
    public void testJoinWithMultipleInSubqueryClauses()
    {
        super.testJoinWithMultipleInSubqueryClauses();
    }

    @Test(enabled = false)
    @Override
    public void testJoinWithMultipleScalarSubqueryClauses()
    {
        super.testJoinWithMultipleScalarSubqueryClauses();
    }

    @Test(enabled = false)
    @Override
    public void testJoinWithScalarSubqueryToBeExecutedAsPostJoinFilter()
    {
        super.testJoinWithScalarSubqueryToBeExecutedAsPostJoinFilter();
    }

    @Test(enabled = false)
    @Override
    public void testJoinWithScalarSubqueryToBeExecutedAsPostJoinFilterWithEmptyInnerTable()
    {
        super.testJoinWithScalarSubqueryToBeExecutedAsPostJoinFilterWithEmptyInnerTable();
    }

    @Test(enabled = false)
    @Override
    public void testScalarSubquery()
    {
        super.testScalarSubquery();
    }

    @Test(enabled = false)
    @Override
    public void testRightJoinWithNullValues()
    {
        super.testRightJoinWithNullValues();
    }
}
