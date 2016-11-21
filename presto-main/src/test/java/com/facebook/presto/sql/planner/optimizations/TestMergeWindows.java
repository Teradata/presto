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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.Session;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.assertions.ExpectedWindowNodeSpecification;
import com.facebook.presto.sql.planner.assertions.PlanAssert;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.tree.FrameBound;
import com.facebook.presto.sql.tree.WindowFrame;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyNot;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.window;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

public class TestMergeWindows
{
    private final LocalQueryRunner queryRunner;

    private final String quantitySymbolAlias = "Q";
    private final String discountSymbolAlias = "D";
    private final String extendedSymbolAlias = "E";
    private final String suppkeySymbolAlias = "SK";
    private final String shipdateSymbolAlias = "SD";
    private final String orderkeySymbolAlias = "O";

    private final PlanMatchPattern tableScanFromLineitem = tableScan("lineitem")
            .withSymbol("quantity", quantitySymbolAlias)
            .withSymbol("discount", discountSymbolAlias)
            .withSymbol("suppkey", suppkeySymbolAlias)
            .withSymbol("orderkey", orderkeySymbolAlias);

    private final Optional<WindowFrame> rowsUnboundedPrecedingCurrentRowFrame = Optional.of(new WindowFrame(
            WindowFrame.Type.ROWS,
            new FrameBound(FrameBound.Type.UNBOUNDED_PRECEDING),
            Optional.of(new FrameBound(FrameBound.Type.CURRENT_ROW))));

    private final Optional<WindowFrame> noFrame = Optional.empty();

    private final ExpectedWindowNodeSpecification partitionBySuppkeyOrderByOrderkey = windowSpecification(
            ImmutableList.of(suppkeySymbolAlias),
            ImmutableList.of(orderkeySymbolAlias),
            ImmutableMap.of(orderkeySymbolAlias, SortOrder.ASC_NULLS_LAST));

    private final ExpectedWindowNodeSpecification partitionByOrderkeyOrderByShipdate = windowSpecification(
            ImmutableList.of(orderkeySymbolAlias),
            ImmutableList.of(shipdateSymbolAlias),
            ImmutableMap.of(shipdateSymbolAlias, SortOrder.ASC_NULLS_LAST));

    public TestMergeWindows()
    {
        queryRunner = new LocalQueryRunner(testSessionBuilder()
                .setCatalog("local")
                .setSchema("tiny")
                .build());

        queryRunner.createCatalog(queryRunner.getDefaultSession().getCatalog().get(),
                new TpchConnectorFactory(1),
                ImmutableMap.of());
    }

    /**
     * There are two types of tests in here, and they answer two different
     * questions about MergeWindows (MW):
     * <p>
     * 1) Is MW working as it's supposed to be? The tests running the minimal
     * set of optimizers can tell us this.
     * 2) Has some other optimizer changed the plan in such a way that MW no
     * longer merges windows with identical specifications because the plan
     * that MW sees cannot be optimized by MW? The test running the full set
     * of optimizers answers this, though it isn't actually meaningful unless
     * we know the answer to question 1 is "yes".
     * <p>
     * The tests that use only the minimal set of optimizers are closer to true
     * "unit" tests in that they verify the behavior of MW with as few
     * external dependencies as possible. Those dependencies to include the
     * parser and analyzer, so the phrase "unit" tests should be taken with a
     * grain of salt. Using the parser and anayzler instead of creating plan
     * nodes by hand does have a couple of advantages over a true unit test:
     * 1) The tests are more self-maintaining.
     * 2) They're a lot easier to read.
     * 3) It's a lot less typing.
     * <p>
     * The test that runs with all of the optimzers acts as an integration test
     * and ensures that MW is effective when run with the complete set of
     * optimizers.
     */
    @Test
    public void testMergeableWindowsAllOptimizers()
    {
        @Language("SQL") String sql = "SELECT " +
                "SUM(quantity) OVER (PARTITION BY suppkey ORDER BY orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_quantity_A, " +
                "AVG(quantity) OVER (PARTITION BY orderkey ORDER BY shipdate ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_quantity_B, " +
                "MIN(discount) OVER (PARTITION BY suppkey ORDER BY orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_discount_A " +
                "FROM lineitem";

        PlanMatchPattern pattern =
                anyTree(
                        window(
                                partitionBySuppkeyOrderByOrderkey,
                                ImmutableList.of(
                                        functionCall("SUM", rowsUnboundedPrecedingCurrentRowFrame, quantitySymbolAlias),
                                        functionCall("MIN", rowsUnboundedPrecedingCurrentRowFrame, discountSymbolAlias)),
                                anyTree(
                                        window(
                                                partitionByOrderkeyOrderByShipdate,
                                                ImmutableList.of(
                                                        functionCall("AVG", rowsUnboundedPrecedingCurrentRowFrame, quantitySymbolAlias)),
                                                anyNot(WindowNode.class,
                                                                tableScan("lineitem")
                                                                        .withSymbol("quantity", quantitySymbolAlias)
                                                                        .withSymbol("discount", discountSymbolAlias)
                                                                        .withSymbol("suppkey", suppkeySymbolAlias)
                                                                        .withSymbol("orderkey", orderkeySymbolAlias)
                                                                        .withSymbol("shipdate", shipdateSymbolAlias))))));

        Plan actualPlan = queryRunner.inTransaction(transactionSession -> queryRunner.createPlan(transactionSession, sql));
        queryRunner.inTransaction(transactionSession -> {
            PlanAssert.assertPlan(transactionSession, queryRunner.getMetadata(), actualPlan, pattern);
            return null;
        });
    }

    @Test
    public void testIdenticalWindowSpecificationsABA()
    {
        @Language("SQL") String sql = "SELECT " +
                "SUM(quantity) OVER (PARTITION BY suppkey ORDER BY orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_quantity_A, " +
                "AVG(quantity) OVER (PARTITION BY orderkey ORDER BY shipdate ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_quantity_B, " +
                "MAX(discount) OVER (PARTITION BY suppkey ORDER BY orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_discount_A " +
                "FROM lineitem";

        assertUnitPlan(sql,
                anyTree(
                        window(
                                partitionByOrderkeyOrderByShipdate,
                                ImmutableList.of(
                                        functionCall("AVG", rowsUnboundedPrecedingCurrentRowFrame, quantitySymbolAlias)),
                                window(
                                        partitionBySuppkeyOrderByOrderkey,
                                        ImmutableList.of(
                                                functionCall("SUM", rowsUnboundedPrecedingCurrentRowFrame, quantitySymbolAlias),
                                                functionCall("MAX", rowsUnboundedPrecedingCurrentRowFrame, discountSymbolAlias)),
                                        tableScanFromLineitem))));
    }

    @Test
    public void testIdenticalWindowSpecificationsABcpA()
    {
        @Language("SQL") String sql = "SELECT " +
                "SUM(quantity) OVER (PARTITION BY suppkey ORDER BY orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_quantity_A, " +
                "LAG(quantity, 1, 0.0) OVER (PARTITION BY orderkey ORDER BY shipdate ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_quantity_B, " +
                "SUM(discount) OVER (PARTITION BY suppkey ORDER BY orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_discount_A " +
                "FROM lineitem";

        assertUnitPlan(sql,
                anyTree(
                        window(
                                partitionBySuppkeyOrderByOrderkey,
                                ImmutableList.of(functionCall("sum", rowsUnboundedPrecedingCurrentRowFrame, discountSymbolAlias)),
                                window(
                                        partitionByOrderkeyOrderByShipdate,
                                        ImmutableList.of(functionCall("lag", rowsUnboundedPrecedingCurrentRowFrame, quantitySymbolAlias, "ANY_1", "ANY_2")),
                                        project(
                                                window(
                                                        partitionBySuppkeyOrderByOrderkey,
                                                        ImmutableList.of(
                                                                functionCall("sum", rowsUnboundedPrecedingCurrentRowFrame, quantitySymbolAlias)),
                                                        tableScanFromLineitem))))));
    }

    @Test
    public void testIdenticalWindowSpecificationsAAcpA()
    {
        @Language("SQL") String sql = "SELECT " +
                "SUM(quantity) OVER (PARTITION BY suppkey ORDER BY orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_quantity_A, " +
                "LAG(quantity, 1, 0.0) OVER (PARTITION BY suppkey ORDER BY orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_quantity_B, " +
                "SUM(discount) OVER (PARTITION BY suppkey ORDER BY orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_discount_A " +
                "FROM lineitem";

        assertUnitPlan(sql,
                anyTree(
                        window(
                                partitionBySuppkeyOrderByOrderkey,
                                ImmutableList.of(
                                        functionCall("sum", rowsUnboundedPrecedingCurrentRowFrame, discountSymbolAlias),
                                        functionCall("lag", rowsUnboundedPrecedingCurrentRowFrame, quantitySymbolAlias, "ANY_1", "ANY_2")),
                                project(
                                        window(
                                                partitionBySuppkeyOrderByOrderkey,
                                                ImmutableList.of(
                                                        functionCall("sum", rowsUnboundedPrecedingCurrentRowFrame, quantitySymbolAlias)),
                                                tableScanFromLineitem)))));
    }

    @Test
    public void testIdenticalWindowSpecificationsDefaultFrame()
    {
        ExpectedWindowNodeSpecification partitionBySuppkeyOrderByOrderkey = windowSpecification(
                ImmutableList.of(suppkeySymbolAlias),
                ImmutableList.of(orderkeySymbolAlias),
                ImmutableMap.of(orderkeySymbolAlias, SortOrder.ASC_NULLS_LAST));

        ExpectedWindowNodeSpecification partitionByOrderkeyOrderByShipdate = windowSpecification(
                ImmutableList.of(orderkeySymbolAlias),
                ImmutableList.of(shipdateSymbolAlias),
                ImmutableMap.of(shipdateSymbolAlias, SortOrder.ASC_NULLS_LAST));

        @Language("SQL") String sql = "SELECT " +
                "AVG(quantity) OVER (PARTITION By suppkey ORDER BY orderkey), " +
                "SUM(quantity) OVER (PARTITION BY orderkey ORDER BY shipdate), " +
                "MIN(discount) OVER (PARTITION BY suppkey ORDER BY orderkey) " +
                "FROM lineitem";

        assertUnitPlan(sql,
                anyTree(
                        window(
                                partitionByOrderkeyOrderByShipdate,
                                ImmutableList.of(
                                        functionCall("sum", noFrame, quantitySymbolAlias)),
                                window(partitionBySuppkeyOrderByOrderkey,
                                        ImmutableList.of(
                                                functionCall("avg", noFrame, quantitySymbolAlias),
                                                functionCall("min", noFrame, discountSymbolAlias)),
                                        tableScanFromLineitem))));
    }

    private static ExpectedWindowNodeSpecification windowSpecification(List<String> partitionByAliases, List<String> orderByAliases, Map<String, SortOrder> orderings)
    {
        return new ExpectedWindowNodeSpecification(partitionByAliases, orderByAliases, orderings);
    }

    @Test
    public void testMergeDifferentFrames()
    {
        Optional<WindowFrame> frameC = Optional.of(new WindowFrame(
                WindowFrame.Type.ROWS,
                new FrameBound(FrameBound.Type.UNBOUNDED_PRECEDING),
                Optional.of(new FrameBound(FrameBound.Type.CURRENT_ROW))));

        ExpectedWindowNodeSpecification partitionBySuppkeyOrderByOrderkey = windowSpecification(
                ImmutableList.of(suppkeySymbolAlias),
                ImmutableList.of(orderkeySymbolAlias),
                ImmutableMap.of(orderkeySymbolAlias, SortOrder.ASC_NULLS_LAST));

        Optional<WindowFrame> frameD = Optional.of(new WindowFrame(
                WindowFrame.Type.ROWS,
                new FrameBound(FrameBound.Type.CURRENT_ROW),
                Optional.of(new FrameBound(FrameBound.Type.UNBOUNDED_FOLLOWING))));

        @Language("SQL") String sql = "SELECT " +
                "SUM(quantity) OVER (PARTITION BY suppkey ORDER BY orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_quantity_C, " +
                "AVG(quantity) OVER (PARTITION BY suppkey ORDER BY orderkey ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) avg_quantity_D, " +
                "MIN(discount) OVER (PARTITION BY suppkey ORDER BY orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_discount_C " +
                "FROM lineitem";

        assertUnitPlan(sql,
                anyTree(
                        window(
                                partitionBySuppkeyOrderByOrderkey,
                                ImmutableList.of(
                                        functionCall("AVG", frameD, quantitySymbolAlias),
                                        functionCall("MIN", frameC, discountSymbolAlias),
                                        functionCall("SUM", frameC, quantitySymbolAlias)),
                                tableScanFromLineitem)));
    }

    @Test
    public void testMergeDifferentFramesWithDefault()
    {
        Optional<WindowFrame> rowsCurrentRowUnboundedFollowingFrame = Optional.of(new WindowFrame(
                WindowFrame.Type.ROWS,
                new FrameBound(FrameBound.Type.CURRENT_ROW),
                Optional.of(new FrameBound(FrameBound.Type.UNBOUNDED_FOLLOWING))));

        ExpectedWindowNodeSpecification partitionBySuppkeyOrderByOrderkey = windowSpecification(
                ImmutableList.of(suppkeySymbolAlias),
                ImmutableList.of(orderkeySymbolAlias),
                ImmutableMap.of(orderkeySymbolAlias, SortOrder.ASC_NULLS_LAST));

        @Language("SQL") String sql = "SELECT " +
                "SUM(quantity) OVER (PARTITION BY suppkey ORDER BY orderkey) sum_quantity_C, " +
                "AVG(quantity) OVER (PARTITION BY suppkey ORDER BY orderkey ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) avg_quantity_D, " +
                "MIN(discount) OVER (PARTITION BY suppkey ORDER BY orderkey) sum_discount_C " +
                "FROM lineitem";

        assertUnitPlan(sql,
                anyTree(
                        window(
                                partitionBySuppkeyOrderByOrderkey,
                                ImmutableList.of(
                                        functionCall("AVG", rowsCurrentRowUnboundedFollowingFrame, quantitySymbolAlias),
                                        functionCall("MIN", noFrame, discountSymbolAlias),
                                        functionCall("SUM", noFrame, quantitySymbolAlias)),
                                tableScanFromLineitem)));
    }

    @Test
    public void testNotMergeAcrossJoinBranches()
    {
        @Language("SQL") String sql = "WITH foo AS (" +
                "SELECT " +
                "suppkey, orderkey, partkey, " +
                "SUM(discount) OVER (PARTITION BY orderkey ORDER BY shipdate, quantity DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) a " +
                "FROM lineitem WHERE (partkey = 272 OR partkey = 273) AND suppkey > 50 " +
                "), " +
                "bar AS ( " +
                "SELECT " +
                "suppkey, orderkey, partkey, " +
                "AVG(quantity) OVER (PARTITION BY orderkey ORDER BY shipdate, quantity DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) b " +
                "FROM lineitem WHERE (partkey = 272 OR partkey = 273) AND suppkey > 50 " +
                ")" +
                "SELECT * FROM foo, bar WHERE foo.a = bar.b";

        ExpectedWindowNodeSpecification partitionByOrderkeyOrderByShipdateQuantity = windowSpecification(
                ImmutableList.of(orderkeySymbolAlias),
                ImmutableList.of(shipdateSymbolAlias, quantitySymbolAlias),
                ImmutableMap.of(
                        shipdateSymbolAlias, SortOrder.ASC_NULLS_LAST,
                        quantitySymbolAlias, SortOrder.DESC_NULLS_LAST));

        ExpectedWindowNodeSpecification partitionByOrderkeyOrderByShipdate2Quantity2 = windowSpecification(
                ImmutableList.of("O2"),
                ImmutableList.of("SD2", "Q2"),
                ImmutableMap.of(
                        "SD2", SortOrder.ASC_NULLS_LAST,
                        "Q2", SortOrder.DESC_NULLS_LAST));

        assertUnitPlan(sql,
                anyTree(
                        join(JoinNode.Type.INNER, ImmutableList.of(),
                                project(
                                        window(
                                                partitionByOrderkeyOrderByShipdateQuantity,
                                                ImmutableList.of(functionCall("SUM", rowsUnboundedPrecedingCurrentRowFrame, discountSymbolAlias)),
                                                anyNot(WindowNode.class,
                                                        tableScan("lineitem")
                                                                .withSymbol("quantity", quantitySymbolAlias)
                                                                .withSymbol("discount", discountSymbolAlias)
                                                                .withSymbol("suppkey", suppkeySymbolAlias)
                                                                .withSymbol("orderkey", orderkeySymbolAlias)
                                                                .withSymbol("shipdate", shipdateSymbolAlias)))),
                                project(
                                        window(
                                                partitionByOrderkeyOrderByShipdate2Quantity2,
                                                ImmutableList.of(functionCall("AVG", rowsUnboundedPrecedingCurrentRowFrame, "Q2")),
                                                anyNot(WindowNode.class,
                                                        tableScan("lineitem")
                                                                .withSymbol("quantity", "Q2")
                                                                .withSymbol("shipdate", "SD2")
                                                                .withSymbol("orderkey", "O2")))))));
    }

    @Test
    public void testNotMergeDifferentPartition()
    {
        @Language("SQL") String sql = "SELECT " +
                "MAX(extendedprice) OVER (PARTITION BY suppkey ORDER BY orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_extendedprice_A, " +
                "MIN(quantity) over (PARTITION BY quantity ORDER BY orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_quantity_C " +
                "FROM lineitem";

        ExpectedWindowNodeSpecification specificationC = windowSpecification(
                ImmutableList.of(quantitySymbolAlias),
                ImmutableList.of(orderkeySymbolAlias),
                ImmutableMap.of(orderkeySymbolAlias, SortOrder.ASC_NULLS_LAST));

        assertUnitPlan(sql,
                anyTree(
                        window(
                                specificationC,
                                ImmutableList.of(
                                        functionCall("MIN", rowsUnboundedPrecedingCurrentRowFrame, quantitySymbolAlias)),
                                window(
                                        partitionBySuppkeyOrderByOrderkey,
                                        ImmutableList.of(
                                                functionCall("MAX", rowsUnboundedPrecedingCurrentRowFrame, extendedSymbolAlias)),
                                        tableScan("lineitem")
                                                .withSymbol("quantity", quantitySymbolAlias)
                                                .withSymbol("suppkey", suppkeySymbolAlias)
                                                .withSymbol("orderkey", orderkeySymbolAlias)))));
    }

    @Test
    public void testNotMergeDifferentOrderBy()
    {
        @Language("SQL") String sql = "SELECT " +
                "SUM(extendedprice) OVER (PARTITION BY suppkey ORDER BY orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_extendedprice_A, " +
                "MAX(quantity) OVER (PARTITION BY suppkey ORDER BY quantity ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_quantity_C " +
                "FROM lineitem";

        ExpectedWindowNodeSpecification specificationC = windowSpecification(
                ImmutableList.of(suppkeySymbolAlias),
                ImmutableList.of(quantitySymbolAlias),
                ImmutableMap.of(quantitySymbolAlias, SortOrder.ASC_NULLS_LAST));

        assertUnitPlan(sql,
                anyTree(
                        window(
                                specificationC,
                                ImmutableList.of(
                                        functionCall("MAX", rowsUnboundedPrecedingCurrentRowFrame, quantitySymbolAlias)),
                                window(
                                        partitionBySuppkeyOrderByOrderkey,
                                        ImmutableList.of(
                                                functionCall("SUM", rowsUnboundedPrecedingCurrentRowFrame, extendedSymbolAlias)),
                                        tableScan("lineitem")
                                                .withSymbol("quantity", quantitySymbolAlias)
                                                .withSymbol("suppkey", suppkeySymbolAlias)
                                                .withSymbol("orderkey", orderkeySymbolAlias)))));
    }

    @Test
    public void testNotMergeDifferentOrdering()
    {
        @Language("SQL") String sql = "SELECT " +
                "SUM(extendedprice) OVER (PARTITION BY suppkey ORDER BY orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_extendedprice_A, " +
                "MAX(quantity) over (PARTITION BY suppkey ORDER BY orderkey DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_quantity_C, " +
                "MIN(discount) over (PARTITION BY suppkey ORDER BY orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_discount_A " +
                "FROM lineitem";

        ExpectedWindowNodeSpecification specificationC = windowSpecification(
                ImmutableList.of(suppkeySymbolAlias),
                ImmutableList.of(orderkeySymbolAlias),
                ImmutableMap.of(orderkeySymbolAlias, SortOrder.DESC_NULLS_LAST));

        assertUnitPlan(sql,
                anyTree(
                        window(
                                specificationC,
                                ImmutableList.of(
                                        functionCall("MAX", rowsUnboundedPrecedingCurrentRowFrame, quantitySymbolAlias)),
                                window(
                                        partitionBySuppkeyOrderByOrderkey,
                                        ImmutableList.of(
                                                functionCall("SUM", rowsUnboundedPrecedingCurrentRowFrame, extendedSymbolAlias),
                                                functionCall("MIN", rowsUnboundedPrecedingCurrentRowFrame, discountSymbolAlias)),
                                        tableScanFromLineitem))));
    }

    @Test
    public void testNotMergeDifferentNullOrdering()
    {
        @Language("SQL") String sql = "SELECT " +
                "AVG(extendedprice) OVER (PARTITION BY suppkey ORDER BY orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_extendedprice_A, " +
                "SUM(quantity) OVER (PARTITION BY suppkey ORDER BY orderkey NULLS FIRST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_quantity_C, " +
                "MIN(discount) OVER (PARTITION BY suppkey ORDER BY orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_discount_A " +
                "FROM lineitem";

        ExpectedWindowNodeSpecification parititionBySuppkeyOrderByOrderkey = windowSpecification(
                ImmutableList.of(suppkeySymbolAlias),
                ImmutableList.of(orderkeySymbolAlias),
                ImmutableMap.of(orderkeySymbolAlias, SortOrder.ASC_NULLS_FIRST));

        assertUnitPlan(sql,
                anyTree(
                        window(
                                parititionBySuppkeyOrderByOrderkey,
                                ImmutableList.of(
                                        functionCall("SUM", rowsUnboundedPrecedingCurrentRowFrame, quantitySymbolAlias)),
                                window(
                                        partitionBySuppkeyOrderByOrderkey,
                                        ImmutableList.of(
                                                functionCall("AVG", rowsUnboundedPrecedingCurrentRowFrame, extendedSymbolAlias),
                                                functionCall("MIN", rowsUnboundedPrecedingCurrentRowFrame, discountSymbolAlias)),
                                        tableScanFromLineitem))));
    }

    private void assertUnitPlan(@Language("SQL") String sql, PlanMatchPattern pattern)
    {
        queryRunner.inTransaction(transactionSession -> {
            Plan actualPlan = unitPlan(transactionSession, sql);
            PlanAssert.assertPlan(transactionSession, queryRunner.getMetadata(), actualPlan, pattern);
            return null;
        });
    }

    private Plan unitPlan(Session transactionSession, @Language("SQL") String sql)
    {
        FeaturesConfig featuresConfig = new FeaturesConfig()
                .setExperimentalSyntaxEnabled(true)
                .setDistributedIndexJoinsEnabled(false)
                .setOptimizeHashGeneration(true);
        List<PlanOptimizer> optimizers = ImmutableList.of(
                new UnaliasSymbolReferences(),
                new PruneIdentityProjections(),
                new MergeWindows(),
                new PruneUnreferencedOutputs());
        return queryRunner.createPlan(transactionSession, sql, featuresConfig, optimizers);
    }
}
