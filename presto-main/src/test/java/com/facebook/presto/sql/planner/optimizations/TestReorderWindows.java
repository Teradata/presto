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

import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.assertions.PlanAssert;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.tree.SymbolReference;
import com.facebook.presto.sql.tree.WindowFrame;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Provider;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.window;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;

public class TestReorderWindows
{
    private final LocalQueryRunner queryRunner;

    private final Optional<WindowFrame> commonFrame;

    private final WindowNode.Specification windowA;
    private final WindowNode.Specification windowAp;
    private final WindowNode.Specification windowApp;
    private final WindowNode.Specification windowB;

    private final Symbol tax;
    private final Symbol suppkey;
    private final Symbol orderkey;
    private final Symbol partkey;
    private final Symbol shipdate;
    private final Symbol receiptdate;

    private final SymbolReference discountReference;
    private final SymbolReference quantityReference;
    private final SymbolReference taxReference;

    public TestReorderWindows()
    {
        queryRunner = new LocalQueryRunner(testSessionBuilder()
                .setCatalog("local")
                .setSchema("tiny")
                .build());

        queryRunner.createCatalog(queryRunner.getDefaultSession().getCatalog().get(),
                new TpchConnectorFactory(queryRunner.getNodeManager(), 1),
                ImmutableMap.of());

        commonFrame = Optional.empty();

        suppkey = new Symbol("suppkey");
        orderkey = new Symbol("orderkey");
        partkey = new Symbol("partkey");
        shipdate = new Symbol("shipdate");
        receiptdate = new Symbol("receiptdate");
        tax = new Symbol("tax");

        discountReference = new SymbolReference("discount");
        quantityReference = new SymbolReference("quantity");
        taxReference = new SymbolReference("tax");

        windowA = new WindowNode.Specification(
                ImmutableList.of(suppkey),
                ImmutableList.of(orderkey),
                ImmutableMap.of(orderkey, SortOrder.ASC_NULLS_LAST));

        windowAp = new WindowNode.Specification(
                ImmutableList.of(suppkey),
                ImmutableList.of(shipdate),
                ImmutableMap.of(shipdate, SortOrder.ASC_NULLS_LAST));

        windowApp = new WindowNode.Specification(
                ImmutableList.of(suppkey, tax),
                ImmutableList.of(receiptdate),
                ImmutableMap.of(receiptdate, SortOrder.ASC_NULLS_LAST));

        windowB = new WindowNode.Specification(
                ImmutableList.of(partkey),
                ImmutableList.of(receiptdate),
                ImmutableMap.of(receiptdate, SortOrder.ASC_NULLS_LAST));
    }

    private static String getWindowDescription(WindowNode.Specification specification)
    {
        return format("partition by %s order by %s",
                specification.getPartitionBy().stream().map(Symbol::getName).collect(Collectors.joining(", ")),
                specification.getOrderBy().stream().map(orderSymbol -> sortItemToString(orderSymbol, specification.getOrderings())).collect(Collectors.joining(", ")));
    }

    private static String sortItemToString(Symbol sortItem, Map<Symbol, SortOrder> orderings)
    {
        SortOrder sortOrder = orderings.get(sortItem);
        return format("%s %s %s", sortItem, (sortOrder.isAscending() ? "ASC" : "DESC"), (sortOrder.isNullsFirst() ? "NULLS FIRST" : "NULLS LAST"));
    }

    @Test
    public void testNonMergeableABAReordersToAABAllOptimizers()
    {
        @Language("SQL") String sql = "select " +
                "sum(quantity) over(" + getWindowDescription(windowA) + ") sum_quantity_A, " +
                "avg(discount) over(" + getWindowDescription(windowB) + ") avg_discount_B, " +
                "min(tax) over(" + getWindowDescription(windowAp) + ") min_tax_A " +
                "from lineitem";

        PlanMatchPattern pattern =
                anyTree(
                        window(windowAp,
                                ImmutableList.of(
                                        functionCall("min", commonFrame, taxReference)),
                                window(windowA,
                                        ImmutableList.of(
                                                functionCall("sum", commonFrame, quantityReference)),
                                        anyTree(
                                        window(windowB,
                                                ImmutableList.of(
                                                        functionCall("avg", commonFrame, discountReference)),
                                                anyTree())))));

        Plan actualPlan = queryRunner.inTransaction(transactionSession -> queryRunner.createPlan(transactionSession, sql));
        queryRunner.inTransaction(transactionSession -> {
            PlanAssert.assertPlan(transactionSession, queryRunner.getMetadata(), actualPlan, pattern);
            return null;
        });
    }

    @Test
    public void testNonMergeableABAReordersToAAB()
    {
        @Language("SQL") String sql = "select " +
                "sum(quantity) over(" + getWindowDescription(windowA) + ") sum_quantity_A, " +
                "avg(discount) over(" + getWindowDescription(windowB) + ") avg_discount_B, " +
                "min(tax) over(" + getWindowDescription(windowAp) + ") min_tax_A " +
                "from lineitem";

        assertUnitPlan(sql,
                anyTree(
                        window(windowAp,
                                ImmutableList.of(
                                        functionCall("min", commonFrame, taxReference)),
                                        window(windowA,
                                                ImmutableList.of(
                                                        functionCall("sum", commonFrame, quantityReference)),
                                                        window(windowB,
                                                                ImmutableList.of(
                                                                        functionCall("avg", commonFrame, discountReference)),
                                                anyTree())))));
    }

    @Test
    public void testPrefixOfPartitionComesFirstRegardlessOfTheirOrderInSQL()
    {
        {
            @Language("SQL") String sql = "select " +
                    "avg(discount) over(" + getWindowDescription(windowApp) + ") avg_discount_A, " +
                    "sum(quantity) over(" + getWindowDescription(windowA) + ") sum_quantity_A " +
                    "from lineitem";

            assertUnitPlan(sql,
                    anyTree(window(windowApp,
                            ImmutableList.of(
                                    functionCall("avg", commonFrame, discountReference)),
                            window(windowA,
                                    ImmutableList.of(
                                            functionCall("sum", commonFrame, quantityReference)),
                                    anyTree()))));
        }

        {
            @Language("SQL") String sql = "select " +
                    "sum(quantity) over(" + getWindowDescription(windowA) + ") sum_quantity_A, " +
                    "avg(discount) over(" + getWindowDescription(windowApp) + ") avg_discount_A " +
                    "from lineitem";

            assertUnitPlan(sql,
                    anyTree(window(windowApp,
                            ImmutableList.of(
                                    functionCall("avg", commonFrame, discountReference)),
                            window(windowA,
                                    ImmutableList.of(
                                            functionCall("sum", commonFrame, quantityReference)),
                                    anyTree()))));
        }
    }

    @Test
    public void testNotReorderAcrossNonWindowNodes()
    {
        @Language("SQL") String sql = "select " +
                "avg(discount) over(" + getWindowDescription(windowApp) + ") avg_discount_A, " +
                "lag(quantity, 1) over(" + getWindowDescription(windowA) + ") lag_quantity_A " + // produces ProjectNode because of constant 1
                "from lineitem";

        assertUnitPlan(sql,
                anyTree(window(windowA,
                        ImmutableList.of(
                                functionCall("lag", commonFrame, quantityReference, new SymbolReference("expr"))),
                        project(
                        window(windowApp,
                                ImmutableList.of(
                                        functionCall("avg", commonFrame, discountReference)),
                                anyTree())))));
    }

    private void assertUnitPlan(@Language("SQL") String sql, PlanMatchPattern pattern)
    {
        Plan actualPlan = unitPlan(sql);
        queryRunner.inTransaction(transactionSession -> {
            PlanAssert.assertPlan(transactionSession, queryRunner.getMetadata(), actualPlan, pattern);
            return null;
        });
    }

    private Plan unitPlan(@Language("SQL") String sql)
    {
        FeaturesConfig featuresConfig = new FeaturesConfig()
                .setExperimentalSyntaxEnabled(true)
                .setDistributedIndexJoinsEnabled(false)
                .setOptimizeHashGeneration(true);
        Provider<List<PlanOptimizer>> optimizerProvider = () -> ImmutableList.of(
                new UnaliasSymbolReferences(),
                new PruneIdentityProjections(),
                new ReorderWindows(),
                new PruneUnreferencedOutputs()
        );

        return queryRunner.inTransaction(transactionSession -> queryRunner.createPlan(transactionSession, sql, featuresConfig, optimizerProvider));
    }
}
