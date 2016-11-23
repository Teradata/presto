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
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.cost.CoefficientBasedCostCalculator;
import com.facebook.presto.cost.CostCalculator;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.assertions.PlanAssert;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aliasPair;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.semiJoin;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

public class TestDetermineJoinDistributionType
{
    private final LocalQueryRunner queryRunner;
    private final Session automaticJoinDistributionSession;
    private final Session partitionedJoinSession;
    private final CostCalculator costCalculator;

    public TestDetermineJoinDistributionType()
    {
        this.queryRunner = new LocalQueryRunner(testSessionBuilder()
                .setCatalog("local")
                .setSchema("sf10")
                .build());

        automaticJoinDistributionSession = Session.builder(queryRunner.getDefaultSession())
                .setSystemProperty(SystemSessionProperties.JOIN_DISTRIBUTION_TYPE, "automatic")
                .setSystemProperty(SystemSessionProperties.JOIN_REORDERING, "true")
                .build();

        partitionedJoinSession = Session.builder(queryRunner.getDefaultSession())
                .setSystemProperty(SystemSessionProperties.JOIN_DISTRIBUTION_TYPE, "repartitioned")
                .setSystemProperty(SystemSessionProperties.JOIN_REORDERING, "true")
                .build();

        queryRunner.createCatalog(queryRunner.getDefaultSession().getCatalog().get(),
                new TpchConnectorFactory(1),
                ImmutableMap.<String, String>of());
        costCalculator = new CoefficientBasedCostCalculator(queryRunner.getMetadata());
    }

    @Test
    public void testReplicatesSmallJoin()
    {
        @Language("SQL") String sql = "select * from nation join region on nation.regionkey = region.regionkey";

        PlanMatchPattern pattern =
                anyTree(
                        join(JoinNode.Type.INNER,
                                ImmutableList.of(aliasPair("X", "Y")),
                                JoinNode.DistributionType.REPLICATED,
                                anyTree(tableScan("nation")),
                                anyTree(tableScan("region"))));

        assertPlan(automaticJoinDistributionSession, sql, pattern);
    }

    @Test
    public void testReplicatesAndFlipsSmallJoin()
    {
        @Language("SQL") String sql = "select * from nation join lineitem on nation.regionkey = lineitem.orderkey";

        PlanMatchPattern pattern =
                anyTree(
                        join(JoinNode.Type.INNER,
                                ImmutableList.of(aliasPair("X", "Y")),
                                JoinNode.DistributionType.REPLICATED,
                                anyTree(tableScan("lineitem")),
                                anyTree(tableScan("nation"))));

        assertPlan(automaticJoinDistributionSession, sql, pattern);
    }

    @Test
    public void testDoesNotFlipWhenIllegal()
    {
        @Language("SQL") String sql = "select * from nation left outer join lineitem on nation.regionkey = lineitem.orderkey";

        PlanMatchPattern pattern =
                anyTree(
                        join(JoinNode.Type.LEFT,
                                ImmutableList.of(aliasPair("X", "Y")),
                                JoinNode.DistributionType.PARTITIONED,
                                anyTree(tableScan("nation")),
                                anyTree(tableScan("lineitem"))));

        assertPlan(automaticJoinDistributionSession, sql, pattern);

        sql = "select * from lineitem left outer join nation on nation.regionkey = lineitem.orderkey";
        pattern =
                anyTree(
                        join(JoinNode.Type.LEFT,
                                ImmutableList.of(aliasPair("X", "Y")),
                                JoinNode.DistributionType.REPLICATED,
                                anyTree(tableScan("lineitem")),
                                anyTree(tableScan("nation"))));

        assertPlan(automaticJoinDistributionSession, sql, pattern);
    }

    @Test
    public void testPartitionsLargeJoin()
    {
        @Language("SQL") String sql = "select * from  lineitem l1 join lineitem l2 on l1.orderkey = l2.orderkey";

        PlanMatchPattern pattern =
                anyTree(
                        join(JoinNode.Type.INNER,
                                ImmutableList.of(aliasPair("X", "Y")),
                                JoinNode.DistributionType.PARTITIONED,
                                anyTree(tableScan("lineitem")),
                                anyTree(tableScan("lineitem"))));

        assertPlan(automaticJoinDistributionSession, sql, pattern);
    }

    @Test
    public void testObeysSessionPropertyForJoin()
    {
        @Language("SQL") String sql = "select * from region join nation on nation.regionkey = region.regionkey";

        PlanMatchPattern pattern =
                anyTree(
                        join(JoinNode.Type.INNER,
                                ImmutableList.of(aliasPair("X", "Y")),
                                JoinNode.DistributionType.PARTITIONED,
                                anyTree(tableScan("region")),
                                anyTree(tableScan("nation"))));

        assertPlan(partitionedJoinSession, sql, pattern);
    }

    @Test
    public void testReplicatesSmallSemiJoin()
    {
        @Language("SQL") String sql = "SELECT * from nation where nationkey in (SELECT regionkey FROM region)";

        PlanMatchPattern pattern =
                anyTree(
                        semiJoin(
                                "X", "Y", "S",
                                SemiJoinNode.DistributionType.REPLICATED,
                                anyTree(tableScan("nation")),
                                anyTree(tableScan("region"))));

        assertPlan(automaticJoinDistributionSession, sql, pattern);
    }

    @Test
    public void testPartitionsLargeSemiJoin()
    {
        @Language("SQL") String sql = "SELECT * from nation where nationkey in (SELECT orderkey FROM orders)";

        PlanMatchPattern pattern =
                anyTree(
                        semiJoin(
                                "X", "Y", "S",
                                SemiJoinNode.DistributionType.PARTITIONED,
                                anyTree(tableScan("nation")),
                                anyTree(tableScan("orders"))));

        assertPlan(automaticJoinDistributionSession, sql, pattern);
    }

    @Test
    public void testObeysSessionPropertySemiJoin()
    {
        @Language("SQL") String sql = "SELECT * from nation where nationkey in (SELECT regionkey FROM region)";

        PlanMatchPattern pattern =
                anyTree(
                        semiJoin(
                                "X", "Y", "S",
                                SemiJoinNode.DistributionType.PARTITIONED,
                                anyTree(tableScan("nation")),
                                anyTree(tableScan("region"))));

        assertPlan(partitionedJoinSession, sql, pattern);
    }

    private void assertPlan(Session session, @Language("SQL") String sql, PlanMatchPattern pattern)
    {
        queryRunner.inTransaction(session, transactionSession -> {
            Plan actualPlan = queryRunner.createPlan(transactionSession, sql);
            PlanAssert.assertPlan(transactionSession, queryRunner.getMetadata(), costCalculator, actualPlan, pattern);
            return null;
        });
    }
}
