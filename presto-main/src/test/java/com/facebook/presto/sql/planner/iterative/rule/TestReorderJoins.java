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
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.Session;
import com.facebook.presto.cost.CostComparator;
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleTester;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.testing.LocalQueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.plan.JoinNode.DistributionType.PARTITIONED;
import static com.facebook.presto.sql.planner.plan.JoinNode.DistributionType.REPLICATED;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static com.facebook.presto.testing.LocalQueryRunner.queryRunnerWithFakeNodeCountForStats;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

public class TestReorderJoins
{
    @Test
    public void testChoosesLeastCostOrder()
    {
        LocalQueryRunner queryRunner = new LocalQueryRunner(testSessionBuilder().setSystemProperty("join_reordering_strategy", "COST_BASED").build());
        new RuleTester(queryRunner).assertThat(new ReorderJoins(new CostComparator(1, 1, 1)))
                .on(p ->
                        p.join(
                                INNER,
                                p.values(new PlanNodeId("valuesA"), p.symbol("A1", BIGINT)),
                                p.join(
                                        INNER,
                                        p.values(new PlanNodeId("valuesB"), p.symbol("B1", BIGINT), p.symbol("B2", BIGINT)),
                                        p.values(new PlanNodeId("ValuesC"), p.symbol("C1", BIGINT)),
                                        ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("B1", BIGINT), p.symbol("C1", BIGINT))),
                                        ImmutableList.of(p.symbol("B1", BIGINT)),
                                        Optional.empty()),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("A1", BIGINT), p.symbol("B2", BIGINT))),
                                ImmutableList.of(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT)),
                                Optional.empty()))
                .withStats(ImmutableMap.of(
                        new PlanNodeId("valuesA"), PlanNodeStatsEstimate.builder().setOutputRowCount(new Estimate(10)).build(),
                        new PlanNodeId("valuesB"), PlanNodeStatsEstimate.builder().setOutputRowCount(new Estimate(10)).build(),
                        new PlanNodeId("ValuesC"), PlanNodeStatsEstimate.builder().setOutputRowCount(new Estimate(1000)).build()))
                .matches(join(
                        INNER,
                        ImmutableList.of(equiJoinClause("C1", "B1")),
                        Optional.empty(),
                        Optional.of(PARTITIONED),
                        values(ImmutableMap.of("C1", 0)),
                        join(
                                INNER,
                                ImmutableList.of(equiJoinClause("A1", "B2")),
                                Optional.empty(),
                                Optional.of(PARTITIONED),
                                values(ImmutableMap.of("A1", 0)),
                                values(ImmutableMap.of("B1", 0, "B2", 1))
                        )
                ));
    }

    @Test
    public void testKeepsOutputSymbols()
    {
        LocalQueryRunner queryRunner = new LocalQueryRunner(testSessionBuilder().setSystemProperty("join_reordering_strategy", "COST_BASED").build());
        new RuleTester(queryRunner).assertThat(new ReorderJoins(new CostComparator(1, 1, 1)))
                .on(p ->
                        p.join(
                                INNER,
                                p.values(new PlanNodeId("valuesA"), p.symbol("A1", BIGINT), p.symbol("A2", BIGINT)),
                                p.values(new PlanNodeId("valuesB"), p.symbol("B1", BIGINT)),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT))),
                                ImmutableList.of(p.symbol("A2", BIGINT)),
                                Optional.empty()))
                .withStats(ImmutableMap.of(
                        new PlanNodeId("valuesA"), PlanNodeStatsEstimate.builder().setOutputRowCount(new Estimate(10000)).build(),
                        new PlanNodeId("valuesB"), PlanNodeStatsEstimate.builder().setOutputRowCount(new Estimate(10000)).build()))
                .matches(join(
                        INNER,
                        ImmutableList.of(equiJoinClause("A1", "B1")),
                        Optional.empty(),
                        Optional.of(PARTITIONED),
                        values(ImmutableMap.of("A1", 0, "A2", 1)),
                        values(ImmutableMap.of("B1", 0))
                ));
    }

    @Test
    public void testReplicatesAndFlipsWhenOneTableMuchSmaller()
    {
        Session session = testSessionBuilder()
                .setCatalog("local")
                .setSchema("tiny")
                .setSystemProperty("join_distribution_type", "automatic")
                .setSystemProperty("join_reordering_strategy", "COST_BASED")
                .build();
        LocalQueryRunner queryRunner = queryRunnerWithFakeNodeCountForStats(session, 4);
        new RuleTester(queryRunner)
                .assertThat(new ReorderJoins(new CostComparator(1, 1, 1)))
                .on(p ->
                        p.join(
                                INNER,
                                p.values(new PlanNodeId("valuesA"), p.symbol("A1", BIGINT)),
                                p.values(new PlanNodeId("valuesB"), p.symbol("B1", BIGINT)),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT))),
                                ImmutableList.of(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT)),
                                Optional.empty()))
                .withStats(ImmutableMap.of(
                        new PlanNodeId("valuesA"), PlanNodeStatsEstimate.builder().setOutputRowCount(new Estimate(100)).build(),
                        new PlanNodeId("valuesB"), PlanNodeStatsEstimate.builder().setOutputRowCount(new Estimate(10000)).build()))
                .matches(join(
                        INNER,
                        ImmutableList.of(equiJoinClause("B1", "A1")),
                        Optional.empty(),
                        Optional.of(REPLICATED),
                        values(ImmutableMap.of("B1", 0)),
                        values(ImmutableMap.of("A1", 0))
                ));
    }

    @Test
    public void testRepartitionsWhenRequiredBySession()
    {
        Session session = testSessionBuilder()
                .setCatalog("local")
                .setSchema("tiny")
                .setSystemProperty("join_distribution_type", "repartitioned")
                .setSystemProperty("join_reordering_strategy", "COST_BASED")
                .build();
        LocalQueryRunner queryRunner = queryRunnerWithFakeNodeCountForStats(session, 4);
        new RuleTester(queryRunner)
                .assertThat(new ReorderJoins(new CostComparator(1, 1, 1)))
                .on(p ->
                        p.join(
                                INNER,
                                p.values(new PlanNodeId("valuesA"), p.symbol("A1", BIGINT)),
                                p.values(new PlanNodeId("valuesB"), p.symbol("B1", BIGINT)),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT))),
                                ImmutableList.of(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT)),
                                Optional.empty()))
                .withStats(ImmutableMap.of(
                        new PlanNodeId("valuesA"), PlanNodeStatsEstimate.builder().setOutputRowCount(new Estimate(100)).build(),
                        new PlanNodeId("valuesB"), PlanNodeStatsEstimate.builder().setOutputRowCount(new Estimate(10000)).build()))
                .matches(join(
                        INNER,
                        ImmutableList.of(equiJoinClause("B1", "A1")),
                        Optional.empty(),
                        Optional.of(PARTITIONED),
                        values(ImmutableMap.of("B1", 0)),
                        values(ImmutableMap.of("A1", 0))
                ));
    }

    @Test
    public void testRepartitionsWhenBothTablesEqual()
    {
        Session session = testSessionBuilder()
                .setCatalog("local")
                .setSchema("tiny")
                .setSystemProperty("join_distribution_type", "automatic")
                .setSystemProperty("join_reordering_strategy", "COST_BASED")
                .build();
        LocalQueryRunner queryRunner = queryRunnerWithFakeNodeCountForStats(session, 4);
        new RuleTester(queryRunner)
                .assertThat(new ReorderJoins(new CostComparator(1, 1, 1)))
                .on(p ->
                        p.join(
                                INNER,
                                p.values(new PlanNodeId("valuesA"), p.symbol("A1", BIGINT)),
                                p.values(new PlanNodeId("valuesB"), p.symbol("B1", BIGINT)),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT))),
                                ImmutableList.of(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT)),
                                Optional.empty()))
                .withStats(ImmutableMap.of(
                        new PlanNodeId("valuesA"), PlanNodeStatsEstimate.builder().setOutputRowCount(new Estimate(10000)).build(),
                        new PlanNodeId("valuesB"), PlanNodeStatsEstimate.builder().setOutputRowCount(new Estimate(10000)).build()))
                .matches(join(
                        INNER,
                        ImmutableList.of(equiJoinClause("A1", "B1")),
                        Optional.empty(),
                        Optional.of(PARTITIONED),
                        values(ImmutableMap.of("A1", 0)),
                        values(ImmutableMap.of("B1", 0))
                ));
    }

    @Test
    public void testReplicatesWhenRequiredBySession()
    {
        Session session = testSessionBuilder()
                .setCatalog("local")
                .setSchema("tiny")
                .setSystemProperty("join_distribution_type", "replicated")
                .setSystemProperty("join_reordering_strategy", "COST_BASED")
                .build();
        LocalQueryRunner queryRunner = queryRunnerWithFakeNodeCountForStats(session, 4);
        new RuleTester(queryRunner)
                .assertThat(new ReorderJoins(new CostComparator(1, 1, 1)))
                .on(p ->
                        p.join(
                                INNER,
                                p.values(new PlanNodeId("valuesA"), p.symbol("A1", BIGINT)),
                                p.values(new PlanNodeId("valuesB"), p.symbol("B1", BIGINT)),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT))),
                                ImmutableList.of(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT)),
                                Optional.empty()))
                .withStats(ImmutableMap.of(
                        new PlanNodeId("valuesA"), PlanNodeStatsEstimate.builder().setOutputRowCount(new Estimate(10000)).build(),
                        new PlanNodeId("valuesB"), PlanNodeStatsEstimate.builder().setOutputRowCount(new Estimate(10000)).build()))
                .matches(join(
                        INNER,
                        ImmutableList.of(equiJoinClause("A1", "B1")),
                        Optional.empty(),
                        Optional.of(REPLICATED),
                        values(ImmutableMap.of("A1", 0)),
                        values(ImmutableMap.of("B1", 0))
                ));
    }
}
