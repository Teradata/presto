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

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.tree.ArithmeticBinaryExpression;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.testing.LocalQueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.sql.ExpressionUtils.and;
import static com.facebook.presto.sql.planner.assertions.PlanAssert.assertPlan;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.joinGraph;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.LEFT;
import static com.facebook.presto.sql.tree.ArithmeticBinaryExpression.Type.ADD;
import static com.facebook.presto.sql.tree.ComparisonExpressionType.GREATER_THAN;
import static com.facebook.presto.sql.tree.ComparisonExpressionType.LESS_THAN;
import static com.facebook.presto.sql.tree.ComparisonExpressionType.NOT_EQUAL;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

public class TestJoinGraphNodeBuilder
{
    private final LocalQueryRunner queryRunner = new LocalQueryRunner(testSessionBuilder().build());

    @Test(expectedExceptions = IllegalStateException.class)
    public void testDoesNotFireForOuterJoins()
    {
        PlanBuilder p = new PlanBuilder(new PlanNodeIdAllocator(), queryRunner.getMetadata());
        JoinNode outerJoin = p.join(
                JoinNode.Type.FULL,
                p.values(p.symbol("A1", BIGINT)),
                p.values(p.symbol("B1", BIGINT)),
                ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT))),
                ImmutableList.of(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT)),
                Optional.empty());
        new JoinGraphNode.JoinGraphNodeBuilder(outerJoin, queryRunner.getLookup());
    }

    @Test
    public void testDoesNotConvertNestedOuterJoins()
    {
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        PlanBuilder planBuilder = new PlanBuilder(idAllocator, queryRunner.getMetadata());
        JoinNode joinNode = planBuilder.join(
                INNER,
                planBuilder.join(
                        LEFT,
                        planBuilder.values(planBuilder.symbol("A1", BIGINT)),
                        planBuilder.values(planBuilder.symbol("B1", BIGINT)),
                        ImmutableList.of(new JoinNode.EquiJoinClause(planBuilder.symbol("A1", BIGINT), planBuilder.symbol("B1", BIGINT))),
                        ImmutableList.of(planBuilder.symbol("A1", BIGINT), planBuilder.symbol("B1", BIGINT)),
                        Optional.empty()),
                planBuilder.values(planBuilder.symbol("C1", BIGINT)),
                ImmutableList.of(new JoinNode.EquiJoinClause(planBuilder.symbol("A1", BIGINT), planBuilder.symbol("C1", BIGINT))),
                ImmutableList.of(planBuilder.symbol("A1", BIGINT), planBuilder.symbol("B1", BIGINT), planBuilder.symbol("C1", BIGINT)),
                Optional.empty());
        PlanMatchPattern pattern = joinGraph(
                "A1=C1",
                ImmutableList.of("A1", "B1", "C1"),
                join(LEFT, ImmutableList.of(equiJoinClause("A1", "B1")),
                        values(ImmutableMap.of("A1", 0)),
                        values(ImmutableMap.of("B1", 0))),
                values(ImmutableMap.of("C1", 0)));
        testJoinGraphBuilder(joinNode, pattern, planBuilder.getSymbols(), idAllocator);
    }

    @Test
    public void testRetainsOutputSymbols()
    {
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        PlanBuilder planBuilder = new PlanBuilder(idAllocator, queryRunner.getMetadata());
        JoinNode joinNode = planBuilder.join(
                INNER,
                planBuilder.values(planBuilder.symbol("A1", BIGINT)),
                planBuilder.join(
                        INNER,
                        planBuilder.values(planBuilder.symbol("B1", BIGINT), planBuilder.symbol("B2", BIGINT)),
                        planBuilder.values(planBuilder.symbol("C1", BIGINT), planBuilder.symbol("C2", BIGINT)),
                        ImmutableList.of(new JoinNode.EquiJoinClause(planBuilder.symbol("B1", BIGINT), planBuilder.symbol("C1", BIGINT))),
                        ImmutableList.of(
                                planBuilder.symbol("B1", BIGINT),
                                planBuilder.symbol("B2", BIGINT),
                                planBuilder.symbol("C1", BIGINT),
                                planBuilder.symbol("C2", BIGINT)),
                        Optional.empty()),
                ImmutableList.of(new JoinNode.EquiJoinClause(planBuilder.symbol("A1", BIGINT), planBuilder.symbol("B1", BIGINT))),
                ImmutableList.of(planBuilder.symbol("A1", BIGINT), planBuilder.symbol("B1", BIGINT)),
                Optional.empty());
        PlanMatchPattern pattern = joinGraph(
                "B1=C1 AND A1=B1",
                ImmutableList.of("A1", "B1"),
                values(ImmutableMap.of("A1", 0)),
                values(ImmutableMap.of("B1", 0, "B2", 1)),
                values(ImmutableMap.of("C1", 0, "C2", 1)));
        testJoinGraphBuilder(joinNode, pattern, planBuilder.getSymbols(), idAllocator);
    }

    @Test
    public void testCombinesCriteriaAndFilters()
    {
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        PlanBuilder planBuilder = new PlanBuilder(idAllocator, queryRunner.getMetadata());
        JoinNode joinNode = planBuilder.join(
                INNER,
                planBuilder.values(planBuilder.symbol("A1", BIGINT)),
                planBuilder.join(
                        INNER,
                        planBuilder.values(planBuilder.symbol("B1", BIGINT), planBuilder.symbol("B2", BIGINT)),
                        planBuilder.values(planBuilder.symbol("C1", BIGINT), planBuilder.symbol("C2", BIGINT)),
                        ImmutableList.of(new JoinNode.EquiJoinClause(planBuilder.symbol("B1", BIGINT), planBuilder.symbol("C1", BIGINT))),
                        ImmutableList.of(
                                planBuilder.symbol("B1", BIGINT),
                                planBuilder.symbol("B2", BIGINT),
                                planBuilder.symbol("C1", BIGINT),
                                planBuilder.symbol("C2", BIGINT)),
                        Optional.of(and(
                                new ComparisonExpression(GREATER_THAN, planBuilder.symbol("C2", BIGINT).toSymbolReference(), new LongLiteral("0")),
                                new ComparisonExpression(NOT_EQUAL, planBuilder.symbol("C2", BIGINT).toSymbolReference(), new LongLiteral("7")),
                                new ComparisonExpression(GREATER_THAN, planBuilder.symbol("B2", BIGINT).toSymbolReference(), planBuilder.symbol("C2", BIGINT).toSymbolReference())))),
                ImmutableList.of(new JoinNode.EquiJoinClause(planBuilder.symbol("A1", BIGINT), planBuilder.symbol("B1", BIGINT))),
                ImmutableList.of(planBuilder.symbol("A1", BIGINT), planBuilder.symbol("B1", BIGINT), planBuilder.symbol("B2", BIGINT), planBuilder.symbol("C1", BIGINT), planBuilder.symbol("C2", BIGINT)),
                Optional.of(new ComparisonExpression(
                        LESS_THAN,
                        new ArithmeticBinaryExpression(ADD, planBuilder.symbol("A1", BIGINT).toSymbolReference(), planBuilder.symbol("C1", BIGINT).toSymbolReference()),
                        planBuilder.symbol("B1", BIGINT).toSymbolReference())));
        PlanMatchPattern pattern = joinGraph(
                "B1=C1 AND A1=B1 AND C2 > 0 AND C2 <>7 AND B2 > C2 AND A1+ C1 < B1",
                ImmutableList.of("A1", "B1", "B2", "C1", "C2"),
                values(ImmutableMap.of("A1", 0)),
                values(ImmutableMap.of("B1", 0, "B2", 1)),
                values(ImmutableMap.of("C1", 0, "C2", 1)));
        testJoinGraphBuilder(joinNode, pattern, planBuilder.getSymbols(), idAllocator);
    }

    @Test
    public void testConvertsBushyTrees()
    {
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        PlanBuilder planBuilder = new PlanBuilder(idAllocator, queryRunner.getMetadata());
        JoinNode joinNode = planBuilder.join(
                INNER,
                planBuilder.join(
                        INNER,
                        planBuilder.join(
                                INNER,
                                planBuilder.values(planBuilder.symbol("A1", BIGINT)),
                                planBuilder.values(planBuilder.symbol("B1", BIGINT)),
                                ImmutableList.of(new JoinNode.EquiJoinClause(planBuilder.symbol("A1", BIGINT), planBuilder.symbol("B1", BIGINT))),
                                ImmutableList.of(planBuilder.symbol("A1", BIGINT), planBuilder.symbol("B1", BIGINT)),
                                Optional.empty()),
                        planBuilder.values(planBuilder.symbol("C1", BIGINT)),
                        ImmutableList.of(new JoinNode.EquiJoinClause(planBuilder.symbol("A1", BIGINT), planBuilder.symbol("C1", BIGINT))),
                        ImmutableList.of(planBuilder.symbol("A1", BIGINT), planBuilder.symbol("B1", BIGINT), planBuilder.symbol("C1", BIGINT)),
                        Optional.empty()),
                planBuilder.join(
                        INNER,
                        planBuilder.values(planBuilder.symbol("D1", BIGINT), planBuilder.symbol("D2", BIGINT)),
                        planBuilder.values(planBuilder.symbol("E1", BIGINT), planBuilder.symbol("E2", BIGINT)),
                        ImmutableList.of(
                                new JoinNode.EquiJoinClause(planBuilder.symbol("D1", BIGINT), planBuilder.symbol("E1", BIGINT)),
                                new JoinNode.EquiJoinClause(planBuilder.symbol("D2", BIGINT), planBuilder.symbol("E2", BIGINT))),
                        ImmutableList.of(
                                planBuilder.symbol("D1", BIGINT),
                                planBuilder.symbol("D2", BIGINT),
                                planBuilder.symbol("E1", BIGINT),
                                planBuilder.symbol("E2", BIGINT)),
                        Optional.empty()),
                ImmutableList.of(new JoinNode.EquiJoinClause(planBuilder.symbol("B1", BIGINT), planBuilder.symbol("E1", BIGINT))),
                ImmutableList.of(
                        planBuilder.symbol("A1", BIGINT),
                        planBuilder.symbol("B1", BIGINT),
                        planBuilder.symbol("C1", BIGINT),
                        planBuilder.symbol("D1", BIGINT),
                        planBuilder.symbol("D2", BIGINT),
                        planBuilder.symbol("E1", BIGINT),
                        planBuilder.symbol("E2", BIGINT)),
                Optional.empty());
        PlanMatchPattern pattern = joinGraph(
                "A1=B1 AND A1=C1 AND D1=E1 AND D2=E2 AND B1=E1",
                ImmutableList.of("A1", "B1", "C1", "D1", "D2", "E1", "E2"),
                values(ImmutableMap.of("A1", 0)),
                values(ImmutableMap.of("B1", 0)),
                values(ImmutableMap.of("C1", 0)),
                values(ImmutableMap.of("D1", 0, "D2", 1)),
                values(ImmutableMap.of("E1", 0, "E2", 1)));
        testJoinGraphBuilder(joinNode, pattern, planBuilder.getSymbols(), idAllocator);
    }

    private void testJoinGraphBuilder(JoinNode joinNode, PlanMatchPattern pattern, Map<Symbol, Type> types, PlanNodeIdAllocator idAllocator)
    {
        PlanNode result = new JoinGraphNode.JoinGraphNodeBuilder(joinNode, queryRunner.getLookup()).toJoinGraphNode(idAllocator);
        assertPlan(queryRunner.getDefaultSession(), queryRunner.getMetadata(), queryRunner.getLookup(), new Plan(result, types, queryRunner.getLookup(), queryRunner.getDefaultSession()), pattern);
    }
}
