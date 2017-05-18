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

import com.facebook.presto.sql.planner.iterative.rule.test.RuleTester;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.tree.ArithmeticBinaryExpression;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.testing.LocalQueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.sql.ExpressionUtils.and;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.joinGraph;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.LEFT;
import static com.facebook.presto.sql.tree.ArithmeticBinaryExpression.Type.ADD;
import static com.facebook.presto.sql.tree.ComparisonExpressionType.GREATER_THAN;
import static com.facebook.presto.sql.tree.ComparisonExpressionType.LESS_THAN;
import static com.facebook.presto.sql.tree.ComparisonExpressionType.NOT_EQUAL;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

public class TestConvertJoinTreeToJoinGraph
{
    @Test
    public void testDoesNotFireForOuterJoins()
    {
        new RuleTester().assertThat(new ConvertJoinTreeToJoinGraph())
                .on(p -> p.join(
                        JoinNode.Type.FULL,
                        p.values(p.symbol("A1", BIGINT)),
                        p.values(p.symbol("B1", BIGINT)),
                        ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT))),
                        ImmutableList.of(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT)),
                        Optional.empty()))
                .doesNotFire();
    }

    @Test
    public void testDoesNotConvertNestedOuterJoins()
    {
        LocalQueryRunner queryRunner = new LocalQueryRunner(testSessionBuilder().setSystemProperty("reorder_joins", "true").build());
        new RuleTester(queryRunner).assertThat(new ConvertJoinTreeToJoinGraph())
                .on(p -> p.join(
                        INNER,
                        p.join(
                                LEFT,
                                p.values(p.symbol("A1", BIGINT)),
                                p.values(p.symbol("B1", BIGINT)),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT))),
                                ImmutableList.of(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT)),
                                Optional.empty()),
                        p.values(p.symbol("C1", BIGINT)),
                        ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("A1", BIGINT), p.symbol("C1", BIGINT))),
                        ImmutableList.of(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT), p.symbol("C1", BIGINT)),
                        Optional.empty()))
                .matches(joinGraph(
                        "A1=C1",
                        join(LEFT, ImmutableList.of(equiJoinClause("A1", "B1")),
                                values(ImmutableMap.of("A1", 0)),
                                values(ImmutableMap.of("B1", 0))),
                        values(ImmutableMap.of("C1", 0))));
    }

    @Test
    public void testAddsProjectToMaintainOutputSymbols()
    {
        LocalQueryRunner queryRunner = new LocalQueryRunner(testSessionBuilder().setSystemProperty("reorder_joins", "true").build());
        new RuleTester(queryRunner).assertThat(new ConvertJoinTreeToJoinGraph())
                .on(p ->
                        p.join(
                                INNER,
                                p.values(p.symbol("A1", BIGINT)),
                                p.join(
                                        INNER,
                                        p.values(p.symbol("B1", BIGINT), p.symbol("B2", BIGINT)),
                                        p.values(p.symbol("C1", BIGINT), p.symbol("C2", BIGINT)),
                                        ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("B1", BIGINT), p.symbol("C1", BIGINT))),
                                        ImmutableList.of(
                                                p.symbol("B1", BIGINT),
                                                p.symbol("B2", BIGINT),
                                                p.symbol("C1", BIGINT),
                                                p.symbol("C2", BIGINT)),
                                        Optional.empty()),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT))),
                                ImmutableList.of(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT)),
                                Optional.empty()))
                .matches(
                        project(
                                ImmutableMap.of(
                                        "A1", expression("A1"),
                                        "B1", expression("B1")),
                                joinGraph(
                                        "B1=C1 AND A1=B1",
                                        values(ImmutableMap.of("A1", 0)),
                                        values(ImmutableMap.of("B1", 0, "B2", 1)),
                                        values(ImmutableMap.of("C1", 0, "C2", 1)))));
    }

    @Test
    public void testCombinesCriteriaAndFilters()
    {
        LocalQueryRunner queryRunner = new LocalQueryRunner(testSessionBuilder().setSystemProperty("reorder_joins", "true").build());
        new RuleTester(queryRunner).assertThat(new ConvertJoinTreeToJoinGraph())
                .on(p ->
                        p.join(
                                INNER,
                                p.values(p.symbol("A1", BIGINT)),
                                p.join(
                                        INNER,
                                        p.values(p.symbol("B1", BIGINT), p.symbol("B2", BIGINT)),
                                        p.values(p.symbol("C1", BIGINT), p.symbol("C2", BIGINT)),
                                        ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("B1", BIGINT), p.symbol("C1", BIGINT))),
                                        ImmutableList.of(
                                                p.symbol("B1", BIGINT),
                                                p.symbol("B2", BIGINT),
                                                p.symbol("C1", BIGINT),
                                                p.symbol("C2", BIGINT)),
                                        Optional.of(and(
                                                new ComparisonExpression(GREATER_THAN, p.symbol("C2", BIGINT).toSymbolReference(), new LongLiteral("0")),
                                                new ComparisonExpression(NOT_EQUAL, p.symbol("C2", BIGINT).toSymbolReference(), new LongLiteral("7")),
                                                new ComparisonExpression(GREATER_THAN, p.symbol("B2", BIGINT).toSymbolReference(), p.symbol("C2", BIGINT).toSymbolReference())))),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT))),
                                ImmutableList.of(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT), p.symbol("B2", BIGINT), p.symbol("C1", BIGINT), p.symbol("C2", BIGINT)),
                                Optional.of(new ComparisonExpression(
                                        LESS_THAN,
                                        new ArithmeticBinaryExpression(ADD, p.symbol("A1", BIGINT).toSymbolReference(), p.symbol("C1", BIGINT).toSymbolReference()),
                                        p.symbol("B1", BIGINT).toSymbolReference()))))
                .matches(joinGraph(
                        "B1=C1 AND A1=B1 AND C2 > 0 AND C2 <>7 AND B2 > C2 AND A1+ C1 < B1",
                        values(ImmutableMap.of("A1", 0)),
                        values(ImmutableMap.of("B1", 0, "B2", 1)),
                        values(ImmutableMap.of("C1", 0, "C2", 1))));
    }

    @Test
    public void testConvertsBushyTrees()
    {
        LocalQueryRunner queryRunner = new LocalQueryRunner(testSessionBuilder().setSystemProperty("reorder_joins", "true").build());
        new RuleTester(queryRunner).assertThat(new ConvertJoinTreeToJoinGraph())
                .on(p -> p.join(
                        INNER,
                        p.join(
                                INNER,
                                p.join(
                                        INNER,
                                        p.values(p.symbol("A1", BIGINT)),
                                        p.values(p.symbol("B1", BIGINT)),
                                        ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT))),
                                        ImmutableList.of(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT)),
                                        Optional.empty()),
                                p.values(p.symbol("C1", BIGINT)),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("A1", BIGINT), p.symbol("C1", BIGINT))),
                                ImmutableList.of(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT), p.symbol("C1", BIGINT)),
                                Optional.empty()),
                        p.join(
                                INNER,
                                p.values(p.symbol("D1", BIGINT), p.symbol("D2", BIGINT)),
                                p.values(p.symbol("E1", BIGINT), p.symbol("E2", BIGINT)),
                                ImmutableList.of(
                                        new JoinNode.EquiJoinClause(p.symbol("D1", BIGINT), p.symbol("E1", BIGINT)),
                                        new JoinNode.EquiJoinClause(p.symbol("D2", BIGINT), p.symbol("E2", BIGINT))),
                                ImmutableList.of(
                                        p.symbol("D1", BIGINT),
                                        p.symbol("D2", BIGINT),
                                        p.symbol("E1", BIGINT),
                                        p.symbol("E2", BIGINT)),
                                Optional.empty()),
                        ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("B1", BIGINT), p.symbol("E1", BIGINT))),
                        ImmutableList.of(
                                p.symbol("A1", BIGINT),
                                p.symbol("B1", BIGINT),
                                p.symbol("C1", BIGINT),
                                p.symbol("D1", BIGINT),
                                p.symbol("D2", BIGINT),
                                p.symbol("E1", BIGINT),
                                p.symbol("E2", BIGINT)),
                        Optional.empty()))
                .matches(joinGraph(
                        "A1=B1 AND A1=C1 AND D1=E1 AND D2=E2 AND B1=E1",
                        values(ImmutableMap.of("A1", 0)),
                        values(ImmutableMap.of("B1", 0)),
                        values(ImmutableMap.of("C1", 0)),
                        values(ImmutableMap.of("D1", 0, "D2", 1)),
                        values(ImmutableMap.of("E1", 0, "E2", 1))));
    }
}
