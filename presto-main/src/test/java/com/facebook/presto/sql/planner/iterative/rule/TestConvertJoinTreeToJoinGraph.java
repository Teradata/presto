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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.sql.ExpressionUtils.and;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.joinGraph;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.tree.ArithmeticBinaryExpression.Type.ADD;
import static com.facebook.presto.sql.tree.ComparisonExpressionType.GREATER_THAN;
import static com.facebook.presto.sql.tree.ComparisonExpressionType.LESS_THAN;
import static com.facebook.presto.sql.tree.ComparisonExpressionType.NOT_EQUAL;

public class TestConvertJoinTreeToJoinGraph
{
    @Test
    public void test()
    {
        new RuleTester().assertThat(new ConvertJoinTreeToJoinGraph())
                .on(p -> p.join(
                        JoinNode.Type.INNER,
                        p.join(
                                JoinNode.Type.INNER,
                                p.join(
                                        JoinNode.Type.LEFT,
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
                                JoinNode.Type.INNER,
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
                                Optional.of(and(
                                        new ComparisonExpression(GREATER_THAN, p.symbol("E2", BIGINT).toSymbolReference(), new LongLiteral("0")),
                                        new ComparisonExpression(NOT_EQUAL, p.symbol("E2", BIGINT).toSymbolReference(), new LongLiteral("7")),
                                        new ComparisonExpression(GREATER_THAN, p.symbol("D2", BIGINT).toSymbolReference(), p.symbol("E2", BIGINT).toSymbolReference())))),
                        ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("B1", BIGINT), p.symbol("E1", BIGINT))),
                        ImmutableList.of(
                                p.symbol("A1", BIGINT),
                                p.symbol("B1", BIGINT),
                                p.symbol("C1", BIGINT),
                                p.symbol("D1", BIGINT),
                                p.symbol("D2", BIGINT),
                                p.symbol("E1", BIGINT),
                                p.symbol("E2", BIGINT)),
                        Optional.of(new ComparisonExpression(LESS_THAN,
                                new ArithmeticBinaryExpression(ADD, p.symbol("A1", BIGINT).toSymbolReference(), p.symbol("C1", BIGINT).toSymbolReference()),
                                p.symbol("D1", BIGINT).toSymbolReference()))))
                .matches(joinGraph(
                        ImmutableList.of(
                                equiJoinClause("A1", "C1"),
                                equiJoinClause("D1", "E1"),
                                equiJoinClause("D2", "E2"),
                                equiJoinClause("B1", "E1")),
                        ImmutableList.of(
                                "E2 > 0",
                                "E2 <> 7",
                                "D2 > E2",
                                "A1 + C1 < D1"),
                        join(JoinNode.Type.LEFT, ImmutableList.of(equiJoinClause("A1", "B1")),
                                values(ImmutableMap.of("A1", 0)),
                                values(ImmutableMap.of("B1", 0))),
                        values(ImmutableMap.of("C1", 0)),
                        values(ImmutableMap.of("D1", 0, "D2", 1)),
                        values(ImmutableMap.of("E1", 0, "E2", 1))));
    }
}
