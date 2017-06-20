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
package com.facebook.presto.cost;

import com.facebook.presto.Session;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.sql.ExpressionUtils.combineConjuncts;
import static com.facebook.presto.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static com.facebook.presto.sql.tree.ComparisonExpressionType.EQUAL;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class JoinStatsRule
        implements ComposableStatsCalculator.Rule
{
    private final FilterStatsCalculator filterStatsCalculator;

    public JoinStatsRule(FilterStatsCalculator filterStatsCalculator)
    {
        this.filterStatsCalculator = filterStatsCalculator;
    }

    @Override
    public Optional<PlanNodeStatsEstimate> calculate(PlanNode node, Lookup lookup, Session session, Map<Symbol, Type> types)
    {
        if (!(node instanceof JoinNode)) {
            return Optional.empty();
        }
        JoinNode joinNode = (JoinNode) node;

        PlanNodeStatsEstimate leftStats = lookup.getStats(joinNode.getLeft(), session, types);
        PlanNodeStatsEstimate rightStats = lookup.getStats(joinNode.getRight(), session, types);

        List<Expression> comparisons = joinNode.getCriteria().stream()
                .map(criteria -> new ComparisonExpression(EQUAL, criteria.getLeft().toSymbolReference(), criteria.getRight().toSymbolReference()))
                .collect(toImmutableList());

        PlanNodeStatsEstimate mergedInputCosts = crossJoinStats(leftStats, rightStats);
        Expression predicate = combineConjuncts(combineConjuncts(comparisons), joinNode.getFilter().orElse(TRUE_LITERAL));
        return Optional.of(filterStatsCalculator.filterStats(mergedInputCosts, predicate, session, types));
    }

    private PlanNodeStatsEstimate crossJoinStats(PlanNodeStatsEstimate left, PlanNodeStatsEstimate right)
    {
        ImmutableMap.Builder<Symbol, SymbolStatsEstimate> symbolsStatsBuilder = ImmutableMap.builder();
        symbolsStatsBuilder.putAll(left.getSymbolStatistics()).putAll(right.getSymbolStatistics());

        PlanNodeStatsEstimate.Builder statsBuilder = PlanNodeStatsEstimate.builder();
        return statsBuilder.setSymbolStatistics(symbolsStatsBuilder.build())
                .setOutputRowCount(left.getOutputRowCount() * right.getOutputRowCount())
                .build();
    }
}
