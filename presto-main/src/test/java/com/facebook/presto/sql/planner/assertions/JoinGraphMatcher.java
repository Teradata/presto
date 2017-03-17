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

package com.facebook.presto.sql.planner.assertions;

import com.facebook.presto.Session;
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.sql.planner.iterative.rule.JoinGraphNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.tree.Expression;

import java.util.List;

import static com.facebook.presto.sql.planner.assertions.MatchResult.NO_MATCH;
import static com.facebook.presto.sql.planner.assertions.MatchResult.match;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class JoinGraphMatcher
        implements Matcher
{
    private final List<ExpectedValueProvider<JoinNode.EquiJoinClause>> expectedEquiCriteria;
    private final List<Expression> expectedFilters;

    public JoinGraphMatcher(List<ExpectedValueProvider<JoinNode.EquiJoinClause>> expectedEquiCriteria, List<Expression> expectedFilters)
    {
        this.expectedEquiCriteria = expectedEquiCriteria;
        this.expectedFilters = expectedFilters;
    }

    @Override
    public boolean shapeMatches(PlanNode node)
    {
        return node instanceof JoinGraphNode;
    }

    @Override
    public MatchResult detailMatches(PlanNode node, PlanNodeStatsEstimate planNodeStatsEstimate, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        checkState(shapeMatches(node), "Plan testing framework error: shapeMatches returned false in detailMatches in %s", this.getClass().getName());
        JoinGraphNode joinGraph = (JoinGraphNode) node;
        if (joinGraph.getCriteria().size() != expectedEquiCriteria.size()) {
            return NO_MATCH;
        }
        List<JoinNode.EquiJoinClause> actualCriteria = joinGraph.getCriteria();

        List<JoinNode.EquiJoinClause> expected = expectedEquiCriteria.stream()
                .map(maker -> maker.getExpectedValue(symbolAliases))
                .collect(toImmutableList());
        if (!expected.equals(actualCriteria)) {
            return NO_MATCH;
        }

        if (joinGraph.getFilters().size() != expectedFilters.size()) {
            return NO_MATCH;
        }
        List<Expression> actualFilters = joinGraph.getFilters();
        for (int i = 0; i < actualFilters.size(); i++) {
            if (!new ExpressionVerifier(symbolAliases).process(actualFilters.get(i), expectedFilters.get(i))) {
                return NO_MATCH;
            }
        }

        return match();
    }
}
