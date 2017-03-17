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
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.tree.Expression;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.sql.ExpressionUtils.extractConjuncts;

public class ConvertJoinTreeToJoinGraph
        implements Rule
{
    @Override
    public Optional<PlanNode> apply(PlanNode node, Lookup lookup, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, Session session)
    {
        // We check that join distribution type is absent because we only want to do this transformation once.
        if (node instanceof JoinNode && ((JoinNode) node).getType().equals(JoinNode.Type.INNER) && !((JoinNode) node).getDistributionType().isPresent()) {
            return Optional.of(new FlattenedJoinTree((JoinNode) node).toJoinGraphNode(idAllocator));
        }
        else {
            return Optional.empty();
        }
    }

    private static class FlattenedJoinTree
    {
        private final List<PlanNode> nodes = new ArrayList<>();
        private final List<JoinNode.EquiJoinClause> criteria = new ArrayList<>();
        private final List<Expression> filters = new ArrayList<>();

        private FlattenedJoinTree(JoinNode node)
        {
            flattenNode(node);
        }

        private void flattenNode(PlanNode node)
        {
            if (node instanceof JoinNode && ((JoinNode) node).getType() == JoinNode.Type.INNER) {
                flattenNode(((JoinNode) node).getLeft());
                flattenNode(((JoinNode) node).getRight());
                criteria.addAll(((JoinNode) node).getCriteria());
                ((JoinNode) node).getFilter().ifPresent(f -> filters.addAll(extractConjuncts(f)));
            }
            else {
                nodes.add(node);
            }
        }

        private JoinGraphNode toJoinGraphNode(PlanNodeIdAllocator idAllocator)
        {
            return new JoinGraphNode(idAllocator.getNextId(), nodes, criteria, filters);
        }
    }
}
