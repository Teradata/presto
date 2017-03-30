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
import com.facebook.presto.cost.PlanNodeCostEstimate;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;

import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;

import static com.facebook.presto.sql.planner.iterative.rule.ReorderJoinsUtils.createBinaryJoin;
import static com.facebook.presto.sql.planner.iterative.rule.ReorderJoinsUtils.generatePartitions;
import static com.facebook.presto.sql.planner.iterative.rule.ReorderJoinsUtils.getCostSum;
import static com.google.common.base.Preconditions.checkState;

public class ReorderJoins
        implements Rule
{
    private final ReorderJoinsMemo memo = new ReorderJoinsMemo();

    @Override
    public Optional<PlanNode> apply(PlanNode node, Lookup lookup, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, Session session)
    {
        if (!(node instanceof JoinGraphNode)) {
            return Optional.empty();
        }

        JoinGraphNode joinGraph = (JoinGraphNode) node;
        JoinNode joinNode = chooseJoinOrder(joinGraph, idAllocator, symbolAllocator, lookup, session);
        return Optional.of(joinNode);
    }

    private JoinNode chooseJoinOrder(JoinGraphNode joinGraph, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, Lookup lookup, Session session)
    {
        Optional<JoinNode> plan = memo.getPlan(joinGraph);
        if (plan.isPresent()) {
            return plan.get();
        }
        else {
            PriorityQueue<JoinNode> joinOrders = new PriorityQueue<>((node1, node2) -> {
                PlanNodeCostEstimate node1Cost = lookup.getCumulativeCost(session, symbolAllocator.getTypes(), node1);
                PlanNodeCostEstimate node2Cost = lookup.getCumulativeCost(session, symbolAllocator.getTypes(), node2);
                return Double.compare(getCostSum(node1Cost), getCostSum(node2Cost));
            });
            // TODO: eliminate cross joins
            for (Set<Integer> partitioning : generatePartitions(joinGraph.getSources().size())) {
                joinOrders.add(createJoinNodeTree(joinGraph, partitioning, idAllocator, symbolAllocator, lookup, session));
            }
            checkState(joinOrders.size() >= 1, "joinOrders cannot be empty");
            JoinNode node = joinOrders.poll();
            memo.add(joinGraph, node);
            return node;
        }
    }

    private JoinNode createJoinNodeTree(JoinGraphNode joinGraph, Set<Integer> partitioning, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, Lookup lookup, Session session)
    {
        JoinNode joinNode = createBinaryJoin(joinGraph, partitioning, idAllocator);
        PlanNode left = lookup.resolve(joinNode.getLeft());
        PlanNode right = lookup.resolve(joinNode.getRight());
        if (left instanceof JoinGraphNode) {
            left = chooseJoinOrder((JoinGraphNode) left, idAllocator, symbolAllocator, lookup, session);
        }
        if (right instanceof JoinGraphNode) {
            right = chooseJoinOrder((JoinGraphNode) right, idAllocator, symbolAllocator, lookup, session);
        }

        // TODO: choose distribution type and right vs left.
        return new JoinNode(
                idAllocator.getNextId(),
                JoinNode.Type.INNER,
                left,
                right,
                joinNode.getCriteria(),
                joinNode.getOutputSymbols(),
                joinNode.getFilter(),
                joinNode.getLeftHashSymbol(),
                joinNode.getRightHashSymbol(),
                joinNode.getDistributionType());
    }
}
