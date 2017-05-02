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
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.getJoinDistributionType;
import static com.facebook.presto.sql.planner.iterative.rule.ReorderJoinsUtils.createJoinAccordingToPartitioning;
import static com.facebook.presto.sql.planner.iterative.rule.ReorderJoinsUtils.generatePartitions;
import static com.facebook.presto.sql.planner.iterative.rule.ReorderJoinsUtils.planPriorityQueue;
import static com.facebook.presto.sql.planner.plan.JoinNode.DistributionType.PARTITIONED;
import static com.facebook.presto.sql.planner.plan.JoinNode.DistributionType.REPLICATED;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static com.google.common.base.Preconditions.checkState;

public class ReorderJoins
        implements Rule
{
    private final CostComparator costComparator;

    public ReorderJoins(CostComparator costComparator)
    {
        this.costComparator = costComparator;
    }

    @Override
    public Optional<PlanNode> apply(PlanNode node, Lookup lookup, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, Session session)
    {
        if (!(node instanceof JoinGraphNode)) {
            return Optional.empty();
        }

        JoinGraphNode joinGraph = (JoinGraphNode) node;
        JoinNode joinNode = new JoinEnumerator().chooseJoinOrder(joinGraph, idAllocator, symbolAllocator, lookup, session);
        return Optional.of(joinNode);
    }

    private class JoinEnumerator
    {
        private final Map<JoinGraphNode, JoinNode> memo = new HashMap<>();

        private JoinNode chooseJoinOrder(JoinGraphNode joinGraph, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, Lookup lookup, Session session)
        {
            if (memo.containsKey(joinGraph)) {
                return memo.get(joinGraph);
            }
            else {
                PriorityQueue<JoinNode> joinOrders = planPriorityQueue(symbolAllocator, lookup, session, costComparator);
                // TODO: eliminate cross joins
                for (Set<Integer> partitioning : generatePartitions(joinGraph.getSources().size())) {
                    joinOrders.add(createJoinNodeTree(joinGraph, partitioning, idAllocator, symbolAllocator, lookup, session));
                }
                checkState(joinOrders.size() >= 1, "joinOrders cannot be empty");
                JoinNode node = joinOrders.poll();
                memo.put(joinGraph, node);
                return node;
            }
        }

        private JoinNode createJoinNodeTree(JoinGraphNode joinGraph, Set<Integer> partitioning, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, Lookup lookup, Session session)
        {
            JoinNode placeholderJoin = createJoinAccordingToPartitioning(joinGraph, partitioning, idAllocator);
            PlanNode left = lookup.resolve(placeholderJoin.getLeft());
            PlanNode right = lookup.resolve(placeholderJoin.getRight());
            if (left instanceof JoinGraphNode) {
                left = chooseJoinOrder((JoinGraphNode) left, idAllocator, symbolAllocator, lookup, session);
            }
            if (right instanceof JoinGraphNode) {
                right = chooseJoinOrder((JoinGraphNode) right, idAllocator, symbolAllocator, lookup, session);
            }

            JoinNode joinNode = new JoinNode(
                    idAllocator.getNextId(),
                    INNER,
                    left,
                    right,
                    placeholderJoin.getCriteria(),
                    placeholderJoin.getOutputSymbols(),
                    placeholderJoin.getFilter(),
                    placeholderJoin.getLeftHashSymbol(),
                    placeholderJoin.getRightHashSymbol(),
                    Optional.empty());
            return chooseJoinNodeProperties(joinNode, symbolAllocator, lookup, session);
        }

        private JoinNode chooseJoinNodeProperties(JoinNode joinNode, SymbolAllocator symbolAllocator, Lookup lookup, Session session)
        {
            PriorityQueue<JoinNode> possibleJoinNodes = planPriorityQueue(symbolAllocator, lookup, session, costComparator);
            String joinDistributionType = getJoinDistributionType(session);
            if (joinDistributionType.equals(FeaturesConfig.JoinDistributionType.REPARTITIONED) || joinDistributionType.equals(FeaturesConfig.JoinDistributionType.AUTOMATIC)) {
                JoinNode repartitionedJoin = joinNode.withDistributionType(PARTITIONED);
                possibleJoinNodes.add(repartitionedJoin);
                possibleJoinNodes.add(repartitionedJoin.flip());
            }
            if (joinDistributionType.equals(FeaturesConfig.JoinDistributionType.REPLICATED) || joinDistributionType.equals(FeaturesConfig.JoinDistributionType.AUTOMATIC)) {
                JoinNode replicatedJoin = joinNode.withDistributionType(REPLICATED);
                possibleJoinNodes.add(replicatedJoin);
                possibleJoinNodes.add(replicatedJoin.flip());
            }
            return possibleJoinNodes.poll();
        }
    }
}
