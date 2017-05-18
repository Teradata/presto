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
import com.facebook.presto.cost.PlanNodeCostEstimate;
import com.facebook.presto.sql.ExpressionUtils;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.optimizations.InnerJoinPredicateUtils;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;

import static com.facebook.presto.SystemSessionProperties.getJoinDistributionType;
import static com.facebook.presto.SystemSessionProperties.isJoinReorderingEnabled;
import static com.facebook.presto.sql.ExpressionUtils.extractConjuncts;
import static com.facebook.presto.sql.planner.optimizations.InnerJoinPredicateUtils.sortPredicatesForInnerJoin;
import static com.facebook.presto.sql.planner.plan.JoinNode.DistributionType.PARTITIONED;
import static com.facebook.presto.sql.planner.plan.JoinNode.DistributionType.REPLICATED;
import static com.facebook.presto.sql.tree.ComparisonExpressionType.EQUAL;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;

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
        if (!(node instanceof JoinGraphNode) || !isJoinReorderingEnabled(session)) {
            return Optional.empty();
        }

        JoinGraphNode joinGraph = (JoinGraphNode) node;
        JoinNode joinNode = new JoinEnumerator(idAllocator, symbolAllocator, session, lookup).chooseJoinOrder(joinGraph);
        return Optional.of(joinNode);
    }

    @VisibleForTesting
    public class JoinEnumerator
    {
        private final Map<String, JoinNode> memo = new HashMap<>();
        private final PlanNodeIdAllocator idAllocator;
        private final SymbolAllocator symbolAllocator;
        private final Session session;
        private final Lookup lookup;

        public JoinEnumerator(PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, Session session, Lookup lookup)
        {
            this.idAllocator = idAllocator;
            this.symbolAllocator = symbolAllocator;
            this.session = session;
            this.lookup = lookup;
        }

        private JoinNode chooseJoinOrder(JoinGraphNode joinGraph)
        {
            if (memo.containsKey(joinGraph.getKey())) {
                return memo.get(joinGraph.getKey());
            }
            else {
                List<JoinNode> joinOrders = new ArrayList<>();
                // TODO: don't consider partitions with unnecessary cross joins
                for (Set<Integer> partitioning : generatePartitions(joinGraph.getSources().size())) {
                    joinOrders.add(createJoinNodeTree(joinGraph, partitioning, idAllocator));
                }
                checkState(joinOrders.size() >= 1, "joinOrders cannot be empty");
                JoinNode node = planNodeOrdering().min(joinOrders);
                memo.put(joinGraph.getKey(), node);
                return node;
            }
        }

        private JoinNode createJoinNodeTree(JoinGraphNode joinGraph, Set<Integer> partitioning, PlanNodeIdAllocator idAllocator)
        {
            JoinNode placeholderJoin = createJoinAccordingToPartitioning(joinGraph, partitioning, idAllocator);
            PlanNode left = placeholderJoin.getLeft();
            PlanNode right = placeholderJoin.getRight();
            if (left instanceof JoinGraphNode) {
                left = chooseJoinOrder((JoinGraphNode) left);
            }
            if (right instanceof JoinGraphNode) {
                right = chooseJoinOrder((JoinGraphNode) right);
            }

            JoinNode joinNode = (JoinNode) placeholderJoin.replaceChildren(ImmutableList.of(left, right));
            return chooseJoinNodeProperties(joinNode);
        }

        private JoinNode chooseJoinNodeProperties(JoinNode joinNode)
        {
            List<JoinNode> possibleJoinNodes = new ArrayList<>();
            String joinDistributionType = getJoinDistributionType(session);
            if (joinDistributionType.equals(FeaturesConfig.JoinDistributionType.REPARTITIONED) || joinDistributionType.equals(FeaturesConfig.JoinDistributionType.AUTOMATIC)) {
                possibleJoinNodes.add(joinNode.withDistributionType(PARTITIONED));
                possibleJoinNodes.add(joinNode.flip().withDistributionType(PARTITIONED));
            }
            if (joinDistributionType.equals(FeaturesConfig.JoinDistributionType.REPLICATED) || joinDistributionType.equals(FeaturesConfig.JoinDistributionType.AUTOMATIC)) {
                possibleJoinNodes.add(joinNode.withDistributionType(REPLICATED));
                possibleJoinNodes.add(joinNode.flip().withDistributionType(REPLICATED));
            }

            return planNodeOrdering().min(possibleJoinNodes);
        }

        private Ordering<PlanNode> planNodeOrdering()
        {
            return new Ordering<PlanNode>()
            {
                @Override
                public int compare(PlanNode node1, PlanNode node2)
                {
                    PlanNodeCostEstimate node1Cost = lookup.getCumulativeCost(node1, session, symbolAllocator.getTypes());
                    PlanNodeCostEstimate node2Cost = lookup.getCumulativeCost(node2, session, symbolAllocator.getTypes());
                    return costComparator.compare(session, node1Cost, node2Cost);
                }
            };
        }
    }

    /**
     * This method generates all the ways of dividing  totalNodes into two sets
     * each containing at least one node. It will generate one set for each
     * possible partitioning. The other partition is implied in the absent values.
     * In order not to generate the inverse of any set, we always include the 0th
     * node in our sets.
     *
     * @param totalNodes
     * @return A set of sets each of which defines a partitioning of totalNodes
     */
    public static Set<Set<Integer>> generatePartitions(int totalNodes)
    {
        Set<Integer> numbers = IntStream.range(0, totalNodes)
                .boxed()
                .collect(toImmutableSet());
        return Sets.powerSet(numbers).stream()
                .filter(subSet -> subSet.contains(0))
                .filter(subSet -> subSet.size() < numbers.size())
                .collect(toImmutableSet());
    }

    public static JoinNode createJoinAccordingToPartitioning(JoinGraphNode joinGraph, Set<Integer> leftSources, PlanNodeIdAllocator idAllocator)
    {
        List<PlanNode> sources = joinGraph.getSources();
        ImmutableList.Builder<PlanNode> leftNodes = ImmutableList.builder();
        Set<Symbol> leftSymbols = new HashSet<>();
        ImmutableList.Builder<PlanNode> rightNodes = ImmutableList.builder();
        for (int i = 0; i < sources.size(); i++) {
            PlanNode source = sources.get(i);
            if (leftSources.contains(i)) {
                leftNodes.add(source);
                leftSymbols.addAll(source.getOutputSymbols());
            }
            else {
                rightNodes.add(source);
            }
        }

        InnerJoinPredicateUtils.InnerJoinPushDownResult pushDownResult = sortPredicatesForInnerJoin(leftSymbols, ImmutableList.of(joinGraph.getFilter()), ImmutableList.of());

        PlanNode left = getJoinSource(idAllocator, leftNodes.build(), pushDownResult.getLeftPredicate());
        PlanNode right = getJoinSource(idAllocator, rightNodes.build(), pushDownResult.getRightPredicate());

        // Create new projections for the new join clauses
        ImmutableList.Builder<JoinNode.EquiJoinClause> joinConditionBuilder = ImmutableList.builder();
        ImmutableList.Builder<Expression> joinFilterBuilder = ImmutableList.builder();
        for (Expression conjunct : extractConjuncts(pushDownResult.getJoinPredicate())) {
            if (isJoinEqualityCondition(conjunct, leftSymbols)) {
                ComparisonExpression equality = (ComparisonExpression) conjunct;

                boolean alignedComparison = leftSymbols.contains(Symbol.from(equality.getLeft()));
                Expression leftExpression = alignedComparison ? equality.getLeft() : equality.getRight();
                Expression rightExpression = alignedComparison ? equality.getRight() : equality.getLeft();

                Symbol leftSymbol = Symbol.from(leftExpression);
                Symbol rightSymbol = Symbol.from(rightExpression);
                joinConditionBuilder.add(new JoinNode.EquiJoinClause(leftSymbol, rightSymbol));
            }
            else {
                joinFilterBuilder.add(conjunct);
            }
        }

        List<Expression> filters = joinFilterBuilder.build();

        return new JoinNode(
                idAllocator.getNextId(),
                JoinNode.Type.INNER,
                left,
                right,
                flipJoinCriteriaIfNecessary(joinConditionBuilder.build(), left),
                ImmutableList.<Symbol>builder().addAll(left.getOutputSymbols()).addAll(right.getOutputSymbols()).build(),
                filters.isEmpty() ? Optional.empty() : Optional.of(ExpressionUtils.and(filters)),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    private static boolean isJoinEqualityCondition(Expression expression, Collection<Symbol> leftSymbols)
    {
        return expression instanceof ComparisonExpression
                && ((ComparisonExpression) expression).getType() == EQUAL
                && ((ComparisonExpression) expression).getLeft() instanceof SymbolReference
                && ((ComparisonExpression) expression).getRight() instanceof SymbolReference;
    }

    private static List<JoinNode.EquiJoinClause> flipJoinCriteriaIfNecessary(List<JoinNode.EquiJoinClause> joinCriteria, PlanNode left)
    {
        return joinCriteria
                .stream()
                .map(criterion -> left.getOutputSymbols().contains(criterion.getLeft()) ? criterion : criterion.flip())
                .collect(toImmutableList());
    }

    private static PlanNode getJoinSource(PlanNodeIdAllocator idAllocator, List<PlanNode> nodes, Expression filter)
    {
        PlanNode planNode;
        if (nodes.size() == 1) {
            planNode = getOnlyElement(nodes);
            if (!(BooleanLiteral.TRUE_LITERAL).equals(filter)) {
                planNode = new FilterNode(idAllocator.getNextId(), planNode, filter);
            }
        }
        else {
            planNode = new JoinGraphNode(idAllocator.getNextId(), nodes, filter);
        }
        return planNode;
    }
}
