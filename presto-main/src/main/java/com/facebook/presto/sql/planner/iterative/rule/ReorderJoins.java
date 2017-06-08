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
import com.facebook.presto.sql.planner.DependencyExtractor;
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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.facebook.presto.SystemSessionProperties.getJoinDistributionType;
import static com.facebook.presto.SystemSessionProperties.isJoinReorderingEnabled;
import static com.facebook.presto.sql.ExpressionUtils.extractConjuncts;
import static com.facebook.presto.sql.planner.optimizations.InnerJoinPredicateUtils.sortPredicatesForInnerJoin;
import static com.facebook.presto.sql.planner.plan.JoinNode.DistributionType.PARTITIONED;
import static com.facebook.presto.sql.planner.plan.JoinNode.DistributionType.REPLICATED;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static com.facebook.presto.sql.tree.ComparisonExpressionType.EQUAL;
import static com.google.common.base.Preconditions.checkArgument;
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
        // We check that join distribution type is absent because we only want to do this transformation once (reordered joins will have distribution type already set).
        if (!(node instanceof JoinNode) || !isJoinReorderingEnabled(session)) {
            return Optional.empty();
        }

        JoinNode joinNode = (JoinNode) node;
        if (!(joinNode.getType() == INNER) || joinNode.getDistributionType().isPresent()) {
            return Optional.empty();
        }

        JoinGraphNode joinGraph = new JoinGraphNode.JoinGraphNodeBuilder(joinNode, lookup).toJoinGraphNode(idAllocator);
        return Optional.of(new JoinEnumerator(idAllocator, symbolAllocator, session, lookup, joinGraph, costComparator).chooseJoinOrder(joinGraph));
    }

    @VisibleForTesting
    public static class JoinEnumerator
    {
        private final Map<JoinGraphNode, JoinNode> memo = new HashMap<>();
        private final PlanNodeIdAllocator idAllocator;
        private final Session session;
        private final Ordering<PlanNode> planNodeOrdering;
        private final CostComparator costComparator;

        public JoinEnumerator(PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, Session session, Lookup lookup, JoinGraphNode joinGraphNode, CostComparator costComparator)
        {
            this.idAllocator = idAllocator;
            this.session = session;
            this.planNodeOrdering = new Ordering<PlanNode>()
            {
                @Override
                public int compare(PlanNode node1, PlanNode node2)
                {
                    PlanNodeCostEstimate node1Cost = lookup.getCumulativeCost(node1, session, symbolAllocator.getTypes());
                    PlanNodeCostEstimate node2Cost = lookup.getCumulativeCost(node2, session, symbolAllocator.getTypes());
                    return JoinEnumerator.this.costComparator.compare(session, node1Cost, node2Cost);
                }
            };
            this.costComparator = costComparator;
        }

        private static boolean isJoinEqualityCondition(Expression expression)
        {
            return expression instanceof ComparisonExpression
                    && ((ComparisonExpression) expression).getType() == EQUAL
                    && ((ComparisonExpression) expression).getLeft() instanceof SymbolReference
                    && ((ComparisonExpression) expression).getRight() instanceof SymbolReference;
        }

        private static JoinNode.EquiJoinClause toEquiJoinClause(ComparisonExpression equality, Set<Symbol> leftSymbols)
        {
            boolean alignedComparison = leftSymbols.contains(Symbol.from(equality.getLeft()));
            Expression leftExpression = alignedComparison ? equality.getLeft() : equality.getRight();
            Expression rightExpression = alignedComparison ? equality.getRight() : equality.getLeft();

            Symbol leftSymbol = Symbol.from(leftExpression);
            Symbol rightSymbol = Symbol.from(rightExpression);
            return new JoinNode.EquiJoinClause(leftSymbol, rightSymbol);
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
        public static Stream<Set<Integer>> generatePartitions(int totalNodes)
        {
            checkArgument(totalNodes > 0, "totalNodes must be greater than 0");
            Set<Integer> numbers = IntStream.range(0, totalNodes)
                    .boxed()
                    .collect(toImmutableSet());
            return Sets.powerSet(numbers).stream()
                    .filter(subSet -> subSet.contains(0))
                    .filter(subSet -> subSet.size() < numbers.size());
        }

        public static JoinNode createJoinAccordingToPartitioning(JoinGraphNode joinGraph, Set<Integer> partitioning, PlanNodeIdAllocator idAllocator)
        {
            List<PlanNode> sources = joinGraph.getSources();
            List<PlanNode> leftSources = partitioning.stream()
                    .map(sources::get)
                    .collect(toImmutableList());
            List<PlanNode> rightSources = ImmutableList.copyOf(Sets.difference(ImmutableSet.copyOf(sources), ImmutableSet.copyOf(leftSources)));
            Set<Symbol> leftSymbols = leftSources.stream()
                    .flatMap(node -> node.getOutputSymbols().stream())
                    .collect(toImmutableSet());

            InnerJoinPredicateUtils.InnerJoinPushDownResult pushDownResult = sortPredicatesForInnerJoin(leftSymbols, ImmutableList.of(joinGraph.getFilter()), ImmutableList.of());

            Set<Symbol> joinSymbols = ImmutableSet.<Symbol>builder()
                    .addAll(joinGraph.getOutputSymbols())
                    .addAll(DependencyExtractor.extractUnique(pushDownResult.getJoinPredicate()))
                    .build();
            PlanNode left = getJoinSource(
                    idAllocator,
                    leftSources,
                    pushDownResult.getLeftPredicate(),
                    joinSymbols.stream().filter(leftSymbols::contains).collect(toImmutableList()));
            PlanNode right = getJoinSource(
                    idAllocator,
                    rightSources,
                    pushDownResult.getRightPredicate(),
                    joinSymbols.stream()
                            .filter(symbol -> !leftSymbols.contains(symbol))
                            .collect(toImmutableList()));

            List<Expression> joinPredicates = extractConjuncts(pushDownResult.getJoinPredicate());
            List<JoinNode.EquiJoinClause> joinConditions = joinPredicates.stream()
                    .filter(JoinEnumerator::isJoinEqualityCondition)
                    .map(predicate -> toEquiJoinClause((ComparisonExpression) predicate, leftSymbols))
                    .collect(toImmutableList());
            List<Expression> joinFilters = joinPredicates.stream()
                    .filter(predicate -> !isJoinEqualityCondition(predicate))
                    .collect(toImmutableList());

            // sort output symbols so that the left input symbols are first
            List<Symbol> inputSymbols = ImmutableList.<Symbol>builder()
                    .addAll(left.getOutputSymbols())
                    .addAll(right.getOutputSymbols())
                    .build();
            List<Symbol> outputSymbols = inputSymbols.stream()
                    .filter(joinGraph.getOutputSymbols()::contains)
                    .collect(toImmutableList());
            return new JoinNode(
                    idAllocator.getNextId(),
                    INNER,
                    left,
                    right,
                    flipJoinCriteriaIfNecessary(joinConditions, left),
                    outputSymbols,
                    joinFilters.isEmpty() ? Optional.empty() : Optional.of(ExpressionUtils.and(joinFilters)),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty());
        }

        private static List<JoinNode.EquiJoinClause> flipJoinCriteriaIfNecessary(List<JoinNode.EquiJoinClause> joinCriteria, PlanNode left)
        {
            return joinCriteria
                    .stream()
                    .map(criterion -> left.getOutputSymbols().contains(criterion.getLeft()) ? criterion : criterion.flip())
                    .collect(toImmutableList());
        }

        private static PlanNode getJoinSource(PlanNodeIdAllocator idAllocator, List<PlanNode> nodes, Expression filter, List<Symbol> outputSymbols)
        {
            PlanNode planNode;
            if (nodes.size() == 1) {
                planNode = getOnlyElement(nodes);
                if (!(BooleanLiteral.TRUE_LITERAL).equals(filter)) {
                    return new FilterNode(idAllocator.getNextId(), planNode, filter);
                }
                return planNode;
            }
            return new JoinGraphNode(idAllocator.getNextId(), nodes, filter, outputSymbols);
        }

        private JoinNode chooseJoinOrder(JoinGraphNode joinGraph)
        {
            JoinNode join = memo.get(joinGraph);
            if (join == null) {
                join = generatePartitions(joinGraph.getSources().size())
                        .map(partitioning -> createJoinNodeTree(joinGraph, partitioning, idAllocator))
                        .min(planNodeOrdering)
                        .orElseThrow(() -> new IllegalStateException("joinOrders cannot be empty"));
                memo.put(joinGraph, join);
            }
            return join;
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
            FeaturesConfig.JoinDistributionType joinDistributionType = getJoinDistributionType(session);
            if (joinDistributionType.canRepartition()) {
                possibleJoinNodes.add(joinNode.withDistributionType(PARTITIONED));
                possibleJoinNodes.add(joinNode.flipChildren().withDistributionType(PARTITIONED));
            }
            if (joinDistributionType.canReplicate()) {
                possibleJoinNodes.add(joinNode.withDistributionType(REPLICATED));
                possibleJoinNodes.add(joinNode.flipChildren().withDistributionType(REPLICATED));
            }

            return planNodeOrdering.min(possibleJoinNodes);
        }
    }
}
