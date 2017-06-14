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
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.planner.DependencyExtractor;
import com.facebook.presto.sql.planner.EqualityInference;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import io.airlift.log.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.facebook.presto.SystemSessionProperties.getJoinDistributionType;
import static com.facebook.presto.SystemSessionProperties.getJoinReorderingStrategy;
import static com.facebook.presto.sql.ExpressionUtils.and;
import static com.facebook.presto.sql.ExpressionUtils.combineConjuncts;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinReorderingStrategy.COST_BASED;
import static com.facebook.presto.sql.planner.EqualityInference.createEqualityInference;
import static com.facebook.presto.sql.planner.iterative.rule.MultiJoinNode.toMultiJoinNode;
import static com.facebook.presto.sql.planner.plan.Assignments.identity;
import static com.facebook.presto.sql.planner.plan.JoinNode.DistributionType.PARTITIONED;
import static com.facebook.presto.sql.planner.plan.JoinNode.DistributionType.REPLICATED;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static com.facebook.presto.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static com.facebook.presto.sql.tree.ComparisonExpressionType.EQUAL;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.in;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

public class ReorderJoins
        implements Rule
{
    private static final Logger log = Logger.get(ReorderJoins.class);

    private final CostComparator costComparator;

    public ReorderJoins(CostComparator costComparator)
    {
        this.costComparator = requireNonNull(costComparator, "costComparator is null");
    }

    @Override
    public Optional<PlanNode> apply(PlanNode node, Lookup lookup, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, Session session)
    {
        if (!(node instanceof JoinNode) || getJoinReorderingStrategy(session) != COST_BASED) {
            return Optional.empty();
        }

        JoinNode joinNode = (JoinNode) node;
        // We check that join distribution type is absent because we only want to do this transformation once (reordered joins will have distribution type already set).
        if (!(joinNode.getType() == INNER) || joinNode.getDistributionType().isPresent()) {
            return Optional.empty();
        }

        MultiJoinNode multiJoinNode = toMultiJoinNode(joinNode, lookup);
        return Optional.of(new JoinEnumerator(idAllocator, symbolAllocator, session, lookup, multiJoinNode.getFilter(), costComparator).chooseJoinOrder(multiJoinNode.getSources(), multiJoinNode.getOutputSymbols()));
    }

    @VisibleForTesting
    public static class JoinEnumerator
    {
        private final Map<Set<PlanNode>, PlanNode> memo = new HashMap<>();
        private final PlanNodeIdAllocator idAllocator;
        private final Session session;
        private final Ordering<PlanNode> planNodeOrdering;
        private final EqualityInference allInference;
        private final Expression allFilter;

        public JoinEnumerator(PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, Session session, Lookup lookup, Expression filter, CostComparator costComparator)
        {
            requireNonNull(idAllocator, "idAllocator is null");
            requireNonNull(symbolAllocator, "symbolAllocator is null");
            requireNonNull(session, "session is null");
            requireNonNull(lookup, "lookup is null");
            requireNonNull(filter, "filter is null");
            requireNonNull(costComparator, "costComparator is null");
            this.idAllocator = idAllocator;
            this.session = session;
            this.planNodeOrdering = getPlanNodeOrdering(costComparator, lookup, session, symbolAllocator);
            this.allInference = createEqualityInference(filter);
            this.allFilter = filter;
        }

        private static Ordering<PlanNode> getPlanNodeOrdering(CostComparator costComparator, Lookup lookup, Session session, SymbolAllocator symbolAllocator)
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

        private PlanNode chooseJoinOrder(List<PlanNode> sources, List<Symbol> outputSymbols)
        {
            Set<PlanNode> multiJoinKey = ImmutableSet.copyOf(sources);
            PlanNode node = memo.get(multiJoinKey);
            if (node == null) {
                checkState(sources.size() > 1);
                node = generatePartitions(sources.size())
                        .map(partitioning -> createJoinAccordingToPartitioning(sources, outputSymbols, partitioning))
                        .filter(Optional::isPresent)
                        .map(Optional::get)
                        .min(planNodeOrdering)
                        .orElseGet(() -> createCrossJoins(sources, outputSymbols));
                log.debug("Least cost join was: " + node.toString());
                memo.put(multiJoinKey, node);
            }
            return node;
        }

        /**
         * This method produces a cross join of all the sources in multiJoinNode.
         * This gets called once there are no longer edges between any of the join sources.
         */
        private PlanNode createCrossJoins(List<PlanNode> sources, List<Symbol> outputSymbols)
        {
            // cross joins cannot filter output symbols
            PlanNode result = createJoin(
                    ImmutableSet.of(sources.get(0)),
                    ImmutableSet.copyOf(sources.subList(1, sources.size())),
                    outputSymbols,
                    true,
                    this::createCrossJoins).orElseThrow(() -> new IllegalStateException("cross join not present"));
            return result;
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
        @VisibleForTesting
        static Stream<Set<Integer>> generatePartitions(int totalNodes)
        {
            checkArgument(totalNodes >= 2, "totalNodes must be greater than or equal to 2");
            Set<Integer> numbers = IntStream.range(0, totalNodes)
                    .boxed()
                    .collect(toImmutableSet());
            return Sets.powerSet(numbers).stream()
                    .filter(subSet -> subSet.contains(0))
                    .filter(subSet -> subSet.size() < numbers.size());
        }

        @VisibleForTesting
        Optional<PlanNode> createJoinAccordingToPartitioning(List<PlanNode> sources, List<Symbol> outputSymbols, Set<Integer> partitioning)
        {
            Set<PlanNode> leftSources = partitioning.stream()
                    .map(sources::get)
                    .collect(toImmutableSet());
            Set<PlanNode> rightSources = Sets.difference(ImmutableSet.copyOf(sources), ImmutableSet.copyOf(leftSources));
            return createJoin(leftSources, rightSources, outputSymbols, false, this::chooseJoinOrder);
        }

        private Optional<PlanNode> createJoin(
                Set<PlanNode> leftSources,
                Set<PlanNode> rightSources,
                List<Symbol> outputSymbols,
                boolean allowCrossJoins,
                BiFunction<List<PlanNode>, List<Symbol>, PlanNode> joinCreationFunction)
        {
            Set<Symbol> leftSymbols = leftSources.stream()
                    .flatMap(node -> node.getOutputSymbols().stream())
                    .collect(toImmutableSet());
            Set<Symbol> rightSymbols = rightSources.stream()
                    .flatMap(node -> node.getOutputSymbols().stream())
                    .collect(toImmutableSet());
            ImmutableList.Builder<Expression> joinPredicatesBuilder = ImmutableList.builder();

            // add join conjucts that were not used for inference
            StreamSupport.stream(EqualityInference.nonInferrableConjuncts(allFilter).spliterator(), false)
                    .map(conjuct -> allInference.rewriteExpression(conjuct, symbol -> leftSymbols.contains(symbol) || rightSymbols.contains(symbol)))
                    .filter(Objects::nonNull)
                    // filter expressions that contain only left or right symbols
                    .filter(conjuct -> allInference.rewriteExpression(conjuct, leftSymbols::contains) == null)
                    .filter(conjuct -> allInference.rewriteExpression(conjuct, rightSymbols::contains) == null)
                    .forEach(joinPredicatesBuilder::add);

            // create equality inference on available symbols
            // TODO: make generateEqualitiesPartitionedBy take left and right scope
            List<Expression> joinEqualities = allInference.generateEqualitiesPartitionedBy(symbol -> leftSymbols.contains(symbol) || rightSymbols.contains(symbol)).getScopeEqualities();
            EqualityInference joinInference = createEqualityInference(joinEqualities.toArray(new Expression[joinEqualities.size()]));
            joinPredicatesBuilder.addAll(joinInference.generateEqualitiesPartitionedBy(in(leftSymbols)).getScopeStraddlingEqualities());

            List<Expression> joinPredicates = joinPredicatesBuilder.build();
            List<JoinNode.EquiJoinClause> joinConditions = joinPredicates.stream()
                    .filter(JoinEnumerator::isJoinEqualityCondition)
                    .map(predicate -> toEquiJoinClause((ComparisonExpression) predicate, leftSymbols))
                    .collect(toImmutableList());
            if (!allowCrossJoins && joinConditions.isEmpty()) {
                return Optional.empty();
            }
            List<Expression> joinFilters = joinPredicates.stream()
                    .filter(predicate -> !isJoinEqualityCondition(predicate))
                    .collect(toImmutableList());

            Set<Symbol> requiredJoinSymbols = ImmutableSet.<Symbol>builder()
                    .addAll(outputSymbols)
                    .addAll(DependencyExtractor.extractUnique(joinPredicates))
                    .build();
            PlanNode left = getJoinSource(
                    idAllocator,
                    ImmutableList.copyOf(leftSources),
                    requiredJoinSymbols.stream().filter(leftSymbols::contains).collect(toImmutableList()),
                    joinCreationFunction);
            PlanNode right = getJoinSource(
                    idAllocator,
                    ImmutableList.copyOf(rightSources),
                    requiredJoinSymbols.stream()
                            .filter(symbol -> !leftSymbols.contains(symbol))
                            .collect(toImmutableList()),
                    joinCreationFunction);

            // sort output symbols so that the left input symbols are first
            List<Symbol> sortedOutputSymbols = Stream.concat(left.getOutputSymbols().stream(), right.getOutputSymbols().stream())
                    .filter(outputSymbols::contains)
                    .collect(toImmutableList());

            // Cross joins can't filter symbols as part of the join
            // If we're doing a cross join, use all output symbols from the inputs and add a project node
            // on top
            List<Symbol> joinOutputSymbols = sortedOutputSymbols;
            if (joinConditions.isEmpty() && joinFilters.isEmpty()) {
                joinOutputSymbols = Stream.concat(left.getOutputSymbols().stream(), right.getOutputSymbols().stream())
                        .collect(toImmutableList());
            }

            PlanNode result =
                    setJoinNodeProperties(new JoinNode(
                            idAllocator.getNextId(),
                            INNER,
                            left,
                            right,
                            joinConditions,
                            joinOutputSymbols,
                            joinFilters.isEmpty() ? Optional.empty() : Optional.of(and(joinFilters)),
                            Optional.empty(),
                            Optional.empty(),
                            Optional.empty()));
            if (!joinOutputSymbols.equals(sortedOutputSymbols)) {
                result = new ProjectNode(idAllocator.getNextId(), result, identity(sortedOutputSymbols));
            }
            return Optional.of(result);
        }

        private PlanNode getJoinSource(PlanNodeIdAllocator idAllocator, List<PlanNode> nodes, List<Symbol> outputSymbols, BiFunction<List<PlanNode>, List<Symbol>, PlanNode> joinCreationFunction)
        {
            PlanNode planNode;
            if (nodes.size() == 1) {
                planNode = getOnlyElement(nodes);
                Expression filter = combineConjuncts(allInference.generateEqualitiesPartitionedBy(outputSymbols::contains).getScopeEqualities());
                if (!(TRUE_LITERAL).equals(filter)) {
                    return new FilterNode(idAllocator.getNextId(), planNode, filter);
                }
                return planNode;
            }
            return joinCreationFunction.apply(nodes, outputSymbols);
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
            Symbol leftSymbol = Symbol.from(equality.getLeft());
            Symbol rightSymbol = Symbol.from(equality.getRight());
            JoinNode.EquiJoinClause equiJoinClause = new JoinNode.EquiJoinClause(leftSymbol, rightSymbol);
            return leftSymbols.contains(leftSymbol) ? equiJoinClause : equiJoinClause.flip();
        }

        private JoinNode setJoinNodeProperties(JoinNode joinNode)
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
