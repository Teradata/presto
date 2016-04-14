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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.ExpressionUtils;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.IntersectNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.UnionNode;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.metadata.FunctionKind.AGGREGATE;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.sql.planner.plan.AggregationNode.Step;
import static com.facebook.presto.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static com.facebook.presto.sql.tree.ComparisonExpression.Type.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Converts INTERSECT query into UNION ALL..GROUP BY...WHERE
 * Eg:  SELECT a FROM foo INTERSECT SELECT x FROM bar
 * <p>
 * =>
 * <p>
 * SELECT a
 * FROM
 * (SELECT a,
 * COUNT(foo_marker) AS foo_cnt,
 * COUNT(bar_marker) AS bar_cnt
 * FROM
 * (
 * SELECT a, true as foo_marker, null as bar_marker FROM foo
 * UNION ALL
 * SELECT x, null as foo_marker, true as bar_marker FROM bar
 * ) T1
 * GROUP BY a) T2
 * WHERE foo_cnt >= 1 AND bar_cnt >= 1;
 */
public class ImplementIntersectAsUnion
        extends PlanOptimizer
{
    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");
        requireNonNull(types, "types is null");
        requireNonNull(symbolAllocator, "symbolAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");

        return SimplePlanRewriter.rewriteWith(new Rewriter(idAllocator, symbolAllocator), plan);
    }

    private static class Rewriter
            extends SimplePlanRewriter<Integer>
    {
        private static final String INTERSECT_MARKER = "intersect_marker";
        private final PlanNodeIdAllocator idAllocator;
        private final SymbolAllocator symbolAllocator;

        private Rewriter(PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator)
        {
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
        }

        @Override
        public PlanNode visitIntersect(IntersectNode node, RewriteContext<Integer> rewriteContext)
        {
            List<PlanNode> sources = node.getSources();
            int numberOfSources = sources.size();

            ImmutableList.Builder<PlanNode> newProjectNodes = ImmutableList.builder();
            ImmutableListMultimap.Builder<Symbol, Symbol> symbolMappings = ImmutableListMultimap.builder();
            List<Symbol> unionOutputSymbols = null;
            ImmutableMap.Builder<Symbol, FunctionCall> aggregationsBuilder = ImmutableMap.builder();
            ImmutableMap.Builder<Symbol, Expression> outputProjections = ImmutableMap.builder();

            for (int i = 0; i < numberOfSources; i++) {
                ProjectNode newProjectNode = createProjectNode(node, sources, i);
                newProjectNodes.add(newProjectNode);

                List<Symbol> projectOutputSymbols = newProjectNode.getOutputSymbols();
                if (unionOutputSymbols == null) {
                    // Use the first Relation to derive output symbol names
                    unionOutputSymbols = generateOutputSybmbolsFrom(projectOutputSymbols);
                }

                checkArgument(projectOutputSymbols.size() == unionOutputSymbols.size(),
                        "Intersect output should have %s symbols but has %s symbols",
                        unionOutputSymbols.size(),
                        projectOutputSymbols.size());

                int unionFieldId = 0;
                for (Symbol symbol : projectOutputSymbols) {
                    symbolMappings.put(unionOutputSymbols.get(unionFieldId), symbol);
                    unionFieldId++;
                }
            }

            UnionNode unionNode = new UnionNode(node.getId(), newProjectNodes.build(), symbolMappings.build(), ImmutableList.copyOf(symbolMappings.build().keySet()));
            AggregationNode aggregationNode = createAggregationNodeForIntersect(node, unionOutputSymbols, unionNode, aggregationsBuilder, outputProjections);
            FilterNode filterNode = createFilterNodeForIntersect(aggregationNode, aggregationsBuilder.build());

            return new ProjectNode(idAllocator.getNextId(), filterNode, outputProjections.build());
        }

        private List<Symbol> generateOutputSybmbolsFrom(List<Symbol> sourceSymbols)
        {
            ImmutableList.Builder<Symbol> outputSymbolBuilder = ImmutableList.builder();
            for (Symbol symbol : sourceSymbols) {
                outputSymbolBuilder.add(allocateNewSymbolFrom(symbol));
            }
            return outputSymbolBuilder.build();
        }

        private Symbol allocateNewSymbolFrom(Symbol symbol)
        {
            return allocateNewSymbolFrom(symbol.getName(), symbolAllocator.getTypes().get(symbol));
        }

        private AggregationNode createAggregationNodeForIntersect(IntersectNode node, List<Symbol> outputSymbols, UnionNode sourceNode, ImmutableMap.Builder<Symbol, FunctionCall> aggregationsBuilder, ImmutableMap.Builder<Symbol, Expression> outputProjections)
        {
            ImmutableMap.Builder<Symbol, Signature> functionsBuilder = ImmutableMap.builder();
            ImmutableList.Builder<Symbol> groupByKeys = ImmutableList.builder();

            List<Symbol> intersectOutputSymbols = node.getOutputSymbols();
            Signature countSignature = new Signature("count", AGGREGATE, "bigint", "boolean");

            for (int i = intersectOutputSymbols.size(); i < outputSymbols.size(); i++) {
                Symbol countSymbol = allocateNewSymbolFrom("count", BIGINT);
                aggregationsBuilder.put(countSymbol, new FunctionCall(QualifiedName.of("count"), ImmutableList.<Expression>of(new QualifiedNameReference(outputSymbols.get(i).toQualifiedName()))));
                functionsBuilder.put(countSymbol, countSignature);
            }

            for (Symbol nodeSymbol : intersectOutputSymbols) {
                int fieldIndex = intersectOutputSymbols.indexOf(nodeSymbol);
                Symbol outputSymbol = outputSymbols.get(fieldIndex);
                groupByKeys.add(outputSymbol);
                outputProjections.put(nodeSymbol, new QualifiedNameReference(outputSymbol.toQualifiedName()));
            }

            return new AggregationNode(idAllocator.getNextId(),
                    sourceNode,
                    groupByKeys.build(),
                    aggregationsBuilder.build(),
                    functionsBuilder.build(),
                    ImmutableMap.<Symbol, Symbol>of(),
                    Step.SINGLE,
                    Optional.empty(),
                    1.0,
                    Optional.empty());
        }

        private Symbol allocateNewSymbolFrom(String name, Type type)
        {
            return symbolAllocator.newSymbol(name, type);
        }

        private ProjectNode createProjectNode(IntersectNode intersectNode, List<PlanNode> sources, int sourceIndex)
        {
            ImmutableMap.Builder<Symbol, Expression> projectionAssignments = ImmutableMap.builder();
            Map<Symbol, QualifiedNameReference> sourceSymbolMap = intersectNode.sourceSymbolMap(sourceIndex);

            // add existing intersect symbols to projection
            for (Symbol intersectSymbol : sourceSymbolMap.keySet()) {
                Symbol symbol = allocateNewSymbolFrom(intersectSymbol);
                projectionAssignments.put(symbol, sourceSymbolMap.get(intersectSymbol));
            }

            // add extra marker fields to the projection
            List<Expression> markers = new ArrayList<>(Collections.nCopies(sources.size(), new Cast(new NullLiteral(), StandardTypes.BOOLEAN)));
            markers.set(sourceIndex, TRUE_LITERAL);
            for (Expression markerExpression : markers) {
                projectionAssignments.put(allocateNewSymbolFrom(INTERSECT_MARKER, BOOLEAN), markerExpression);
            }

            return new ProjectNode(idAllocator.getNextId(), sources.get(sourceIndex), projectionAssignments.build());
        }

        private FilterNode createFilterNodeForIntersect(AggregationNode sourceNode, Map<Symbol, FunctionCall> aggregations)
        {
            ImmutableList<Expression> predicatesForIntersection = aggregations.keySet().stream()
                    .map(aggregation -> new ComparisonExpression(GREATER_THAN_OR_EQUAL, aggregation.toQualifiedNameReference(), new LongLiteral("1")))
                    .collect(toImmutableList());
            return new FilterNode(idAllocator.getNextId(), sourceNode, ExpressionUtils.and(predicatesForIntersection));
        }
    }
}
