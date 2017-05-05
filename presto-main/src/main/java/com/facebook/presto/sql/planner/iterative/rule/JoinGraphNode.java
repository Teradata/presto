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

import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.sql.ExpressionUtils.and;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static com.facebook.presto.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static com.facebook.presto.sql.tree.ComparisonExpressionType.EQUAL;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * This class represents a set of inner joins that can be executed in any order.
 * The nodes of the join graph are the sources to be joined. The edges are
 * filters. If there is no filter between a pair
 * of sources, then a possible cross join is implied
 */
public class JoinGraphNode
        extends PlanNode
{
    private final List<PlanNode> sources;
    private final Expression filter;

    public JoinGraphNode(PlanNodeId id, List<PlanNode> sources, Expression filter)
    {
        super(id);

        requireNonNull(sources, "sources is null");
        requireNonNull(filter, "filters is null");

        this.sources = ImmutableList.copyOf(sources);
        this.filter = filter;
    }

    public Expression getFilter()
    {
        return filter;
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context)
    {
        return visitor.visitJoinGraph(this, context);
    }

    @Override
    public List<PlanNode> getSources()
    {
        return sources;
    }

    @Override
    public List<Symbol> getOutputSymbols()
    {
        return sources.stream()
                .flatMap(source -> source.getOutputSymbols().stream())
                .collect(toImmutableList());
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        return new JoinGraphNode(getId(), newChildren, filter);
    }

    public String getKey()
    {
        List<String> sourceIds = sources.stream()
                .map(source -> source.getId().toString())
                .collect(toImmutableList());
        return sourceIds.toString() + filter.toString();
    }

    public static class JoinGraphNodeBuilder
    {
        private final List<PlanNode> sources = new ArrayList<>();
        private final List<Expression> filters = new ArrayList<>();

        // WARNING: this does not maintain the output symbols of the input joins.
        // a project may need to be added above the JoinGraphNode to ensure correctness.
        public JoinGraphNodeBuilder(JoinNode node, Lookup lookup)
        {
            flattenNode(node, lookup);
        }

        private void flattenNode(PlanNode node, Lookup lookup)
        {
            PlanNode resolved = lookup.resolve(node);
            if (resolved instanceof JoinNode && ((JoinNode) resolved).getType() == INNER) {
                JoinNode joinNode = (JoinNode) resolved;
                flattenNode(joinNode.getLeft(), lookup);
                flattenNode(joinNode.getRight(), lookup);
                filters.addAll(
                        joinNode.getCriteria().stream()
                                .map(criterion -> new ComparisonExpression(EQUAL, criterion.getLeft().toSymbolReference(), criterion.getRight().toSymbolReference()))
                                .collect(toImmutableList()));
                (joinNode).getFilter().ifPresent(filters::add);
            }
            else {
                sources.add(node);
            }
        }

        public JoinGraphNode toJoinGraphNode(PlanNodeIdAllocator idAllocator)
        {
            if (filters.isEmpty()) {
                filters.add(TRUE_LITERAL);
            }
            return new JoinGraphNode(idAllocator.getNextId(), sources, and(filters));
        }
    }
}
