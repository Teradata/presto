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

import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * This class represents a set of inner joins that can be executed in any order.
 * The nodes of the join graph are those to be joined. The edges are
 * the criteria and filters. If there is no criterion or filter between a pair
 * of nodes, then a possible cross join is implied
 */
public class JoinGraphNode
        extends PlanNode
{
    private final List<PlanNode> nodes;
    private final List<JoinNode.EquiJoinClause> criteria;
    private final List<Expression> filters;

    public JoinGraphNode(PlanNodeId id, List<PlanNode> nodes, List<JoinNode.EquiJoinClause> criteria, List<Expression> filters)
    {
        super(id);
        this.nodes = nodes;
        this.criteria = criteria;
        this.filters = filters;
    }

    public List<JoinNode.EquiJoinClause> getCriteria()
    {
        return criteria;
    }

    public List<Expression> getFilters()
    {
        return filters;
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context)
    {
        return visitor.visitJoinGraph(this, context);
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.copyOf(nodes);
    }

    @Override
    public List<Symbol> getOutputSymbols()
    {
        ImmutableList.Builder<Symbol> symbolBuilder = ImmutableList.builder();
        for (PlanNode source : nodes) {
            symbolBuilder.addAll(source.getOutputSymbols());
        }
        return symbolBuilder.build();
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        return new JoinGraphNode(getId(), newChildren, criteria, filters);
    }
}
