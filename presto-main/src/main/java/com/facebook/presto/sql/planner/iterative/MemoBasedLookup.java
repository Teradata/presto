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

package com.facebook.presto.sql.planner.iterative;

import com.facebook.presto.Session;
import com.facebook.presto.cost.CostCalculator;
import com.facebook.presto.cost.PlanNodeCost;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.PlanNode;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class MemoBasedLookup
        implements Lookup
{
    private final Memo memo;
    private final Map<PlanNode, PlanNodeCost> costs = new HashMap<>();
    private final CostCalculator costCalculator;

    public MemoBasedLookup(Memo memo, CostCalculator costCalculator)
    {
        this.memo = memo;
        this.costCalculator = requireNonNull(costCalculator, "costCalculator is null");
    }

    @Override
    public PlanNode resolve(PlanNode node)
    {
        if (node instanceof GroupReference) {
            return memo.getNode(((GroupReference) node).getGroupId());
        }
        return node;
    }

    @Override
    public PlanNodeCost getCost(PlanNode planNode, Session session, Map<Symbol, Type> types)
    {
        return costs.computeIfAbsent(resolve(planNode), node -> costCalculator.calculateCost(
                node,
                node.getSources().stream()
                        .map(sourceNode -> getCost(sourceNode, session, types))
                        .collect(toImmutableList()),
                session,
                types));
    }
}
