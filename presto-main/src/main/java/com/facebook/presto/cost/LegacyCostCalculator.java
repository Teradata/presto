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
package com.facebook.presto.cost;

import com.facebook.presto.Session;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;

import java.util.HashMap;
import java.util.Map;

import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;

/**
 * Legacy cost calculator for existing code that uses the old CostCalculator API
 *
 * New code should use the new costCalculator API through the Lookup.getCost() method
 */
@Deprecated
public class LegacyCostCalculator
{
    private final CostCalculator costCalculator;

    @Inject
    public LegacyCostCalculator(CostCalculator costCalculator)
    {
        this.costCalculator = costCalculator;
    }

    public Map<PlanNode, PlanNodeCost> calculateCostForPlan(Session session, Map<Symbol, Type> types, PlanNode planNode)
    {
        return new CostMapBuilder().getCosts(session, types, planNode);
    }

    private class CostMapBuilder
    {
        private final Map<PlanNode, PlanNodeCost> costs = new HashMap<>();

        private PlanNodeCost calculateCost(Session session, Map<Symbol, Type> types, PlanNode planNode)
        {
            ImmutableList<PlanNodeCost> sourceCosts = planNode.getSources().stream()
                    .map(node -> calculateCost(session, types, node))
                    .collect(toImmutableList());
            PlanNodeCost cost = costCalculator.calculateCostForNode(session, types, planNode, sourceCosts);
                costs.put(planNode, cost);

                return cost;
        }

        Map<PlanNode, PlanNodeCost> getCosts(Session session, Map<Symbol, Type> types, PlanNode root)
        {
            calculateCost(session, types, root);
            return ImmutableMap.copyOf(costs);
        }
    }
}
