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
package com.facebook.presto.execution;

import com.facebook.presto.Session;
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.util.ImmutableCollectors.toImmutableMap;

public class QueryPlan
{
    private final Plan plan;
    private final Map<PlanNode, PlanNodeStatsEstimate> planNodeStatsEstimates;

    public QueryPlan(Plan plan, Lookup lookup, Session session)
    {
        this.plan = plan;
        this.planNodeStatsEstimates = getPlanNodes(
                plan.getRoot()).stream()
                .collect(toImmutableMap(node -> node, node -> lookup.getStats(session, plan.getTypes(), node)));
    }

    public Plan getPlan()
    {
        return plan;
    }

    private List<PlanNode> getPlanNodes(PlanNode root)
    {
        ImmutableList.Builder<PlanNode> planNodes = ImmutableList.builder();
        planNodes.add(root);
        for (PlanNode source : root.getSources()) {
            planNodes.addAll(getPlanNodes(source));
        }
        return planNodes.build();
    }

    public Map<PlanNode, PlanNodeStatsEstimate> getPlanNodeStatsEstimates()
    {
        return planNodeStatsEstimates;
    }
}
