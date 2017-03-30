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

import com.facebook.presto.sql.planner.plan.JoinNode;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class ReorderJoinsMemo
{
    private final Map<JoinGraphNode, MemoInfo> memo = new HashMap<>();

    public void add(JoinGraphNode joinGraph, JoinNode node)
    {
        memo.put(joinGraph, new MemoInfo(node));
    }

    public Optional<JoinNode> getPlan(JoinGraphNode node)
    {
        if (memo.containsKey(node)) {
            return Optional.of(memo.get(node).leastCostPlan);
        }
        return Optional.empty();
    }

    private class MemoInfo
    {
        private final JoinNode leastCostPlan;

        private MemoInfo(JoinNode leastCostPlan)
        {
            this.leastCostPlan = leastCostPlan;
        }
    }
}
