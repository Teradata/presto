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
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.isJoinReorderingEnabled;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;

public class ConvertJoinTreeToJoinGraph
        implements Rule
{
    @Override
    public Optional<PlanNode> apply(PlanNode node, Lookup lookup, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, Session session)
    {
        // We check that join distribution type is absent because we only want to do this transformation once (reordered joins will have distribution type already set).
        if (!(node instanceof JoinNode) || !isJoinReorderingEnabled(session)) {
            return Optional.empty();
        }

        JoinNode joinNode = (JoinNode) node;
        if (!joinNode.getType().equals(INNER) || joinNode.getDistributionType().isPresent()) {
            return Optional.empty();
        }
        PlanNode result = new JoinGraphNode.JoinGraphNodeBuilder(joinNode, lookup).toJoinGraphNode(idAllocator);
        if (!result.getOutputSymbols().equals(node.getOutputSymbols())) {
            Assignments assignments = Assignments.builder().putIdentities(node.getOutputSymbols()).build();
            result = new ProjectNode(idAllocator.getNextId(), result, assignments);
        }
        return Optional.of(result);
    }
}
