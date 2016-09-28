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
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.sql.planner.plan.JoinNode.Method.booleanToJoinMethod;
import static com.facebook.presto.sql.planner.plan.JoinNode.canDistributeJoin;
import static java.util.Objects.requireNonNull;

public class TransformUncorrelatedScalarToJoin
        implements PlanOptimizer
{
    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        return SimplePlanRewriter.rewriteWith(new Rewriter(idAllocator, session), plan, null);
    }

    private class Rewriter
            extends SimplePlanRewriter<PlanNode>
    {
        private final PlanNodeIdAllocator idAllocator;
        private final Session session;

        public Rewriter(PlanNodeIdAllocator idAllocator, Session session)
        {
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.session = requireNonNull(session, "session is null");
        }

        @Override
        public PlanNode visitApply(ApplyNode node, RewriteContext<PlanNode> context)
        {
            ApplyNode rewrittenNode = (ApplyNode) context.defaultRewrite(node, context.get());
            if (rewrittenNode.getCorrelation().isEmpty() && rewrittenNode.getSubquery() instanceof EnforceSingleRowNode) {
                PlanNode rightNode = rewrittenNode.getSubquery();
                List<JoinNode.EquiJoinClause> equiJoinClauses = ImmutableList.of();
                JoinNode.Type type = JoinNode.Type.INNER;
                return new JoinNode(
                        idAllocator.getNextId(),
                        type,
                        booleanToJoinMethod(canDistributeJoin(session, rightNode, equiJoinClauses, type)),
                        rewrittenNode.getInput(),
                        rightNode,
                        equiJoinClauses,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty());
            }
            return rewrittenNode;
        }
    }
}
