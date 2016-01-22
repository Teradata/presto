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
package com.facebook.presto.sql.planner;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.sql.analyzer.SemanticErrorCode;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.SubqueryExpression;
import com.google.common.collect.ImmutableList;

import javax.inject.Provider;

import java.util.List;

import static com.facebook.presto.sql.planner.PlanVerifiersFactory.ExpressionExtractor.extractExpressions;

public class PlanVerifiersFactory
        implements Provider<List<PlanVerifier>>
{
    private final List<PlanVerifier> verifiers;

    public PlanVerifiersFactory()
    {
        this.verifiers = ImmutableList.of(new NoSubqueryExpressionLeft(),
                new NoApplyNodeLeft());
    }

    @Override
    public List<PlanVerifier> get()
    {
        return verifiers;
    }

    private class NoSubqueryExpressionLeft
            implements PlanVerifier
    {
        @Override
        public void verify(PlanNode plan)
        {
            for (Expression expression : extractExpressions(plan)) {
                new DefaultTraversalVisitor<Void, Void>()
                {
                    @Override
                    protected Void visitSubqueryExpression(SubqueryExpression node, Void context)
                    {
                        throw new SemanticException(SemanticErrorCode.NOT_SUPPORTED, node, "Given subquery is not yet supported");
                    }
                }.process(expression, null);
            }
        }
    }

    private class NoApplyNodeLeft
            implements PlanVerifier
    {
        @Override
        public void verify(PlanNode plan)
        {
            plan.accept(new SimplePlanVisitor()
            {
                @Override
                public Object visitApply(ApplyNode node, Object context)
                {
                    if (node.getCorrelation().isEmpty()) {
                        throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "Not supported subquery");
                    }
                    else {
                        throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "Correlated subquery is not yet supported");
                    }
                }
            }, null);
        }
    }

    public static class ExpressionExtractor
            extends SimplePlanVisitor<ImmutableList.Builder<Expression>>
    {
        public static List<Expression> extractExpressions(PlanNode plan)
        {
            ImmutableList.Builder<Expression> expressionsBuilder = ImmutableList.builder();
            plan.accept(new ExpressionExtractor(), expressionsBuilder);
            return expressionsBuilder.build();
        }

        @Override
        public Void visitFilter(FilterNode node, ImmutableList.Builder<Expression> context)
        {
            context.add(node.getPredicate());
            return super.visitFilter(node, context);
        }

        @Override
        public Void visitProject(ProjectNode node, ImmutableList.Builder<Expression> context)
        {
            context.addAll(node.getAssignments().values());
            return super.visitProject(node, context);
        }

        @Override
        public Void visitTableScan(TableScanNode node, ImmutableList.Builder<Expression> context)
        {
            context.add(node.getOriginalConstraint());
            return super.visitTableScan(node, context);
        }

        @Override
        public Void visitValues(ValuesNode node, ImmutableList.Builder<Expression> context)
        {
            node.getRows().forEach(context::addAll);
            return super.visitValues(node, context);
        }
    }
}
