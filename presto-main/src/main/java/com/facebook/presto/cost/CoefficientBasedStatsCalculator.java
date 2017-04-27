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
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.DomainTranslator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableMap;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.cost.PlanNodeStatsEstimate.UNKNOWN_STATS;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;

/**
 * Simple implementation of CostCalculator. It make many arbitrary decisions (e.g filtering selectivity, join matching).
 * It serves POC purpose. To be replaced with more advanced implementation.
 */
@ThreadSafe
public class CoefficientBasedStatsCalculator
        implements StatsCalculator
{
    private static final Double FILTER_COEFFICIENT = 0.5;
    private static final Double JOIN_MATCHING_COEFFICIENT = 2.0;

    // todo some computation for outputSizeInBytes

    private final Metadata metadata;

    @Inject
    public CoefficientBasedStatsCalculator(Metadata metadata)
    {
        this.metadata = metadata;
    }

    @Override
    public PlanNodeStatsEstimate calculateStats(
            PlanNode planNode,
            List<PlanNodeStatsEstimate> sourceCosts,
            Session session,
            Map<Symbol, Type> types)
    {
        Visitor visitor = new Visitor(session, types);
        return planNode.accept(visitor, sourceCosts);
    }

    private class Visitor
            extends PlanVisitor<List<PlanNodeStatsEstimate>, PlanNodeStatsEstimate>
    {
        private final Session session;
        private final Map<Symbol, Type> types;

        public Visitor(Session session, Map<Symbol, Type> types)
        {
            this.session = session;
            this.types = ImmutableMap.copyOf(types);
        }

        @Override
        protected PlanNodeStatsEstimate visitPlan(PlanNode node, List<PlanNodeStatsEstimate> sourceCosts)
        {
            // TODO: Explicitly visit GroupIdNode and throw an IllegalArgumentException
            // this can only be done once we get rid of the StatelessLookup
            return UNKNOWN_STATS;
        }

        @Override
        public PlanNodeStatsEstimate visitOutput(OutputNode node, List<PlanNodeStatsEstimate> sourceCosts)
        {
            return getOnlyElement(sourceCosts);
        }

        @Override
        public PlanNodeStatsEstimate visitFilter(FilterNode node, List<PlanNodeStatsEstimate> sourceCosts)
        {
            PlanNodeStatsEstimate sourceCost = getOnlyElement(sourceCosts);
            return sourceCost
                    .mapOutputRowCount(value -> value * FILTER_COEFFICIENT);
        }

        @Override
        public PlanNodeStatsEstimate visitProject(ProjectNode node, List<PlanNodeStatsEstimate> sourceCosts)
        {
            return getOnlyElement(sourceCosts);
        }

        @Override
        public PlanNodeStatsEstimate visitJoin(JoinNode node, List<PlanNodeStatsEstimate> sourceCosts)
        {
            checkSourceCostsCount(sourceCosts, 2);
            PlanNodeStatsEstimate leftCost = sourceCosts.get(0);
            PlanNodeStatsEstimate rightCost = sourceCosts.get(1);

            PlanNodeStatsEstimate.Builder joinCost = PlanNodeStatsEstimate.builder();
            if (!leftCost.getOutputRowCount().isValueUnknown() && !rightCost.getOutputRowCount().isValueUnknown()) {
                double joinOutputRowCount = Math.max(leftCost.getOutputRowCount().getValue(), rightCost.getOutputRowCount().getValue()) * JOIN_MATCHING_COEFFICIENT;
                joinCost.setOutputRowCount(new Estimate(joinOutputRowCount));
            }
            return joinCost.build();
        }

        @Override
        public PlanNodeStatsEstimate visitExchange(ExchangeNode node, List<PlanNodeStatsEstimate> sourceCosts)
        {
            Estimate exchangeOutputRowCount = new Estimate(0);
            checkSourceCostsCount(sourceCosts, node.getSources().size());
            for (int i = 0; i < node.getSources().size(); i++) {
                PlanNodeStatsEstimate childCost = sourceCosts.get(i);
                if (childCost.getOutputRowCount().isValueUnknown()) {
                    exchangeOutputRowCount = Estimate.unknownValue();
                }
                else {
                    exchangeOutputRowCount = exchangeOutputRowCount.map(value -> value + childCost.getOutputRowCount().getValue());
                }
            }

            return PlanNodeStatsEstimate.builder()
                    .setOutputRowCount(exchangeOutputRowCount)
                    .build();
        }

        @Override
        public PlanNodeStatsEstimate visitTableScan(TableScanNode node, List<PlanNodeStatsEstimate> sourceCosts)
        {
            Constraint<ColumnHandle> constraint = getConstraint(node, BooleanLiteral.TRUE_LITERAL);
            PlanNodeStatsEstimate.Builder tableScanCost = PlanNodeStatsEstimate.builder();

            TableStatistics tableStatistics = metadata.getTableStatistics(session, node.getTable(), constraint);
            tableScanCost.setOutputRowCount(tableStatistics.getRowCount());
            return tableScanCost.build();
        }

        private Constraint<ColumnHandle> getConstraint(TableScanNode node, Expression predicate)
        {
            DomainTranslator.ExtractionResult decomposedPredicate = DomainTranslator.fromPredicate(
                    metadata,
                    session,
                    predicate,
                    types);

            TupleDomain<ColumnHandle> simplifiedConstraint = decomposedPredicate.getTupleDomain()
                    .transform(node.getAssignments()::get)
                    .intersect(node.getCurrentConstraint());

            return new Constraint<>(simplifiedConstraint, bindings -> true);
        }

        @Override
        public PlanNodeStatsEstimate visitValues(ValuesNode node, List<PlanNodeStatsEstimate> sourceCosts)
        {
            Estimate valuesCount = new Estimate(node.getRows().size());
            return PlanNodeStatsEstimate.builder()
                    .setOutputRowCount(valuesCount)
                    .build();
        }

        @Override
        public PlanNodeStatsEstimate visitEnforceSingleRow(EnforceSingleRowNode node, List<PlanNodeStatsEstimate> sourceCosts)
        {
            return PlanNodeStatsEstimate.builder()
                    .setOutputRowCount(new Estimate(1.0))
                    .build();
        }

        @Override
        public PlanNodeStatsEstimate visitSemiJoin(SemiJoinNode node, List<PlanNodeStatsEstimate> sourceCosts)
        {
            checkState(sourceCosts.size() == 2, "must have exactly two child costs.");
            PlanNodeStatsEstimate sourceCost = sourceCosts.get(0);
            return sourceCost.mapOutputRowCount(rowCount -> rowCount * JOIN_MATCHING_COEFFICIENT);
        }

        @Override
        public PlanNodeStatsEstimate visitLimit(LimitNode node, List<PlanNodeStatsEstimate> sourceCosts)
        {
            PlanNodeStatsEstimate sourceCost = getOnlyElement(sourceCosts);
            PlanNodeStatsEstimate.Builder limitCost = PlanNodeStatsEstimate.builder();
            if (sourceCost.getOutputRowCount().getValue() < node.getCount()) {
                limitCost.setOutputRowCount(sourceCost.getOutputRowCount());
            }
            else {
                limitCost.setOutputRowCount(new Estimate(node.getCount()));
            }
            return limitCost.build();
        }

        private void checkSourceCostsCount(List<PlanNodeStatsEstimate> sourceCosts, int expectedCount)
        {
            checkArgument(sourceCosts.size() == expectedCount, "expected %s source costs, but found %s", sourceCosts.size(), expectedCount);
        }
    }
}
