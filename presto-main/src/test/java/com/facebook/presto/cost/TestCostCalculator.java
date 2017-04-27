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
import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.TestingColumnHandle;
import com.facebook.presto.sql.planner.TestingTableHandle;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.cost.PlanNodeCostEstimate.cpuCost;
import static com.facebook.presto.metadata.FunctionKind.AGGREGATE;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestCostCalculator
{
    private static final int NUMBER_OF_NODES = 10;
    private final CostCalculator costCalculatorUsingExchanges = new CostCalculatorUsingExchanges(NUMBER_OF_NODES);
    private final CostCalculator costCalculatorWithEstimatedExchanges = new CostCalculatorWithEstimatedExchanges(costCalculatorUsingExchanges, NUMBER_OF_NODES);
    private Session session = testSessionBuilder().build();

    @Test
    public void testTableScan()
    {
        assertCost(
                tableScan(1, "orderkey"),
                ImmutableMap.of(),
                ImmutableMap.of(1, statsEstimate(1000)))
                .cpu(1000)
                .memory(0)
                .network(0);

        assertCostEstimatedExchanges(
                tableScan(1, "orderkey"),
                ImmutableMap.of(),
                ImmutableMap.of(1, statsEstimate(1000)))
                .cpu(1000)
                .memory(0)
                .network(0);
    }

    @Test
    public void testProject()
    {
        PlanNode project = project(2, tableScan(1, "orderkey"), "string", new Cast(new SymbolReference("orderkey"), "STRING"));
        Map<Integer, PlanNodeCostEstimate> costs = ImmutableMap.of(1, cpuCost(new Estimate(1000)));
        Map<Integer, PlanNodeStatsEstimate> stats = ImmutableMap.of(2, statsEstimate(4000), 1, statsEstimate(1000));
        assertCost(
                project,
                costs,
                stats)
                .cpu(1000 + 4000)
                .memory(0)
                .network(0);

        assertCostEstimatedExchanges(
                project,
                costs,
                stats)
                .cpu(1000 + 4000)
                .memory(0)
                .network(0);
    }

    @Test
    public void testJoin()
    {
        Map<Integer, PlanNodeCostEstimate> costs = ImmutableMap.of(
                1, cpuCost(new Estimate(6000)),
                2, cpuCost(new Estimate(1000)));

        Map<Integer, PlanNodeStatsEstimate> stats = ImmutableMap.of(
                3, statsEstimate(12000),
                1, statsEstimate(6000),
                2, statsEstimate(1000));

        JoinNode partitionedJoin = join(3,
                tableScan(1, "orderkey"),
                tableScan(2, "orderkey_0"),
                JoinNode.DistributionType.PARTITIONED,
                "orderkey",
                "orderkey_0");

        assertCost(
                partitionedJoin,
                costs,
                stats)
                .cpu(12000 + 6000 + 1000 + 6000 + 1000)
                .memory(1000)
                .network(0);

        assertCostEstimatedExchanges(
                partitionedJoin,
                costs,
                stats)
                .cpu(12000 + 6000 + 1000 + 6000 + 1000 + 6000 + 1000)
                .memory(1000)
                .network(6000 + 1000);

        JoinNode replicatedJoin = join(3,
                tableScan(1, "orderkey"),
                tableScan(2, "orderkey_0"),
                JoinNode.DistributionType.REPLICATED,
                "orderkey",
                "orderkey_0");

        assertCost(
                replicatedJoin,
                costs,
                stats)
                .cpu(12000 + 6000 + 10000 + 6000 + 1000)
                .memory(10000)
                .network(0);

        assertCostEstimatedExchanges(
                replicatedJoin,
                costs,
                stats)
                .cpu(12000 + 6000 + 10000 + 6000 + 1000)
                .memory(10000)
                .network(NUMBER_OF_NODES * 1000);
    }

    @Test
    public void testAggregation()
    {
        Map<Integer, PlanNodeCostEstimate> costs = ImmutableMap.of(1, cpuCost(new Estimate(6000)));
        Map<Integer, PlanNodeStatsEstimate> stats = ImmutableMap.of(
                1, statsEstimate(6000),
                2, statsEstimate(8));

        AggregationNode aggregationNode = aggregation(2,
                tableScan(1, "orderkey"));

        assertCost(aggregationNode, costs, stats)
                .cpu(6000 + 6000)
                .memory(8)
                .network(0);

        assertCostEstimatedExchanges(aggregationNode, costs, stats)
                .cpu(6000 + 6000 + 6000)
                .memory(8)
                .network(6000);
    }

    private CostAssertionBuilder assertCost(
            PlanNode node,
            Map<Integer, PlanNodeCostEstimate> costs,
            Map<Integer, PlanNodeStatsEstimate> stats)
    {
        return new CostAssertionBuilder(costCalculatorUsingExchanges.calculateCumulativeCost(
                session,
                ImmutableMap.of(),
                node,
                new FixedLookup(costs, stats)));
    }

    private CostAssertionBuilder assertCostEstimatedExchanges(
            PlanNode node,
            Map<Integer, PlanNodeCostEstimate> costs,
            Map<Integer, PlanNodeStatsEstimate> stats)
    {
        return new CostAssertionBuilder(costCalculatorWithEstimatedExchanges.calculateCumulativeCost(
                session,
                ImmutableMap.of(),
                node,
                new FixedLookup(costs, stats)));
    }

    private static class CostAssertionBuilder
    {
        private final PlanNodeCostEstimate actual;

        public CostAssertionBuilder(PlanNodeCostEstimate actual)
        {
            this.actual = requireNonNull(actual, "actual is null");
        }

        public CostAssertionBuilder network(double value)
        {
            assertEquals(actual.getNetworkCost().getValue(), value, 0.1);
            return this;
        }

        public CostAssertionBuilder cpu(double value)
        {
            assertEquals(actual.getCpuCost().getValue(), value, 0.1);
            return this;
        }

        public CostAssertionBuilder memory(double value)
        {
            assertEquals(actual.getMemoryCost().getValue(), value, 0.1);
            return this;
        }
    }

    private static PlanNodeStatsEstimate statsEstimate(int outputSizeInBytes)
    {
        return PlanNodeStatsEstimate.builder()
                .setOutputRowCount(new Estimate(Math.max(outputSizeInBytes / 8, 1)))
                .setOutputSizeInBytes(new Estimate(outputSizeInBytes)).build();
    }

    private TableScanNode tableScan(int planNodeId, String... symbols)
    {
        List<Symbol> symbolsList = Arrays.stream(symbols).map(Symbol::new).collect(toImmutableList());
        ImmutableMap.Builder<Symbol, ColumnHandle> assignments = ImmutableMap.builder();

        for (Symbol symbol : symbolsList) {
            assignments.put(symbol, new TestingColumnHandle(symbol.getName()));
        }

        return new TableScanNode(
                new PlanNodeId(Integer.toString(planNodeId)),
                new TableHandle(new ConnectorId("tpch"), new TestingTableHandle()),
                symbolsList,
                assignments.build(),
                Optional.empty(),
                TupleDomain.none(),
                null);
    }

    private PlanNode project(int planNodeId, PlanNode source, String symbol, Expression expression)
    {
        return new ProjectNode(
                new PlanNodeId(Integer.toString(planNodeId)),
                source,
                Assignments.of(new Symbol(symbol), expression));
    }

    private String symbol(String name)
    {
        return name;
    }

    private AggregationNode aggregation(int planNodeId, PlanNode source)
    {
        AggregationNode.Aggregation dupa = new AggregationNode.Aggregation(
                new FunctionCall(QualifiedName.of("count"), ImmutableList.of()),
                new Signature("count", AGGREGATE, parseTypeSignature(StandardTypes.BIGINT)));

        return new AggregationNode(
                new PlanNodeId(Integer.toString(planNodeId)),
                source,
                ImmutableMap.of(new Symbol("count"), dupa),
                ImmutableList.of(source.getOutputSymbols()),
                AggregationNode.Step.FINAL,
                Optional.empty(),
                Optional.empty());
    }

    private JoinNode join(int planNodeId, PlanNode left, PlanNode right, String... symbols)
    {
        return join(planNodeId, left, right, JoinNode.DistributionType.PARTITIONED, symbols);
    }

    /**
     * EquiJoinClause is created from symbols in form of:
     * symbol[0] = symbol[1] AND symbol[2] = symbol[3] AND ...
     */
    private JoinNode join(int planNodeId, PlanNode left, PlanNode right, JoinNode.DistributionType distributionType, String... symbols)
    {
        checkArgument(symbols.length % 2 == 0);
        ImmutableList.Builder<JoinNode.EquiJoinClause> criteria = ImmutableList.builder();

        for (int i = 0; i < symbols.length; i += 2) {
            criteria.add(new JoinNode.EquiJoinClause(new Symbol(symbols[i]), new Symbol(symbols[i + 1])));
        }

        return new JoinNode(
                new PlanNodeId(Integer.toString(planNodeId)),
                JoinNode.Type.INNER,
                left,
                right,
                criteria.build(),
                ImmutableList.<Symbol>builder()
                        .addAll(left.getOutputSymbols())
                        .addAll(right.getOutputSymbols())
                        .build(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(distributionType));
    }

    private ValuesNode values(int planNodeId, String... symbols)
    {
        return new ValuesNode(
                new PlanNodeId(Integer.toString(planNodeId)),
                Arrays.stream(symbols)
                        .map(Symbol::new)
                        .collect(toImmutableList()),
                ImmutableList.of());
    }

    private class FixedLookup
            implements Lookup
    {
        Map<PlanNodeId, PlanNodeCostEstimate> costs;
        Map<PlanNodeId, PlanNodeStatsEstimate> stats;

        public FixedLookup(Map<Integer, PlanNodeCostEstimate> costs, Map<Integer, PlanNodeStatsEstimate> stats)
        {
            this.costs = costs.entrySet()
                    .stream()
                    .collect(toMap(entry -> new PlanNodeId(entry.getKey().toString()), Map.Entry::getValue));
            this.stats = stats.entrySet()
                    .stream()
                    .collect(toMap(entry -> new PlanNodeId(entry.getKey().toString()), Map.Entry::getValue));
        }

        @Override
        public PlanNode resolve(PlanNode node)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public PlanNodeStatsEstimate getStats(Session session, Map<Symbol, Type> types, PlanNode node)
        {
            return stats.get(node.getId());
        }

        @Override
        public PlanNodeCostEstimate getCumulativeCost(Session session, Map<Symbol, Type> types, PlanNode node)
        {
            return costs.get(node.getId());
        }
    }
}
