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
package com.facebook.presto.sql.planner.iterative.rule.test;

import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.metadata.TableLayoutHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.connector.ConnectorPartitioningHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.ExpressionUtils;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.Partitioning;
import com.facebook.presto.sql.planner.PartitioningHandle;
import com.facebook.presto.sql.planner.PartitioningScheme;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.TestingTableHandle;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.AssignUniqueId;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.DeleteNode;
import com.facebook.presto.sql.planner.plan.ExceptNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.IntersectNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.MarkDistinctNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SampleNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.TableFinishNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.TableWriterNode;
import com.facebook.presto.sql.planner.plan.UnionNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.facebook.presto.spi.block.SortOrder.ASC_NULLS_LAST;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

public class PlanBuilder
{
    private final PlanNodeIdAllocator idAllocator;
    private final Map<Symbol, Type> symbols = new HashMap<>();

    public PlanBuilder(PlanNodeIdAllocator idAllocator)
    {
        this.idAllocator = idAllocator;
    }

    public ValuesNode values(Symbol... columns)
    {
        return new ValuesNode(
                idAllocator.getNextId(),
                ImmutableList.copyOf(columns),
                ImmutableList.of());
    }

    public ValuesNode values(List<Symbol> columns, List<List<Expression>> rows)
    {
        return new ValuesNode(idAllocator.getNextId(), columns, rows);
    }

    public LimitNode limit(long limit, PlanNode source)
    {
        return new LimitNode(idAllocator.getNextId(), source, limit, false);
    }

    public SampleNode sample(double sampleRatio, SampleNode.Type type, PlanNode source)
    {
        return new SampleNode(idAllocator.getNextId(), source, sampleRatio, type);
    }

    public SortNode sort(List<Symbol> orderBy, PlanNode source)
    {
        return new SortNode(
                idAllocator.getNextId(),
                source,
                orderBy,
                orderBy.stream().collect(Collectors.toMap(Function.identity(), symbol -> ASC_NULLS_LAST)));
    }

    public SortNode sort(List<Symbol> orderBy, Map<Symbol, SortOrder> orderings, PlanNode source)
    {
        return new SortNode(idAllocator.getNextId(), source, orderBy, orderings);
    }

    public ProjectNode project(Assignments assignments, PlanNode source)
    {
        return new ProjectNode(idAllocator.getNextId(), source, assignments);
    }

    public FilterNode filter(Expression predicate, PlanNode source)
    {
        return new FilterNode(idAllocator.getNextId(), source, predicate);
    }

    public ApplyNode apply(Assignments subqueryAssignments, List<Symbol> correlation, PlanNode input, PlanNode subquery)
    {
        return new ApplyNode(idAllocator.getNextId(), input, subquery, subqueryAssignments, correlation);
    }

    public AggregationNode aggregate(Map<Symbol, AggregationNode.Aggregation> assignments, List<List<Symbol>> groupingSets, PlanNode source)
    {
        return new AggregationNode(idAllocator.getNextId(), source, assignments, groupingSets, AggregationNode.Step.SINGLE, Optional.empty(), Optional.empty());
    }

    public TableScanNode tableScan(List<Symbol> symbols, Map<Symbol, ColumnHandle> assignments)
    {
        TableHandle tableHandle = new TableHandle(
                new ConnectorId("testConnector"),
                new TestingTableHandle());
        return tableScanWithTableLayout(symbols, assignments, tableHandle, null);
    }

    public TableScanNode tableScan(List<Symbol> symbols, Map<Symbol, ColumnHandle> assignments, TableHandle tableHandle)
    {
        return tableScanWithTableLayout(symbols, assignments, tableHandle, null);
    }

    public TableScanNode tableScanWithTableLayout(List<Symbol> symbols, Map<Symbol, ColumnHandle> assignments, TableHandle tableHandle, TableLayoutHandle tableLayout)
    {
        Expression originalConstraint = null;
        return new TableScanNode(
                idAllocator.getNextId(),
                tableHandle,
                symbols,
                assignments,
                Optional.ofNullable(tableLayout),
                TupleDomain.all(),
                originalConstraint
        );
    }

    public TableFinishNode tableDelete(SchemaTableName schemaTableName, PlanNode deleteSource, Symbol deleteRowId)
    {
        TableWriterNode.DeleteHandle deleteHandle = new TableWriterNode.DeleteHandle(
                new TableHandle(
                        new ConnectorId("testConnector"),
                        new TestingTableHandle()),
                schemaTableName
        );
        return new TableFinishNode(
                idAllocator.getNextId(),
                exchange(e -> e
                        .addSource(new DeleteNode(
                                idAllocator.getNextId(),
                                deleteSource,
                                deleteHandle,
                                deleteRowId,
                                ImmutableList.of(deleteRowId)
                        ))
                        .addInputsSet(deleteRowId)
                        .singleDistributionPartitioningScheme(deleteRowId)
                ),
                deleteHandle,
                ImmutableList.of(deleteRowId)
        );
    }

    public ExchangeNode exchange(Consumer<ExchangeBuilder> exchangeBuilderConsumer)
    {
        ExchangeBuilder exchangeBuilder = new ExchangeBuilder();
        exchangeBuilderConsumer.accept(exchangeBuilder);
        return exchangeBuilder.build();
    }

    public class ExchangeBuilder
    {
        private ExchangeNode.Type type = ExchangeNode.Type.GATHER;
        private ExchangeNode.Scope scope = ExchangeNode.Scope.REMOTE;
        private PartitioningScheme partitioningScheme;
        private List<PlanNode> sources = new ArrayList<>();
        private List<List<Symbol>> inputs = new ArrayList<>();

        public ExchangeBuilder type(ExchangeNode.Type type)
        {
            this.type = type;
            return this;
        }

        public ExchangeBuilder scope(ExchangeNode.Scope scope)
        {
            this.scope = scope;
            return this;
        }

        public ExchangeBuilder singleDistributionPartitioningScheme(Symbol... outputSymbols)
        {
            return partitioningScheme(new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), ImmutableList.copyOf(outputSymbols)));
        }

        public ExchangeBuilder fixedHashDistributionParitioningScheme(List<Symbol> outputSymbols, List<Symbol> partitioningSymbols)
        {
            return partitioningScheme(new PartitioningScheme(Partitioning.create(FIXED_HASH_DISTRIBUTION, ImmutableList.copyOf(partitioningSymbols)), ImmutableList.copyOf(outputSymbols)));
        }

        public ExchangeBuilder fixedHashDistributionParitioningScheme(List<Symbol> outputSymbols, List<Symbol> partitioningSymbols, Symbol hashSymbol)
        {
            return partitioningScheme(new PartitioningScheme(Partitioning.create(FIXED_HASH_DISTRIBUTION, ImmutableList.copyOf(partitioningSymbols)), ImmutableList.copyOf(outputSymbols), Optional.of(hashSymbol)));
        }

        public ExchangeBuilder partitioningScheme(PartitioningScheme partitioningScheme)
        {
            this.partitioningScheme = partitioningScheme;
            return this;
        }

        public ExchangeBuilder addSource(PlanNode source)
        {
            this.sources.add(source);
            return this;
        }

        public ExchangeBuilder addInputsSet(Symbol... inputs)
        {
            this.inputs.add(ImmutableList.copyOf(inputs));
            return this;
        }

        protected ExchangeNode build()
        {
            return new ExchangeNode(idAllocator.getNextId(), type, scope, partitioningScheme, sources, inputs);
        }
    }

    public AssignUniqueId assignUniqueId(Symbol uniqueId, PlanNode source)
    {
        return new AssignUniqueId(idAllocator.getNextId(), source, uniqueId);
    }

    public PlanNode markDistinct(Symbol marker, PlanNode source)
    {
        return new MarkDistinctNode(idAllocator.getNextId(), source, marker, source.getOutputSymbols(), Optional.empty());
    }

    public UnionNode union(ListMultimap<Symbol, Symbol> mapping, PlanNode... sources)
    {
        checkMappingsMatchesSources(mapping, sources);
        ImmutableList<Symbol> outputs = ImmutableList.copyOf(mapping.keySet());
        return new UnionNode(idAllocator.getNextId(), ImmutableList.copyOf(sources), mapping, outputs);
    }

    public IntersectNode intersect(ListMultimap<Symbol, Symbol> mapping, PlanNode... sources)
    {
        checkMappingsMatchesSources(mapping, sources);
        ImmutableList<Symbol> outputs = ImmutableList.copyOf(mapping.keySet());
        return new IntersectNode(idAllocator.getNextId(), ImmutableList.copyOf(sources), mapping, outputs);
    }

    public ExceptNode except(ListMultimap<Symbol, Symbol> mapping, PlanNode... sources)
    {
        checkMappingsMatchesSources(mapping, sources);
        ImmutableList<Symbol> outputs = ImmutableList.copyOf(mapping.keySet());
        return new ExceptNode(idAllocator.getNextId(), ImmutableList.copyOf(sources), mapping, outputs);
    }

    public ExchangeNode exchange(ListMultimap<Symbol, Symbol> mapping, PlanNode... sources)
    {
        ConnectorPartitioningHandle connectorPartitioningHandle = new ConnectorPartitioningHandle()
        {
            @Override
            public boolean isSingleNode()
            {
                return false;
            }

            @Override
            public boolean isCoordinatorOnly()
            {
                return false;
            }
        };
        PartitioningHandle handle = new PartitioningHandle(Optional.empty(), Optional.empty(), connectorPartitioningHandle);

        List<Symbol> outputSymbols = ImmutableList.copyOf(mapping.keySet());
        List<List<Symbol>> inputs = IntStream.range(0, sources.length)
                .mapToObj(i -> outputSymbols.stream()
                        .map(mapping::get)
                        .map(columnValues -> columnValues.get(i))
                        .collect(toImmutableList()))
                .collect(toImmutableList());

        return new ExchangeNode(
                idAllocator.getNextId(),
                ExchangeNode.Type.GATHER,
                ExchangeNode.Scope.REMOTE,
                new PartitioningScheme(Partitioning.create(handle, outputSymbols), outputSymbols),
                ImmutableList.copyOf(sources),
                inputs);
    }

    private void checkMappingsMatchesSources(ListMultimap<Symbol, Symbol> mapping, PlanNode... sources)
    {
        checkArgument(
                mapping.keySet().size() == sources.length,
                "Mapping keys size does not match length of sources: %s vs %s",
                mapping.keySet().size(),
                sources.length);
    }

    public MarkDistinctNode markDistinct(Symbol markerSymbol, List<Symbol> distinctSymbols, PlanNode source)
    {
        return new MarkDistinctNode(idAllocator.getNextId(), source, markerSymbol, distinctSymbols, Optional.empty());
    }

    public UnionNode union(List<? extends PlanNode> sources, ListMultimap<Symbol, Symbol> outputsToInputs, List<Symbol> outputs)
    {
        return new UnionNode(idAllocator.getNextId(), (List<PlanNode>) sources, outputsToInputs, outputs);
    }

    public Symbol symbol(String name, Type type)
    {
        Symbol symbol = new Symbol(name);

        Type old = symbols.put(symbol, type);
        if (old != null && !old.equals(type)) {
            throw new IllegalArgumentException(format("Symbol '%s' already registered with type '%s'", name, old));
        }

        if (old == null) {
            symbols.put(symbol, type);
        }

        return symbol;
    }

    public WindowNode window(WindowNode.Specification specification, Map<Symbol, WindowNode.Function> functions, PlanNode source)
    {
        return new WindowNode(
                idAllocator.getNextId(),
                source,
                specification,
                ImmutableMap.copyOf(functions),
                Optional.empty(),
                ImmutableSet.of(),
                0);
    }

    public static Expression expression(String sql)
    {
        return ExpressionUtils.rewriteIdentifiersToSymbolReferences(new SqlParser().createExpression(sql));
    }

    public static List<Expression> expressions(String... expressions)
    {
        return Stream.of(expressions)
                .map(PlanBuilder::expression)
                .collect(toImmutableList());
    }

    public Map<Symbol, Type> getSymbols()
    {
        return Collections.unmodifiableMap(symbols);
    }
}
