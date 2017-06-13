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
import com.facebook.presto.spi.statistics.ColumnStatistics;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.sql.planner.ExpressionInterpreter.evaluateConstantExpression;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.stream.Collectors.toList;

public class ValuesStatsRule
        implements ComposableStatsCalculator.Rule
{
    private final Metadata metadata;

    public ValuesStatsRule(Metadata metadata)
    {
        this.metadata = metadata;
    }

    @Override
    public Optional<PlanNodeStatsEstimate> calculate(PlanNode node, Lookup lookup, Session session, Map<Symbol, Type> types)
    {
        if (!(node instanceof ValuesNode)) {
            return Optional.empty();
        }
        ValuesNode valuesNode = (ValuesNode) node;

        PlanNodeStatsEstimate.Builder statsBuilder = PlanNodeStatsEstimate.builder();
        statsBuilder.setOutputRowCount(new Estimate(valuesNode.getRows().size()));

        for (int symbolId = 0; symbolId < valuesNode.getOutputSymbols().size(); ++symbolId) {
            Symbol symbol = valuesNode.getOutputSymbols().get(symbolId);
            List<Object> symbolValues = getSymbolValues(valuesNode, symbolId, session, types.get(symbol));
            statsBuilder.addSymbolStatistics(symbol, buildSymbolStatistics(symbolValues, session, types.get(symbol)));
        }

        return Optional.of(statsBuilder.build());
    }

    private List<Object> getSymbolValues(ValuesNode valuesNode, int symbolId, Session session, Type symbolType)
    {
        return valuesNode.getRows().stream()
                .map(row -> row.get(symbolId))
                .map(expression -> evaluateConstantExpression(expression, symbolType, metadata, session, ImmutableList.of()))
                .collect(toList());
    }

    private ColumnStatistics buildSymbolStatistics(List<Object> values, Session session, Type type)
    {
        TypeStatOperatorCaller operatorCaller = new TypeStatOperatorCaller(type, metadata.getFunctionRegistry(), session.toConnectorSession());

        List<Object> nonNullValues = values.stream()
                .filter(Objects::nonNull)
                .collect(toImmutableList());

        if (nonNullValues.isEmpty()) {
            return ColumnStatistics.builder()
                    .setNullsFraction(Estimate.of(1.0))
                    .addRange(rangeStatistics -> rangeStatistics
                            .setDistinctValuesCount(Estimate.zeroValue())
                            .setFraction(Estimate.zeroValue()))
                    .build();
        }
        else {
            Object lowValue = operatorCaller.getOrdering().min(nonNullValues);
            Object highValue = operatorCaller.getOrdering().max(nonNullValues);
            int valuesCount = values.size();
            int nonNullValuesCount = nonNullValues.size();
            return ColumnStatistics.builder()
                    .setNullsFraction(Estimate.of(((double) (valuesCount - nonNullValuesCount)) / valuesCount))
                    .addRange(rangeStatistics -> rangeStatistics
                            .setLowValue(Optional.of(lowValue))
                            .setHighValue(Optional.of(highValue))
                            .setFraction(Estimate.of(((double) nonNullValuesCount) / valuesCount))
                            .setDistinctValuesCount(Estimate.of(nonNullValues.stream().distinct().count())))
                    .build();
        }
    }
}
