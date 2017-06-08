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
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.SymbolReference;

import javax.inject.Inject;

import java.util.Map;

import static com.facebook.presto.sql.planner.LiteralInterpreter.evaluate;
import static java.util.Objects.requireNonNull;

public class ScalarStatsCalculator
{
    private final Metadata metadata;

    @Inject
    public ScalarStatsCalculator(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata can not be null");
    }

    public ColumnStatistics calculate(Expression scalarExpression, PlanNodeStatsEstimate inputStatistics, Session session, Map<Symbol, Type> types)
    {
        return new Visitor(inputStatistics, session, types).process(scalarExpression);
    }

    private class Visitor
            extends AstVisitor<ColumnStatistics, Void>
    {
        private final PlanNodeStatsEstimate input;
        private final Session session;
        private final Map<Symbol, Type> types;

        Visitor(PlanNodeStatsEstimate input, Session session, Map<Symbol, Type> types)
        {
            this.input = input;
            this.session = session;
            this.types = types;
        }

        @Override
        protected ColumnStatistics visitNode(Node node, Void context)
        {
            return ColumnStatistics.UNKNOWN_COLUMN_STATISTICS;
        }

        @Override
        protected ColumnStatistics visitSymbolReference(SymbolReference node, Void context)
        {
            return input.getSymbolStatistics().getOrDefault(Symbol.from(node), ColumnStatistics.UNKNOWN_COLUMN_STATISTICS);
        }

        @Override
        protected ColumnStatistics visitNullLiteral(NullLiteral node, Void context)
        {
            ColumnStatistics.Builder builder = ColumnStatistics.builder();
            builder.addRange(rb -> rb
                    .setFraction(Estimate.of(0.0))
                    .setDistinctValuesCount(Estimate.zeroValue()));
            builder.setNullsFraction(Estimate.of(1.0));
            return builder.build();
        }

        @Override
        protected ColumnStatistics visitLiteral(Literal node, Void context)
        {
            Object literalValue = evaluate(metadata, session.toConnectorSession(), node);
            ColumnStatistics.Builder builder = ColumnStatistics.builder();
            builder.addRange(literalValue, literalValue, rb -> rb
                    .setFraction(Estimate.of(1.0))
                    .setDistinctValuesCount(Estimate.of(1.0)));
            builder.setNullsFraction(Estimate.zeroValue());
            return builder.build();
        }
    }
}
