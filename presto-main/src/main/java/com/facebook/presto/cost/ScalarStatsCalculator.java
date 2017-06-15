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
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.tree.ArithmeticBinaryExpression;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.SymbolReference;

import javax.inject.Inject;

import java.util.Map;

import static com.facebook.presto.sql.planner.LiteralInterpreter.evaluate;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

public class ScalarStatsCalculator
{
    private final Metadata metadata;

    @Inject
    public ScalarStatsCalculator(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata can not be null");
    }

    public SymbolStatsEstimate calculate(Expression scalarExpression, PlanNodeStatsEstimate inputStatistics, Session session, Map<Symbol, Type> types)
    {
        return new Visitor(inputStatistics, session, types).process(scalarExpression);
    }

    private class Visitor
            extends AstVisitor<SymbolStatsEstimate, Void>
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
        protected SymbolStatsEstimate visitNode(Node node, Void context)
        {
            return SymbolStatsEstimate.UNKNOWN_STATS;
        }

        @Override
        protected SymbolStatsEstimate visitSymbolReference(SymbolReference node, Void context)
        {
            return input.getSymbolStatistics()
                    .getOrDefault(Symbol.from(node), SymbolStatsEstimate.UNKNOWN_STATS);
        }

        @Override
        protected SymbolStatsEstimate visitNullLiteral(NullLiteral node, Void context)
        {
            return SymbolStatsEstimate.builder()
                    .setDistinctValuesCount(0)
                    .setNullsFraction(1)
                    .build();
        }

        @Override
        protected SymbolStatsEstimate visitLiteral(Literal node, Void context)
        {
            Object literalValue = evaluate(metadata, session.toConnectorSession(), node);
            // TODO transform literalValue to double and and set in result; would be nice to have literal type here and use TypeStatOperatorCaller
            return SymbolStatsEstimate.builder()
                    .setNullsFraction(0)
                    .setDistinctValuesCount(1)
                    .build();
        }

        @Override
        protected SymbolStatsEstimate visitArithmeticBinary(ArithmeticBinaryExpression node, Void context)
        {
            requireNonNull(node, "node is null");
            checkArgument(node.getType() != ArithmeticBinaryExpression.Type.MODULUS, "Modulus operator not yet supported");
            SymbolStatsEstimate left = process(node.getLeft());
            SymbolStatsEstimate right = process(node.getRight());

            SymbolStatsEstimate.Builder result = SymbolStatsEstimate.builder()
                    .setDataSize(Math.max(left.getDataSize(), right.getDataSize()))
                    .setNullsFraction(left.getNullsFraction() + right.getNullsFraction())
                    .setDistinctValuesCount(left.getDistinctValuesCount() * right.getDistinctValuesCount());

            double leftLow = left.getLowValue();
            double leftHigh = left.getHighValue();
            double rightLow = right.getLowValue();
            double rightHigh = right.getHighValue();
            if (isInfinity(leftLow) || isInfinity(leftHigh) || isInfinity(rightLow) || isInfinity(rightHigh)) {
                result.setLowValue(Double.MIN_VALUE)
                        .setHighValue(Double.MAX_VALUE);
            }
            else {
                double v1 = operate(node.getType(), leftLow, rightLow);
                double v2 = operate(node.getType(), leftLow, rightHigh);
                double v3 = operate(node.getType(), leftHigh, rightLow);
                double v4 = operate(node.getType(), leftHigh, rightHigh);

                result.setLowValue(min(v1, min(v2, min(v3, v4))))
                        .setHighValue(max(v1, max(v2, max(v3, v4))));
            }

            return result.build();
        }

        private boolean isInfinity(double value)
        {
            return value == Double.MIN_VALUE || value == Double.MAX_VALUE;
        }

        private double operate(ArithmeticBinaryExpression.Type type, double left, double right)
        {
            switch (type) {
                case ADD:
                    return left + right;
                case SUBTRACT:
                    return left - right;
                case MULTIPLY:
                    return left * right;
                case DIVIDE:
                    return left / right;
                case MODULUS:
                    return left % right;
                default:
                    throw new IllegalStateException("Unsupported ArithmeticBinaryExpression.Type: " + type);
            }
        }
    }
}
