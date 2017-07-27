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

import com.facebook.presto.sql.relational.RowExpression;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FieldReference;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.sql.ExpressionUtils.extractConjuncts;
import static com.facebook.presto.sql.tree.LogicalBinaryExpression.Type.AND;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

/**
 * Currently this class handles simple expressions like:
 * <p>
 * A.a < B.x
 * and queries with range predicates involving a single build symbol
 * reference (A.a) like:
 * <p>
 * A.a < B.x + 20 AND A.a > B.x + 10
 * <p>
 * where a is the build side symbol reference mapping to any expression
 * which has been pushed down to the corresponding Scan node.
 * <p>
 * It could be extended to handle any expressions like:
 * <p>
 * A.a * sin(A.b) / log(B.x) < cos(B.z)
 * <p>
 * by transforming it to:
 * <p>
 * f(A.a, A.b) < g(B.x, B.z)
 * <p>
 * Where f(...) and g(...) would be some functions/expressions. That
 * would allow us to perform binary search on arbitrary complex expressions
 * by sorting position links according to the result of f(...) function.
 */
public final class SortExpressionExtractor
{
    private SortExpressionExtractor() {}

    public static Optional<Expression> extractSortExpression(Set<Symbol> buildSymbols, Expression filter)
    {
        if (!DeterminismEvaluator.isDeterministic(filter)) {
            return Optional.empty();
        }

        return new SortExpressionVisitor(buildSymbols).process(filter);
    }

    private static class SortExpressionVisitor
            extends AstVisitor<Optional<Expression>, Void>
    {
        private final Set<Symbol> buildSymbols;

        public SortExpressionVisitor(Set<Symbol> buildSymbols)
        {
            this.buildSymbols = buildSymbols;
        }

        @Override
        protected Optional<Expression> visitExpression(Expression expression, Void context)
        {
            return Optional.empty();
        }

        @Override
        protected Optional<Expression> visitLogicalBinaryExpression(LogicalBinaryExpression binaryExpression, Void context)
        {
            if (binaryExpression.getType() != AND) {
                return Optional.empty();
            }
            Optional<Expression> leftProcessed = process(binaryExpression.getLeft());
            Optional<Expression> rightProcessed = process(binaryExpression.getRight());

            if (!leftProcessed.isPresent() || !rightProcessed.isPresent() || !leftProcessed.get().equals(rightProcessed.get())) {
                return Optional.empty();
            }
            return leftProcessed;
        }

        @Override
        protected Optional<Expression> visitComparisonExpression(ComparisonExpression comparison, Void context)
        {
            switch (comparison.getType()) {
                case GREATER_THAN:
                case GREATER_THAN_OR_EQUAL:
                case LESS_THAN:
                case LESS_THAN_OR_EQUAL:
                    Optional<SymbolReference> sortChannel = asBuildSymbolReference(buildSymbols, comparison.getRight());
                    boolean hasBuildReferencesOnOtherSide = hasBuildSymbolReference(buildSymbols, comparison.getLeft());
                    if (!sortChannel.isPresent()) {
                        sortChannel = asBuildSymbolReference(buildSymbols, comparison.getLeft());
                        hasBuildReferencesOnOtherSide = hasBuildSymbolReference(buildSymbols, comparison.getRight());
                    }
                    if (sortChannel.isPresent() && !hasBuildReferencesOnOtherSide) {
                        return sortChannel.map(symbolReference -> (Expression) symbolReference);
                    }
                    return Optional.empty();
                default:
                    return Optional.empty();
            }
        }
    }

    private static Optional<SymbolReference> asBuildSymbolReference(Set<Symbol> buildLayout, Expression expression)
    {
        // this is required to make sure the expression is pushed down (sort channel has the expression result)
        if (expression instanceof SymbolReference) {
            SymbolReference symbolReference = (SymbolReference) expression;
            if (buildLayout.contains(new Symbol(symbolReference.getName()))) {
                return Optional.of(symbolReference);
            }
        }
        return Optional.empty();
    }

    private static boolean hasBuildSymbolReference(Set<Symbol> buildSymbols, Expression expression)
    {
        return new BuildSymbolReferenceFinder(buildSymbols).process(expression);
    }

    private static class BuildSymbolReferenceFinder
            extends AstVisitor<Boolean, Void>
    {
        private final Set<String> buildSymbols;

        public BuildSymbolReferenceFinder(Set<Symbol> buildSymbols)
        {
            this.buildSymbols = requireNonNull(buildSymbols, "buildSymbols is null").stream()
                    .map(Symbol::getName)
                    .collect(toImmutableSet());
        }

        @Override
        protected Boolean visitNode(Node node, Void context)
        {
            for (Node child : node.getChildren()) {
                if (process(child, context)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        protected Boolean visitSymbolReference(SymbolReference symbolReference, Void context)
        {
            return buildSymbols.contains(symbolReference.getName());
        }
    }

    public static class SortExpression
    {
        private final int channel;
        private List<RowExpression> inequalityJoinFilterConjuncts;

        public SortExpression(int channel, List<RowExpression> inequalityJoinFilterConjuncts)
        {
            this.channel = channel;
            this.inequalityJoinFilterConjuncts = inequalityJoinFilterConjuncts;
        }

        public int getChannel()
        {
            return channel;
        }

        public List<RowExpression> getInequalityJoinFilterConjuncts()
        {
            return inequalityJoinFilterConjuncts;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            SortExpression other = (SortExpression) obj;
            return Objects.equals(channel, other.channel) &&
                    Objects.equals(inequalityJoinFilterConjuncts, other.inequalityJoinFilterConjuncts);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(channel, inequalityJoinFilterConjuncts);
        }

        public String toString()
        {
            return toStringHelper(this)
                    .add("channel", channel)
                    .add("inequalityJoinFilterConjuncts", inequalityJoinFilterConjuncts)
                    .toString();
        }

        public static int fieldReferenceIndex(Expression expression)
        {
            checkState(expression instanceof FieldReference, "Unsupported expression type [%s]", expression);
            return ((FieldReference) expression).getFieldIndex();
        }

        public static List<Expression> inequalityJoinFilterConjuncts(Expression expression)
        {
            return new FilterExpressionsVisitor().process(expression);
        }

        private static class FilterExpressionsVisitor
                extends AstVisitor<List<Expression>, Void>
        {
            @Override
            protected List<Expression> visitExpression(Expression expression, Void context)
            {
                return expression.getChildren().stream()
                    .flatMap(child -> process(child).stream())
                    .collect(toImmutableList());
            }

            @Override
            protected List<Expression> visitLogicalBinaryExpression(LogicalBinaryExpression expression, Void context)
            {
                return extractConjuncts(expression);
            }

            @Override
            protected List<Expression> visitComparisonExpression(ComparisonExpression expression, Void context)
            {
                return ImmutableList.of(expression);
            }
        }
    }
}
