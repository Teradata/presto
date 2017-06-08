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

import com.facebook.presto.sql.planner.EqualityInference;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableList;

import java.util.Collection;
import java.util.Set;

import static com.facebook.presto.sql.ExpressionUtils.combineConjuncts;
import static com.facebook.presto.sql.planner.DependencyExtractor.extractUnique;
import static com.facebook.presto.sql.planner.DeterminismEvaluator.isDeterministic;
import static com.facebook.presto.sql.planner.EqualityInference.createEqualityInference;
import static com.facebook.presto.sql.planner.EqualityInference.nonInferrableConjuncts;
import static com.google.common.base.Predicates.in;
import static java.util.Collections.disjoint;

public class ReorderJoinsPredicateUtils
{
    private ReorderJoinsPredicateUtils() {}

    public static SortedPredicatesResult sortPredicatesForJoin(Collection<Symbol> leftSymbols, Expression predicate)
    {
        ImmutableList.Builder<Expression> leftConjuncts = ImmutableList.builder();
        ImmutableList.Builder<Expression> rightConjuncts = ImmutableList.builder();
        ImmutableList.Builder<Expression> joinConjuncts = ImmutableList.builder();

        EqualityInference equalityInference = createEqualityInference(predicate);
        // See if we can push any parts of the predicates to either side
        for (Expression conjunct : nonInferrableConjuncts(predicate)) {
            if (isDeterministic(conjunct)) {
                Expression leftRewritten = equalityInference.rewriteExpression(conjunct, in(leftSymbols));
                if (leftRewritten != null) {
                    leftConjuncts.add(leftRewritten);
                }

                Expression rightRewritten = equalityInference.rewriteExpression(conjunct, symbol -> !leftSymbols.contains(symbol));
                if (rightRewritten != null) {
                    rightConjuncts.add(rightRewritten);
                }

                if (leftRewritten == null && rightRewritten == null) {
                    joinConjuncts.add(conjunct);
                }
            }
            else {
                Set<Symbol> conjunctSymbols = extractUnique(conjunct);
                if (leftSymbols.containsAll(conjunctSymbols)) {
                    leftConjuncts.add(conjunct);
                }
                else if (disjoint(leftSymbols, conjunctSymbols)) {
                    rightConjuncts.add(conjunct);
                }
                else {
                    joinConjuncts.add(conjunct);
                }
            }
        }

        // Add equalities from the inference back in
        // TODO: make generateEqualitiesPartitionedBy take left and right scope so we can reuse  a single equality inference
        leftConjuncts.addAll(equalityInference.generateEqualitiesPartitionedBy(leftSymbols::contains).getScopeEqualities());
        rightConjuncts.addAll(equalityInference.generateEqualitiesPartitionedBy(symbol -> !leftSymbols.contains(symbol)).getScopeEqualities());
        joinConjuncts.addAll(equalityInference.generateEqualitiesPartitionedBy(leftSymbols::contains).getScopeStraddlingEqualities()); // scope straddling equalities get dropped in as part of the join predicate

        return new SortedPredicatesResult(combineConjuncts(leftConjuncts.build()), combineConjuncts(rightConjuncts.build()), combineConjuncts(joinConjuncts.build()));
    }

    public static class SortedPredicatesResult
    {
        private final Expression leftPredicate;
        private final Expression rightPredicate;
        private final Expression joinPredicate;

        private SortedPredicatesResult(Expression leftPredicate, Expression rightPredicate, Expression joinPredicate)
        {
            this.leftPredicate = leftPredicate;
            this.rightPredicate = rightPredicate;
            this.joinPredicate = joinPredicate;
        }

        public Expression getLeftPredicate()
        {
            return leftPredicate;
        }

        public Expression getRightPredicate()
        {
            return rightPredicate;
        }

        public Expression getJoinPredicate()
        {
            return joinPredicate;
        }
    }
}
