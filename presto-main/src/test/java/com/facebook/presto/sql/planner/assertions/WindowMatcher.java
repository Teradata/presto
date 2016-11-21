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

package com.facebook.presto.sql.planner.assertions;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Window;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.util.ImmutableCollectors.toImmutableMap;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

final class WindowMatcher
        implements Matcher
{
    private final ExpectedWindowNodeSpecification specification;
    private final Map<QualifiedName, ExpectedFunctionCall> expectedFunctionCalls;

    WindowMatcher(ExpectedWindowNodeSpecification specification, List<ExpectedFunctionCall> expectedFunctionCalls)
    {
        this.specification = specification;
        this.expectedFunctionCalls = requireNonNull(expectedFunctionCalls, "expectedFunctionCalls is null").stream()
                .collect(toImmutableMap(ExpectedFunctionCall::getName));
    }

    @Override
    public boolean matches(PlanNode node, Session session, Metadata metadata, ExpressionAliases expressionAliases)
    {
        WindowNode windowNode = (WindowNode) node;

        if (!samePartitionAndOrder(specification, windowNode.getSpecification(), expressionAliases)) {
            return false;
        }

        Map<QualifiedName, FunctionCall> actualFunctionCalls = windowNode.getWindowFunctions().values().stream()
                .map(WindowNode.Function::getFunctionCall)
                .collect(toImmutableMap(FunctionCall::getName));

        if (actualFunctionCalls.size() != expectedFunctionCalls.size()) {
            return false;
        }

        if (!expectedFunctionCalls.keySet().containsAll(actualFunctionCalls.keySet())) {
            return false;
        }

        for (QualifiedName functionName : actualFunctionCalls.keySet()) {
            FunctionCall actual = actualFunctionCalls.get(functionName);
            Window actualWindow = actual.getWindow().get();
            ExpectedFunctionCall expected = expectedFunctionCalls.get(functionName);

            if (actualWindow.getFrame().isPresent()) {
                if (!expected.getFrame()
                        .map(expectedFrame -> expectedFrame.equals(actualWindow.getFrame().get()))
                        .orElse(false)) {
                    return false;
                }
            }

            if (!areAliasesMatch(actual.getArguments(), expected.getArgumentAliases(), expressionAliases)) {
                return false;
            }
        }

        return true;
    }

    private static boolean samePartitionAndOrder(
            ExpectedWindowNodeSpecification expected,
            WindowNode.Specification actual,
            ExpressionAliases expressionAliases)
    {
        if (!areAliasesMatch(actual.getPartitionBy(), expected.getPartitionByAliases(), expressionAliases)) {
            return false;
        }

        if (!areAliasesMatch(actual.getOrderBy(), expected.getOrderByAliases(), expressionAliases)) {
            return false;
        }

        Map<String, SortOrder> expectedOrderings = expected.getOrderings();
        Map<Symbol, SortOrder> actualOrderings = actual.getOrderings();

        if (expectedOrderings.size() != actualOrderings.size()) {
            return false;
        }

        checkState(expectedOrderings.size() == expected.getOrderByAliases().size());
        checkState(actualOrderings.size() == actual.getOrderBy().size());

        for (int i = 0; i < actualOrderings.size(); i++) {
            SortOrder actualSortOrder = actualOrderings.get(requireNonNull(actual.getOrderBy().get(i)));
            SortOrder expectedSortOrder = expectedOrderings.get(requireNonNull(expected.getOrderByAliases().get(i)));
            if (actualSortOrder != expectedSortOrder) {
                return false;
            }
        }

        return true;
    }

    private static boolean areAliasesMatch(
            List actualsSymbolOrExpressions,
            List<String> expectedAliases,
            ExpressionAliases expressionAliases)
    {
        if (expectedAliases.size() != actualsSymbolOrExpressions.size()) {
            return false;
        }
        for (int i = 0; i < expectedAliases.size(); i++) {
            Object actual = actualsSymbolOrExpressions.get(i);
            if (actual instanceof Symbol) {
                expressionAliases.put(expectedAliases.get(i), ((Symbol) actual).toSymbolReference());
            }
            else if (actual instanceof Expression) {
                expressionAliases.put(expectedAliases.get(i), ((Expression) actual));
            }
            else {
                throw new IllegalStateException("Unknown actual object type: " + actual.getClass());
            }
        }
        return true;
    }

    @Override
    public String toString()
    {
        return expectedFunctionCalls.values().toString() + " " + specification;
    }
}
