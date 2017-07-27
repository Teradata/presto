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

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.sql.ExpressionUtils.rewriteIdentifiersToSymbolReferences;
import static org.testng.AssertJUnit.assertEquals;

public class TestSortExpressionExtractor
{
    private static final Set<Symbol> BUILD_SYMBOLS = ImmutableSet.of(new Symbol("b1"), new Symbol("b2"));

    @Test
    public void testGetSortExpression()
    {
        assertGetSortExpression(expression("p1 > b1"), "b1");

        assertGetSortExpression(expression("b2 <= p1"), "b2");

        assertGetSortExpression(expression("b2 > p1"), "b2");

        assertGetSortExpression(expression("b2 > sin(p1)"), "b2");

        assertGetSortExpression(expression("b2 > random(p1)"));

        assertGetSortExpression(expression("b1 > p1 + b2"));

        assertGetSortExpression(expression("sin(b1) > p1"));

        assertGetSortExpression(expression("b1 <= p1 OR b2 <= p1"));

        assertGetSortExpression(expression("sin(b2) > p1 AND (b2 <= p1 OR b2 <= p1 + 10)"));

        assertGetSortExpression(expression("sin(b2) > p1 AND (b2 <= p1 AND b2 <= p1 + 10)"));

        assertGetSortExpression(expression("b1 > p1 AND b1 <= p1"), "b1");
    }

    private Expression expression(String sql)
    {
        return rewriteIdentifiersToSymbolReferences(new SqlParser().createExpression(sql));
    }

    private static void assertGetSortExpression(Expression expression)
    {
        Optional<Expression> actual = SortExpressionExtractor.extractSortExpression(BUILD_SYMBOLS, expression);
        assertEquals(Optional.empty(), actual);
    }

    private static void assertGetSortExpression(Expression expression, String expectedSymbol)
    {
        Optional<Expression> expected = Optional.of(new SymbolReference(expectedSymbol));
        Optional<Expression> actual = SortExpressionExtractor.extractSortExpression(BUILD_SYMBOLS, expression);
        assertEquals(expected, actual);
    }
}
