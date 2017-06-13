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
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.DecimalLiteral;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.presto.cost.PlanNodeStatsEstimate.UNKNOWN_STATS;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static io.airlift.slice.Slices.utf8Slice;
import static java.util.Collections.emptyMap;

public class TestScalarStatsCalculator
{
    private ScalarStatsCalculator calculator;
    private Session session;

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        calculator = new ScalarStatsCalculator(MetadataManager.createTestMetadataManager());
        session = testSessionBuilder().build();
    }

    private ColumnStatisticsAssertion assertCalculate(Expression scalarExpression)
    {
        return assertCalculate(scalarExpression, UNKNOWN_STATS);
    }

    private ColumnStatisticsAssertion assertCalculate(Expression scalarExpression, PlanNodeStatsEstimate inputStatistics)
    {
        return assertCalculate(scalarExpression, inputStatistics, emptyMap());
    }

    private ColumnStatisticsAssertion assertCalculate(Expression scalarExpression, PlanNodeStatsEstimate inputStatistics, Map<Symbol, Type> types)
    {
        return ColumnStatisticsAssertion.assertThat(calculator.calculate(scalarExpression, inputStatistics, session, types));
    }

    @Test
    public void testLiteral()
    {
        assertCalculate(new DoubleLiteral("7.5"))
                .distinctValuesCount(1.0)
                .lowValue(7.5)
                .highValue(7.5)
                .nullsFraction(0.0);

        assertCalculate(new DecimalLiteral("7.5"))
                .distinctValuesCount(1.0)
                .lowValue(75L)
                .highValue(75L)
                .nullsFraction(0.0);

        assertCalculate(new StringLiteral("blah"))
                .distinctValuesCount(1.0)
                .lowValue(utf8Slice("blah"))
                .highValue(utf8Slice("blah"))
                .nullsFraction(0.0);

        assertCalculate(new NullLiteral())
                .distinctValuesCount(0.0)
                .lowValueUnknown()
                .highValueUnknown()
                .nullsFraction(1.0);
    }

    @Test
    public void testCastDoubleToBigint()
    {
        Map<Symbol, Type> types = ImmutableMap.of(new Symbol("a"), DOUBLE);

        PlanNodeStatsEstimate inputStatistics = PlanNodeStatsEstimate.builder()
                .addSymbolStatistics(new Symbol("a"), symbolStatistics -> symbolStatistics
                        .setNullsFraction(Estimate.of(0.3))
                        .addRange(1.6, 17.3, rangeStatistics -> rangeStatistics
                                .setFraction(Estimate.of(0.7))
                                .setDistinctValuesCount(Estimate.of(4))
                                .setDataSize(Estimate.of(5))))
                .build();

        assertCalculate(new Cast(new SymbolReference("a"), "bigint"), inputStatistics, types)
                .lowValue(2L)
                .highValue(17L)
                .distinctValuesCount(4)
                .nullsFraction(0.3)
                .dataSizeUnknown();
    }

    @Test
    public void testCastUnknown()
    {
        Map<Symbol, Type> types = ImmutableMap.of(new Symbol("a"), DOUBLE);
        assertCalculate(new Cast(new SymbolReference("a"), "bigint"), UNKNOWN_STATS, types)
                .lowValueUnknown()
                .highValueUnknown()
                .distinctValuesCountUnknown()
                .nullsFractionUnknown()
                .dataSizeUnknown();
    }
}
