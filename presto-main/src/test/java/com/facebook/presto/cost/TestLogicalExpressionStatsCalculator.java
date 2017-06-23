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

import com.facebook.presto.sql.planner.Symbol;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.facebook.presto.cost.SymbolStatsEstimate.buildFrom;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.NaN;
import static java.lang.Double.POSITIVE_INFINITY;

@Test(singleThreaded = true)
public class TestLogicalExpressionStatsCalculator
{
    private LogicalExpressionStatsCalculator statsCalculator;
    PlanNodeStatsEstimate standardInputStatistics;

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        SymbolStatsEstimate xStats = SymbolStatsEstimate.builder()
                .setAverageRowSize(4.0)
                .setDistinctValuesCount(40.0)
                .setLowValue(-10.0)
                .setHighValue(10.0)
                .setNullsFraction(0.25)
                .build();
        SymbolStatsEstimate yStats = SymbolStatsEstimate.builder()
                .setAverageRowSize(4.0)
                .setDistinctValuesCount(20.0)
                .setLowValue(0.0)
                .setHighValue(5.0)
                .setNullsFraction(0.5)
                .build();
        SymbolStatsEstimate zStats = SymbolStatsEstimate.builder()
                .setAverageRowSize(4.0)
                .setDistinctValuesCount(5.0)
                .setLowValue(-100.0)
                .setHighValue(100.0)
                .setNullsFraction(0.1)
                .build();
        SymbolStatsEstimate leftOpenStats = SymbolStatsEstimate.builder()
                .setAverageRowSize(4.0)
                .setDistinctValuesCount(50.0)
                .setLowValue(NEGATIVE_INFINITY)
                .setHighValue(15.0)
                .setNullsFraction(0.1)
                .build();
        SymbolStatsEstimate rightOpenStats = SymbolStatsEstimate.builder()
                .setAverageRowSize(4.0)
                .setDistinctValuesCount(50.0)
                .setLowValue(-15.0)
                .setHighValue(POSITIVE_INFINITY)
                .setNullsFraction(0.1)
                .build();
        SymbolStatsEstimate unknownRangeStats = SymbolStatsEstimate.builder()
                .setAverageRowSize(4.0)
                .setDistinctValuesCount(50.0)
                .setLowValue(NEGATIVE_INFINITY)
                .setHighValue(POSITIVE_INFINITY)
                .setNullsFraction(0.1)
                .build();
        SymbolStatsEstimate emptyRangeStats = SymbolStatsEstimate.builder()
                .setAverageRowSize(4.0)
                .setDistinctValuesCount(0.0)
                .setLowValue(NaN)
                .setHighValue(NaN)
                .setNullsFraction(NaN)
                .build();
        standardInputStatistics = PlanNodeStatsEstimate.builder()
                .addSymbolStatistics(new Symbol("x"), xStats)
                .addSymbolStatistics(new Symbol("y"), yStats)
                .addSymbolStatistics(new Symbol("z"), zStats)
                .addSymbolStatistics(new Symbol("leftOpen"), leftOpenStats)
                .addSymbolStatistics(new Symbol("rightOpen"), rightOpenStats)
                .addSymbolStatistics(new Symbol("unknownRange"), unknownRangeStats)
                .addSymbolStatistics(new Symbol("emptyRange"), emptyRangeStats)
                .setOutputRowCount(1000.0)
                .build();

        statsCalculator = new LogicalExpressionStatsCalculator(standardInputStatistics);
    }

    private PlanNodeStatsAssertion assertOr(PlanNodeStatsEstimate left, PlanNodeStatsEstimate right)
    {
        return PlanNodeStatsAssertion.assertThat(statsCalculator.unionStats(left, right));
    }

    private PlanNodeStatsAssertion assertAnd(PlanNodeStatsEstimate left, PlanNodeStatsEstimate right)
    {
        return PlanNodeStatsAssertion.assertThat(statsCalculator.intersectStats(left, right));
    }

    private PlanNodeStatsAssertion assertNot(PlanNodeStatsEstimate inner)
    {
        return PlanNodeStatsAssertion.assertThat(statsCalculator.negateStats(inner));
    }

    @Test
    public void testSimpleORCalculation()
    {
        PlanNodeStatsEstimate leftStats =
                standardInputStatistics.mapOutputRowCount(x -> x * 0.5)
                    .mapSymbolColumnStatistics(new Symbol("x"),
                            x ->
                                    buildFrom(x).setAverageRowSize(4.0)
                                            .setNullsFraction(0.0)
                                            .setDistinctValuesCount(20.0)
                                            .setLowValue(-5.0)
                                            .setHighValue(5.0).build());
        PlanNodeStatsEstimate rightStats =
                standardInputStatistics.mapOutputRowCount(x -> x * 0.25)
                        .mapSymbolColumnStatistics(new Symbol("x"),
                                x ->
                                        buildFrom(x).setAverageRowSize(4.0)
                                                .setNullsFraction(0.0)
                                                .setDistinctValuesCount(10.0)
                                                .setLowValue(-5.0)
                                                .setHighValue(0.0).build());
        assertOr(leftStats, rightStats)
                .outputRowsCount(500)
                .symbolStats(new Symbol("x"), symbolAssert ->
                    symbolAssert.averageRowSize(4.0)
                        .lowValue(-5.0)
                        .highValue(5.0)
                        .distinctValuesCount(20.0)
                        .nullsFraction(0.0)
                );
    }

    @Test
    public void testSimpleANDCalculation()
    {
        PlanNodeStatsEstimate leftStats =
                standardInputStatistics.mapOutputRowCount(x -> x * 0.5)
                        .mapSymbolColumnStatistics(new Symbol("x"),
                                x ->
                                        buildFrom(x).setAverageRowSize(4.0)
                                                .setNullsFraction(0.0)
                                                .setDistinctValuesCount(20.0)
                                                .setLowValue(-5.0)
                                                .setHighValue(5.0).build());
        PlanNodeStatsEstimate rightStats =
                standardInputStatistics.mapOutputRowCount(x -> x * 0.25)
                        .mapSymbolColumnStatistics(new Symbol("x"),
                                x ->
                                        buildFrom(x).setAverageRowSize(4.0)
                                                .setNullsFraction(0.0)
                                                .setDistinctValuesCount(10.0)
                                                .setLowValue(-5.0)
                                                .setHighValue(0.0).build());
        assertAnd(leftStats, rightStats)
                .outputRowsCount(250)
                .symbolStats(new Symbol("x"), symbolAssert ->
                        symbolAssert.averageRowSize(4.0)
                                .lowValue(-5.0)
                                .highValue(0.0)
                                .distinctValuesCount(10.0)
                                .nullsFraction(0.0)
                );
    }

    @Test
    public void testSimpleNOTCalculation()
    {
        PlanNodeStatsEstimate inner =
                standardInputStatistics.mapOutputRowCount(x -> 400.0)
                        .mapSymbolColumnStatistics(new Symbol("x"),
                                x ->
                                        buildFrom(x).setAverageRowSize(4.0)
                                                .setNullsFraction(0.1)
                                                .setDistinctValuesCount(10.0)
                                                .setLowValue(-5.0)
                                                .setHighValue(5.0).build());

        assertNot(inner)
                .outputRowsCount(600)
                .symbolStats(new Symbol("x"), symbolAssert ->
                        symbolAssert.averageRowSize(4.0)
                                .lowValue(-10.0)
                                .highValue(10.0)
                                .distinctValuesCount(30.0)
                                .nullsFraction((250.0 - 40.0) / 600.0) // 40 nulls in INNER and universe has 250 nulls. 600 is new row count
                );
    }

    @Test
    public void randomDeMorganTest()
    {

    }
}
