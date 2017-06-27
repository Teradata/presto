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

import java.util.Random;

import static com.facebook.presto.cost.SymbolStatsEstimate.buildFrom;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.NaN;
import static java.lang.Double.POSITIVE_INFINITY;
import static java.lang.Double.max;
import static java.lang.Double.min;

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
                )
                .symbolStats(new Symbol("y"), symbolAssert ->
                        symbolAssert.averageRowSize(4.0)
                                .nullsFraction(0.5)
                                .lowValue(0.0)
                                .highValue(5.0)
                                .distinctValuesCount(20.0)
                );
    }

    @Test
    public void simpleDeMorganTest()
    {
        PlanNodeStatsEstimate leftStats =
                standardInputStatistics.mapOutputRowCount(x -> x * 0.5)
                        .mapSymbolColumnStatistics(new Symbol("x"),
                                x ->
                                        buildFrom(x).setAverageRowSize(4.0)
                                                .setNullsFraction(0.1)
                                                .setDistinctValuesCount(20.0)
                                                .setLowValue(-5.0)
                                                .setHighValue(5.0).build());

        PlanNodeStatsEstimate rightStats =
                standardInputStatistics.mapOutputRowCount(x -> x * 0.25)
                        .mapSymbolColumnStatistics(new Symbol("x"),
                                x ->
                                        buildFrom(x).setAverageRowSize(4.0)
                                                .setNullsFraction(0.1)
                                                .setDistinctValuesCount(10.0)
                                                .setLowValue(-10.0)
                                                .setHighValue(-2.5).build());

        PlanNodeStatsEstimate orStatistics = statsCalculator.unionStats(leftStats, rightStats);
        assertOr(leftStats, rightStats)
                .outputRowsCount(612.5)
                .symbolStats(new Symbol("x"), symbolStatsAssertion ->
                        symbolStatsAssertion.distinctValuesCount(26.6666666)
                                .lowValue(-10.0)
                                .highValue(5.0)
                                .nullsFraction(50 / 612.5));

        PlanNodeStatsEstimate andStatistics = statsCalculator.intersectStats(leftStats, rightStats);
        assertAnd(leftStats, rightStats)
                .outputRowsCount(137.5)
                .symbolStats(new Symbol("x"), symbolStatsAssertion ->
                        symbolStatsAssertion.distinctValuesCount(3.33333333)
                                .lowValue(-5.0)
                                .highValue(-2.5)
                                .nullsFraction(25.0 / 137.5));

        PlanNodeStatsEstimate notLeftStatistics = statsCalculator.negateStats(leftStats);
        assertNot(leftStats)
                .outputRowsCount(500.0)
                .symbolStats(new Symbol("x"), symbolStatsAssertion ->
                        symbolStatsAssertion.distinctValuesCount(20.0)
                                .lowValue(-10.0)
                                .highValue(10.0)
                                .nullsFraction(200.0 / 500.0));

        PlanNodeStatsEstimate notRightStatistics = statsCalculator.negateStats(rightStats);
        assertNot(rightStats)
                .outputRowsCount(750.0)
                .symbolStats(new Symbol("x"), symbolStatsAssertion ->
                        symbolStatsAssertion.distinctValuesCount(30.0)
                                .lowValue(-2.5)
                                .highValue(10.0)
                                .nullsFraction(225.0 / 750.0));

        PlanNodeStatsEstimate orOfNotStatistics = statsCalculator.unionStats(notLeftStatistics, notRightStatistics);
        assertOr(notLeftStatistics, notRightStatistics)
                .outputRowsCount(750.0)
                .symbolStats(new Symbol("x"), symbolStatsAssertion ->
                        symbolStatsAssertion.distinctValuesCount(37.5)
                                .lowValue(-10.0)
                                .highValue(10.0)
                                .nullsFraction(300.0 / 750.0));

        PlanNodeStatsEstimate andOfNotStatistics = statsCalculator.intersectStats(notLeftStatistics, notRightStatistics);

        //assertNot(orStatistics).equalTo(andOfNotStatistics);
        assertNot(andStatistics).equalTo(orOfNotStatistics);
    }

    @Test
    public void randomDeMorganTest()
    {
        double TAU = Math.PI * 2; // Now this code is much better!
        Random rand = new Random((long) (TAU * 100000000.0));

        for (int i = 0; i < 1000; ++i) {
            double lowOrHighLeft1 = rand.nextDouble() * -2.0 + 1.0;
            double lowOrHighLeft2 = rand.nextDouble() * -2.0 + 1.0;
            PlanNodeStatsEstimate leftStats =
                    standardInputStatistics.mapOutputRowCount(x -> x * rand.nextDouble())
                            .mapSymbolColumnStatistics(new Symbol("x"),
                                    x ->
                                            buildFrom(x).setAverageRowSize(rand.nextDouble() * 2.0 * 4.0)
                                                    .setNullsFraction(rand.nextDouble())
                                                    .setDistinctValuesCount(40.0 * rand.nextDouble())
                                                    .setLowValue(min(lowOrHighLeft1, lowOrHighLeft2))
                                                    .setHighValue(max(lowOrHighLeft1, lowOrHighLeft2)).build());

            double lowOrHighRight1 = rand.nextDouble() * -2.0 + 1.0;
            double lowOrHighRight2 = rand.nextDouble() * -2.0 + 1.0;
            PlanNodeStatsEstimate rightStats =
                    standardInputStatistics.mapOutputRowCount(x -> x * rand.nextDouble())
                            .mapSymbolColumnStatistics(new Symbol("x"),
                                    x ->
                                            buildFrom(x).setAverageRowSize(rand.nextDouble() * 2.0 * 4.0)
                                                    .setNullsFraction(rand.nextDouble())
                                                    .setDistinctValuesCount(40.0 * rand.nextDouble())
                                                    .setLowValue(min(lowOrHighRight1, lowOrHighRight2))
                                                    .setHighValue(max(lowOrHighRight1, lowOrHighRight2)).build());

            PlanNodeStatsEstimate orStatistics = statsCalculator.unionStats(leftStats, rightStats);
            PlanNodeStatsEstimate andStatistics = statsCalculator.intersectStats(leftStats, rightStats);
            PlanNodeStatsEstimate notLeftStatistics = statsCalculator.negateStats(leftStats);
            PlanNodeStatsEstimate notRightStatistics = statsCalculator.negateStats(leftStats);
            PlanNodeStatsEstimate orOfNotStatistics = statsCalculator.unionStats(notLeftStatistics, notRightStatistics);
            PlanNodeStatsEstimate andOfNotStatistics = statsCalculator.intersectStats(notLeftStatistics, notRightStatistics);

            assertNot(orStatistics).equalTo(andOfNotStatistics);
            assertNot(andStatistics).equalTo(orOfNotStatistics);
        }
    }
}
