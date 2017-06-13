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

import com.facebook.presto.spi.statistics.ColumnStatistics;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.sql.planner.Symbol;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.expression;

public class TestValuesNodeStats
{
    private StatsCalculatorTester tester;

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        tester = new StatsCalculatorTester();
    }

    @Test
    public void testStatsForValuesNode()
            throws Exception
    {
        PlanNodeStatsEstimate stats = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(Estimate.of(3))
                .addSymbolStatistics(
                        new Symbol("a"),
                        ColumnStatistics.builder()
                                .setNullsFraction(Estimate.of(0.0))
                                .addRange(rb -> rb
                                        .setLowValue(Optional.of(6L))
                                        .setHighValue(Optional.of(55L))
                                        .setFraction(Estimate.of(1.0))
                                        .setDistinctValuesCount(Estimate.of(2)))
                                .build())
                .addSymbolStatistics(
                        new Symbol("b"),
                        ColumnStatistics.builder()
                                .setNullsFraction(Estimate.of(0.33333333333333333))
                                .addRange(rb -> rb
                                        .setLowValue(Optional.of(13.5))
                                        .setHighValue(Optional.of(13.5))
                                        .setFraction(Estimate.of(0.66666666666666666))
                                        .setDistinctValuesCount(Estimate.of(1)))
                                .build())
                .build();

        tester.assertStatsFor(pb -> pb
                .values(ImmutableList.of(pb.symbol("a", BIGINT), pb.symbol("b", DOUBLE)),
                        ImmutableList.of(
                                ImmutableList.of(expression("3+3"), expression("13.5")),
                                ImmutableList.of(expression("55"), expression("null")),
                                ImmutableList.of(expression("6"), expression("13.5")))))
                .check(outputStats -> outputStats.equalTo(stats));
    }
}
