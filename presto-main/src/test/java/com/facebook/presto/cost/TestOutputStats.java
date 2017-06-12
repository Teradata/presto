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
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;

public class TestOutputStats
{
    private StatsCalculatorTester tester;

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        tester = new StatsCalculatorTester();
    }

    @Test
    public void testStatsForOutputNode()
            throws Exception
    {
        PlanNodeStatsEstimate stats = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(Estimate.of(100))
                .addSymbolStatistics(
                        new Symbol("a"),
                        ColumnStatistics.builder()
                                .setNullsFraction(Estimate.of(0.3))
                                .addRange(rb -> rb
                                        .setLowValue(Optional.of(1L))
                                        .setHighValue(Optional.of(10L))
                                        .setFraction(Estimate.of(0.7))
                                        .setDistinctValuesCount(Estimate.of(20)))
                                .build())
                .addSymbolStatistics(
                        new Symbol("b"),
                        ColumnStatistics.builder()
                                .setNullsFraction(Estimate.of(0.6))
                                .addRange(rb -> rb
                                        .setLowValue(Optional.of(13.5))
                                        .setHighValue(Optional.of(18989.0))
                                        .setFraction(Estimate.of(0.4))
                                        .setDistinctValuesCount(Estimate.of(40)))
                                .build())
                .build();

        tester.assertStatsFor(pb -> pb
                .output(outputBuilder -> {
                    Symbol a = pb.symbol("a", BIGINT);
                    Symbol b = pb.symbol("b", DOUBLE);
                    outputBuilder
                            .source(pb.values(a, b))
                            .column(a, "a1")
                            .column(a, "a2")
                            .column(b, "b");
                }))
                .withSourceStats(stats)
                .check(outputStats -> outputStats.equalTo(stats));
    }
}
