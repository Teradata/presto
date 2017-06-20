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

import static com.facebook.presto.cost.EstimateAssertion.assertEstimateEquals;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.POSITIVE_INFINITY;
import static java.lang.Double.isNaN;
import static org.testng.Assert.assertTrue;

public class SymbolStatsAssertion
{
    private final SymbolStatsEstimate statistics;

    private SymbolStatsAssertion(SymbolStatsEstimate statistics)
    {
        this.statistics = statistics;
    }

    public static SymbolStatsAssertion assertThat(SymbolStatsEstimate actual)
    {
        return new SymbolStatsAssertion(actual);
    }

    public SymbolStatsAssertion nullsFraction(double expected)
    {
        // we bind nullsFraction and nonNullsFraction together
        assertEstimateEquals(statistics.getNullsFraction(), expected, "nullsFraction mismatch");
        return this;
    }

    public SymbolStatsAssertion nullsFractionUnknown()
    {
        // we bind nullsFraction and nonNullsFraction together
        assertTrue(isNaN(statistics.getNullsFraction()), "expected unknown nullsFraction but got " + statistics.getNullsFraction());
        return this;
    }

    public SymbolStatsAssertion lowValue(double expected)
    {
        assertEstimateEquals(statistics.getLowValue(), expected, "lowValue mismatch");
        return this;
    }

    public SymbolStatsAssertion lowValueUnknown()
    {
        return lowValue(NEGATIVE_INFINITY);
    }

    public SymbolStatsAssertion highValue(double expected)
    {
        assertEstimateEquals(statistics.getHighValue(), expected, "highValue mismatch");
        return this;
    }

    public SymbolStatsAssertion highValueUnknown()
    {
        return highValue(POSITIVE_INFINITY);
    }

    public SymbolStatsAssertion distinctValuesCount(double expected)
    {
        assertEstimateEquals(statistics.getDistinctValuesCount(), expected, "distinctValuesCount mismatch");
        return this;
    }

    public SymbolStatsAssertion distinctValuesCountUnknown()
    {
        assertTrue(isNaN(statistics.getDistinctValuesCount()), "expected unknown distinctValuesCount but got " + statistics.getDistinctValuesCount());
        return this;
    }

    public SymbolStatsAssertion dataSize(double expected)
    {
        assertEstimateEquals(statistics.getDataSize(), expected, "dataSize mismatch");
        return this;
    }

    public SymbolStatsAssertion dataSizeUnknown()
    {
        assertTrue(isNaN(statistics.getDataSize()), "expected unknown dataSize but got " + statistics.getDistinctValuesCount());
        return this;
    }

    public SymbolStatsAssertion isEqualTo(SymbolStatsEstimate expected) {
        assertEstimateEquals(statistics.getNullsFraction(), expected.getNullsFraction(), "nullsFraction mismatch");
        assertEstimateEquals(statistics.getLowValue(), expected.getLowValue(), "lowValue mismatch");
        assertEstimateEquals(statistics.getHighValue(), expected.getHighValue(), "highValue mismatch");
        assertEstimateEquals(statistics.getDistinctValuesCount(), expected.getDistinctValuesCount(), "distinct values count mismatch");
        assertEstimateEquals(statistics.getDataSize(), expected.getDataSize(), "datasize mismatch for");
        return this;
    }
}
