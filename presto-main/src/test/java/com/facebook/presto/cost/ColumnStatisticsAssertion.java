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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class ColumnStatisticsAssertion
{
    private final ColumnStatistics statistics;

    private ColumnStatisticsAssertion(ColumnStatistics statistics)
    {
        this.statistics = statistics;
    }

    public static ColumnStatisticsAssertion assertThat(ColumnStatistics actual)
    {
        return new ColumnStatisticsAssertion(actual);
    }

    public ColumnStatisticsAssertion nullsFraction(double expected)
    {
        // we bind nullsFraction and nonNullsFraction together
        assertEquals(statistics.getNullsFraction().getValue(), expected, "nullsFraction mismatch");
        assertEquals(statistics.getOnlyRangeColumnStatistics().getFraction().getValue(), 1.0 - expected, "nonNullsFraction mismatch");
        return this;
    }

    public ColumnStatisticsAssertion nullsFractionUnknown()
    {
        // we bind nullsFraction and nonNullsFraction together
        assertTrue(statistics.getNullsFraction().isValueUnknown(),
                "expected unknown nullsFraction but got " + statistics.getNullsFraction().getValue());
        assertTrue(statistics.getOnlyRangeColumnStatistics().getFraction().isValueUnknown(),
                "expected unknown nonNullsFraction but got " + statistics.getNullsFraction().getValue());
        return this;
    }

    public ColumnStatisticsAssertion lowValue(Object expected)
    {
        assertTrue(statistics.getOnlyRangeColumnStatistics().getLowValue().isPresent(), "lowValue unknown");
        assertEquals(statistics.getOnlyRangeColumnStatistics().getLowValue().get(), expected, "lowValue mismatch");
        return this;
    }

    public ColumnStatisticsAssertion lowValueUnknown()
    {
        assertFalse(statistics.getOnlyRangeColumnStatistics().getLowValue().isPresent(),
                "expected unknown lowValue but got " + statistics.getOnlyRangeColumnStatistics().getLowValue());
        return this;
    }

    public ColumnStatisticsAssertion highValue(Object expected)
    {
        assertTrue(statistics.getOnlyRangeColumnStatistics().getHighValue().isPresent(), "highValue unknown");
        assertEquals(statistics.getOnlyRangeColumnStatistics().getHighValue().get(), expected, "highValue mismatch");
        return this;
    }

    public ColumnStatisticsAssertion highValueUnknown()
    {
        assertFalse(statistics.getOnlyRangeColumnStatistics().getHighValue().isPresent(),
                "expected unknown highValue but got " + statistics.getOnlyRangeColumnStatistics().getHighValue());
        return this;
    }

    public ColumnStatisticsAssertion distinctValuesCount(double expected)
    {
        assertEquals(statistics.getOnlyRangeColumnStatistics().getDistinctValuesCount().getValue(), expected, "distinctValuesCount mismatch");
        return this;
    }

    public ColumnStatisticsAssertion distinctValuesCountUnknown()
    {
        assertTrue(statistics.getOnlyRangeColumnStatistics().getDistinctValuesCount().isValueUnknown(),
                "expected unknown distinctValuesCount but got " + statistics.getOnlyRangeColumnStatistics().getDistinctValuesCount().getValue());
        return this;
    }

    public ColumnStatisticsAssertion dataSize(double expected)
    {
        assertEquals(statistics.getOnlyRangeColumnStatistics().getDataSize().getValue(), expected, "dataSize mismatch");
        return this;
    }

    public ColumnStatisticsAssertion dataSizeUnknown()
    {
        assertTrue(statistics.getOnlyRangeColumnStatistics().getDataSize().isValueUnknown(),
                "expected unknown dataSize but got " + statistics.getOnlyRangeColumnStatistics().getDataSize().getValue());
        return this;
    }
}
