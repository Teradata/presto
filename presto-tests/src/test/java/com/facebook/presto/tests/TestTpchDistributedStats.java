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
package com.facebook.presto.tests;

import com.facebook.presto.tests.statistics.MetricComparison;
import org.testng.annotations.Test;

import static com.facebook.presto.tests.statistics.MetricComparison.Result.DIFFER;
import static com.facebook.presto.tests.statistics.MetricComparison.Result.MATCH;
import static com.facebook.presto.tests.statistics.MetricComparison.Result.NO_ESTIMATE;
import static org.testng.Assert.assertEquals;

public class TestTpchDistributedStats
        extends AbstractTestDistributedStats
{
    public TestTpchDistributedStats()
            throws Exception
    {}

    @Test
    void testTableScanStats()
    {
        assertEquals(compareMetric("SELECT * FROM nation").result(), MATCH);
        assertEquals(compareMetric("SELECT * FROM part").result(), MATCH);
        assertEquals(compareMetric("SELECT * FROM partsupp").result(), MATCH);
        assertEquals(compareMetric("SELECT * FROM region").result(), MATCH);
        assertEquals(compareMetric("SELECT * FROM supplier").result(), MATCH);
        assertEquals(compareMetric("SELECT * FROM lineitem").result(), MATCH);
        assertEquals(compareMetric("SELECT * FROM lineitem").result(), MATCH);
    }

    @Test
    void testFilter()
    {
        String query = "" +
                "SELECT * " +
                "FROM lineitem " +
                "WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY";
        MetricComparison metricComparison = compareMetric(query);
        assertEquals(metricComparison.result(), DIFFER);
        assertEquals(metricComparison.getEstimate().get(), 30000, 1);
    }

    @Test
    void testJoin()
    {
        String query = "SELECT * FROM  part, partsupp WHERE p_partkey = ps_partkey";
        MetricComparison metricComparison = compareMetric(query);
        assertEquals(metricComparison.result(), DIFFER);
        assertEquals(metricComparison.getEstimate().get(), 16000, 1);
    }

    @Test
    void testUnion()
    {
        String query = "SELECT * FROM nation UNION SELECT * FROM nation";
        MetricComparison metricComparison = compareMetric(query);
        assertEquals(metricComparison.result(), NO_ESTIMATE);
    }

    @Test
    void testIntersect()
    {
        String query = "SELECT * FROM nation INTERSECT SELECT * FROM nation";
        MetricComparison metricComparison = compareMetric(query);
        assertEquals(metricComparison.result(), NO_ESTIMATE);
    }

    @Test
    void testExcept()
    {
        String query = "SELECT * FROM nation EXCEPT SELECT * FROM nation";
        MetricComparison metricComparison = compareMetric(query);
        assertEquals(metricComparison.result(), NO_ESTIMATE);
    }

    @Test
    void testEnforceSingleRow()
    {
        String query = "SELECT (SELECT max(n_regionkey) FROM nation)";
        MetricComparison metricComparison = compareMetric(query);
        assertEquals(metricComparison.result(), MATCH);
    }

    @Test
    void testValues()
    {
        String query = "VALUES 1";
        MetricComparison metricComparison = compareMetric(query);
        assertEquals(metricComparison.result(), MATCH);
    }

    @Test
    void testSemiJoin()
    {
        String query = "SELECT * FROM nation WHERE n_regionkey IN (SELECT r_regionkey FROM region)";
        MetricComparison metricComparison = compareMetric(query);
        assertEquals(metricComparison.result(), MATCH);
    }

    @Test
    void testUselessLimit()
    {
        String query = "SELECT * FROM nation LIMIT 30";
        MetricComparison metricComparison = compareMetric(query);
        assertEquals(metricComparison.result(), MATCH);
    }

    @Test
    void testLimit()
    {
        String query = "SELECT * FROM nation LIMIT 10";
        MetricComparison metricComparison = compareMetric(query);
        assertEquals(metricComparison.result(), MATCH);
        assertEquals(metricComparison.getEstimate().get(), 10, 1);
    }

    @Test
    void testGroupBy()
    {
        String query = "" +
                "SELECT l_returnflag, l_linestatus " +
                "FROM lineitem " +
                "GROUP BY l_returnflag, l_linestatus";
        assertEquals(compareMetric(query).result(), NO_ESTIMATE);
    }
}
