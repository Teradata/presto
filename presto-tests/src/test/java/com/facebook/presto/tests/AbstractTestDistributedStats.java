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

import com.facebook.presto.execution.QueryPlan;
import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.tests.statistics.Metric;
import com.facebook.presto.tests.statistics.MetricComparator;
import com.facebook.presto.tests.statistics.MetricComparison;
import com.facebook.presto.tpch.ColumnNaming;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import static com.facebook.presto.tests.tpch.TpchQueryRunner.createQueryRunnerWithoutCatalogs;
import static java.util.Collections.emptyMap;

public class AbstractTestDistributedStats
{
    private final DistributedQueryRunner runner;

    public AbstractTestDistributedStats()
            throws Exception
    {
        runner = createQueryRunnerWithoutCatalogs(emptyMap(), emptyMap());
        runner.createCatalog("tpch", "tpch", ImmutableMap.of(
                "tpch.column-naming", ColumnNaming.STANDARD.name()
        ));
    }

    protected String getTpchQuery(int i)
    {
        try {
            String queryClassPath = "/io/airlift/tpch/queries/q" + i + ".sql";
            return Resources.toString(getClass().getResource(queryClassPath), Charset.defaultCharset());
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    protected MetricComparison compareMetric(String query)
    {
        String queryId = executeQuery(query);

        QueryPlan queryPlan = getQueryPlan(queryId);

        return getRootOutputRowCountComparison(queryId, queryPlan);
    }

    protected MetricComparison getRootOutputRowCountComparison(String queryId, QueryPlan queryPlan)
    {
        List<MetricComparison> comparisons = new MetricComparator().getMetricComparisons(queryPlan, getOutputStageInfo(queryId));
        return comparisons.stream()
                .filter(comparison -> comparison.getMetric().equals(Metric.OUTPUT_ROW_COUNT))
                .filter(comparison -> comparison.getPlanNode().equals(queryPlan.getPlan().getRoot()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("No comparison for root node found"));
    }

    protected QueryPlan getQueryPlan(String queryId)
    {
        return runner.getQueryPlan(new QueryId(queryId));
    }

    protected String executeQuery(String query)
    {
        return runner.executeWithQueryId(runner.getDefaultSession(), query).getQueryId();
    }

    StageInfo getOutputStageInfo(String queryId)
    {
        return runner.getQueryInfo(new QueryId(queryId)).getOutputStage().get();
    }
}
