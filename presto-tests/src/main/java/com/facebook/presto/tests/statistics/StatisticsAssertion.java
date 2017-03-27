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

package com.facebook.presto.tests.statistics;

import com.facebook.presto.execution.QueryPlan;
import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableList;
import org.intellij.lang.annotations.Language;

import java.util.List;
import java.util.function.Predicate;

import static com.facebook.presto.sql.planner.optimizations.Predicates.alwaysTrue;
import static com.facebook.presto.tests.statistics.MetricComparison.Result.MATCH;
import static com.facebook.presto.tests.statistics.MetricComparison.Result.NO_BASELINE;
import static com.facebook.presto.tests.statistics.MetricComparison.Result.NO_ESTIMATE;
import static com.facebook.presto.tests.statistics.MetricComparisonStrategies.defaultTolerance;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;

public class StatisticsAssertion
{
    private final DistributedQueryRunner runner;

    public StatisticsAssertion(DistributedQueryRunner runner)
    {
        this.runner = requireNonNull(runner, "runner is null");
    }

    public Result check(@Language("SQL") String query)
    {
        return new Result(metricComparisons(query));
    }

    public List<MetricComparison> metricComparisons(@Language("SQL") String query)
    {
        String queryId = runner.executeWithQueryId(runner.getDefaultSession(), query).getQueryId();
        QueryPlan queryPlan = runner.getQueryPlan(new QueryId(queryId));

        StageInfo stageInfo = runner.getQueryInfo(new QueryId(queryId)).getOutputStage().get();
        return new MetricComparator().getMetricComparisons(queryPlan, stageInfo);
    }

    public static class Result
    {
        public static final Predicate<PlanNode> IS_OUTPUT_NODE = OutputNode.class::isInstance;

        private final List<MetricComparison> metricComparisons;

        public Result(List<MetricComparison> metricComparisons)
        {
            checkArgument(!metricComparisons.isEmpty(), "No metric comparisons given");
            this.metricComparisons = ImmutableList.copyOf(metricComparisons);
        }

        public Result matches()
        {
            return match(alwaysTrue(), Metric.OUTPUT_ROW_COUNT, defaultTolerance());
        }

        public Result outputHas(Metric metric, MetricComparisonStrategy strategy)
        {
            return match(IS_OUTPUT_NODE, metric, strategy);
        }

        private Result match(Predicate<PlanNode> planNodeFilter, Metric metric, MetricComparisonStrategy strategy)
        {
            return testMetrics(planNodeFilter, metric, metricComparison -> metricComparison.result(strategy) == MATCH);
        }

        private Result testMetrics(Predicate<PlanNode> planNodeFilter, Metric metric, Predicate<MetricComparison> assertCondition)
        {
            List<MetricComparison> differingMetricComparisons = metricComparisons.stream()
                    .filter(metricComparison -> metricComparison.getMetric() == metric)
                    .filter(metricComparison -> planNodeFilter.test(metricComparison.getPlanNode()))
                    .filter(assertCondition.negate())
                    .collect(toList());

            assertEquals(
                    differingMetricComparisons.size(),
                    0,
                    String.format("Following metrics do not match: " + differingMetricComparisons));
            return this;
        }

        public Result outputHasNoEstimate(Metric metric)
        {
            // TODO: remove passing strategy here
            return testMetrics(IS_OUTPUT_NODE, metric, metricComparison -> metricComparison.result(defaultTolerance()) == NO_ESTIMATE);
        }

        public Result outputHasNoBaseline(Metric metric)
        {
            // TODO: remove passing strategy here
            return testMetrics(IS_OUTPUT_NODE, metric, metricComparison -> metricComparison.result(defaultTolerance()) == NO_BASELINE);
        }
    }
}
