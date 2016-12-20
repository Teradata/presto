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
package com.facebook.presto.sql.analyzer;

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import io.airlift.units.DataSize;
import org.testng.annotations.Test;

import java.nio.file.Paths;
import java.util.Map;

import static com.facebook.presto.sql.analyzer.FeaturesConfig.ProcessingOptimization.COLUMNAR_DICTIONARY;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.ProcessingOptimization.DISABLED;
import static com.facebook.presto.sql.analyzer.RegexLibrary.JONI;
import static com.facebook.presto.sql.analyzer.RegexLibrary.RE2J;
import static io.airlift.configuration.testing.ConfigAssertions.assertDeprecatedEquivalence;
import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;

public class TestFeaturesConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(ConfigAssertions.recordDefaults(FeaturesConfig.class)
                .setResourceGroupsEnabled(false)
                .setDistributedIndexJoinsEnabled(false)
                .setJoinDistributionType("repartitioned")
                .setColocatedJoinsEnabled(false)
                .setJoinReorderingEnabled(false)
                .setRedistributeWrites(true)
                .setOptimizeMetadataQueries(false)
                .setOptimizeHashGeneration(true)
                .setOptimizeSingleDistinct(true)
                .setPushTableWriteThroughUnion(true)
                .setProcessingOptimization(DISABLED)
                .setDictionaryAggregation(false)
                .setLegacyArrayAgg(false)
                .setRegexLibrary(JONI)
                .setRe2JDfaStatesLimit(Integer.MAX_VALUE)
                .setRe2JDfaRetries(5)
                .setSpillEnabled(false)
                .setOperatorMemoryLimitBeforeSpill(DataSize.valueOf("4MB"))
                .setSpillerSpillPaths(Paths.get(System.getProperty("java.io.tmpdir"), "presto", "spills").toString())
                .setSpillerThreads(4)
                .setOptimizeMixedDistinctAggregations(false)
                .setReorderWindows(true)
                .setSpillMinimumFreeSpaceThreshold(0.9)
                .setParseDecimalLiteralsAsDouble(false)
                .setSmallTableCoefficient(0.01));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> propertiesLegacy = new ImmutableMap.Builder<String, String>()
                .put("experimental.resource-groups-enabled", "true")
                .put("deprecated.legacy-array-agg", "true")
                .put("distributed-index-joins-enabled", "true")
                .put("join-distribution-type", "replicated")
                .put("colocated-joins-enabled", "true")
                .put("reorder-joins", "true")
                .put("redistribute-writes", "false")
                .put("optimizer.optimize-metadata-queries", "true")
                .put("optimizer.optimize-hash-generation", "false")
                .put("optimizer.optimize-single-distinct", "false")
                .put("optimizer.optimize-mixed-distinct-aggregations", "true")
                .put("optimizer.push-table-write-through-union", "false")
                .put("optimizer.processing-optimization", "columnar_dictionary")
                .put("optimizer.dictionary-aggregation", "true")
                .put("optimizer.reorder-windows", "false")
                .put("regex-library", "RE2J")
                .put("re2j.dfa-states-limit", "42")
                .put("re2j.dfa-retries", "42")
                .put("beta.spill-enabled", "true")
                .put("beta.operator-memory-limit-before-spill", "100MB")
                .put("beta.spiller-spill-path", "/tmp/custom/spill/path1,/tmp/custom/spill/path2")
                .put("beta.spiller-threads", "42")
                .put("beta.spiller-minimum-free-space-threshold", "0.8")
                .put("parse-decimal-literals-as-double", "true")
                .put("small-table-coefficient", "0.2")
                .build();
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("experimental.resource-groups-enabled", "true")
                .put("deprecated.legacy-array-agg", "true")
                .put("distributed-index-joins-enabled", "true")
                .put("join-distribution-type", "replicated")
                .put("colocated-joins-enabled", "true")
                .put("reorder-joins", "true")
                .put("redistribute-writes", "false")
                .put("optimizer.optimize-metadata-queries", "true")
                .put("optimizer.optimize-hash-generation", "false")
                .put("optimizer.optimize-single-distinct", "false")
                .put("optimizer.optimize-mixed-distinct-aggregations", "true")
                .put("optimizer.push-table-write-through-union", "false")
                .put("optimizer.processing-optimization", "columnar_dictionary")
                .put("optimizer.dictionary-aggregation", "true")
                .put("optimizer.reorder-windows", "false")
                .put("regex-library", "RE2J")
                .put("re2j.dfa-states-limit", "42")
                .put("re2j.dfa-retries", "42")
                .put("beta.spill-enabled", "true")
                .put("beta.operator-memory-limit-before-spill", "100MB")
                .put("beta.spiller-spill-path", "/tmp/custom/spill/path1,/tmp/custom/spill/path2")
                .put("beta.spiller-threads", "42")
                .put("beta.spiller-minimum-free-space-threshold", "0.8")
                .put("parse-decimal-literals-as-double", "true")
                .put("small-table-coefficient", "0.2")
                .build();

        FeaturesConfig expected = new FeaturesConfig()
                .setResourceGroupsEnabled(true)
                .setDistributedIndexJoinsEnabled(true)
                .setJoinDistributionType("replicated")
                .setColocatedJoinsEnabled(true)
                .setJoinReorderingEnabled(true)
                .setRedistributeWrites(false)
                .setOptimizeMetadataQueries(true)
                .setOptimizeHashGeneration(false)
                .setOptimizeSingleDistinct(false)
                .setOptimizeMixedDistinctAggregations(true)
                .setPushTableWriteThroughUnion(false)
                .setProcessingOptimization(COLUMNAR_DICTIONARY)
                .setDictionaryAggregation(true)
                .setLegacyArrayAgg(true)
                .setRegexLibrary(RE2J)
                .setRe2JDfaStatesLimit(42)
                .setRe2JDfaRetries(42)
                .setSpillEnabled(true)
                .setOperatorMemoryLimitBeforeSpill(DataSize.valueOf("100MB"))
                .setSpillerSpillPaths("/tmp/custom/spill/path1,/tmp/custom/spill/path2")
                .setSpillerThreads(42)
                .setSpillMinimumFreeSpaceThreshold(0.8)
                .setReorderWindows(false)
                .setParseDecimalLiteralsAsDouble(true)
                .setSmallTableCoefficient(0.2);

        assertFullMapping(properties, expected);
        assertDeprecatedEquivalence(FeaturesConfig.class, properties, propertiesLegacy);
    }
}
