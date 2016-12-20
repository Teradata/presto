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

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.DefunctConfig;
import io.airlift.units.DataSize;

import javax.validation.constraints.Min;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static com.facebook.presto.sql.analyzer.RegexLibrary.JONI;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;

@DefunctConfig({
        "resource-group-manager",
        "experimental-syntax-enabled",
        "analyzer.experimental-syntax-enabled"
})
public class FeaturesConfig
{
    public static class ProcessingOptimization
    {
        public static final String DISABLED = "disabled";
        public static final String COLUMNAR = "columnar";
        public static final String COLUMNAR_DICTIONARY = "columnar_dictionary";

        public static final List<String> AVAILABLE_OPTIONS = ImmutableList.of(DISABLED, COLUMNAR, COLUMNAR_DICTIONARY);
    }

    public static class JoinDistributionType
    {
        public static final String AUTOMATIC = "automatic";
        public static final String REPLICATED = "replicated";
        public static final String REPARTITIONED = "repartitioned";

        public static final List<String> AVAILABLE_OPTIONS = ImmutableList.of(AUTOMATIC, REPLICATED, REPARTITIONED);
    }

    private boolean distributedIndexJoinsEnabled;
    private boolean colocatedJoinsEnabled;
    private boolean reorderJoins;
    private boolean redistributeWrites = true;
    private boolean optimizeMetadataQueries;
    private boolean optimizeHashGeneration = true;
    private boolean optimizeSingleDistinct = true;
    private boolean optimizerReorderWindows = true;
    private boolean pushTableWriteThroughUnion = true;
    private boolean legacyArrayAgg;
    private boolean optimizeMixedDistinctAggregations;
    private boolean joinReorderingEnabled = false;
    private String joinDistributionType = JoinDistributionType.REPARTITIONED;
    private double smallTableCoefficient = 0.01;

    private String processingOptimization = ProcessingOptimization.DISABLED;
    private boolean dictionaryAggregation;
    private boolean resourceGroups;

    private int re2JDfaStatesLimit = Integer.MAX_VALUE;
    private int re2JDfaRetries = 5;
    private RegexLibrary regexLibrary = JONI;
    private boolean spillEnabled;
    private DataSize operatorMemoryLimitBeforeSpill = new DataSize(4, DataSize.Unit.MEGABYTE);
    private List<Path> spillerSpillPaths = ImmutableList.of(Paths.get(System.getProperty("java.io.tmpdir"), "presto", "spills"));
    private int spillerThreads = 4;
    private double spillMinimumFreeSpaceThreshold = 0.9;
    private boolean parseDecimalLiteralsAsDouble;

    public boolean isResourceGroupsEnabled()
    {
        return resourceGroups;
    }

    @Config("experimental.resource-groups-enabled")
    public FeaturesConfig setResourceGroupsEnabled(boolean enabled)
    {
        resourceGroups = enabled;
        return this;
    }

    public boolean isDistributedIndexJoinsEnabled()
    {
        return distributedIndexJoinsEnabled;
    }

    @Config("distributed-index-joins-enabled")
    public FeaturesConfig setDistributedIndexJoinsEnabled(boolean distributedIndexJoinsEnabled)
    {
        this.distributedIndexJoinsEnabled = distributedIndexJoinsEnabled;
        return this;
    }

    @Config("deprecated.legacy-array-agg")
    public FeaturesConfig setLegacyArrayAgg(boolean legacyArrayAgg)
    {
        this.legacyArrayAgg = legacyArrayAgg;
        return this;
    }

    public boolean isLegacyArrayAgg()
    {
        return legacyArrayAgg;
    }

    public boolean isColocatedJoinsEnabled()
    {
        return colocatedJoinsEnabled;
    }

    @Config("colocated-joins-enabled")
    @ConfigDescription("Experimental: Use a colocated join when possible")
    public FeaturesConfig setColocatedJoinsEnabled(boolean colocatedJoinsEnabled)
    {
        this.colocatedJoinsEnabled = colocatedJoinsEnabled;
        return this;
    }

    public boolean isJoinReorderingEnabled()
    {
        return reorderJoins;
    }

    @Config("reorder-joins")
    @ConfigDescription("Experimental: Reorder joins to optimize plan")
    public FeaturesConfig setJoinReorderingEnabled(boolean reorderJoins)
    {
        this.reorderJoins = reorderJoins;
        return this;
    }

    public boolean isRedistributeWrites()
    {
        return redistributeWrites;
    }

    @Config("redistribute-writes")
    public FeaturesConfig setRedistributeWrites(boolean redistributeWrites)
    {
        this.redistributeWrites = redistributeWrites;
        return this;
    }

    public boolean isOptimizeMetadataQueries()
    {
        return optimizeMetadataQueries;
    }

    @Config("optimizer.optimize-metadata-queries")
    public FeaturesConfig setOptimizeMetadataQueries(boolean optimizeMetadataQueries)
    {
        this.optimizeMetadataQueries = optimizeMetadataQueries;
        return this;
    }

    public boolean isOptimizeHashGeneration()
    {
        return optimizeHashGeneration;
    }

    @Config("optimizer.optimize-hash-generation")
    public FeaturesConfig setOptimizeHashGeneration(boolean optimizeHashGeneration)
    {
        this.optimizeHashGeneration = optimizeHashGeneration;
        return this;
    }

    public boolean isOptimizeSingleDistinct()
    {
        return optimizeSingleDistinct;
    }

    @Config("optimizer.optimize-single-distinct")
    public FeaturesConfig setOptimizeSingleDistinct(boolean optimizeSingleDistinct)
    {
        this.optimizeSingleDistinct = optimizeSingleDistinct;
        return this;
    }

    public boolean isPushTableWriteThroughUnion()
    {
        return pushTableWriteThroughUnion;
    }

    @Config("optimizer.push-table-write-through-union")
    public FeaturesConfig setPushTableWriteThroughUnion(boolean pushTableWriteThroughUnion)
    {
        this.pushTableWriteThroughUnion = pushTableWriteThroughUnion;
        return this;
    }

    public String getProcessingOptimization()
    {
        return processingOptimization;
    }

    @Config("optimizer.processing-optimization")
    public FeaturesConfig setProcessingOptimization(String processingOptimization)
    {
        if (!ProcessingOptimization.AVAILABLE_OPTIONS.contains(processingOptimization)) {
            throw new IllegalStateException(String.format("Value %s is not valid for processingOptimization.", processingOptimization));
        }
        this.processingOptimization = processingOptimization;
        return this;
    }

    public boolean isDictionaryAggregation()
    {
        return dictionaryAggregation;
    }

    @Config("optimizer.dictionary-aggregation")
    public FeaturesConfig setDictionaryAggregation(boolean dictionaryAggregation)
    {
        this.dictionaryAggregation = dictionaryAggregation;
        return this;
    }

    public boolean isReorderWindows()
    {
        return optimizerReorderWindows;
    }

    @Config("optimizer.reorder-windows")
    public FeaturesConfig setReorderWindows(boolean reorderWindows)
    {
        this.optimizerReorderWindows = reorderWindows;
        return this;
    }

    @Min(2)
    public int getRe2JDfaStatesLimit()
    {
        return re2JDfaStatesLimit;
    }

    @Config("re2j.dfa-states-limit")
    public FeaturesConfig setRe2JDfaStatesLimit(int re2JDfaStatesLimit)
    {
        this.re2JDfaStatesLimit = re2JDfaStatesLimit;
        return this;
    }

    @Min(0)
    public int getRe2JDfaRetries()
    {
        return re2JDfaRetries;
    }

    @Config("re2j.dfa-retries")
    public FeaturesConfig setRe2JDfaRetries(int re2JDfaRetries)
    {
        this.re2JDfaRetries = re2JDfaRetries;
        return this;
    }

    public RegexLibrary getRegexLibrary()
    {
        return regexLibrary;
    }

    @Config("regex-library")
    public FeaturesConfig setRegexLibrary(RegexLibrary regexLibrary)
    {
        this.regexLibrary = regexLibrary;
        return this;
    }

    public boolean isSpillEnabled()
    {
        return spillEnabled;
    }

    @Config("beta.spill-enabled")
    public FeaturesConfig setSpillEnabled(boolean spillEnabled)
    {
        this.spillEnabled = spillEnabled;
        return this;
    }

    public DataSize getOperatorMemoryLimitBeforeSpill()
    {
        return operatorMemoryLimitBeforeSpill;
    }

    @Config("beta.operator-memory-limit-before-spill")
    public FeaturesConfig setOperatorMemoryLimitBeforeSpill(DataSize operatorMemoryLimitBeforeSpill)
    {
        this.operatorMemoryLimitBeforeSpill = operatorMemoryLimitBeforeSpill;
        return this;
    }

    public List<Path> getSpillerSpillPaths()
    {
        return spillerSpillPaths;
    }

    @Config("beta.spiller-spill-path")
    public FeaturesConfig setSpillerSpillPaths(String spillPaths)
    {
        List<String> spillPathsSplit = ImmutableList.copyOf(Splitter.on(",").trimResults().omitEmptyStrings().split(spillPaths));
        Preconditions.checkArgument(spillPathsSplit.size() >= 1, "At least one path required for beta.spiller-spill-path config property");
        this.spillerSpillPaths = spillPathsSplit.stream().map(path -> Paths.get(path)).collect(toImmutableList());
        return this;
    }

    public int getSpillerThreads()
    {
        return spillerThreads;
    }

    @Config("beta.spiller-threads")
    public FeaturesConfig setSpillerThreads(int spillerThreads)
    {
        this.spillerThreads = spillerThreads;
        return this;
    }

    public double getSpillMinimumFreeSpaceThreshold()
    {
        return spillMinimumFreeSpaceThreshold;
    }

    @Config("beta.spiller-minimum-free-space-threshold")
    public FeaturesConfig setSpillMinimumFreeSpaceThreshold(double spillMinimumFreeSpaceThreshold)
    {
        this.spillMinimumFreeSpaceThreshold = spillMinimumFreeSpaceThreshold;
        return this;
    }

    public boolean isOptimizeMixedDistinctAggregations()
    {
        return optimizeMixedDistinctAggregations;
    }

    @Config("optimizer.optimize-mixed-distinct-aggregations")
    public FeaturesConfig setOptimizeMixedDistinctAggregations(boolean value)
    {
        this.optimizeMixedDistinctAggregations = value;
        return this;
    }

    public boolean isParseDecimalLiteralsAsDouble()
    {
        return parseDecimalLiteralsAsDouble;
    }

    @Config("parse-decimal-literals-as-double")
    public FeaturesConfig setParseDecimalLiteralsAsDouble(boolean parseDecimalLiteralsAsDouble)
    {
        this.parseDecimalLiteralsAsDouble = parseDecimalLiteralsAsDouble;
        return this;
    }

    @Config("join-distribution-type")
    public FeaturesConfig setJoinDistributionType(String joinDistributionType)
    {
        if (!JoinDistributionType.AVAILABLE_OPTIONS.contains(joinDistributionType)) {
            throw new IllegalStateException(String.format("Value %s is not valid for join-distribution-type.", joinDistributionType));
        }
        this.joinDistributionType = joinDistributionType;
        return this;
    }

    public String getJoinDistributionType()
    {
        return joinDistributionType;
    }

    @Config("small-table-coefficient")
    public FeaturesConfig setSmallTableCoefficient(double smallTableCoefficient)
    {
        this.smallTableCoefficient = smallTableCoefficient;
        return this;
    }

    public double getSmallTableCoefficient()
    {
        return smallTableCoefficient;
    }
}
