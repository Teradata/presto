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

package com.facebook.presto.spiller;

import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newFixedThreadPool;

public class BinaryFileSingleStreamSpillerFactory
        implements SingleStreamSpillerFactory
{
    public static final String SPILLER_THREAD_NAME_PREFIX = "binary-spiller";

    private final ListeningExecutorService executor;
    private final BlockEncodingSerde blockEncodingSerde;
    private final List<Path> spillPaths;
    private final SpillerStats spillerStats;
    private final double minimumFreeSpaceThreshold;
    private int roundRobinIndex;

    @Inject
    public BinaryFileSingleStreamSpillerFactory(BlockEncodingSerde blockEncodingSerde, SpillerStats spillerStats, FeaturesConfig featuresConfig)
    {
        this(createExecutorServiceOfSize(requireNonNull(featuresConfig, "featuresConfig is null").getSpillerThreads()),
                blockEncodingSerde,
                spillerStats,
                requireNonNull(featuresConfig, "featuresConfig is null").getSpillerSpillPaths(),
                requireNonNull(featuresConfig, "featuresConfig is null").getSpillMinimumFreeSpaceThreshold());
    }

    public BinaryFileSingleStreamSpillerFactory(
            ListeningExecutorService executor,
            BlockEncodingSerde blockEncodingSerde,
            SpillerStats spillerStats,
            List<Path> spillPaths,
            double minimumFreeSpaceThreshold)
    {
        this.blockEncodingSerde = requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.spillerStats = requireNonNull(spillerStats, "spillerStats can not be null");
        requireNonNull(spillPaths, "spillPaths is null");
        checkArgument(spillPaths.size() >= 1, "At least one spill path required");
        this.spillPaths = ImmutableList.copyOf(spillPaths);
        spillPaths.forEach(path -> path.toFile().mkdirs());
        this.minimumFreeSpaceThreshold = requireNonNull(minimumFreeSpaceThreshold, "minimumFreeSpaceThreshold can not be null");
        this.roundRobinIndex = 0;
    }

    private static ListeningExecutorService createExecutorServiceOfSize(int nThreads)
    {
        ThreadFactory threadFactory = daemonThreadsNamed(SPILLER_THREAD_NAME_PREFIX + "-%s");
        ExecutorService executorService = newFixedThreadPool(nThreads, threadFactory);
        return MoreExecutors.listeningDecorator(executorService);
    }

    @Override
    public SingleStreamSpiller create(List<Type> types, LocalSpillContext localSpillContext)
    {
        return new BinaryFileSingleStreamSpiller(blockEncodingSerde, executor, getNextSpillPath(), spillerStats, localSpillContext);
    }

    private synchronized Path getNextSpillPath()
    {
        int spillPathsCount = spillPaths.size();
        for (int i = 0; i < spillPathsCount; ++i) {
            Path path = spillPaths.get((roundRobinIndex + i) % spillPathsCount);
            if (hasEnoughDiskSpace(path)) {
                roundRobinIndex = (roundRobinIndex + i + 1) % spillPathsCount;
                return path;
            }
        }
        throw new RuntimeException("No free space available for spill");
    }

    private boolean hasEnoughDiskSpace(Path path)
    {
        return path.toFile().getFreeSpace() > path.toFile().getTotalSpace() * (1.0 - minimumFreeSpaceThreshold);
    }
}
