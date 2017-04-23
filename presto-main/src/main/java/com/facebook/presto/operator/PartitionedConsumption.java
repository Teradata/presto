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
package com.facebook.presto.operator;

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;

import javax.annotation.concurrent.GuardedBy;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static java.lang.String.format;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.completedFuture;

/**
 * Coordinates consumption of a resource by multiple consumers
 * in a partition-by-partition manner.
 * <p>
 * The partitions to consume can be obtained by consumers using the
 * {@code Iterable<Partition<T>> getPartitions()} method.
 * <p>
 * A {@code Partition} is only loaded after at least one consumer has called
 * {@code Partition#load()} on it and all the {@code consumersCount} consumers
 * have {@code release()}-d the previous {@code Partition} (if any).
 * <p>
 * The loaded object is {@code close()}-d when all the consumers have released
 * its {@code Partition} and thus must implement {@code Closable}.
 * <p>
 * The partitions contents are loaded using the {@code Function<Integer, CompletableFuture<T>> loader} passed
 * upon construction. The integer argument in the loader function is the number of the partition to load.
 * <p>
 * The partition number can be accessed using {@code Partition#number()} and - for a given partition -
 * it takes the value of the respective element of {@code partitionNumbers} passed upon construction.
 *
 * @param <T> type of the object loaded for each {@code Partition}. Must be {@code Closable}.
 */
public class PartitionedConsumption<T extends Closeable>
        implements Closeable
{
    private static final Logger LOG = Logger.get(PartitionedConsumption.class);

    private final int consumersCount;
    private final Function<Integer, CompletableFuture<T>> loader;
    private final Iterable<Partition<T>> partitions;

    PartitionedConsumption(int consumersCount, Iterable<Integer> partitionNumbers, Function<Integer, CompletableFuture<T>> loader)
    {
        checkArgument(consumersCount > 0, "consumersCount must be positive");
        this.consumersCount = consumersCount;
        this.loader = loader;
        this.partitions = createPartitions(partitionNumbers);
    }

    private Iterable<Partition<T>> createPartitions(Iterable<Integer> partitionNumbers)
    {
        List<Partition<T>> partitions = new ArrayList<>();
        for (Integer partitionNumber : partitionNumbers) {
            Optional<Partition<T>> previousPartition = getLast(partitions);
            Optional<CompletableFuture<?>> previousReleased = previousPartition.map(partition -> partition.released);
            partitions.add(new Partition<>(consumersCount, partitionNumber, loader, previousReleased));
        }
        checkArgument(!partitions.isEmpty(), "partitionNumbers can't be empty");
        return ImmutableList.copyOf(partitions);
    }

    private static <T> Optional<T> getLast(List<T> partitions)
    {
        return partitions.isEmpty() ? Optional.empty() : Optional.of(partitions.get(partitions.size() - 1));
    }

    public int getConsumersCount()
    {
        return consumersCount;
    }

    public Iterable<Partition<T>> getPartitions()
    {
        return partitions;
    }

    @Override
    public void close()
            throws IOException
    {
        partitions.forEach(partition -> partition.loaded.thenAccept(closeable -> {
            try {
                closeable.close();
            }
            catch (IOException e) {
                LOG.error(format("Error closing partition %s", partition.number()), e);
            }
        }));
    }

    public static class Partition<S extends Closeable>
    {
        private final int partitionNumber;
        private final CompletableFuture<Void> requested;
        private final CompletableFuture<S> loaded;
        private final CompletableFuture<Void> released;

        @GuardedBy("this")
        private int pendingReleases;

        public Partition(
                int consumersCount,
                int partitionNumber,
                Function<Integer, CompletableFuture<S>> loader,
                Optional<CompletableFuture<?>> previousReleasedOption)
        {
            this.partitionNumber = partitionNumber;
            this.requested = new CompletableFuture<>();
            CompletableFuture<?> previousReleased = previousReleasedOption.orElse(completedFuture(null));
            this.loaded = allOf(requested, previousReleased)
                    .thenCompose(ignored -> loader.apply(partitionNumber));
            this.released = new CompletableFuture<>();
            this.pendingReleases = consumersCount;
        }

        public int number()
        {
            return partitionNumber;
        }

        public CompletableFuture<S> load()
        {
            requested.complete(null);
            return loaded;
        }

        public synchronized void release()
        {
            checkState(loaded.isDone());
            pendingReleases--;
            checkState(pendingReleases >= 0);
            if (pendingReleases == 0) {
                try {
                    getFutureValue(loaded).close();
                }
                catch (IOException e) {
                    throw new RuntimeException("Error while releasing partition", e);
                }
                released.complete(null);
            }
        }
    }
}
