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
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.log.Logger;

import javax.annotation.concurrent.GuardedBy;

import java.util.List;
import java.util.function.IntConsumer;
import java.util.function.IntFunction;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.allAsList;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Objects.requireNonNull;

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
public class PartitionedConsumption<T>
{
    private static final Logger log = Logger.get(PartitionedConsumption.class);

    private final int consumersCount;

    // TODO FIXME Queue or something? So that we don't keep content for already released partitions

    private final List<Partition<T>> partitions;

    PartitionedConsumption(int consumersCount, Iterable<Integer> partitionNumbers, IntFunction<ListenableFuture<T>> loader, IntConsumer disposer)
    {
        this(consumersCount, immediateFuture(null), partitionNumbers, loader, disposer);
    }

    PartitionedConsumption(
            int consumersCount,
            ListenableFuture<?> activator,
            Iterable<Integer> partitionNumbers,
            IntFunction<ListenableFuture<T>> loader,
            IntConsumer disposer)
    {
        checkArgument(consumersCount > 0, "consumersCount must be positive");
        this.consumersCount = consumersCount;
        this.partitions = createPartitions(activator, partitionNumbers, loader, disposer);
    }

    private List<Partition<T>> createPartitions(
            ListenableFuture<?> activator,
            Iterable<Integer> partitionNumbers,
            IntFunction<ListenableFuture<T>> loader,
            IntConsumer disposer)
    {
        requireNonNull(partitionNumbers, "partitionNumbers is null");
        requireNonNull(loader, "loader is null");
        requireNonNull(disposer, "disposer is null");

        ImmutableList.Builder<Partition<T>> partitions = ImmutableList.builder();
        ListenableFuture<?> partitionActivator = activator;
        for (Integer partitionNumber : partitionNumbers) {
            Partition<T> partition = new Partition<>(consumersCount, partitionNumber, loader, partitionActivator, disposer);
            partitions.add(partition);
            partitionActivator = partition.released;
        }
        return partitions.build();
    }

    public int getConsumersCount()
    {
        return consumersCount;
    }

    public Iterable<Partition<T>> getPartitions()
    {
        return partitions;
    }

    public static class Partition<T>
    {
        private final int partitionNumber;
        private final SettableFuture<?> requested;
        private final ListenableFuture<T> loaded;
        private final SettableFuture<?> released;

        @GuardedBy("this")
        private int pendingReleases;

        public Partition(
                int consumersCount,
                int partitionNumber,
                IntFunction<ListenableFuture<T>> loader,
                ListenableFuture<?> previousReleased,
                IntConsumer disposer)
        {
            this.partitionNumber = partitionNumber;
            this.requested = SettableFuture.create();
            this.loaded = Futures.transformAsync(
                    allAsList(requested, previousReleased),
                    ignored -> loader.apply(partitionNumber));
            this.released = SettableFuture.create();
            released.addListener(() -> disposer.accept(partitionNumber), directExecutor());
            this.pendingReleases = consumersCount;
        }

        public int number()
        {
            return partitionNumber;
        }

        public ListenableFuture<T> load()
        {
            requested.set(null);
            return loaded;
        }

        public synchronized void release()
        {
            checkState(loaded.isDone());
            pendingReleases--;
            checkState(pendingReleases >= 0);
            if (pendingReleases == 0) {
                released.set(null);
            }
        }
    }
}
