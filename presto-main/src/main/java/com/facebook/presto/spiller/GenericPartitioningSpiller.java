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

import com.facebook.presto.memory.AggregatedMemoryContext;
import com.facebook.presto.operator.SpillContext;
import com.facebook.presto.operator.exchange.LocalPartitionGenerator;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.function.IntPredicate;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class GenericPartitioningSpiller
        implements PartitioningSpiller
{
    private final List<Type> types;
    private final int partitionsCount;
    private final PageBuilder[] pageBuilders;
    private final LocalPartitionGenerator partitionGenerator;
    private final Supplier<SpillContext> spillContextSupplier; // TODO use. Why Supplier ?
    private final SingleStreamSpiller[] spillers;
    private final Closer closer = Closer.create();
    private boolean readingStarted;

    public GenericPartitioningSpiller(
            List<Type> types,
            LocalPartitionGenerator partitionGenerator,
            int partitionsCount,
            Supplier<SpillContext> spillContextSupplier,
            AggregatedMemoryContext memoryContext,
            SingleStreamSpillerFactory spillerFactory)
    {
        this.partitionsCount = partitionsCount;
        this.types = requireNonNull(types, "types is null");
        this.spillers = new SingleStreamSpiller[partitionsCount];
        this.pageBuilders = new PageBuilder[partitionsCount];
        this.partitionGenerator = requireNonNull(partitionGenerator, "partitionGenerator is null");
        this.spillContextSupplier = requireNonNull(spillContextSupplier, "spillContextSupplier is null");

        for (int partition = 0; partition < partitionsCount; partition++) {
            pageBuilders[partition] = new PageBuilder(types);
            SingleStreamSpiller spiller = spillerFactory.create(types, spillContextSupplier.get(), memoryContext.newLocalMemoryContext());
            spillers[partition] = spiller;
            closer.register(spiller);
        }
    }

    @Override
    public synchronized Iterator<Page> getSpilledPages(int partition)
    {
        readingStarted = true;
        getFutureValue(flush(partition));
        return spillers[partition].getSpilledPages();
    }

    @Override
    public synchronized PartitioningSpillResult partitionAndSpill(Page page, IntPredicate spillPartitionMask)
    {
        requireNonNull(page, "page is null");
        requireNonNull(spillPartitionMask, "spillPartitionMask is null");
        checkArgument(page.getChannelCount() == types.size(), "Wrong page channel count, expected %s but got %s", types.size(), page.getChannelCount());

        checkState(!readingStarted);
        IntArrayList unspilledPositions = partitionPage(page, spillPartitionMask);
        ListenableFuture<?> future = flush();

        return new PartitioningSpillResult(future, unspilledPositions);
    }

    private synchronized IntArrayList partitionPage(Page page, IntPredicate spillPartitionMask)
    {
        IntArrayList unspilledPositions = new IntArrayList();

        for (int position = 0; position < page.getPositionCount(); position++) {
            int partition = partitionGenerator.getPartition(position, page);

            if (!spillPartitionMask.test(partition)) {
                unspilledPositions.add(position);
                continue;
            }

            PageBuilder pageBuilder = pageBuilders[partition];
            pageBuilder.declarePosition();
            for (int channel = 0; channel < types.size(); channel++) {
                Type type = types.get(channel);
                type.appendTo(page.getBlock(channel), position, pageBuilder.getBlockBuilder(channel));
            }
        }

        return unspilledPositions;
    }

    private synchronized ListenableFuture<?> flush()
    {
        ImmutableList.Builder<ListenableFuture<?>> futures = ImmutableList.builder();

        for (int partition = 0; partition < partitionsCount; partition++) {
            PageBuilder pageBuilder = pageBuilders[partition];
            if (pageBuilder.isFull()) {
                futures.add(flush(partition));
            }
        }

        return Futures.allAsList(futures.build());
    }

    private synchronized ListenableFuture<?> flush(int partition)
    {
        PageBuilder pageBuilder = pageBuilders[partition];
        if (pageBuilder.isEmpty()) {
            return Futures.immediateFuture(null);
        }
        Page page = pageBuilder.build();
        pageBuilder.reset();
        return spillers[partition].spill(page);
    }

    @Override
    public synchronized void close()
    {
        try {
            closer.close();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
