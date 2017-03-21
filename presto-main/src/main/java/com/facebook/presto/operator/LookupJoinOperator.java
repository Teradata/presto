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

import com.facebook.presto.operator.LookupJoinOperators.JoinType;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.MappedBlock;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spiller.PartitioningSpiller;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.io.Closeable;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static com.facebook.presto.operator.LookupJoinOperators.JoinType.FULL_OUTER;
import static com.facebook.presto.operator.LookupJoinOperators.JoinType.PROBE_OUTER;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class LookupJoinOperator
        implements Operator, Closeable
{
    private final LookupJoiner lookupJoiner;
    private final OperatorContext operatorContext;

    private final List<Type> allTypes;
    private final List<Type> probeTypes;
    private final ListenableFuture<? extends LookupSource> lookupSourceFuture;
    private final LookupSourceFactory lookupSourceFactory;
    private final JoinProbeFactory joinProbeFactory;
    private final Runnable onClose;

    private final AtomicInteger lookupJoinsCount;
    private final HashGenerator hashGenerator;
    private final boolean probeOnOuterSide;

    private Optional<SpillledLookupJoiner> spilledLookupJoiner = Optional.empty();

    private boolean finishing;
    private boolean closed;

    private Optional<PartitioningSpiller> spiller = Optional.empty();
    private ListenableFuture<?> spillInProgress = NOT_BLOCKED;
    private Optional<Iterator<Integer>> spilledPartitions = Optional.empty();

    public LookupJoinOperator(
            OperatorContext operatorContext,
            List<Type> allTypes,
            List<Type> probeTypes,
            JoinType joinType,
            LookupSourceFactory lookupSourceFactory,
            JoinProbeFactory joinProbeFactory,
            Runnable onClose,
            AtomicInteger lookupJoinsCount,
            HashGenerator hashGenerator)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.allTypes = ImmutableList.copyOf(requireNonNull(allTypes, "allTypes is null"));
        this.probeTypes = ImmutableList.copyOf(requireNonNull(probeTypes, "probeTypes is null"));

        requireNonNull(joinType, "joinType is null");

        this.lookupSourceFactory = requireNonNull(lookupSourceFactory, "lookupSourceFactory is null");
        this.joinProbeFactory = requireNonNull(joinProbeFactory, "joinProbeFactory is null");
        this.onClose = requireNonNull(onClose, "onClose is null");
        this.lookupJoinsCount = requireNonNull(lookupJoinsCount, "lookupJoinsCount is null");
        this.hashGenerator = requireNonNull(hashGenerator, "hashGenerator is null");

        this.lookupSourceFuture = lookupSourceFactory.createLookupSource();
        this.probeOnOuterSide = joinType == PROBE_OUTER || joinType == FULL_OUTER;
        this.lookupJoiner = new LookupJoiner(allTypes, lookupSourceFuture, joinProbeFactory, probeOnOuterSide);
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public List<Type> getTypes()
    {
        return allTypes;
    }

    @Override
    public void finish()
    {
        lookupJoiner.finish();

        if (!finishing && lookupSourceFactory.hasSpilled()) {
            // Last LookupJoinOperator will handle spilled pages
            int count = lookupJoinsCount.decrementAndGet();
            checkState(count >= 0);
            if (count > 0) {
                spiller = Optional.empty();
            }
            else {
                // last LookupJoinOperator might not spilled at all, but other parallel joins could
                // so we have to load spiller if this operator hasn't used one yet
                ensureSpillerLoaded();
            }
        }
        finishing = true;
    }

    private void unspillNextLookupSource()
    {
        checkState(spiller.isPresent());
        checkState(spilledPartitions.isPresent());
        if (spilledPartitions.get().hasNext()) {
            int currentSpilledPartition = spilledPartitions.get().next();

            spilledLookupJoiner = Optional.of(new SpillledLookupJoiner(
                    allTypes,
                    lookupSourceFactory.readSpilledLookupSource(operatorContext.getSession(), currentSpilledPartition),
                    joinProbeFactory,
                    spiller.get().getSpilledPages(currentSpilledPartition),
                    probeOnOuterSide));
        }
        else {
            spiller.get().close();
            spiller = Optional.empty();
        }
    }

    @Override
    public boolean isFinished()
    {
        boolean finished = lookupJoiner.isFinished() && !spiller.isPresent();

        // if finished drop references so memory is freed early
        if (finished) {
            close();
        }
        return finished;
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        if (!spillInProgress.isDone()) {
            checkState(lookupSourceFuture.isDone());
            return spillInProgress;
        }
        if (spilledLookupJoiner.isPresent()) {
            return spilledLookupJoiner.get().isBlocked();
        }
        return lookupJoiner.isBlocked();
    }

    @Override
    public boolean needsInput()
    {
        return lookupJoiner.needsInput();
    }

    @Override
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");

        if (lookupSourceFactory.hasSpilled()) {
            page = spillAndMaskSpilledPositions(page);
        }

        lookupJoiner.addInput(page);
    }

    private Page spillAndMaskSpilledPositions(Page page)
    {
        ensureSpillerLoaded();

        PartitioningSpiller.PartitioningSpillResult spillResult = spiller.get().partitionAndSpill(page);

        if (!spillResult.isBlocked().isDone()) {
            this.spillInProgress = spillResult.isBlocked();
        }
        IntArrayList unspilledPositions = spillResult.getUnspilledPositions();

        return mapPage(unspilledPositions, page);
    }

    private void ensureSpillerLoaded()
    {
        checkState(lookupSourceFactory.hasSpilled());
        checkState(lookupSourceFuture.isDone());
        if (!spiller.isPresent()) {
            spiller = Optional.of(lookupSourceFactory.createProbeSpiller(operatorContext, probeTypes, hashGenerator));
        }
        if (!spilledPartitions.isPresent()) {
            spilledPartitions = Optional.of(ImmutableSet.copyOf(lookupSourceFactory.getSpilledPartitions()).iterator());
        }
    }

    private Page mapPage(IntArrayList unspilledPositions, Page page)
    {
        Block[] mappedBlocks = new Block[page.getChannelCount()];
        for (int channel = 0; channel < page.getChannelCount(); channel++) {
            mappedBlocks[channel] = new MappedBlock(page.getBlock(channel), unspilledPositions.elements(), 0, unspilledPositions.size());
        }
        return new Page(mappedBlocks);
    }

    @Override
    public Page getOutput()
    {
        if (finishing && spiller.isPresent()) {
            if (!spilledLookupJoiner.isPresent() || spilledLookupJoiner.get().isFinished()) {
                unspillNextLookupSource();
                return null;
            }
            SpillledLookupJoiner joiner = spilledLookupJoiner.get();
            operatorContext.setMemoryReservation(joiner.getInMemorySizeInBytes());
            return joiner.getOutput();
        }
        else {
            return lookupJoiner.getOutput();
        }
    }

    @Override
    public void close()
    {
        // Closing the lookupSource is always safe to do, but we don't want to release the supplier multiple times, since its reference counted
        if (closed) {
            return;
        }
        closed = true;
        lookupJoiner.close();
        onClose.run();
    }
}
