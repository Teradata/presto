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
import com.facebook.presto.operator.LookupSourceProvider.LookupSourceLease;
import com.facebook.presto.operator.PartitionedConsumption.Partition;
import com.facebook.presto.operator.exchange.LocalPartitionGenerator;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.DictionaryBlock;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spiller.PartitioningSpiller;
import com.facebook.presto.spiller.PartitioningSpiller.PartitioningSpillResult;
import com.facebook.presto.spiller.PartitioningSpillerFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.io.Closeable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.IntPredicate;

import static com.facebook.presto.operator.LookupJoinOperators.JoinType.FULL_OUTER;
import static com.facebook.presto.operator.LookupJoinOperators.JoinType.PROBE_OUTER;
import static com.facebook.presto.operator.Operators.checkNoFailure;
import static com.facebook.presto.operator.Operators.getDone;
import static com.facebook.presto.operator.Operators.runAll;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static java.util.Collections.emptyIterator;
import static java.util.Objects.requireNonNull;

public class LookupJoinOperator
        implements Operator, Closeable
{
    private static final int MAX_POSITIONS_EVALUATED_PER_CALL = 10000;

    private final OperatorContext operatorContext;
    private final List<Type> allTypes;
    private final List<Type> probeTypes;
    private final JoinProbeFactory joinProbeFactory;
    private final Runnable onClose;
    private final OptionalInt lookupJoinsCount;
    private final HashGenerator hashGenerator;
    private final LookupSourceFactory lookupSourceFactory;
    private final PartitioningSpillerFactory partitioningSpillerFactory;

    private final JoinStatisticsCounter statisticsCounter;

    private final PageBuilder pageBuilder;

    private final boolean probeOnOuterSide;

    private final ListenableFuture<LookupSourceProvider> lookupSourceProviderFuture;
    private LookupSourceProvider lookupSourceProvider;
    private JoinProbe probe;

    private Optional<PartitioningSpiller> spiller = Optional.empty();
    private Optional<LocalPartitionGenerator> partitionGenerator = Optional.empty();
    private ListenableFuture<?> spillInProgress = NOT_BLOCKED;
    private long inputPageSpillEpoch;
    private boolean closed;
    private boolean finishing;
    private boolean unspilling;
    private boolean finished;
    private long joinPosition = -1;
    private int joinSourcePositions = 0;

    private boolean currentProbePositionProducedRow;

    private final Map<Integer, SavedRow> savedRows = new HashMap<>();
    private Iterator<Partition<LookupSource>> lookupPartitions;
    private Optional<Partition<LookupSource>> currentPartition = Optional.empty();
    private Optional<ListenableFuture<LookupSource>> unspilledLookupSource = Optional.empty();
    private Iterator<Page> unspilledInputPages = emptyIterator();

    public LookupJoinOperator(
            OperatorContext operatorContext,
            List<Type> allTypes,
            List<Type> probeTypes,
            JoinType joinType,
            LookupSourceFactory lookupSourceFactory,
            JoinProbeFactory joinProbeFactory,
            Runnable onClose,
            OptionalInt lookupJoinsCount,
            HashGenerator hashGenerator,
            PartitioningSpillerFactory partitioningSpillerFactory)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.allTypes = ImmutableList.copyOf(requireNonNull(allTypes, "allTypes is null"));
        this.probeTypes = ImmutableList.copyOf(requireNonNull(probeTypes, "probeTypes is null"));

        requireNonNull(joinType, "joinType is null");
        // Cannot use switch case here, because javac will synthesize an inner class and cause IllegalAccessError
        probeOnOuterSide = joinType == PROBE_OUTER || joinType == FULL_OUTER;

        this.joinProbeFactory = requireNonNull(joinProbeFactory, "joinProbeFactory is null");
        this.onClose = requireNonNull(onClose, "onClose is null");
        this.lookupJoinsCount = requireNonNull(lookupJoinsCount, "lookupJoinsCount is null");
        this.hashGenerator = requireNonNull(hashGenerator, "hashGenerator is null");
        this.lookupSourceFactory = requireNonNull(lookupSourceFactory, "lookupSourceFactory is null");
        this.partitioningSpillerFactory = requireNonNull(partitioningSpillerFactory, "partitioningSpillerFactory is null");
        this.lookupSourceProviderFuture = lookupSourceFactory.createLookupSourceProvider();

        this.statisticsCounter = new JoinStatisticsCounter(joinType);
        operatorContext.setInfoSupplier(this.statisticsCounter);

        this.pageBuilder = new PageBuilder(allTypes);
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
        if (finishing) {
            return;
        }

        if (!spillInProgress.isDone()) {
            // Not ready yet.
            return;
        }

        checkNoFailure(spillInProgress);
        finishing = true;
    }

    @Override
    public boolean isFinished()
    {
        boolean finished = this.finished && probe == null && pageBuilder.isEmpty();

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
            /*
             * Input spilling can happen only after lookupSourceProviderFuture was done.
             */
            return spillInProgress;
        }
        if (unspilledLookupSource.isPresent()) {
            /*
             * Unspilling can happen only after lookupSourceProviderFuture was done.
             */
            return unspilledLookupSource.get();
        }

        return lookupSourceProviderFuture;
    }

    @Override
    public boolean needsInput()
    {
        return !finishing
                && lookupSourceProviderFuture.isDone()
                && spillInProgress.isDone()
                && probe == null;
    }

    // TODO verify spilling doesn't create too many too small files

    @Override
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");
        checkState(probe == null, "Current page has not been completely processed yet");

        checkState(hasLookupSourceProvider(), "Not ready to handle input yet");

        boolean hasSpilled;
        long spillEpoch;
        IntPredicate spillMask;
        try (LookupSourceLease lookupSourceLease = lookupSourceProvider.leaseLookupSource()) {
            hasSpilled = lookupSourceLease.hasSpilled();
            spillEpoch = lookupSourceLease.spillEpoch();
            spillMask = lookupSourceLease.getSpillMask();
        }

        addInput(page, hasSpilled, spillEpoch, spillMask);
    }

    private void addInput(Page page, boolean hasSpilled, long spillEpoch, IntPredicate spillMask)
    {
        if (hasSpilled) {
            page = spillAndMaskSpilledPositions(page, spillMask);
            if (page.getPositionCount() == 0) {
                return;
            }
        }

        // create probe
        inputPageSpillEpoch = spillEpoch;
        probe = joinProbeFactory.createJoinProbe(page);

        // initialize to invalid join position to force output code to advance the cursors
        joinPosition = -1;
    }

    private boolean hasLookupSourceProvider()
    {
        if (lookupSourceProvider == null) {
            if (!lookupSourceProviderFuture.isDone()) {
                return false;
            }
            lookupSourceProvider = requireNonNull(getDone(lookupSourceProviderFuture));
        }
        return true;
    }

    private Page spillAndMaskSpilledPositions(Page page, IntPredicate spillMask)
    {
        checkState(spillInProgress.isDone(), "Previous spill still in progress");
        checkNoFailure(spillInProgress);

        if (!spiller.isPresent()) {
            spiller = Optional.of(partitioningSpillerFactory.create(
                    probeTypes,
                    getPartitionGenerator(),
                    lookupSourceFactory.partitions(),
                    operatorContext.getSpillContext()::newLocalSpillContext,
                    operatorContext.getSystemMemoryContext().newAggregatedMemoryContext()));
        }

        PartitioningSpillResult result = spiller.get().partitionAndSpill(page, spillMask);
        spillInProgress = result.getSpillingFuture();
        return mask(page, result.getUnspilledPositions());
    }

    public LocalPartitionGenerator getPartitionGenerator()
    {
        if (!partitionGenerator.isPresent()) {
            partitionGenerator = Optional.of(new LocalPartitionGenerator(hashGenerator, lookupSourceFactory.partitions()));
        }
        return partitionGenerator.get();
    }

    @Override
    public Page getOutput()
    {
        if (!spillInProgress.isDone()) {
            return null;
        }
        checkNoFailure(spillInProgress);

        if (probe == null && pageBuilder.isEmpty() && !finishing) {
            // Fast exit path when lookup source is still being build
            return null;
        }

        if (!hasLookupSourceProvider()) {
            // TODO handle the case when there probe side is empty and finish early
            return null;
        }

        if (probe == null && finishing && !unspilling) {
            /*
             * We do not have input probe and we won't have any, as we're finishing.
             * Let LookupSourceFactory know LookupSources can be disposed as far as we're concerned.
             */
            finishRegularInput();
            unspilling = true;
        }

        if (probe == null && unspilling && !finished) {
            /*
             * If no current partition or it was exhausted, unspill next one.
             * Add input there when it needs one, produce output. Be Happy.
             */
            if (!tryUnspillNext()) {
                return null;
            }
        }

        if (probe != null) {
            processProbe();
        }

        return producePage();
    }

    private void finishRegularInput()
    {
        checkState(lookupPartitions == null);
        lookupPartitions = lookupSourceFactory.finishProbeOperator(lookupJoinsCount)
                .getPartitions().iterator();
    }

    private boolean tryUnspillNext()
    {
        verify(probe == null);

        if (unspilledInputPages.hasNext()) {
            addInput(unspilledInputPages.next());
            verify(probe != null);
            return true;
        }
        else if (unspilledLookupSource.isPresent()) {
            if (!unspilledLookupSource.get().isDone()) {
                // Not unspilled yet
                return false;
            }
            LookupSource lookupSource = getDone(unspilledLookupSource.get());
            unspilledLookupSource = Optional.empty();

            lookupSourceProvider.close();
            lookupSourceProvider = new SimpleLookupSourceProvider(lookupSource);

            int partition = currentPartition.get().number();
            unspilledInputPages = spiller.map(spiller -> spiller.getSpilledPages(partition))
                    .orElse(emptyIterator());

            SavedRow savedRow = savedRows.remove(partition);
            if (savedRow != null) {
                addInput(savedRow.row);
                verify(probe != null);
                verify(probe.advanceNextPosition());
                joinPosition = savedRow.joinPositionWithinPartition;
                currentProbePositionProducedRow = savedRow.currentProbePositionProducedRow;
            }

            return false;
        }
        else if (lookupPartitions.hasNext()) {
            currentPartition.ifPresent(Partition::release);
            currentPartition = Optional.of(lookupPartitions.next());
            unspilledLookupSource = Optional.of(currentPartition.get().load());

            return false;
        }
        else {
            finished = true;
            /*
             * We did not create new probe, but let the getOutput() flush page builder if needed.
             */
            return true;
        }
    }

    private void processProbe()
    {
        verify(probe != null);

        boolean hasSpilled;
        long spillEpoch;
        IntPredicate spillMask;
        long joinPositionWithinPartition;

        try (LookupSourceLease lookupSourceLease = lookupSourceProvider.leaseLookupSource()) {
            if (lookupSourceLease.spillEpoch() == inputPageSpillEpoch) {
                // Spill state didn't change, so process as usual.
                processProbe(lookupSourceLease.getLookupSource());
                return;
            }

            hasSpilled = lookupSourceLease.hasSpilled();
            spillEpoch = lookupSourceLease.spillEpoch();
            spillMask = lookupSourceLease.getSpillMask();

            if (joinPosition >= 0) {
                joinPositionWithinPartition = lookupSourceLease.getLookupSource().joinPositionWithinPartition(joinPosition);
            }
            else {
                joinPositionWithinPartition = -1;
            }
        }

        /*
         * Spill state changed. All probe rows that were not processed yet should be treated as regular input (and be partially spilled).
         * If current row maps to now-spilled a partition, it needs to be saved for later. If it maps to a partition still in memory, it
         * should be added together with not-yet-processed rows. In either case we need to start processing the row since its current position
         * in the lookup source.
         */
        verify(hasSpilled);
        verify(spillEpoch > inputPageSpillEpoch);

        Page currentPage = probe.getPage();
        int currentPosition = probe.getPosition();
        int currentRowPartition = getPartitionGenerator().getPartition(currentPosition, currentPage);
        boolean currentRowSpilled = spillMask.test(currentRowPartition);

        // TODO SavedRow compacts, it should not be used when !currentRowSpilled
        SavedRow savedRow = new SavedRow(currentPage, currentPosition, joinPosition, joinPositionWithinPartition, currentProbePositionProducedRow);

        probe = null;
        joinPosition = -1;

        if (currentRowSpilled) {
            // TODO this can be skipped if (joinPosition<0 && (currentProbePositionProducedRow||!outer)), right?
            savedRows.merge(currentRowPartition, savedRow,
                    (oldValue, newValue) -> {
                        throw new IllegalStateException(String.format("How on earth this could happen that partition %s is spilled in the middle of processing twice?", currentRowPartition));
                    });

            Page unprocessed = pageSlice(currentPage, currentPosition + 1);
            addInput(unprocessed, hasSpilled, spillEpoch, spillMask);
        }
        else {
            Page remaining = pageSlice(currentPage, currentPosition);
            addInput(remaining, hasSpilled, spillEpoch, spillMask);
            verify(probe != null, "first row wasn't spilled so the probe should exist");
            verify(probe.advanceNextPosition());
            joinPosition = savedRow.absoluteJoinPosition;
            currentProbePositionProducedRow = savedRow.currentProbePositionProducedRow;
        }
    }

    private Page pageSlice(Page currentPage, int startAtPosition)
    {
        verify(currentPage.getPositionCount() - startAtPosition >= 0);

        IntArrayList retainedPositions = new IntArrayList(currentPage.getPositionCount() - startAtPosition);
        for (int i = startAtPosition; i < currentPage.getPositionCount(); i++) {
            retainedPositions.add(i);
        }
        return mask(currentPage, retainedPositions);
    }

    public static class SavedRow
    {
        public final long absoluteJoinPosition;
        public final long joinPositionWithinPartition;
        public final boolean currentProbePositionProducedRow;
        // TODO save joinSourcePositions too?

        public final Page row;

        public SavedRow(Page page, int position, long absoluteJoinPosition, long joinPositionWithinPartition, boolean currentProbePositionProducedRow)
        {
            this.absoluteJoinPosition = absoluteJoinPosition;
            this.joinPositionWithinPartition = joinPositionWithinPartition;
            this.currentProbePositionProducedRow = currentProbePositionProducedRow;

            this.row = mask(page, new IntArrayList(ImmutableList.of(position)));
            this.row.compact();
        }
    }

    private Page producePage()
    {
        if (shouldProducePage()) {
            Page page = pageBuilder.build();
            pageBuilder.reset();
            return page;
        }
        return null;
    }

    private boolean shouldProducePage()
    {
        if (pageBuilder.isFull()) {
            return true;
        }

        if (pageBuilder.isEmpty()) {
            return false;
        }

        return probe == null && finished;
    }

    private void processProbe(LookupSource lookupSource)
    {
        if (probe == null) {
            return;
        }

        Counter lookupPositionsConsidered = new Counter();
        while (true) {
            if (probe.getPosition() >= 0) {
                if (!joinCurrentPosition(lookupSource, lookupPositionsConsidered)) {
                    break;
                }
                if (!currentProbePositionProducedRow) {
                    currentProbePositionProducedRow = true;
                    if (!outerJoinCurrentPosition(lookupSource)) {
                        break;
                    }
                }
            }
            currentProbePositionProducedRow = false;
            if (!advanceProbePosition(lookupSource)) {
                break;
            }
            statisticsCounter.recordProbe(joinSourcePositions);
            joinSourcePositions = 0;
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
        probe = null;
        runAll(
                () -> pageBuilder.reset(),
                // closing lookup source is only here for index join
                () -> Optional.ofNullable(lookupSourceProvider).ifPresent(LookupSourceProvider::close),
                () -> spiller.ifPresent(PartitioningSpiller::close),
                onClose
        );
    }

    /**
     * Produce rows matching join condition for the current probe position. If this method was called previously
     * for the current probe position, calling this again will produce rows that wasn't been produced in previous
     * invocations.
     *
     * @return true if all eligible rows have been produced; false otherwise (because pageBuilder became full)
     */
    private boolean joinCurrentPosition(LookupSource lookupSource, Counter lookupPositionsConsidered)
    {
        // while we have a position on lookup side to join against...
        while (joinPosition >= 0) {
            lookupPositionsConsidered.increment();
            if (lookupSource.isJoinPositionEligible(joinPosition, probe.getPosition(), probe.getPage())) {
                currentProbePositionProducedRow = true;

                pageBuilder.declarePosition();
                // write probe columns
                probe.appendTo(pageBuilder);
                // write build columns
                lookupSource.appendTo(joinPosition, pageBuilder, probe.getOutputChannelCount());
                joinSourcePositions++;
            }

            // get next position on lookup side for this probe row
            joinPosition = lookupSource.getNextJoinPosition(joinPosition, probe.getPosition(), probe.getPage());

            if (lookupPositionsConsidered.get() >= MAX_POSITIONS_EVALUATED_PER_CALL) {
                return false;
            }
            if (pageBuilder.isFull()) {
                return false;
            }
        }
        return true;
    }

    /**
     * @return whether there are more positions on probe side
     */
    private boolean advanceProbePosition(LookupSource lookupSource)
    {
        if (!probe.advanceNextPosition()) {
            probe = null;
            return false;
        }

        // update join position
        joinPosition = probe.getCurrentJoinPosition(lookupSource);
        return true;
    }

    /**
     * Produce a row for the current probe position, if it doesn't match any row on lookup side and this is an outer join.
     *
     * @return whether pageBuilder became full
     */
    private boolean outerJoinCurrentPosition(LookupSource lookupSource)
    {
        if (probeOnOuterSide && joinPosition < 0) {
            // write probe columns
            pageBuilder.declarePosition();
            probe.appendTo(pageBuilder);

            // write nulls into build columns
            int outputIndex = probe.getOutputChannelCount();
            for (int buildChannel = 0; buildChannel < lookupSource.getChannelCount(); buildChannel++) {
                pageBuilder.getBlockBuilder(outputIndex).appendNull();
                outputIndex++;
            }
            if (pageBuilder.isFull()) {
                return false;
            }
        }
        return true;
    }

    // This class needs to be public because LookupJoinOperator is isolated.
    public static class Counter
    {
        private int count;

        public void increment()
        {
            count++;
        }

        public int get()
        {
            return count;
        }
    }

    private static Page mask(Page page, IntArrayList retainedPositions)
    {
        requireNonNull(page, "page is null");
        requireNonNull(retainedPositions, "retainedPositions is null");

        int[] ids = retainedPositions.toIntArray();
        Block[] blocks = Arrays.stream(page.getBlocks())
                .map(block -> new DictionaryBlock(block, ids))
                .toArray(Block[]::new);
        return new Page(retainedPositions.size(), blocks);
    }
}
