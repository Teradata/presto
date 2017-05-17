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

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spiller.SingleStreamSpiller;
import com.facebook.presto.spiller.SingleStreamSpillerFactory;
import com.facebook.presto.sql.gen.JoinFilterFunctionCompiler.JoinFilterFunctionFactory;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;

import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;

import static com.facebook.presto.operator.Operators.checkNoFailure;
import static com.facebook.presto.operator.Operators.getDone;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class HashBuilderOperator
        implements Operator
{
    public static class HashBuilderOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final PartitionedLookupSourceFactory lookupSourceFactory;
        private final List<Integer> outputChannels;
        private final List<Integer> hashChannels;
        private final Optional<Integer> preComputedHashChannel;
        private final Optional<JoinFilterFunctionFactory> filterFunctionFactory;
        private final PagesIndex.Factory pagesIndexFactory;

        private final int expectedPositions;
        private final boolean spillEnabled;
        private final SingleStreamSpillerFactory singleStreamSpillerFactory;

        private int partitionIndex;
        private boolean closed;

        public HashBuilderOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<Type> types,
                List<Integer> outputChannels,
                Map<Symbol, Integer> layout,
                List<Integer> hashChannels,
                Optional<Integer> preComputedHashChannel,
                boolean outer,
                Optional<JoinFilterFunctionFactory> filterFunctionFactory,
                int expectedPositions,
                int partitionCount,
                PagesIndex.Factory pagesIndexFactory)
        {
            this(operatorId,
                    planNodeId,
                    types,
                    outputChannels,
                    layout,
                    hashChannels,
                    preComputedHashChannel,
                    outer,
                    filterFunctionFactory,
                    expectedPositions,
                    partitionCount,
                    pagesIndexFactory,
                    false,
                    SingleStreamSpillerFactory.unsupportedSingleStreamSpillerFactory());
        }

        public HashBuilderOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<Type> types,
                List<Integer> outputChannels,
                Map<Symbol, Integer> layout,
                List<Integer> hashChannels,
                Optional<Integer> preComputedHashChannel,
                boolean outer,
                Optional<JoinFilterFunctionFactory> filterFunctionFactory,
                int expectedPositions,
                int partitionCount,
                PagesIndex.Factory pagesIndexFactory,
                boolean spillEnabled,
                SingleStreamSpillerFactory singleStreamSpillerFactory)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");

            checkArgument(Integer.bitCount(partitionCount) == 1, "partitionCount must be a power of 2");
            lookupSourceFactory = new PartitionedLookupSourceFactory(
                    types,
                    outputChannels.stream()
                            .map(types::get)
                            .collect(toImmutableList()),
                    hashChannels,
                    partitionCount,
                    requireNonNull(layout, "layout is null"),
                    outer);

            this.outputChannels = ImmutableList.copyOf(requireNonNull(outputChannels, "outputChannels is null"));
            this.hashChannels = ImmutableList.copyOf(requireNonNull(hashChannels, "hashChannels is null"));
            this.preComputedHashChannel = requireNonNull(preComputedHashChannel, "preComputedHashChannel is null");
            this.filterFunctionFactory = requireNonNull(filterFunctionFactory, "filterFunctionFactory is null");
            this.pagesIndexFactory = requireNonNull(pagesIndexFactory, "pagesIndexFactory is null");
            this.spillEnabled = spillEnabled;
            this.singleStreamSpillerFactory = requireNonNull(singleStreamSpillerFactory, "singleStreamSpillerFactory is null");

            this.expectedPositions = expectedPositions;
        }

        public LookupSourceFactory getLookupSourceFactory()
        {
            return lookupSourceFactory;
        }

        @Override
        public List<Type> getTypes()
        {
            return lookupSourceFactory.getTypes();
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, HashBuilderOperator.class.getSimpleName());
            HashBuilderOperator operator = new HashBuilderOperator(
                    operatorContext,
                    lookupSourceFactory,
                    partitionIndex,
                    outputChannels,
                    hashChannels,
                    preComputedHashChannel,
                    filterFunctionFactory,
                    expectedPositions,
                    pagesIndexFactory,
                    spillEnabled,
                    singleStreamSpillerFactory);

            partitionIndex++;
            return operator;
        }

        @Override
        public void close()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            throw new UnsupportedOperationException("Parallel hash build can not be duplicated");
        }
    }

    private enum State
    {
        /**
         * Operator accepts input
         */
        CONSUMING_INPUT,

        /**
         * Memory revoking occurred during {@link #CONSUMING_INPUT}. Operator accepts input and spills it
         */
        SPILLING_INPUT,

        /**
         * LookupSource has been built and passed on without any spill occurring
         */
        LOOKUP_SOURCE_BUILT,

        /**
         * Input has been finished and spilled
         */
        LOOKUP_SOURCE_SPILLED,

        /**
         * Spilled input is being unspilled
         */
        LOOKUP_SOURCE_UNSPILLING,

        /**
         * Spilled input has been unspilled, LookupSource built from it
         */
        // TODO consider merging this state with LOOKUP_SOURCE_BUILT
        LOOKUP_SOURCE_UNSPILLED_AND_BUILT,

        /**
         * No longer needed
         */
        DISPOSED
    }

    private final OperatorContext operatorContext;
    private final PartitionedLookupSourceFactory lookupSourceFactory;
    private final int partitionIndex;

    private final List<Integer> outputChannels;
    private final List<Integer> hashChannels;
    private final Optional<Integer> preComputedHashChannel;
    private final Optional<JoinFilterFunctionFactory> filterFunctionFactory;

    private final PagesIndex index;

    private final boolean spillEnabled;
    private final SingleStreamSpillerFactory singleStreamSpillerFactory;

    private final HashCollisionsCounter hashCollisionsCounter;

    private State state = State.CONSUMING_INPUT;
    private Optional<ListenableFuture<?>> lookupSourceNotNeeded = Optional.empty();
    private SpilledLookupSourceHandle spilledLookupSourceHandle = new SpilledLookupSourceHandle();
    private Optional<SingleStreamSpiller> spiller = Optional.empty();
    private long spilledPagesInMemorySize;
    private ListenableFuture<?> spillInProgress = NOT_BLOCKED;
    private Optional<ListenableFuture<List<Page>>> unspillInProgress = Optional.empty();

    public HashBuilderOperator(
            OperatorContext operatorContext,
            PartitionedLookupSourceFactory lookupSourceFactory,
            int partitionIndex,
            List<Integer> outputChannels,
            List<Integer> hashChannels,
            Optional<Integer> preComputedHashChannel,
            Optional<JoinFilterFunctionFactory> filterFunctionFactory,
            int expectedPositions,
            PagesIndex.Factory pagesIndexFactory,
            boolean spillEnabled,
            SingleStreamSpillerFactory singleStreamSpillerFactory)
    {
        requireNonNull(pagesIndexFactory, "pagesIndexFactory is null");

        this.operatorContext = operatorContext;
        this.partitionIndex = partitionIndex;
        this.filterFunctionFactory = filterFunctionFactory;

        this.index = pagesIndexFactory.newPagesIndex(lookupSourceFactory.getTypes(), expectedPositions);
        this.lookupSourceFactory = lookupSourceFactory;

        this.outputChannels = outputChannels;
        this.hashChannels = hashChannels;
        this.preComputedHashChannel = preComputedHashChannel;

        this.hashCollisionsCounter = new HashCollisionsCounter(operatorContext);
        operatorContext.setInfoSupplier(hashCollisionsCounter);

        this.spillEnabled = spillEnabled;
        this.singleStreamSpillerFactory = requireNonNull(singleStreamSpillerFactory, "singleStreamSpillerFactory is null");
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public List<Type> getTypes()
    {
        return lookupSourceFactory.getTypes();
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        switch (state) {
            case CONSUMING_INPUT:
                return NOT_BLOCKED;

            case SPILLING_INPUT:
                return spillInProgress;

            case LOOKUP_SOURCE_BUILT:
                return lookupSourceNotNeeded.get();

            case LOOKUP_SOURCE_SPILLED:
                return spilledLookupSourceHandle.getUnspillingRequested();

            case LOOKUP_SOURCE_UNSPILLING:
                return unspillInProgress.get();

            case LOOKUP_SOURCE_UNSPILLED_AND_BUILT:
                return spilledLookupSourceHandle.getDisposeRequested();

            case DISPOSED:
                return lookupSourceFactory.isDestroyed();
        }
        throw new IllegalStateException("Unhandled state: " + state);
    }

    @Override
    public boolean needsInput()
    {
        return state == State.CONSUMING_INPUT
                || (state == State.SPILLING_INPUT && spillInProgress.isDone());
    }

    @Override
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");

        if (state == State.SPILLING_INPUT) {
            spillInput(page);
            return;
        }

        checkState(state == State.CONSUMING_INPUT);
        updateIndex(page);
    }

    private void updateIndex(Page page)
    {
        index.addPage(page);

        if (spillEnabled) {
            // TODO trySetRevocableMemoryReservation else index.compact()

            operatorContext.setRevocableMemoryReservation(index.getEstimatedSize().toBytes());
        }
        else {
            if (!operatorContext.trySetMemoryReservation(index.getEstimatedSize().toBytes())) {
                index.compact();
                operatorContext.setMemoryReservation(index.getEstimatedSize().toBytes());
            }
        }
        operatorContext.recordGeneratedOutput(page.getSizeInBytes(), page.getPositionCount());
    }

    private void spillInput(Page page)
    {
        checkState(spillInProgress.isDone(), "Previous spill still in progress");
        checkNoFailure(spillInProgress);
        spillInProgress = spiller.orElseThrow(() -> new IllegalStateException("Spiller not loaded"))
                .spill(page);
        spilledPagesInMemorySize += page.getSizeInBytes();
    }

    @Override
    public ListenableFuture<?> startMemoryRevoke()
    {
        checkState(spillEnabled, "Spill not enabled, no revokable memory should be reserved");

        if (state == State.LOOKUP_SOURCE_BUILT) {
            // TODO compute checksum of lookupSource's position links for validation when lookupSource is rebuilt
        }

        if (state == State.CONSUMING_INPUT || state == State.LOOKUP_SOURCE_BUILT) {
            checkState(!spiller.isPresent(), "Spiller already loaded");
            spiller = Optional.of(createSpiller());
            return spiller.get().spill(index.getPages());
        }

        // Otherwise this is stale revoking request
        long reservedRevocableBytes = operatorContext.getReservedRevocableBytes();
        if (reservedRevocableBytes == 0) {
            return immediateFuture(null);
        }
        throw new IllegalStateException(format("Remaining %s revocable bytes which I don't know how to revoke", reservedRevocableBytes));
    }

    private SingleStreamSpiller createSpiller()
    {
        return singleStreamSpillerFactory.create(index.getTypes(),
                operatorContext.getSpillContext().newLocalSpillContext(),
                operatorContext.getSystemMemoryContext().newLocalMemoryContext());
    }

    @Override
    public void finishMemoryRevoke()
    {
        if (state == State.CONSUMING_INPUT) {
            index.clear();
            operatorContext.setMemoryReservation(index.getEstimatedSize().toBytes());
            operatorContext.setRevocableMemoryReservation(0L);
            state = State.SPILLING_INPUT;
            return;
        }

        if (state == State.LOOKUP_SOURCE_BUILT) {
            // TODO is there any risk of starvation here? RW-lock in PLSF...
            lookupSourceFactory.setPartitionSpilledLookupSource(partitionIndex, spilledLookupSourceHandle);
            index.clear();
            operatorContext.setMemoryReservation(index.getEstimatedSize().toBytes());
            operatorContext.setRevocableMemoryReservation(0L);
            state = State.LOOKUP_SOURCE_SPILLED;
            return;
        }

        long reservedRevocableBytes = operatorContext.getReservedRevocableBytes();
        if (reservedRevocableBytes == 0) {
            return;
        }

        throw new IllegalStateException("Cannot finish unknown revoking");
    }

    @Override
    public Page getOutput()
    {
        return null;
    }

    @Override
    public void finish()
    {
        switch (state) {
            case CONSUMING_INPUT:
                finishInput();
                return;

            case LOOKUP_SOURCE_BUILT:
                disposeLookupSourceIfRequested();
                return;

            case SPILLING_INPUT:
                finishSpilledInput();
                return;

            case LOOKUP_SOURCE_SPILLED:
                unspillLookupSourceIfRequested();
                return;

            case LOOKUP_SOURCE_UNSPILLING:
                finishLookupSourceUnspilling();
                return;

            case LOOKUP_SOURCE_UNSPILLED_AND_BUILT:
                disposeUnspilledLookupSourceIfRequested();
                return;

            case DISPOSED:
                // no-op
                return;
        }
        throw new IllegalStateException("Unhandled state: " + state);
    }

    private void finishInput()
    {
        LookupSourceSupplier partition = buildLookupSource();
        if (spillEnabled) {
            operatorContext.setRevocableMemoryReservation(partition.get().getInMemorySizeInBytes());
        }
        else {
            operatorContext.setMemoryReservation(partition.get().getInMemorySizeInBytes());
        }
        lookupSourceNotNeeded = Optional.of(lookupSourceFactory.setPartitionLookupSourceSupplier(partitionIndex, partition));

        state = State.LOOKUP_SOURCE_BUILT;
    }

    private void disposeLookupSourceIfRequested()
    {
        checkState(lookupSourceNotNeeded.isPresent());
        if (!lookupSourceNotNeeded.get().isDone()) {
            return;
        }

        index.clear();
        operatorContext.setRevocableMemoryReservation(0L);
        operatorContext.setMemoryReservation(index.getEstimatedSize().toBytes());
        state = State.DISPOSED;
    }

    private void finishSpilledInput()
    {
        if (!spillInProgress.isDone()) {
            // Not ready to handle finish() yet
            return;
        }
        checkNoFailure(spillInProgress);
        lookupSourceFactory.setPartitionSpilledLookupSource(partitionIndex, spilledLookupSourceHandle);

        state = State.LOOKUP_SOURCE_SPILLED;
    }

    private void unspillLookupSourceIfRequested()
    {
        if (!spilledLookupSourceHandle.getUnspillingRequested().isDone()) {
            // Nothing to do yet.
            return;
        }

        checkState(!unspillInProgress.isPresent());
        operatorContext.setMemoryReservation(spilledPagesInMemorySize + index.getEstimatedSize().toBytes());
        unspillInProgress = Optional.of(spiller.get().getAllSpilledPages());

        state = State.LOOKUP_SOURCE_UNSPILLING;
    }

    private void finishLookupSourceUnspilling()
    {
        if (!unspillInProgress.get().isDone()) {
            // Pages have not be unspilled yet.
            return;
        }

        // Use Queue so that Pages already consumed by Index are not retained by us.
        Queue<Page> pages = new ArrayDeque<>(getDone(unspillInProgress.get()));
        long memoryRetainedByRemainingPages = pages.stream()
                .mapToLong(Page::getRetainedSizeInBytes)
                .sum();
        operatorContext.setMemoryReservation(memoryRetainedByRemainingPages + index.getEstimatedSize().toBytes());

        while (!pages.isEmpty()) {
            Page next = pages.remove();
            index.addPage(next);
            // There is no attempt to compact index, since unspilled pages are unlikely to have blocks with retained size > logical size.
            memoryRetainedByRemainingPages -= next.getRetainedSizeInBytes();
            operatorContext.setMemoryReservation(memoryRetainedByRemainingPages + index.getEstimatedSize().toBytes());
        }

        LookupSource lookupSource = buildLookupSource().get();
        operatorContext.setMemoryReservation(lookupSource.getInMemorySizeInBytes());

        // TODO shared LS or Supplier<LS> ?
        spilledLookupSourceHandle.setLookupSource(lookupSource);

        state = State.LOOKUP_SOURCE_UNSPILLED_AND_BUILT;
    }

    private void disposeUnspilledLookupSourceIfRequested()
    {
        if (!spilledLookupSourceHandle.getDisposeRequested().isDone()) {
            return;
        }

        index.clear();
        operatorContext.setMemoryReservation(index.getEstimatedSize().toBytes());

        state = State.DISPOSED;
    }

    private LookupSourceSupplier buildLookupSource()
    {
        // TODO when partition is disposed (either normal or unspilled one), make sure no-one holds references to it. PartitionedConsumption currently does. Who else?
        // TODO Maybe finished, but not closed yet, LJOs? Who else

        LookupSourceSupplier partition = index.createLookupSourceSupplier(operatorContext.getSession(), hashChannels, preComputedHashChannel, filterFunctionFactory, Optional.of(outputChannels));
        hashCollisionsCounter.recordHashCollision(partition.getHashCollisions(), partition.getExpectedHashCollisions());
        return partition;
    }

    @Override
    public boolean isFinished()
    {
        // TODO FIXME state == DISPOSED || LSF is destroyed ?
        return lookupSourceFactory.isDestroyed().isDone();
    }

    @Override
    public void close()
            throws Exception
    {
        spiller.ifPresent(SingleStreamSpiller::close);
    }
}
