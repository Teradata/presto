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

import com.facebook.presto.operator.aggregation.AccumulatorFactory;
import com.facebook.presto.operator.aggregation.builder.HashAggregationBuilder;
import com.facebook.presto.operator.aggregation.builder.InMemoryHashAggregationBuilder;
import com.facebook.presto.operator.aggregation.builder.SpillableHashAggregationBuilder;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spiller.SpillerFactory;
import com.facebook.presto.sql.planner.plan.AggregationNode.Step;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.operator.aggregation.builder.InMemoryHashAggregationBuilder.toTypes;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Objects.requireNonNull;

public class HashAggregationOperator
        implements Operator
{
    private static final double MERGE_WITH_MEMORY_RATIO = 0.9;

    public static class HashAggregationOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final List<Type> groupByTypes;
        private final List<Integer> groupByChannels;
        private final Step step;
        private final List<AccumulatorFactory> accumulatorFactories;
        private final Optional<Integer> hashChannel;
        private final int expectedGroups;
        private final List<Type> types;
        private final long maxPartialMemory;
        private final DataSize memoryLimitBeforeSpill;
        private final DataSize memoryLimitForMergeWithMemory;
        private final Optional<SpillerFactory> spillerFactory;

        private boolean closed;

        public HashAggregationOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<? extends Type> groupByTypes,
                List<Integer> groupByChannels,
                Step step,
                List<AccumulatorFactory> accumulatorFactories,
                Optional<Integer> hashChannel,
                int expectedGroups,
                DataSize maxPartialMemory)
        {
            this(operatorId,
                    planNodeId,
                    groupByTypes,
                    groupByChannels,
                    step,
                    accumulatorFactories,
                    hashChannel,
                    expectedGroups,
                    maxPartialMemory,
                    new DataSize(0, MEGABYTE),
                    new DataSize(0, MEGABYTE),
                    Optional.empty());
        }

        public HashAggregationOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<? extends Type> groupByTypes,
                List<Integer> groupByChannels,
                Step step,
                List<AccumulatorFactory> accumulatorFactories,
                Optional<Integer> hashChannel,
                int expectedGroups,
                DataSize maxPartialMemory,
                DataSize memoryLimitBeforeSpill,
                Optional<SpillerFactory> spillerFactory)
        {
            this(operatorId,
                    planNodeId,
                    groupByTypes,
                    groupByChannels,
                    step,
                    accumulatorFactories,
                    hashChannel,
                    expectedGroups,
                    maxPartialMemory,
                    memoryLimitBeforeSpill,
                    DataSize.succinctBytes((long) (memoryLimitBeforeSpill.toBytes() * MERGE_WITH_MEMORY_RATIO)),
                    spillerFactory);
        }

        @VisibleForTesting
        HashAggregationOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<? extends Type> groupByTypes,
                List<Integer> groupByChannels,
                Step step,
                List<AccumulatorFactory> accumulatorFactories,
                Optional<Integer> hashChannel,
                int expectedGroups,
                DataSize maxPartialMemory,
                DataSize memoryLimitBeforeSpill,
                DataSize memoryLimitForMergeWithMemory,
                Optional<SpillerFactory> spillerFactory)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.hashChannel = requireNonNull(hashChannel, "hashChannel is null");
            this.groupByTypes = ImmutableList.copyOf(groupByTypes);
            this.groupByChannels = ImmutableList.copyOf(groupByChannels);
            this.step = step;
            this.accumulatorFactories = ImmutableList.copyOf(accumulatorFactories);
            this.expectedGroups = expectedGroups;
            this.maxPartialMemory = requireNonNull(maxPartialMemory, "maxPartialMemory is null").toBytes();
            this.memoryLimitBeforeSpill = requireNonNull(memoryLimitBeforeSpill, "memoryLimitBeforeSpill is null");
            this.memoryLimitForMergeWithMemory = requireNonNull(memoryLimitForMergeWithMemory, "memoryLimitForMergeWithMemory is null");
            this.spillerFactory = requireNonNull(spillerFactory, "spillerFactory is null");

            this.types = toTypes(groupByTypes, step, accumulatorFactories, hashChannel);
        }

        @Override
        public List<Type> getTypes()
        {
            return types;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");

            OperatorContext operatorContext;
            if (step.isOutputPartial()) {
                operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, HashAggregationOperator.class.getSimpleName(), maxPartialMemory);
            }
            else {
                operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, HashAggregationOperator.class.getSimpleName());
            }
            HashAggregationOperator hashAggregationOperator = new HashAggregationOperator(
                    operatorContext,
                    groupByTypes,
                    groupByChannels,
                    step,
                    accumulatorFactories,
                    hashChannel,
                    expectedGroups,
                    memoryLimitBeforeSpill,
                    memoryLimitForMergeWithMemory,
                    spillerFactory);
            return hashAggregationOperator;
        }

        @Override
        public void close()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new HashAggregationOperatorFactory(
                    operatorId,
                    planNodeId,
                    groupByTypes,
                    groupByChannels,
                    step,
                    accumulatorFactories,
                    hashChannel,
                    expectedGroups,
                    new DataSize(maxPartialMemory, Unit.BYTE),
                    memoryLimitBeforeSpill,
                    memoryLimitForMergeWithMemory,
                    spillerFactory);
        }
    }

    private final OperatorContext operatorContext;
    private final List<Type> groupByTypes;
    private final List<Integer> groupByChannels;
    private final Step step;
    private final List<AccumulatorFactory> accumulatorFactories;
    private final Optional<Integer> hashChannel;
    private final int expectedGroups;
    private final Optional<SpillerFactory> spillerFactory;
    private final DataSize memoryLimitBeforeSpill;
    private final DataSize memoryLimitForMergeWithMemory;

    private final List<Type> types;

    private HashAggregationBuilder aggregationBuilder;
    private Iterator<Page> outputIterator;
    private boolean finishing;

    public HashAggregationOperator(
            OperatorContext operatorContext,
            List<Type> groupByTypes,
            List<Integer> groupByChannels,
            Step step,
            List<AccumulatorFactory> accumulatorFactories,
            Optional<Integer> hashChannel,
            int expectedGroups,
            DataSize memoryLimitBeforeSpill,
            DataSize memoryLimitForMergeWithMemory,
            Optional<SpillerFactory> spillerFactory)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        requireNonNull(step, "step is null");
        requireNonNull(accumulatorFactories, "accumulatorFactories is null");
        requireNonNull(operatorContext, "operatorContext is null");

        this.groupByTypes = ImmutableList.copyOf(groupByTypes);
        this.groupByChannels = ImmutableList.copyOf(groupByChannels);
        this.accumulatorFactories = ImmutableList.copyOf(accumulatorFactories);
        this.hashChannel = requireNonNull(hashChannel, "hashChannel is null");
        this.step = step;
        this.expectedGroups = expectedGroups;
        this.types = toTypes(groupByTypes, step, accumulatorFactories, hashChannel);
        this.spillerFactory = requireNonNull(spillerFactory, "spillerFactory is null");
        this.memoryLimitBeforeSpill = requireNonNull(memoryLimitBeforeSpill, "memoryLimitBeforeSpill is null");
        this.memoryLimitForMergeWithMemory = requireNonNull(memoryLimitForMergeWithMemory, "memoryLimitForMergeWithMemory is null");
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public List<Type> getTypes()
    {
        return types;
    }

    @Override
    public void finish()
    {
        finishing = true;
    }

    @Override
    public boolean isFinished()
    {
        return finishing && aggregationBuilder == null && (outputIterator == null || !outputIterator.hasNext());
    }

    @Override
    public boolean needsInput()
    {
        if (finishing || outputIterator != null) {
            return false;
        }
        else if (aggregationBuilder != null && (aggregationBuilder.checkFullAndUpdateMemory() || aggregationBuilder.isBusy())) {
            return false;
        }
        else {
            return true;
        }
    }

    @Override
    public void addInput(Page page)
    {
        checkState(!finishing, "Operator is already finishing");
        requireNonNull(page, "page is null");
        if (aggregationBuilder == null) {
            if (step.isOutputPartial() || memoryLimitBeforeSpill.toBytes() == 0) {
                aggregationBuilder = new InMemoryHashAggregationBuilder(
                        accumulatorFactories,
                        step,
                        expectedGroups,
                        groupByTypes,
                        groupByChannels,
                        hashChannel,
                        operatorContext);
            }
            else {
                aggregationBuilder = new SpillableHashAggregationBuilder(
                        accumulatorFactories,
                        step,
                        expectedGroups,
                        groupByTypes,
                        groupByChannels,
                        hashChannel,
                        operatorContext,
                        memoryLimitBeforeSpill,
                        memoryLimitForMergeWithMemory,
                        spillerFactory);
            }

            // assume initial aggregationBuilder is not full
        }
        else {
            checkState(!aggregationBuilder.checkFullAndUpdateMemory(), "Aggregation buffer is full");
        }
        aggregationBuilder.processPage(page);
    }

    @Override
    public Page getOutput()
    {
        if (outputIterator == null) {
            // current output iterator is done
            outputIterator = null;

            // no data
            if (aggregationBuilder == null) {
                return null;
            }

            // only flush if we are finishing or the aggregation builder is full
            if (!finishing && !aggregationBuilder.checkFullAndUpdateMemory()) {
                return null;
            }

            // don't ask for output if builder is busy
            if (aggregationBuilder.isBusy()) {
                return null;
            }

            outputIterator = aggregationBuilder.buildResult();

            if (!outputIterator.hasNext()) {
                // current output iterator is done
                closeAggregationBuilder();
                return null;
            }
        }

        Page output = outputIterator.next();
        if (!outputIterator.hasNext()) {
            closeAggregationBuilder();
        }
        return output;
    }

    @Override
    public void close()
            throws IOException
    {
        closeAggregationBuilder();
    }

    private void closeAggregationBuilder()
    {
        outputIterator = null;
        if (aggregationBuilder != null) {
            aggregationBuilder.close();
            aggregationBuilder = null;
        }
    }
}
