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

import com.facebook.presto.RowPagesBuilder;
import com.facebook.presto.SequencePageBuilder;
import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.memory.AggregatedMemoryContext;
import com.facebook.presto.operator.PartitionFunction;
import com.facebook.presto.operator.SpillContext;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spiller.PartitioningSpiller.PartitioningSpillResult;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.List;
import java.util.function.IntPredicate;

import static com.facebook.presto.operator.PageAssertions.assertPageEquals;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.io.Files.createTempDir;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestGenericPartitioningSpiller
{
    private static final int FIRST_PARTITION_START = -10;
    private static final int SECOND_PARTITION_START = 0;
    private static final int THIRD_PARTITION_START = 10;
    private static final int FOURTH_PARTITION_START = 20;

    private static final List<Type> TYPES = ImmutableList.of(BIGINT, VARCHAR, DOUBLE, BIGINT);
    private static final PartitionFunction POSITIVE_AND_NEGATIVE_PARTITIONS_HASH_GENERATOR = new FourPartitionsPartitionFunction(0);
    private final BlockEncodingSerde blockEncodingSerde = new BlockEncodingManager(new TypeRegistry(ImmutableSet.of(BIGINT, DOUBLE, VARBINARY)));
    private final SingleStreamSpillerFactory singleStreamSpillerFactory = new FileSingleStreamSpillerFactory(blockEncodingSerde, new SpillerStats(), getFeaturesConfig());

    private FeaturesConfig getFeaturesConfig()
    {
        FeaturesConfig featuresConfig = new FeaturesConfig();
        featuresConfig.setSpillerSpillPaths(createTempDir().getAbsolutePath());
        return featuresConfig;
    }

    private final GenericPartitioningSpillerFactory factory = new GenericPartitioningSpillerFactory(singleStreamSpillerFactory);

    @Test
    public void testFileSpiller()
            throws Exception
    {
        try (PartitioningSpiller spiller = factory.create(
                TYPES,
                POSITIVE_AND_NEGATIVE_PARTITIONS_HASH_GENERATOR,
                mockSpillContext(),
                mockMemoryContext())) {

            RowPagesBuilder builder = RowPagesBuilder.rowPagesBuilder(TYPES);
            builder.addSequencePage(10, SECOND_PARTITION_START, 5, 10, 15);
            builder.addSequencePage(10, FIRST_PARTITION_START, -5, 0, 5);

            List<Page> firstSpill = builder.build();

            builder = RowPagesBuilder.rowPagesBuilder(TYPES);
            builder.addSequencePage(10, THIRD_PARTITION_START, 15, 20, 25);
            builder.addSequencePage(10, FOURTH_PARTITION_START, 25, 30, 35);

            List<Page> secondSpill = builder.build();

            IntPredicate spillPartitionMask = ImmutableSet.of(1, 2)::contains;
            PartitioningSpillResult result = spiller.partitionAndSpill(firstSpill.get(0), spillPartitionMask);
            result.getSpillingFuture().get();
            assertEquals(result.getRetained().getPositionCount(), 0);

            result = spiller.partitionAndSpill(firstSpill.get(1), spillPartitionMask);
            result.getSpillingFuture().get();
            assertEquals(result.getRetained().getPositionCount(), 10);

            result = spiller.partitionAndSpill(secondSpill.get(0), spillPartitionMask);
            result.getSpillingFuture().get();
            assertEquals(result.getRetained().getPositionCount(), 0);

            result = spiller.partitionAndSpill(secondSpill.get(1), spillPartitionMask);
            result.getSpillingFuture().get();
            assertEquals(result.getRetained().getPositionCount(), 10);

            builder = RowPagesBuilder.rowPagesBuilder(TYPES);
            builder.addSequencePage(10, SECOND_PARTITION_START, 5, 10, 15);

            List<Page> secondPartition = builder.build();

            builder = RowPagesBuilder.rowPagesBuilder(TYPES);
            builder.addSequencePage(10, THIRD_PARTITION_START, 15, 20, 25);

            List<Page> thirdPartition = builder.build();

            assertSpilledPages(
                    TYPES,
                    spiller,
                    ImmutableList.of(ImmutableList.of(), secondPartition, thirdPartition, ImmutableList.of()));
        }
    }

    @Test
    public void testCloseDuringReading()
            throws Exception
    {
        Iterator<Page> readingInProgress;
        try (PartitioningSpiller spiller = factory.create(
                TYPES,
                POSITIVE_AND_NEGATIVE_PARTITIONS_HASH_GENERATOR,
                mockSpillContext(),
                mockMemoryContext())) {

            Page page = SequencePageBuilder.createSequencePage(TYPES, 10, FIRST_PARTITION_START, 5, 10, 15);
            PartitioningSpillResult spillResult = spiller.partitionAndSpill(page, i -> true);
            assertEquals(spillResult.getRetained().getPositionCount(), 0);
            getFutureValue(spillResult.getSpillingFuture());

            // We get the iterator but we do not exhaust it, so that close happens during reading
            readingInProgress = spiller.getSpilledPages(0);
        }

        try {
            readingInProgress.hasNext();
            fail("Iterator.hasNext() should fail since underlying resources are closed");
        }
        catch (UncheckedIOException ignored) {
            // expected
        }
    }

    private void assertSpilledPages(
            List<Type> types,
            PartitioningSpiller spiller,
            List<List<Page>> expectedPartitions)
    {
        for (int partition = 0; partition < expectedPartitions.size(); partition++) {
            List<Page> actualSpill = ImmutableList.copyOf(spiller.getSpilledPages(partition));
            List<Page> expectedSpill = expectedPartitions.get(partition);

            assertEquals(actualSpill.size(), expectedSpill.size());
            for (int j = 0; j < actualSpill.size(); j++) {
                assertPageEquals(types, actualSpill.get(j), expectedSpill.get(j));
            }
        }
    }

    private static AggregatedMemoryContext mockMemoryContext()
    {
        return new AggregatedMemoryContext();
    }

    private static SpillContext mockSpillContext()
    {
        return bytes -> {
        };
    }

    private static class FourPartitionsPartitionFunction
            implements PartitionFunction
    {
        private final int valueChannel;

        public FourPartitionsPartitionFunction(int valueChannel)
        {
            this.valueChannel = valueChannel;
        }

        @Override
        public int getPartitionCount()
        {
            return 4;
        }

        @Override
        public int getPartition(Page page, int position)
        {
            long value = page.getBlock(valueChannel).getLong(position, 0);
            if (value >= FOURTH_PARTITION_START) {
                return 3;
            }
            if (value >= THIRD_PARTITION_START) {
                return 2;
            }
            if (value >= SECOND_PARTITION_START) {
                return 1;
            }
            return 0;
        }
    }
}
