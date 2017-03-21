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

import com.facebook.presto.operator.PartitionedConsumption.Partition;
import com.facebook.presto.spi.Page;
import com.google.common.util.concurrent.ListenableFuture;

import javax.annotation.concurrent.GuardedBy;

import java.util.Iterator;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static java.util.Objects.requireNonNull;

public class SpillledLookupJoiner
{
    private final Partition<LookupSource> lookupPartition;
    private final LookupJoiner lookupJoiner;
    private final Iterator<Page> probePages;

    @GuardedBy("this")
    private boolean memoryReserved = false;

    public SpillledLookupJoiner(
            Partition<LookupSource> lookupPartition,
            LookupJoiner lookupJoiner,
            Iterator<Page> probePages)
    {
        this.lookupPartition = requireNonNull(lookupPartition, "lookupPartition is null");
        this.lookupJoiner = lookupJoiner;
        this.probePages = requireNonNull(probePages, "probePages is null");
    }

    public synchronized void reserveMemory(SharedMemoryContext sharedMemoryContext, OperatorContext operatorContext)
    {
        if (!memoryReserved && lookupPartition.load().isDone()) {
            sharedMemoryContext.reserve(lookupPartition.number(), operatorContext, getInMemorySizeInBytes());
            memoryReserved = true;
        }
    }

    public ListenableFuture<?> isBlocked()
    {
        return lookupJoiner.isBlocked();
    }

    public Page getOutput()
    {
        if (!lookupJoiner.needsInput()) {
            return lookupJoiner.getOutput();
        }
        if (!probePages.hasNext()) {
            lookupJoiner.finish();
            return null;
        }
        Page probePage = probePages.next();
        lookupJoiner.addInput(probePage);

        return lookupJoiner.getOutput();
    }

    public boolean isFinished()
    {
        return lookupJoiner.isFinished();
    }

    private long getInMemorySizeInBytes()
    {
        checkState(lookupPartition.load().isDone(), "Size is not known yet");
        return getFutureValue(lookupPartition.load()).getInMemorySizeInBytes();
    }

    public void finish(SharedMemoryContext sharedMemoryContext)
    {
        checkState(lookupPartition.load().isDone());
        sharedMemoryContext.free(lookupPartition.number(), getInMemorySizeInBytes());
        lookupPartition.release();
    }
}
