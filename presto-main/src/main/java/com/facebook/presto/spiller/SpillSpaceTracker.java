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

import com.facebook.presto.ExceededSpillLimitException;
import com.facebook.presto.spi.QueryId;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import org.weakref.jmx.Managed;

import javax.annotation.concurrent.GuardedBy;

import java.util.HashMap;
import java.util.Map;

import static com.facebook.presto.operator.Operator.NOT_BLOCKED;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.units.DataSize.succinctBytes;
import static java.util.Objects.requireNonNull;

public class SpillSpaceTracker
{
    private final long maxBytes;

    @GuardedBy("this")
    private long currentBytes;

    @GuardedBy("this")
    // TODO: It would be better if we just tracked QueryContexts, but their lifecycle is managed by a weak reference, so we can't do that
    private final Map<QueryId, Long> queryMemoryReservations = new HashMap<>();

    public SpillSpaceTracker(DataSize size)
    {
        requireNonNull(size, "size is null");
        maxBytes = size.toBytes();
        currentBytes = 0;
    }

    /**
     * Reserves the given number of bytes to spill. If more than the maximum, throws an exception.
     * @throws ExceededSpillLimitException
     */
    public synchronized ListenableFuture<?> reserve(QueryId queryId, long bytes)
    {
        checkArgument(bytes >= 0, "bytes is negative");

        if ((currentBytes + bytes) < maxBytes) {
            if (bytes != 0) {
                queryMemoryReservations.merge(queryId, bytes, Long::sum);
            }
            currentBytes += bytes;
        }
        else {
            throw ExceededSpillLimitException.exceededLocalLimit(succinctBytes(maxBytes));
        }

        return NOT_BLOCKED;
    }

    public synchronized void free(QueryId queryId, long bytes)
    {
        checkArgument(bytes >= 0, "bytes is negative");
        checkArgument(currentBytes - bytes >= 0, "tried to free more disk space than is reserved");
        if (bytes == 0) {
            // Freeing zero bytes is a no-op
            return;
        }

        Long queryReservation = queryMemoryReservations.get(queryId);
        requireNonNull(queryReservation, "queryReservation is null");
        checkArgument(queryReservation - bytes >= 0, "tried to free more memory than is reserved by query");
        queryReservation -= bytes;
        if (queryReservation == 0) {
            queryMemoryReservations.remove(queryId);
        }
        else {
            queryMemoryReservations.put(queryId, queryReservation);
        }
        currentBytes -= bytes;
    }

    /**
     * Returns the number of bytes currently on disk.
     */
    @Managed
    public synchronized long getCurrentBytes()
    {
        return currentBytes;
    }

    @Managed
    public synchronized long getMaxBytes()
    {
        return maxBytes;
    }

    @Override
    public synchronized String toString()
    {
        return toStringHelper(this)
                .add("maxBytes", maxBytes)
                .add("currentBytes", currentBytes)
                .toString();
    }
}
