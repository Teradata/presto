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
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import org.weakref.jmx.Managed;

import javax.annotation.concurrent.GuardedBy;

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
    public synchronized ListenableFuture<?> reserve(long bytes)
    {
        checkArgument(bytes >= 0, "bytes is negative");

        if ((currentBytes + bytes) < maxBytes) {
            currentBytes += bytes;
        }
        else {
            throw ExceededSpillLimitException.exceededLocalLimit(succinctBytes(maxBytes));
        }

        return NOT_BLOCKED;
    }

    public synchronized void free(long bytes)
    {
        checkArgument(bytes >= 0, "bytes is negative");
        checkArgument(currentBytes - bytes >= 0, "tried to free more disk space than is reserved");
        if (bytes == 0) {
            // Freeing zero bytes is a no-op
            return;
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
