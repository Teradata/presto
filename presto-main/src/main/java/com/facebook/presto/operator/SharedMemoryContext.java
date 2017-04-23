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

import javax.annotation.concurrent.GuardedBy;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;

/**
 * Allows for sharing a memory reservation between a group of parties (operators).
 * The reservation is in effect until at least one of the parties did not {@code free} yet.
 * Only one shared reservation at a time is allowed.
 * <p>
 * The reservations are tracked using a {@code reservationKey} provided by the parties,
 * that must be {@code equal} for all the parties sharing the current reservation.
 * The key is passed and checked upon calls to both {@code reserve} and {@code free}.
 * <p>
 * The {@code reservationBytes} is also checked to be consistent with the current reservation
 * upon calls to both {@code reserve} and {@code free}.
 * <p>
 * The reserved memory is accounted for the taskContext, as there's not a better
 * place for it so far. This is an implementation detail that can be changed later.
 */
public class SharedMemoryContext
{
    @GuardedBy("this")
    private int reservations = 0;

    @GuardedBy("this")
    private Optional<Object> reservationKey;

    @GuardedBy("this")
    private Optional<Long> reservationBytes;

    @GuardedBy("this")
    private Optional<TaskContext> taskContext;

    public synchronized void reserve(Object reservationKey, OperatorContext operatorContext, long inMemorySizeInBytes)
    {
        if (this.reservations == 0) {
            this.reservationKey = Optional.of(reservationKey);
            this.reservationBytes = Optional.of(inMemorySizeInBytes);
            TaskContext taskContext = operatorContext.getDriverContext().getPipelineContext().getTaskContext();
            this.taskContext = Optional.of(taskContext);
            operatorContext.reserveMemory(inMemorySizeInBytes);
            operatorContext.transferMemoryToTaskContext(inMemorySizeInBytes);
        }
        else {
            checkCurrentReservationPresent();
            checkArgument(this.reservationKey.get().equals(reservationKey) && this.reservationBytes.get() == inMemorySizeInBytes,
                    "Inconsistent memory reservation. " + getInconsistenceDescription(reservationKey, inMemorySizeInBytes));
        }
        reservations++;
    }

    public synchronized void free(Object reservationKey, long inMemorySizeInBytes)
    {
        checkArgument(reservations > 0, format(
                "Inconsistent memory release. No current reservation present, " +
                        "requested free for %s bytes for reservation %s.",
                reservationKey,
                inMemorySizeInBytes
        ));
        checkCurrentReservationPresent();
        checkArgument(this.reservationKey.get().equals(reservationKey) && this.reservationBytes.get() == inMemorySizeInBytes,
                "Inconsistent memory de-reservation. " + getInconsistenceDescription(reservationKey, inMemorySizeInBytes));
        reservations--;
        if (reservations == 0) {
            this.reservationKey = null;
            this.reservationBytes = null;
            taskContext.get().freeMemory(inMemorySizeInBytes);
            taskContext = null;
        }
    }

    private void checkCurrentReservationPresent()
    {
        checkState(this.reservationKey.isPresent() && this.reservationBytes.isPresent(),
                "reservationKey, inMemorySizeBytes and taskContext must be present whenever reservations > 0. That's an error.");
    }

    private String getInconsistenceDescription(Object reservationKey, long inMemorySizeInBytes)
    {
        return format("Expected %s bytes for reservation %s, got %s bytes for reservation %s. " +
                        "Current reservations count is %s. " +
                        "This most likely means the operators sharing this memory do not synchronize their reads with each other.",
                this.reservationBytes, this.reservationKey, inMemorySizeInBytes, reservationKey, this.reservations);
    }

    @Override
    public String toString()
    {
        return "SharedMemoryContext{" +
                "reservations=" + reservations +
                ", reservationKey=" + reservationKey +
                ", reservationBytes=" + reservationBytes +
                ", taskContext=" + taskContext +
                '}';
    }
}
