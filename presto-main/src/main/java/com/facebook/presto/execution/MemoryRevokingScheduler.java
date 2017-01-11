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
package com.facebook.presto.execution;

import com.facebook.presto.memory.LocalMemoryManager;
import com.facebook.presto.memory.MemoryPool;
import com.facebook.presto.memory.QueryContext;
import com.facebook.presto.memory.TraversingQueryContextVisitor;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import io.airlift.log.Logger;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public class MemoryRevokingScheduler
{
    private static final Logger log = Logger.get(MemoryRevokingScheduler.class);

    private static final Ordering<SqlTask> ORDER_BY_CREATE_TIME = Ordering.natural().onResultOf(task -> task.getTaskInfo().getStats().getCreateTime());
    private final List<MemoryPool> memoryPools;
    private final Supplier<? extends Collection<SqlTask>> currentTasksSupplier;
    private final ScheduledExecutorService taskManagementExecutor;
    private final double memoryRevokingThreshold;
    private final double memoryRevokingTarget;

    @Inject
    public MemoryRevokingScheduler(
            LocalMemoryManager localMemoryManager,
            SqlTaskManager sqlTaskManager,
            TaskManagementExecutor taskManagementExecutor,
            FeaturesConfig config)
    {
        requireNonNull(localMemoryManager, "localMemoryManager can not be null");
        this.memoryPools = ImmutableList.of(localMemoryManager.getPool(LocalMemoryManager.GENERAL_POOL), localMemoryManager.getPool(LocalMemoryManager.RESERVED_POOL));
        this.currentTasksSupplier = requireNonNull(sqlTaskManager, "sqlTaskManager cannot be null")::getAllTasks;
        this.taskManagementExecutor = requireNonNull(taskManagementExecutor, "taskManagementExecutor cannot be null").getExecutor();
        this.memoryRevokingThreshold = config.getMemoryRevokingThreshold();
        this.memoryRevokingTarget = config.getMemoryRevokingTarget();
    }

    @VisibleForTesting
    MemoryRevokingScheduler(
            List<MemoryPool> memoryPools,
            Supplier<? extends Collection<SqlTask>> currentTasksSupplier,
            ScheduledExecutorService taskManagementExecutor,
            double memoryRevokingThreshold,
            double memoryRevokingTarget)
    {
        this.memoryPools = ImmutableList.copyOf(memoryPools);
        this.currentTasksSupplier = currentTasksSupplier;
        this.taskManagementExecutor = taskManagementExecutor;
        this.memoryRevokingThreshold = memoryRevokingThreshold;
        this.memoryRevokingTarget = memoryRevokingTarget;
    }

    @PostConstruct
    public void start()
    {
        taskManagementExecutor.scheduleWithFixedDelay(() -> {
            try {
                requestMemoryRevokingIfNeeded(currentTasksSupplier.get());
            }
            catch (Throwable e) {
                log.warn(e, "Error requesting system memory revoking");
            }
        }, 0, 1, TimeUnit.SECONDS);
    }

    public void requestMemoryRevokingIfNeeded(Collection<SqlTask> sqlTasks)
    {
        memoryPools.forEach(memoryPool -> requestMemoryRevokingIfNeeded(sqlTasks, memoryPool));
    }

    private void requestMemoryRevokingIfNeeded(Collection<SqlTask> sqlTasks, MemoryPool memoryPool)
    {
        long freeBytes = memoryPool.getFreeBytes();
        if (freeBytes > memoryPool.getMaxBytes() * (1.0 - memoryRevokingThreshold)) {
            return;
        }

        long remainingBytesToRevoke = (long) (-freeBytes + (memoryPool.getMaxBytes() * (1.0 - memoryRevokingTarget)));
        remainingBytesToRevoke -= getMemoryAlreadyBeingRevoked(sqlTasks, memoryPool);
        requestRevoking(remainingBytesToRevoke, sqlTasks, memoryPool);
    }

    private long getMemoryAlreadyBeingRevoked(Collection<SqlTask> sqlTasks, MemoryPool memoryPool)
    {
        AtomicLong memoryAlreadyBeingRevoked = new AtomicLong();
        sqlTasks.stream()
                .filter(task -> task.getTaskInfo().getTaskStatus().getState() == TaskState.RUNNING)
                .filter(task -> task.getQueryContext().getMemoryPool() == memoryPool)
                .forEach(task -> task.getQueryContext().accept(new TraversingQueryContextVisitor<Void, Void>()
                {
                    @Override
                    public Void visitOperatorContext(OperatorContext operatorContext, Void context)
                    {
                        if (operatorContext.isMemoryRevokingRequested()) {
                            memoryAlreadyBeingRevoked.addAndGet(operatorContext.getReservedRevocableBytes());
                        }
                        return null;
                    }
                }, null));
        return memoryAlreadyBeingRevoked.get();
    }

    private void requestRevoking(long remainingBytesToRevoke, Collection<SqlTask> sqlTasks, MemoryPool memoryPool)
    {
        AtomicLong remainingBytesToRevokeAtomic = new AtomicLong(remainingBytesToRevoke);
        sqlTasks.stream()
                .filter(task -> task.getTaskInfo().getTaskStatus().getState() == TaskState.RUNNING)
                .filter(task -> task.getQueryContext().getMemoryPool() == memoryPool)
                .sorted(ORDER_BY_CREATE_TIME)
                .forEach(task -> task.getQueryContext().accept(new TraversingQueryContextVisitor<AtomicLong, Void>()
                {
                    @Override
                    public Void visitQueryContext(QueryContext queryContext, AtomicLong remainingBytesToRevoke)
                    {
                        if (remainingBytesToRevoke.get() < 0) {
                            // exit immediately if no work needs to be done
                            return null;
                        }
                        return super.visitQueryContext(queryContext, remainingBytesToRevoke);
                    }

                    @Override
                    public Void visitOperatorContext(OperatorContext operatorContext, AtomicLong remainingBytesToRevoke)
                    {
                        if (remainingBytesToRevoke.get() > 0) {
                            long revokedBytes = operatorContext.requestMemoryRevoking();
                            if (revokedBytes > 0) {
                                remainingBytesToRevoke.addAndGet(-revokedBytes);
                                log.info("(%s)requested revoking %s; remaining %s", memoryPool.getId(), revokedBytes, remainingBytesToRevoke.get());
                            }
                        }
                        return null;
                    }
                }, remainingBytesToRevokeAtomic));
    }
}
