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
import com.facebook.presto.memory.MemoryPoolListener;
import com.facebook.presto.memory.QueryContext;
import com.facebook.presto.memory.TraversingQueryContextVisitor;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import io.airlift.log.Logger;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static java.util.Collections.synchronizedSet;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class MemoryRevokingScheduler
{
    private static final Logger log = Logger.get(MemoryRevokingScheduler.class);

    private static final Ordering<SqlTask> ORDER_BY_CREATE_TIME = Ordering.natural().onResultOf(task -> task.getTaskInfo().getStats().getCreateTime());
    private final List<MemoryPool> memoryPools;
    private final Supplier<? extends Collection<SqlTask>> currentTasksSupplier;
    private final ScheduledExecutorService taskManagementExecutor;
    private final double memoryRevokingThreshold;
    private final double memoryRevokingTarget;

    private final MemoryPoolListener memoryPoolListener = MemoryPoolListener.onMemoryReserved(this::onMemoryReserved);

    /**
     * Ids of {@link MemoryPool}s that need checking.
     */
    private final Set<MemoryPool> potentiallyOverflowingPools;
    private final AtomicBoolean isRunning = new AtomicBoolean(false);

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
        this.potentiallyOverflowingPools = synchronizedSet(new HashSet<>());
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
        this.potentiallyOverflowingPools = synchronizedSet(new HashSet<>());
    }

    @PostConstruct
    public void start()
    {
        registerPeriodicCheck();
        registerPoolListeners();
    }

    @VisibleForTesting
    void registerPeriodicCheck()
    {
        taskManagementExecutor.scheduleWithFixedDelay(() -> {
            try {
                requestMemoryRevokingIfNeeded();
            }
            catch (Throwable e) {
                log.warn(e, "Error requesting system memory revoking");
            }
        }, 0, 1, TimeUnit.SECONDS);
    }

    @VisibleForTesting
    void registerPoolListeners()
    {
        memoryPools.forEach(memoryPool -> memoryPool.addListener(memoryPoolListener));
    }

    private void onMemoryReserved(MemoryPool memoryPool)
    {
        try {
            if (!memoryRevokingNeeded(memoryPool)) {
                return;
            }

            if (potentiallyOverflowingPools.add(memoryPool)) {
                log.debug("Scheduling check for %s", memoryPool);
                scheduleRevoking();
            }
        }
        catch (Throwable e) {
            log.warn(e, "Error when acting on memory pool reservation");
        }
    }

    @PreDestroy
    public void stop()
    {
        memoryPools.forEach(memoryPool -> memoryPool.removeListener(memoryPoolListener));
    }

    @VisibleForTesting
    void requestMemoryRevokingIfNeeded()
    {
        List<MemoryPool> poolsToCheck = memoryPools.stream()
                .filter(this::memoryRevokingNeeded)
                .collect(toList());
        if (potentiallyOverflowingPools.addAll(poolsToCheck)) {
            log.debug("Scheduling check for pools: ", poolsToCheck);
            scheduleRevoking();
        }
    }

    private void scheduleRevoking()
    {
        taskManagementExecutor.execute(() -> {
            try {
                runMemoryRevoking();
            }
            catch (Throwable e) {
                log.error(e, "Error requesting memory revoking");
            }
        });
    }

    private void runMemoryRevoking()
    {
        while (!potentiallyOverflowingPools.isEmpty()) {
            /*
             * To avoid race, we need to check isRunning only after we check potentiallyOverflowingPools.
             */
            if (!isRunning.compareAndSet(false, true)) {
                return;
            }
            try {
                List<MemoryPool> poolsToCheck;
                synchronized (potentiallyOverflowingPools) {
                    poolsToCheck = new ArrayList<>(potentiallyOverflowingPools);
                    potentiallyOverflowingPools.clear();
                }
                if (!poolsToCheck.isEmpty()) {
                    requestMemoryRevokingIfNeeded(currentTasksSupplier.get(), poolsToCheck);
                }
            }
            finally {
                isRunning.set(false);
            }
        }
    }

    private void requestMemoryRevokingIfNeeded(Collection<SqlTask> sqlTasks, Iterable<MemoryPool> memoryPools)
    {
        for (MemoryPool memoryPool : memoryPools) {
            requestMemoryRevokingIfNeeded(sqlTasks, memoryPool);
        }
    }

    private void requestMemoryRevokingIfNeeded(Collection<SqlTask> sqlTasks, MemoryPool memoryPool)
    {
        if (!memoryRevokingNeeded(memoryPool)) {
            return;
        }

        long remainingBytesToRevoke = (long) (-memoryPool.getFreeBytes() + (memoryPool.getMaxBytes() * (1.0 - memoryRevokingTarget)));
        remainingBytesToRevoke -= getMemoryAlreadyBeingRevoked(sqlTasks, memoryPool);
        requestRevoking(remainingBytesToRevoke, sqlTasks, memoryPool);
    }

    private boolean memoryRevokingNeeded(MemoryPool memoryPool)
    {
        return memoryPool.getReservedRevocableBytes() > 0
                && memoryPool.getFreeBytes() <= memoryPool.getMaxBytes() * (1.0 - memoryRevokingThreshold);
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
