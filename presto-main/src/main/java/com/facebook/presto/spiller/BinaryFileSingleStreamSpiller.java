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

import com.facebook.presto.execution.buffer.PagesSerde;
import com.facebook.presto.execution.buffer.PagesSerdeUtil;
import com.facebook.presto.memory.LocalMemoryContext;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.ListeningExecutorService;
import io.airlift.concurrent.MoreFutures;
import io.airlift.slice.InputStreamSliceInput;
import io.airlift.slice.OutputStreamSliceOutput;
import io.airlift.slice.RuntimeIOException;
import io.airlift.slice.SliceOutput;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.execution.buffer.PagesSerdeUtil.writePage;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spiller.BinaryFileSingleStreamSpillerFactory.SPILL_FILE_PREFIX;
import static com.facebook.presto.spiller.BinaryFileSingleStreamSpillerFactory.SPILL_FILE_SUFFIX;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

@NotThreadSafe
public class BinaryFileSingleStreamSpiller
        implements SingleStreamSpiller
{
    @VisibleForTesting
    static final int BUFFER_SIZE = 4 * 1024;

    private final Path targetFileName;
    private final Closer closer = Closer.create();
    private final PagesSerde serde;
    private final SpillerStats spillerStats;
    private final LocalSpillContext localSpillContext;
    private final LocalMemoryContext memoryContext;

    private final ListeningExecutorService executor;

    private final AtomicBoolean writable = new AtomicBoolean(true);
    private CompletableFuture<?> spillInProgress = CompletableFuture.completedFuture(null);

    public BinaryFileSingleStreamSpiller(
            PagesSerde serde,
            ListeningExecutorService executor,
            Path spillPath,
            SpillerStats spillerStats,
            LocalSpillContext localSpillContext,
            LocalMemoryContext memoryContext)
    {
        this.serde = requireNonNull(serde, "serde is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.spillerStats = requireNonNull(spillerStats, "spillerStats is null");
        this.localSpillContext = requireNonNull(localSpillContext, "localSpillContext can not be null");
        this.memoryContext = requireNonNull(memoryContext, "memoryContext can not be null");
        try {
            targetFileName = Files.createTempFile(spillPath, SPILL_FILE_PREFIX, SPILL_FILE_SUFFIX);
        }
        catch (IOException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to create spill file", e);
        }
    }

    @Override
    public CompletableFuture<?> spill(Iterator<Page> pageIterator)
    {
        checkNoSpillInProgress();
        checkIsWritable();
        spillInProgress = MoreFutures.toCompletableFuture(executor.submit(
                () -> writePages(pageIterator)));
        return spillInProgress;
    }

    private void checkNoSpillInProgress()
    {
        checkState(spillInProgress.isDone(), "spill in progress");
    }

    private void checkIsWritable()
    {
        checkState(writable.get(), "Spilling no longer allowed. " +
                "The spiller has been made non-writable on first read for subsequent reads to be consistent");
    }

    @Override
    public Iterator<Page> getSpilledPages()
    {
        writable.set(false);
        checkNoSpillInProgress();
        return readPages();
    }

    @Override
    public CompletableFuture<List<Page>> getAllSpilledPages()
    {
        writable.set(false);
        return MoreFutures.toCompletableFuture(executor.submit(() ->
                ImmutableList.copyOf(getSpilledPages())
        ));
    }

    private void writePages(Iterator<Page> pageIterator)
    {
        try (SliceOutput output = new OutputStreamSliceOutput(new FileOutputStream(targetFileName.toFile(), true), BUFFER_SIZE)) {
            memoryContext.setBytes(BUFFER_SIZE);
            while (pageIterator.hasNext()) {
                Page page = pageIterator.next();
                long pageSize = page.getSizeInBytes();
                localSpillContext.updateBytes(pageSize);
                spillerStats.addToTotalSpilledBytes(pageSize);
                writePage(serde, output, page);
            }
        }
        catch (RuntimeIOException | IOException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to spill pages", e);
        }
        finally {
            memoryContext.setBytes(0);
        }
    }

    private Iterator<Page> readPages()
    {
        try {
            InputStream input = new FileInputStream(targetFileName.toFile());
            memoryContext.setBytes(BUFFER_SIZE);
            closer.register(input);
            closer.register(() -> memoryContext.setBytes(0));
            return PagesSerdeUtil.readPages(serde, new InputStreamSliceInput(input, BUFFER_SIZE));
        }
        catch (IOException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to read spilled pages", e);
        }
    }

    @Override
    public void close()
    {
        closer.register(() -> Files.delete(targetFileName));
        closer.register(localSpillContext);

        try {
            closer.close();
        }
        catch (Exception e) {
            throw new PrestoException(
                    GENERIC_INTERNAL_ERROR,
                    String.format("Failed to close spiller"),
                    e);
        }
    }
}
