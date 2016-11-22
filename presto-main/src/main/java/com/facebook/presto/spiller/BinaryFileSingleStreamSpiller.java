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

import com.facebook.presto.block.PagesSerde;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.ListeningExecutorService;
import io.airlift.concurrent.MoreFutures;
import io.airlift.slice.InputStreamSliceInput;
import io.airlift.slice.OutputStreamSliceOutput;
import io.airlift.slice.RuntimeIOException;
import io.airlift.slice.SliceOutput;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spiller.BinaryFileSingleStreamSpillerFactory.SPILL_FILE_PREFIX;
import static com.facebook.presto.spiller.BinaryFileSingleStreamSpillerFactory.SPILL_FILE_SUFFIX;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

@NotThreadSafe
public class BinaryFileSingleStreamSpiller
        implements SingleStreamSpiller
{
    private final Path targetFileName;
    private final Closer closer = Closer.create();
    private final BlockEncodingSerde blockEncodingSerde;
    private final SpillerStats spillerStats;
    private final LocalSpillContext localSpillContext;

    private final ListeningExecutorService executor;

    private CompletableFuture<?> spillInProgress = CompletableFuture.completedFuture(null);

    public BinaryFileSingleStreamSpiller(
            BlockEncodingSerde blockEncodingSerde,
            ListeningExecutorService executor,
            Path spillPath,
            SpillerStats spillerStats,
            LocalSpillContext localSpillContext)
    {
        this.blockEncodingSerde = requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.spillerStats = requireNonNull(spillerStats, "spillerStats is null");
        this.localSpillContext = localSpillContext;
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
        spillInProgress = MoreFutures.toCompletableFuture(executor.submit(
                () -> writePages(pageIterator)));
        return spillInProgress;
    }

    private void checkNoSpillInProgress()
    {
        checkState(spillInProgress.isDone(), "spill in progress");
    }

    @Override
    public Iterator<Page> getSpilledPages()
    {
        checkNoSpillInProgress();
        return readPages();
    }

    private void writePages(Iterator<Page> pageIterator)
    {
        try (SliceOutput output = new OutputStreamSliceOutput(new BufferedOutputStream(new FileOutputStream(targetFileName.toFile(), true)))) {
            PagesSerde.PagesWriter pagesWriter = new PagesSerde.PagesWriter(blockEncodingSerde, output);
            writePages(pagesWriter, pageIterator);
        }
        catch (RuntimeIOException | IOException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to spill pages", e);
        }
    }

    private void writePages(PagesSerde.PagesWriter pagesWriter, Iterator<Page> pages)
    {
        while (pages.hasNext()) {
            Page page = pages.next();
            long pageSize = page.getSizeInBytes();
            localSpillContext.updateBytes(pageSize);
            spillerStats.addToTotalSpilledBytes(pageSize);
            pagesWriter.append(page);
        }
    }

    private Iterator<Page> readPages()
    {
        try {
            InputStream input = new BufferedInputStream(new FileInputStream(targetFileName.toFile()));
            closer.register(input);
            return PagesSerde.readPages(blockEncodingSerde, new InputStreamSliceInput(input));
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
