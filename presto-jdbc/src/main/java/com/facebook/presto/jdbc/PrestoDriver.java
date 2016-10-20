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
package com.facebook.presto.jdbc;

import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.ImmutableList;
import io.airlift.http.client.jetty.JettyIoPool;
import io.airlift.http.client.jetty.JettyIoPoolConfig;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.logging.Logger;

import static com.facebook.presto.jdbc.ConnectionProperties.USER;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class PrestoDriver
        implements Driver, Closeable
{
    static final int VERSION_MAJOR = 1;
    static final int VERSION_MINOR = 0;

    static final int JDBC_VERSION_MAJOR = 4;
    static final int JDBC_VERSION_MINOR = 1;

    static final String DRIVER_NAME = "Presto JDBC Driver";
    static final String DRIVER_VERSION = VERSION_MAJOR + "." + VERSION_MINOR;

    private static final String DRIVER_URL_START = "jdbc:presto:";

    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final JettyIoPool jettyIoPool;

    private final ReferenceCountingLoadingCache<HttpClientCreator, QueryExecutor> queryExecutorCache;

    static {
        try {
            DriverManager.registerDriver(new PrestoDriver());
        }
        catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    /*
     * The relationship between the PrestoDriver, QueryExecutors, and
     * PrestoConnections is complicated:
     *
     * PrestoConnection <-many:one-> QueryExecutor <-many:one-> PrestoDriver
     *                               (HTTP client)
     *
     * - A PrestoConnection represents the outside world's view of a JDBC
     * connection to a Presto server.
     * - A PrestoConnection contains a QueryExecutor, which is responsible
     * for actually executing queries against the Presto server.
     * - QueryExecutor contains a reference to a driver-wide pool of execution
     * resources, and an HTTP client that executes requests using the resources
     * in that pool.
     *
     * An HTTP client is expensive to create and destroy, and we would like to
     * share it across as many PrestoConnections as possible. If every
     * connection could use the same HTTP client, life would be simple.
     * Unfortunately, some things that can be specified at the PrestoConnection
     * level must be configured at the HTTP client level. The obvious case of
     * this is the SSL/TLS trust store configuration: An HTTP client has to be
     * configured with the appropriate trust store to make SSL connections to a
     * Presto server.
     *
     * The PrestoDriver deals in QueryExecutors, which have a 1:1 relationship
     * with HTTP clients. In order to create many PrestoConnections with the
     * same QueryExecutor, the PrestoDriver maintains a cache of
     * QueryExecutors.
     *
     * This leaves us having to solve one of the two hard problems in computer
     * science: cache invalidation, which in this case, is closely intertwined
     * with the need to eventually close the HTTP client contained in the
     * cached QueryExecutor.
     *
     * Conceptually, the flow is simple:
     * driver.connect() returns either
     * 1) A cached QueryExecutor that has an HTTP client that satisfies the
     *    requested connection properties.
     * 2) A new QueryExecutor with a new HTTP client if no cached
     *    QueryExecutor is satisfactory.
     *
     * connection.close() calls queryExecutor.close()
     * If this is the last reference to the queryExecutor, close the HTTP
     * client and invalidate() queryExecutor from the cache.
     *
     * Now we need a reference count. And things get hairy.
     *
     * com.google.common.cache.LoadingCache is safe for concurrent access, so
     * the get() call in driver.connect() and the invalidate() call in
     * queryExecutor.close() can't get us into a state where somebody has
     * gotten() a reference that's already been invalidated().
     *
     * The problem is actually the refcount; there's no way to atomically get()
     * a QueryExecutor from the cache and increment its refcount. It gets
     * worse: there's no way to know ahead of time which QueryExecutor you're
     * going to get from get(), so the locking for the refcount has to be done
     * against the cache rather than individual QueryExecutors.
     *
     * The solution is to protect the refcounts by synchronizing on the cache
     * and incrementing and decrementing the refcount while holding the cache's
     * monitor.
     *
     * This logic is encapsulated in ReferenceCountingLoadingCache. get() and
     * close() operations call acquire() and release(), respectively.
     *
     * The last wrinkle is that we need to retain released() QueryExecutors
     * for some short time period to handle cases where somebody releases the
     * last reference to a QE, but is going to reacquire it immediately. This
     * requires scheduling cache invalidation into the future, which adds yet
     * more joy to our lives, as detailed below.
     *
     * The cleaner count handles the following situation:
     *
     * Thread T1 has a reference on V for key K.
     * T1 calls release(K1) scheduling future F1 and increments cleaner count -> 1
     * time passes.
     * A daemon thread starts executing F1, but does not acquire the lock
     * Thread T2 calls acquire() and acquires the lock.
     *   acquire() increments the refcount
     *   acquire tries, but fails to cancel F1 because F1 isn't is running
     *     but is not interruptible.
     *   acquire() releases the lock
     *
     * At this point 2 things can happen
     * 1) F1 resumes executing. At this point the refcount is sufficient to
     *    ensure correctness
     * 2) T2 calls release(K1)
     *      release() acquires the lock.
     *      release schedules a cleanup F2 and increments cleaner count -> 2
     *      release fails to cancel F1 for the same reason acquire did.
     *      release() releases the lock.
     *    F1 resumes executing and acquires the lock.
     *      releaseForCleaning() decrements cleaner count -> 1
     *      nothing further happens.
     *    more time passes.
     *    F2 executes
     *      releaseForCleaning() decrements cleaner count -> 0
     *      refcount is 0
     *      K1 is invalidated from the cache, and V1 is disposed.
     *
     * Without the cleaner count, we run the risk that some thread T3 calls
     * acquire(K1), loading V2 while more time passes. F2 is still using V1's
     * Holder, which has a refcount of zero. F1 then invalidates K1, which
     * disposes of V2 while it's still being used by T3. Havoc ensues.
     *
     * Instead of keeping a cleaner count, we could have a boolean flag in the
     * Holder that tracks whether or not the value has been invalidated. The
     * cleaner count has the advantage of ensuring the desired retention period
     * as well as guarding against the above situation, so it's all around
     * better.
     *
     * The other, other approach would be to set an 'invalidate after' time
     * in the holder. Now we have to worry about clock resolution, jitter, and
     * monotonicity. No thanks; somebody did that work for the ScheduledFuture/
     * ScheduledExecutorService, let's take advantage of that.
     *
     * The locking hierarchy is as follows:
     * 1) The monitor on the ReferenceCountingLoadingCache
     * 2) <internal locking of com.google.common.cache.LoadingCache>
     *
     * The last case of interest is the process that is using PrestoDriver
     * calling prestoDriver.close(). This invalidates all of the cached
     * QueryExecutors regardless of their refcount by calling the cache's
     * close() method, and in doing so closes their HTTP clients. If anybody is
     * still trying to use a PrestoConnection created by the driver, they're
     * SOL.
     */
    private static class ReferenceCountingLoadingCache<K, V>
    {
        private class Holder
        {
            private final V value;
            private int refcount;
            private int cleanerCount;
            private ScheduledFuture currentCleanup;

            private Holder(V value)
            {
                this.value = requireNonNull(value, "value is null");
            }

            private V get()
            {
                return value;
            }

            private int getRefcount()
            {
                return refcount;
            }

            private int getCleanerCount()
            {
                return cleanerCount;
            }

            private int reference()
            {
                setCleanup(null);
                return ++refcount;
            }

            private int dereference()
            {
                return --refcount;
            }

            private void setCleanup(ScheduledFuture cleanup)
            {
                if (currentCleanup != null && currentCleanup.cancel(false)) {
                    --cleanerCount;
                }

                checkState(cleanerCount >= 0, "Negative cleanerCount in setCleanup");

                currentCleanup = cleanup;
                if (cleanup != null) {
                    ++cleanerCount;
                }
            }

            private void scheduleCleanup(
                    ScheduledExecutorService cleanupService,
                    Runnable cleanupTask,
                    long delay,
                    TimeUnit unit)
            {
                checkState(refcount == 0, "non-zero refcount in scheduleCleanup");
                ScheduledFuture cleanup = cleanupService.schedule(cleanupTask, delay, unit);
                setCleanup(cleanup);
            }

            private void releaseForCleaning()
            {
                --cleanerCount;
                checkState(cleanerCount >= 0, "Negative cleanerCount in releaseForCleaning");
            }
        }

        private final ScheduledExecutorService valueCleanupService;
        private final LoadingCache<K, Holder> backingCache;
        private final AtomicBoolean closed = new AtomicBoolean(false);

        // TODO: This is probably on the long side. Maybe change to 30 seconds?
        private static final long retentionPeriod = 2;
        private static final TimeUnit retentionUnit = TimeUnit.MINUTES;

        private ReferenceCountingLoadingCache(CacheLoader<K, V> loader, Consumer<V> disposer)
        {
            this.valueCleanupService = new ScheduledThreadPoolExecutor(1, daemonThreadsNamed("cache-cleanup-%s"));
            this.backingCache = CacheBuilder.newBuilder()
                    .removalListener(new RemovalListener<K, Holder>() {
                        @Override
                        public void onRemoval(RemovalNotification<K, Holder> notification)
                        {
                            Holder holder = notification.getValue();
                            /*
                             * The docs say that both the key and value may be
                             * null if they've already been garbage collected.
                             * We aren't using weak or soft keys or values, so
                             * this shouldn't apply.
                             */
                            requireNonNull(holder, format("holder is null while removing key %s", notification.getKey()));
                            checkState(holder.getRefcount() == 0, "Non-zero refcount disposing %s", notification.getKey());
                            checkState(holder.getCleanerCount() == 0, "Non-zero refcount disposing %s", notification.getKey());
                            disposer.accept(holder.get());
                        }
                    })
                    .build(new CacheLoader<K, Holder>() {
                        @Override
                        public Holder load(K key)
                                throws Exception
                        {
                            return new Holder(loader.load(key));
                        }
                    });
        }

        private void close()
        {
            if (closed.compareAndSet(false, true)) {
                valueCleanupService.shutdownNow();
                backingCache.invalidateAll();
            }
        }

        private V acquire(K key)
        {
            synchronized (this) {
                Holder holder = backingCache.getUnchecked(key);
                holder.reference();
                return holder.get();
            }
        }

        private void release(K key)
        {
            /*
             * Access through the Map interface to avoid creating a Holder and immediately
             * scheduling it for cleanup.
             */
            Holder holder = backingCache.asMap().get(key);
            if (holder == null) {
                return;
            }

            Runnable deferredRelease = () -> {
                synchronized (this) {
                    holder.releaseForCleaning();
                    if (holder.getCleanerCount() ==  0 && holder.getRefcount() == 0) {
                        backingCache.invalidate(key);
                    }
                }
            };

            synchronized (this) {
                if (holder.dereference() == 0) {
                    holder.scheduleCleanup(valueCleanupService, deferredRelease, retentionPeriod, retentionUnit);
                }
            }
        }
    }

    public PrestoDriver()
    {
        this.jettyIoPool = new JettyIoPool("presto-jdbc", new JettyIoPoolConfig());
        this.queryExecutorCache = new ReferenceCountingLoadingCache<>(
                new CacheLoader<HttpClientCreator, QueryExecutor>() {
                    @Override
                    public QueryExecutor load(HttpClientCreator clientCreator)
                    {
                        return QueryExecutor.create(clientCreator.create(QueryExecutor.baseClientConfig()));
                    }
                },
                QueryExecutor::close);
    }

    @Override
    public void close()
    {
        if (closed.compareAndSet(false, true)) {
            queryExecutorCache.close();
            jettyIoPool.close();
        }
    }

    @Override
    public Connection connect(String url, Properties driverProperties)
            throws SQLException
    {
        if (closed.get()) {
            throw new SQLException("Already closed");
        }

        if (!acceptsURL(url)) {
            return null;
        }

        PrestoConnectionConfig uri = new PrestoConnectionConfig(url, driverProperties);
        String user = USER.getValue(uri.getConnectionProperties()).get();

        HttpClientCreator clientCreator = uri.getCreator(DRIVER_NAME + "/" + DRIVER_VERSION, jettyIoPool);

        QueryExecutor queryExecutor = queryExecutorCache.acquire(clientCreator);
        return new PrestoConnection(uri, user, queryExecutor) {
            @Override
            public void close()
                    throws SQLException
            {
                queryExecutorCache.release(clientCreator);
                super.close();
            }
        };
    }

    @Override
    public boolean acceptsURL(String url)
            throws SQLException
    {
        return url.startsWith(DRIVER_URL_START);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties driverProperties)
            throws SQLException
    {
        PrestoConnectionConfig uri = new PrestoConnectionConfig(url, driverProperties);
        ImmutableList.Builder<DriverPropertyInfo> result = ImmutableList.builder();

        Properties mergedProperties = uri.getConnectionProperties();
        for (ConnectionProperty property : ConnectionProperties.allOf()) {
            result.add(property.getDriverPropertyInfo(mergedProperties));
        }

        return result.build().toArray(new DriverPropertyInfo[0]);
    }

    @Override
    public int getMajorVersion()
    {
        return VERSION_MAJOR;
    }

    @Override
    public int getMinorVersion()
    {
        return VERSION_MINOR;
    }

    @Override
    public boolean jdbcCompliant()
    {
        // TODO: pass compliance tests
        return false;
    }

    @Override
    public Logger getParentLogger()
            throws SQLFeatureNotSupportedException
    {
        // TODO: support java.util.Logging
        throw new SQLFeatureNotSupportedException();
    }
}
