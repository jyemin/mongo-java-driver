/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.internal.connection;

import com.mongodb.MongoInterruptedException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ConnectionId;
import com.mongodb.connection.ConnectionPoolSettings;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerId;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.mongodb.event.ConnectionCheckOutFailedEvent;
import com.mongodb.event.ConnectionCheckOutFailedEvent.Reason;
import com.mongodb.event.ConnectionCheckOutStartedEvent;
import com.mongodb.event.ConnectionCheckedInEvent;
import com.mongodb.event.ConnectionCheckedOutEvent;
import com.mongodb.event.ConnectionClosedEvent;
import com.mongodb.event.ConnectionCreatedEvent;
import com.mongodb.event.ConnectionPoolClearedEvent;
import com.mongodb.event.ConnectionPoolClosedEvent;
import com.mongodb.event.ConnectionPoolCreatedEvent;
import com.mongodb.event.ConnectionPoolListener;
import com.mongodb.event.ConnectionReadyEvent;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.connection.ConcurrentPool.Prune;
import com.mongodb.internal.session.SessionContext;
import com.mongodb.internal.thread.DaemonThreadFactory;
import org.bson.ByteBuf;
import org.bson.codecs.Decoder;
import org.bson.types.ObjectId;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.internal.event.EventListenerHelper.getConnectionPoolListener;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@SuppressWarnings("deprecation")
class DefaultConnectionPool implements ConnectionPool {
    private static final Logger LOGGER = Loggers.getLogger("connection");

    private final ConcurrentPool<UsageTrackingInternalConnection> pool;
    private final ConnectionPoolSettings settings;
    private final AtomicInteger generation = new AtomicInteger(0);
    private final AtomicInteger lastPrunedGeneration = new AtomicInteger(0);
    private final ScheduledExecutorService sizeMaintenanceTimer;
    private ExecutorService asyncGetter;
    private final Runnable maintenanceTask;
    private final ConnectionPoolListener connectionPoolListener;
    private final ServerId serverId;
    private final AtomicInteger numPinnedToCursor = new AtomicInteger(0);
    private final AtomicInteger numPinnedToTransaction = new AtomicInteger(0);

    private final Map<ObjectId, ServerStats> serverStatsMap = new HashMap<>();
    private final ConnectionGenerationSupplier connectionGenerationSupplier;
    private volatile boolean closed;

    DefaultConnectionPool(final ServerId serverId, final InternalConnectionFactory internalConnectionFactory,
                          final ConnectionPoolSettings settings) {
        this.serverId = notNull("serverId", serverId);
        this.settings = notNull("settings", settings);
        UsageTrackingInternalConnectionItemFactory connectionItemFactory =
                new UsageTrackingInternalConnectionItemFactory(internalConnectionFactory);
        pool = new ConcurrentPool<UsageTrackingInternalConnection>(settings.getMaxSize(), connectionItemFactory);
        this.connectionPoolListener = getConnectionPoolListener(settings);
        maintenanceTask = createMaintenanceTask();
        sizeMaintenanceTimer = createMaintenanceTimer();
        connectionPoolCreated(connectionPoolListener, serverId, settings);
        connectionGenerationSupplier = new ConnectionGenerationSupplier() {
            @Override
            public int getGeneration() {
                return generation.get();
            }

            @Override
            public int getGeneration(final ObjectId processIdentifier) {
                return getGenerationFromServerStats(processIdentifier);
            }
        };
    }

    @Override
    public void start() {
        if (sizeMaintenanceTimer != null) {
            sizeMaintenanceTimer.scheduleAtFixedRate(maintenanceTask, settings.getMaintenanceInitialDelay(MILLISECONDS),
                    settings.getMaintenanceFrequency(MILLISECONDS), MILLISECONDS);
        }
    }

    @Override
    public InternalConnection get() {
        return get(settings.getMaxWaitTime(MILLISECONDS), MILLISECONDS);
    }

    @Override
    public InternalConnection get(final long timeout, final TimeUnit timeUnit) {
        PooledConnection pooledConnection;
        try {
            connectionPoolListener.connectionCheckOutStarted(new ConnectionCheckOutStartedEvent(serverId));
            pooledConnection = getPooledConnection(timeout, timeUnit);
        } catch (MongoTimeoutException e) {
            MongoTimeoutException replacementException = createTimeoutException();
            emitCheckOutFailedEvent(replacementException);
            throw replacementException;
        } catch (Throwable t) {
            emitCheckOutFailedEvent(t);
            throw t;
        }
        if (!pooledConnection.opened()) {
            try {
                pooledConnection.open();
                if (pooledConnection.getDescription().getProcessId() != null) {
                    addConnectionToServerStats(pooledConnection.getDescription().getProcessId());
                }
            } catch (Throwable t) {
                if (pooledConnection.wrapped.getDescription().getProcessId() != null) {
                    invalidate(pooledConnection.wrapped.getDescription().getProcessId());
                }
                pool.release(pooledConnection.wrapped, true);
                connectionPoolListener.connectionCheckOutFailed(new ConnectionCheckOutFailedEvent(serverId,
                        Reason.CONNECTION_ERROR));
                throw t;
            }
        }
        connectionPoolListener.connectionCheckedOut(
                new ConnectionCheckedOutEvent(pooledConnection.getDescription().getConnectionId()));

        return pooledConnection;
    }

    @Override
    public void getAsync(final SingleResultCallback<InternalConnection> callback) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Asynchronously getting a connection from the pool for server %s", serverId));
        }

        final SingleResultCallback<InternalConnection> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
        PooledConnection connection = null;

        try {
            connectionPoolListener.connectionCheckOutStarted(new ConnectionCheckOutStartedEvent(serverId));
            connection = getPooledConnection(0, MILLISECONDS);
        } catch (MongoTimeoutException e) {
            // fall through
        } catch (Throwable t) {
            emitCheckOutFailedEvent(t);
            callback.onResult(null, t);
            return;
        }

        if (connection != null) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(format("Asynchronously opening pooled connection %s to server %s",
                                           connection.getDescription().getConnectionId(), serverId));
            }
            openAsync(connection, errHandlingCallback);
        } else {
            final long startTimeMillis = System.currentTimeMillis();
            getAsyncGetter().submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        if (getRemainingWaitTime() <= 0) {
                            errHandlingCallback.onResult(null, createTimeoutException());
                        } else {
                            PooledConnection connection = getPooledConnection(getRemainingWaitTime(), MILLISECONDS);
                            openAsync(connection, errHandlingCallback);
                        }
                    } catch (MongoTimeoutException e) {
                        Exception exception = createTimeoutException();
                        emitCheckOutFailedEvent(exception);
                        errHandlingCallback.onResult(null, exception);
                    } catch (Throwable t) {
                        emitCheckOutFailedEvent(t);
                        errHandlingCallback.onResult(null, t);
                    }
                }

                private long getRemainingWaitTime() {
                    return startTimeMillis + settings.getMaxWaitTime(MILLISECONDS) - System.currentTimeMillis();
                }
            });
        }
    }

    private void emitCheckOutFailedEvent(final Throwable t) {
        if (t instanceof MongoTimeoutException) {
            connectionPoolListener.connectionCheckOutFailed(new ConnectionCheckOutFailedEvent(serverId, Reason.TIMEOUT));
        } else if (t instanceof IllegalStateException && t.getMessage().equals("The pool is closed")) {
            connectionPoolListener.connectionCheckOutFailed(new ConnectionCheckOutFailedEvent(serverId, Reason.POOL_CLOSED));
        } else {
            connectionPoolListener.connectionCheckOutFailed(new ConnectionCheckOutFailedEvent(serverId, Reason.UNKNOWN));
        }
    }

    private void openAsync(final PooledConnection pooledConnection,
                           final SingleResultCallback<InternalConnection> callback) {
        if (pooledConnection.opened()) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(format("Pooled connection %s to server %s is already open",
                                           pooledConnection.getDescription().getConnectionId(), serverId));
            }
            connectionPoolListener.connectionCheckedOut(
                    new ConnectionCheckedOutEvent(pooledConnection.getDescription().getConnectionId()));
            callback.onResult(pooledConnection, null);
        } else {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(format("Pooled connection %s to server %s is not yet open",
                                           pooledConnection.getDescription().getConnectionId(), serverId));
            }
            pooledConnection.openAsync(new SingleResultCallback<Void>() {
                @Override
                public void onResult(final Void result, final Throwable t) {
                    if (t != null) {
                        if (LOGGER.isTraceEnabled()) {
                            LOGGER.trace(format("Pooled connection %s to server %s failed to open",
                                                       pooledConnection.getDescription().getConnectionId(), serverId));
                        }
                        connectionPoolListener.connectionCheckOutFailed(new ConnectionCheckOutFailedEvent(serverId,
                                Reason.CONNECTION_ERROR));
                        callback.onResult(null, t);
                        pool.release(pooledConnection.wrapped, true);
                    } else {
                        if (LOGGER.isTraceEnabled()) {
                            LOGGER.trace(format("Pooled connection %s to server %s is now open",
                                                       pooledConnection.getDescription().getConnectionId(), serverId));
                        }
                        connectionPoolListener.connectionCheckedOut(
                                new ConnectionCheckedOutEvent(pooledConnection.getDescription().getConnectionId()));
                        callback.onResult(pooledConnection, null);
                    }
                }
            });
        }
    }

    private synchronized ExecutorService getAsyncGetter() {
        if (asyncGetter == null) {
            asyncGetter = Executors.newSingleThreadExecutor(new DaemonThreadFactory("AsyncGetter"));
        }
        return asyncGetter;
    }

    private synchronized void shutdownAsyncGetter() {
        if (asyncGetter != null) {
            asyncGetter.shutdownNow();
        }
    }

    @Override
    public void invalidate() {
        LOGGER.debug("Invalidating the connection pool");
        generation.incrementAndGet();
        connectionPoolListener.connectionPoolCleared(new ConnectionPoolClearedEvent(serverId));
    }

    public void invalidate(final ObjectId serverId) {
        LOGGER.debug("Invalidating the connection pool for server id " + serverId);
        incrementGenerationInServerStats(serverId);
        // TODO: serverId term has become overloaded
        connectionPoolListener.connectionPoolCleared(new ConnectionPoolClearedEvent(this.serverId, serverId));
    }

    @Override
    public void close() {
        if (!closed) {
            pool.close();
            if (sizeMaintenanceTimer != null) {
                sizeMaintenanceTimer.shutdownNow();
            }
            shutdownAsyncGetter();
            closed = true;
            connectionPoolListener.connectionPoolClosed(new ConnectionPoolClosedEvent(serverId));
        }
    }

    @Override
    public int getGeneration() {
        return generation.get();
    }

    private int getGeneration(final ObjectId serverId) {
        return getGenerationFromServerStats(serverId);
    }

    /**
     * Synchronously prune idle connections and ensure the minimum pool size.
     */
    public void doMaintenance() {
        if (maintenanceTask != null) {
            maintenanceTask.run();
        }
    }

    private PooledConnection getPooledConnection(final long timeout, final TimeUnit timeUnit) {
        UsageTrackingInternalConnection internalConnection = pool.get(timeout, timeUnit);
        while (shouldPrune(internalConnection)) {
            pool.release(internalConnection, true);
            internalConnection = pool.get(timeout, timeUnit);
        }
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Checked out connection [%s] to server %s", getId(internalConnection), serverId.getAddress()));
        }
        return new PooledConnection(internalConnection);
    }

    private MongoTimeoutException createTimeoutException() {
        int numPinnedToCursor = this.numPinnedToCursor.get();
        int numPinnedToTransaction = this.numPinnedToTransaction.get();
        if (numPinnedToCursor == 0 && numPinnedToTransaction == 0) {
            return new MongoTimeoutException(format("Timed out after %d ms while waiting for a connection to server %s.",
                    settings.getMaxWaitTime(MILLISECONDS), serverId.getAddress()));
        } else {
            // TODO: should this new format only apply to load-balancing mode, only when at least one connection is pinned, or always?
            int numOtherInUse = pool.getInUseCount() - numPinnedToCursor - numPinnedToTransaction;
            return new MongoTimeoutException(format("Timed out after %d ms while waiting for a connection to server %s. Details: "
                    + "maxPoolSize: %d, connections in use by cursors: %d, connections in use by transactions: %d, "
                    + "connections in use by other operations: %d",
                    settings.getMaxWaitTime(MILLISECONDS), serverId.getAddress(),
                    settings.getMaxSize(), numPinnedToCursor, numPinnedToTransaction, numOtherInUse));
        }
    }

    ConcurrentPool<UsageTrackingInternalConnection> getPool() {
        return pool;
    }

    private Runnable createMaintenanceTask() {
        Runnable newMaintenanceTask = null;
        if (shouldPrune() || shouldEnsureMinSize()) {
            newMaintenanceTask = new Runnable() {
                @Override
                public synchronized void run() {
                    try {
                        // TODO: all this logic is suspect for load-balanced mode
                        int curGeneration = generation.get();
                        if (shouldPrune() || curGeneration > lastPrunedGeneration.get()) {
                            if (LOGGER.isDebugEnabled()) {
                                LOGGER.debug(format("Pruning pooled connections to %s", serverId.getAddress()));
                            }
                            pool.prune();
                        }
                        lastPrunedGeneration.set(curGeneration);
                        if (shouldEnsureMinSize()) {
                            if (LOGGER.isDebugEnabled()) {
                                LOGGER.debug(format("Ensuring minimum pooled connections to %s", serverId.getAddress()));
                            }
                            pool.ensureMinSize(settings.getMinSize(), true);
                        }
                    } catch (MongoInterruptedException e) {
                        // don't log interruptions due to the shutdownNow call on the ExecutorService
                    } catch (Exception e) {
                        LOGGER.warn("Exception thrown during connection pool background maintenance task", e);
                    }
                }
            };
        }
        return newMaintenanceTask;
    }

    private ScheduledExecutorService createMaintenanceTimer() {
        if (maintenanceTask == null) {
            return null;
        } else {
            return Executors.newSingleThreadScheduledExecutor(new DaemonThreadFactory("MaintenanceTimer"));
        }
    }

    private boolean shouldEnsureMinSize() {
        return settings.getMinSize() > 0;
    }

    private boolean shouldPrune() {
        return settings.getMaxConnectionIdleTime(MILLISECONDS) > 0 || settings.getMaxConnectionLifeTime(MILLISECONDS) > 0;
    }

    private boolean shouldPrune(final UsageTrackingInternalConnection connection) {
        return fromPreviousGeneration(connection) || pastMaxLifeTime(connection) || pastMaxIdleTime(connection);
    }

    private boolean pastMaxIdleTime(final UsageTrackingInternalConnection connection) {
        return expired(connection.getLastUsedAt(), System.currentTimeMillis(), settings.getMaxConnectionIdleTime(MILLISECONDS));
    }

    private boolean pastMaxLifeTime(final UsageTrackingInternalConnection connection) {
        return expired(connection.getOpenedAt(), System.currentTimeMillis(), settings.getMaxConnectionLifeTime(MILLISECONDS));
    }

    private boolean fromPreviousGeneration(final UsageTrackingInternalConnection connection) {
        // TODO: will a magic number burn us?  Is it worth a hasGeneration method for an internal API?
        int generation = connection.getGeneration();
        if (generation == -1) {
            return false;
        }
        if (connection.getDescription().getProcessId() != null) {
            return getGenerationFromServerStats(connection.getDescription().getProcessId()) > generation;
        } else {
            return this.generation.get() > generation;
        }
    }

    private boolean expired(final long startTime, final long curTime, final long maxTime) {
        return maxTime != 0 && curTime - startTime > maxTime;
    }

    // send both current and deprecated events in order to preserve backwards compatibility
    private void connectionPoolCreated(final ConnectionPoolListener connectionPoolListener, final ServerId serverId,
                                             final ConnectionPoolSettings settings) {
        connectionPoolListener.connectionPoolCreated(new ConnectionPoolCreatedEvent(serverId, settings));
        connectionPoolListener.connectionPoolOpened(new com.mongodb.event.ConnectionPoolOpenedEvent(serverId, settings));
    }

    private void connectionCreated(final ConnectionPoolListener connectionPoolListener, final ConnectionId connectionId) {
        connectionPoolListener.connectionAdded(new com.mongodb.event.ConnectionAddedEvent(connectionId));
        connectionPoolListener.connectionCreated(new ConnectionCreatedEvent(connectionId));
    }

    private void connectionClosed(final ConnectionPoolListener connectionPoolListener, final ConnectionId connectionId,
                                  final ConnectionClosedEvent.Reason reason) {
        connectionPoolListener.connectionRemoved(new com.mongodb.event.ConnectionRemovedEvent(connectionId, getReasonForRemoved(reason)));
        connectionPoolListener.connectionClosed(new ConnectionClosedEvent(connectionId, reason));
    }

    private com.mongodb.event.ConnectionRemovedEvent.Reason getReasonForRemoved(final ConnectionClosedEvent.Reason reason) {
        com.mongodb.event.ConnectionRemovedEvent.Reason removedReason = com.mongodb.event.ConnectionRemovedEvent.Reason.UNKNOWN;
        switch (reason) {
            case STALE:
                removedReason = com.mongodb.event.ConnectionRemovedEvent.Reason.STALE;
                break;
            case IDLE:
                removedReason = com.mongodb.event.ConnectionRemovedEvent.Reason.MAX_IDLE_TIME_EXCEEDED;
                break;
            case ERROR:
                removedReason = com.mongodb.event.ConnectionRemovedEvent.Reason.ERROR;
                break;
            case POOL_CLOSED:
                removedReason = com.mongodb.event.ConnectionRemovedEvent.Reason.POOL_CLOSED;
                break;
            default:
                break;
        }
        return removedReason;
    }

    private ConnectionId getId(final InternalConnection internalConnection) {
        return internalConnection.getDescription().getConnectionId();
    }

    private class PooledConnection implements InternalConnection {
        private final UsageTrackingInternalConnection wrapped;
        private final AtomicBoolean isClosed = new AtomicBoolean();

        PooledConnection(final UsageTrackingInternalConnection wrapped) {
            this.wrapped = notNull("wrapped", wrapped);
        }

        @Override
        public int getGeneration() {
            return wrapped.getGeneration();
        }

        @Override
        public void open() {
            isTrue("open", !isClosed.get());
            wrapped.open();
            connectionPoolListener.connectionReady(new ConnectionReadyEvent(getDescription().getConnectionId()));
        }

        @Override
        public void openAsync(final SingleResultCallback<Void> callback) {
            isTrue("open", !isClosed.get());
            wrapped.openAsync(new SingleResultCallback<Void>() {
                @Override
                public void onResult(final Void result, final Throwable t) {
                    if (t != null) {
                        connectionPoolListener.connectionCheckOutFailed(new ConnectionCheckOutFailedEvent(serverId,
                                Reason.CONNECTION_ERROR));
                        callback.onResult(null, t);
                    } else {
                        connectionPoolListener.connectionReady(new ConnectionReadyEvent(getDescription().getConnectionId()));
                        callback.onResult(result, null);
                    }
                }
            });
        }

        @Override
        public void close() {
            // All but the first call is a no-op
            if (!isClosed.getAndSet(true)) {
                connectionPoolListener.connectionCheckedIn(new ConnectionCheckedInEvent(getId(wrapped)));
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace(format("Checked in connection [%s] to server %s", getId(wrapped), serverId.getAddress()));
                }
                pool.release(wrapped, wrapped.isClosed() || shouldPrune(wrapped));
            }
        }

        @Override
        public boolean opened() {
            isTrue("open", !isClosed.get());
            return wrapped.opened();
        }

        @Override
        public boolean isClosed() {
            return isClosed.get() || wrapped.isClosed();
        }

        @Override
        public ByteBuf getBuffer(final int capacity) {
            return wrapped.getBuffer(capacity);
        }

        @Override
        public void sendMessage(final List<ByteBuf> byteBuffers, final int lastRequestId) {
            isTrue("open", !isClosed.get());
            wrapped.sendMessage(byteBuffers, lastRequestId);
        }

        @Override
        public <T> T sendAndReceive(final CommandMessage message, final Decoder<T> decoder, final SessionContext sessionContext) {
            isTrue("open", !isClosed.get());
            return wrapped.sendAndReceive(message, decoder, sessionContext);
        }

        @Override
        public <T> void send(final CommandMessage message, final Decoder<T> decoder, final SessionContext sessionContext) {
            isTrue("open", !isClosed.get());
            wrapped.send(message, decoder, sessionContext);
        }

        @Override
        public <T> T receive(final Decoder<T> decoder, final SessionContext sessionContext) {
            isTrue("open", !isClosed.get());
            return wrapped.receive(decoder, sessionContext);
        }

        @Override
        public boolean supportsAdditionalTimeout() {
            isTrue("open", !isClosed.get());
            return wrapped.supportsAdditionalTimeout();
        }

        @Override
        public <T> T receive(final Decoder<T> decoder, final SessionContext sessionContext, final int additionalTimeout) {
            isTrue("open", !isClosed.get());
            return wrapped.receive(decoder, sessionContext, additionalTimeout);
        }

        @Override
        public boolean hasMoreToCome() {
            isTrue("open", !isClosed.get());
            return wrapped.hasMoreToCome();
        }

        @Override
        public <T> void sendAndReceiveAsync(final CommandMessage message, final Decoder<T> decoder,
                                            final SessionContext sessionContext, final SingleResultCallback<T> callback) {
            isTrue("open", !isClosed.get());
            wrapped.sendAndReceiveAsync(message, decoder, sessionContext, new SingleResultCallback<T>() {
                @Override
                public void onResult(final T result, final Throwable t) {
                    callback.onResult(result, t);
                }
            });
        }

        @Override
        public ResponseBuffers receiveMessage(final int responseTo) {
            isTrue("open", !isClosed.get());
            return wrapped.receiveMessage(responseTo);
        }

        @Override
        public void sendMessageAsync(final List<ByteBuf> byteBuffers, final int lastRequestId, final SingleResultCallback<Void> callback) {
            isTrue("open", !isClosed.get());
            wrapped.sendMessageAsync(byteBuffers, lastRequestId, new SingleResultCallback<Void>() {
                @Override
                public void onResult(final Void result, final Throwable t) {
                    callback.onResult(null, t);
                }
            });
        }

        @Override
        public void receiveMessageAsync(final int responseTo, final SingleResultCallback<ResponseBuffers> callback) {
            isTrue("open", !isClosed.get());
            wrapped.receiveMessageAsync(responseTo, new SingleResultCallback<ResponseBuffers>() {
                @Override
                public void onResult(final ResponseBuffers result, final Throwable t) {
                    callback.onResult(result, t);
                }
            });
        }

        @Override
        public void markAsPinned(final Connection.PinningMode pinningMode) {
            switch (pinningMode) {
                case CURSOR:
                    numPinnedToCursor.incrementAndGet();
                    break;
                case TRANSACTION:
                    numPinnedToTransaction.incrementAndGet();
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported pinning mode: " + pinningMode);
            }
        }

        @Override
        public void unmarkAsPinned(final Connection.PinningMode pinningMode) {
            switch (pinningMode) {
                case CURSOR:
                    numPinnedToCursor.decrementAndGet();
                    break;
                case TRANSACTION:
                    numPinnedToTransaction.decrementAndGet();
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported pinning mode: " + pinningMode);
            }
        }

        @Override
        public ConnectionDescription getDescription() {
            return wrapped.getDescription();
        }

        @Override
        public ServerDescription getInitialServerDescription() {
            isTrue("open", !isClosed.get());
            return wrapped.getInitialServerDescription();
        }
    }

    private class UsageTrackingInternalConnectionItemFactory implements ConcurrentPool.ItemFactory<UsageTrackingInternalConnection> {
        private final InternalConnectionFactory internalConnectionFactory;

        UsageTrackingInternalConnectionItemFactory(final InternalConnectionFactory internalConnectionFactory) {
            this.internalConnectionFactory = internalConnectionFactory;
        }

        @Override
        public UsageTrackingInternalConnection create(final boolean initialize) {
            InternalConnection wrappedInternalConnection = internalConnectionFactory.create(serverId, connectionGenerationSupplier);
            UsageTrackingInternalConnection internalConnection = new UsageTrackingInternalConnection(wrappedInternalConnection
            );
            ConnectionId id = getId(internalConnection);
            connectionCreated(connectionPoolListener, id);
            if (initialize) {
                internalConnection.open();
                connectionPoolListener.connectionReady(new ConnectionReadyEvent(id));
            }

            return internalConnection;
        }

        @Override
        public void close(final UsageTrackingInternalConnection connection) {
            connectionClosed(connectionPoolListener, getId(connection), getReasonForClosing(connection));
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info(format("Closed connection [%s] to %s because %s.", getId(connection), serverId.getAddress(),
                                  getReasonStringForClosing(connection)));
            }
            connection.close();
            if (connection.getDescription().getProcessId() != null) {
                removeConnectionFromServerStats(connection.getDescription().getProcessId());
            }
        }

        private String getReasonStringForClosing(final UsageTrackingInternalConnection connection) {
            String reason;
            if (connection.isClosed()) {
                reason = "there was a socket exception raised by this connection";
            } else if (fromPreviousGeneration(connection)) {
                reason = "there was a socket exception raised on another connection from this pool";
            } else if (pastMaxLifeTime(connection)) {
                reason = "it is past its maximum allowed life time";
            } else if (pastMaxIdleTime(connection)) {
                reason = "it is past its maximum allowed idle time";
            } else {
                reason = "the pool has been closed";
            }
            return reason;
        }

        private ConnectionClosedEvent.Reason getReasonForClosing(final UsageTrackingInternalConnection connection) {
            ConnectionClosedEvent.Reason reason;
            if (connection.isClosed()) {
                reason = ConnectionClosedEvent.Reason.ERROR;
            } else if (fromPreviousGeneration(connection)) {
                reason = ConnectionClosedEvent.Reason.STALE;
            } else if (pastMaxIdleTime(connection)) {
                reason = ConnectionClosedEvent.Reason.IDLE;
            } else {
                reason = ConnectionClosedEvent.Reason.POOL_CLOSED;
            }
            return reason;
        }

        @Override
        public Prune shouldPrune(final UsageTrackingInternalConnection usageTrackingConnection) {
            return DefaultConnectionPool.this.shouldPrune(usageTrackingConnection) ? Prune.YES : Prune.NO;
        }
    }

    private ServerStats getServerStats(final ObjectId serverId) {
        ServerStats serverStats = serverStatsMap.get(serverId);
        if (serverStats == null) {
            serverStats = new ServerStats();
            serverStatsMap.put(serverId, serverStats);
        }
        return serverStats;
    }

    private synchronized void addConnectionToServerStats(final ObjectId serverId) {
        ServerStats serverStats = getServerStats(serverId);
        serverStats.incrementCount();
    }

    private synchronized void removeConnectionFromServerStats(final ObjectId serverId) {
        ServerStats serverStats = serverStatsMap.get(serverId);
        // assert not null
        serverStats.decrementCount();
        if (serverStats.getConnectionCount() == 0) {
            serverStatsMap.remove(serverId);
        }
    }

    private synchronized void incrementGenerationInServerStats(final ObjectId serverId) {
        ServerStats serverStats = getServerStats(serverId);
        // assert not null
        serverStats.incrementGeneration();
    }

    private synchronized int getGenerationFromServerStats(final ObjectId serverId) {
        ServerStats serverStats = getServerStats(serverId);
        return serverStats.getGeneration();
    }

    private static class ServerStats {
        private int generation;
        private int connectionCount;

        void incrementCount() {
            connectionCount++;
        }

        void decrementCount() {
            connectionCount--;
        }

        void incrementGeneration() {
            generation++;
        }

        public int getGeneration() {
            return generation;
        }

        public int getConnectionCount() {
            return connectionCount;
        }
    }
}
