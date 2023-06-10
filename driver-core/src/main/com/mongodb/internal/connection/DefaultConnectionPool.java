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

import com.mongodb.MongoException;
import com.mongodb.MongoInterruptedException;
import com.mongodb.MongoSocketException;
import com.mongodb.MongoSocketReadTimeoutException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.connection.ConnectionPoolSettings;
import com.mongodb.connection.ServerId;
import com.mongodb.internal.session.SessionContext;
import org.bson.codecs.Decoder;

import java.util.Deque;
import java.util.Iterator;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

class DefaultConnectionPool implements ConnectionPool {
    private final InternalConnectionFactory internalConnectionFactory;
    private final AtomicInteger generation = new AtomicInteger(0);
    private final ServerId serverId;
    private final Deque<UsageTrackingInternalConnection> available = new ConcurrentLinkedDeque<>();
    private final Semaphore permits;
    private volatile boolean closed;

    DefaultConnectionPool(final ServerId serverId, final InternalConnectionFactory internalConnectionFactory,
                          final ConnectionPoolSettings settings) {
        this.internalConnectionFactory = internalConnectionFactory;
        this.serverId = serverId;
        permits = new Semaphore(settings.getMaxSize(), true);
    }

    @Override
    public InternalConnection get(final long timeout, final TimeUnit timeUnit) {
        UsageTrackingInternalConnection internalConnection = getInternal(timeout, timeUnit);
        while (fromPreviousGeneration(internalConnection)) {
            releaseInternal(internalConnection, true);
            internalConnection = getInternal(timeout, timeUnit);
        }
        PooledConnection pooledConnection = new PooledConnection(internalConnection);
        if (!pooledConnection.opened()) {
            try {
                pooledConnection.open();
            } catch (Throwable t) {
                releaseInternal(pooledConnection.wrapped, true);
                throw t;
            }
        }
        return pooledConnection;
    }

    @Override
    public void invalidate() {
        generation.incrementAndGet();
    }

    @Override
    public void close() {
        Iterator<UsageTrackingInternalConnection> iter = available.iterator();
        while (iter.hasNext()) {
            UsageTrackingInternalConnection t = iter.next();
            t.close();
            iter.remove();
        }
        closed = true;
    }

    private boolean fromPreviousGeneration(final UsageTrackingInternalConnection connection) {
        return generation.get() > connection.getGeneration();
    }

    private UsageTrackingInternalConnection getInternal(final long timeout, final TimeUnit timeUnit) {
        if (!acquirePermit(timeout, timeUnit)) {
            throw new MongoTimeoutException(String.format("Timeout waiting for a pooled item after %d %s", timeout, timeUnit));
        }

        UsageTrackingInternalConnection connection = available.pollLast();
        if (connection == null) {
            connection = createNew();
        }

        return connection;
    }

    private void releaseInternal(final UsageTrackingInternalConnection connection, final boolean prune) {
        if (closed) {
            connection.close();
            return;
        }

        if (prune) {
            connection.close();
        } else {
            available.addLast(connection);
        }

        permits.release();
    }

    private UsageTrackingInternalConnection createNew() {
        try {
            return new UsageTrackingInternalConnection(internalConnectionFactory.create(serverId), generation.get());
        } catch (RuntimeException e) {
            permits.release();
            throw e;
        }
    }

    private boolean acquirePermit(final long timeout, final TimeUnit timeUnit) {
        try {
            return permits.tryAcquire(timeout, timeUnit);
        } catch (InterruptedException e) {
            throw new MongoInterruptedException("Interrupted acquiring a permit to retrieve an item from the pool ", e);
        }
    }

    private class PooledConnection implements InternalConnection {
        private final UsageTrackingInternalConnection wrapped;
        private final AtomicBoolean isClosed = new AtomicBoolean();

        PooledConnection(final UsageTrackingInternalConnection wrapped) {
            this.wrapped = wrapped;
        }

        @Override
        public void open() {
            wrapped.open();
        }

        @Override
        public void close() {
            if (!isClosed.getAndSet(true)) {
                releaseInternal(wrapped, wrapped.isClosed() || fromPreviousGeneration(wrapped));
            }
        }

        @Override
        public <T> T sendAndReceive(final CommandMessage message, final Decoder<T> decoder, final SessionContext sessionContext) {
            try {
                return wrapped.sendAndReceive(message, decoder, sessionContext);
            } catch (MongoException e) {
                if (e instanceof MongoSocketException && !(e instanceof MongoSocketReadTimeoutException)) {
                    invalidate();
                }
                throw e;
            }
        }
    }
}
