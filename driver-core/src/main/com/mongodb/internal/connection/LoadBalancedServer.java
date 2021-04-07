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

import com.mongodb.MongoCommandException;
import com.mongodb.MongoException;
import com.mongodb.MongoNodeIsRecoveringException;
import com.mongodb.MongoNotPrimaryException;
import com.mongodb.MongoSecurityException;
import com.mongodb.MongoSocketException;
import com.mongodb.MongoSocketReadTimeoutException;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ServerConnectionState;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerId;
import com.mongodb.connection.ServerType;
import com.mongodb.event.CommandListener;
import com.mongodb.event.ServerClosedEvent;
import com.mongodb.event.ServerDescriptionChangedEvent;
import com.mongodb.event.ServerListener;
import com.mongodb.event.ServerOpeningEvent;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.session.SessionContext;
import com.mongodb.lang.Nullable;
import org.bson.types.ObjectId;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.connection.ServerConnectionState.CONNECTING;
import static com.mongodb.internal.connection.ClusterableServer.ConnectionState.AFTER_HANDSHAKE;
import static com.mongodb.internal.connection.ClusterableServer.ConnectionState.BEFORE_HANDSHAKE;
import static java.util.Arrays.asList;

public class LoadBalancedServer implements ClusterableServer {
    // TODO: share with DefaultServer?
    private static final List<Integer> SHUTDOWN_CODES = asList(91, 11600);
    private final AtomicBoolean closed = new AtomicBoolean();
    private final ServerId serverId;
    private final ConnectionPool connectionPool;
    private final ConnectionFactory connectionFactory;
    private final ServerListener serverListener;
    private final CommandListener commandListener;
    private final ClusterClock clusterClock;

    public LoadBalancedServer(final ServerId serverId, final ConnectionPool connectionPool, final ConnectionFactory connectionFactory,
                              final ServerListener serverListener, final CommandListener commandListener, final ClusterClock clusterClock) {
        this.serverId = serverId;
        this.connectionPool = connectionPool;
        this.connectionFactory = connectionFactory;
        this.serverListener = serverListener;
        this.commandListener = commandListener;
        this.clusterClock = clusterClock;

        serverListener.serverOpening(new ServerOpeningEvent(serverId));
        serverListener.serverDescriptionChanged(new ServerDescriptionChangedEvent(serverId,
                ServerDescription.builder()
                        .ok(true)
                        .state(ServerConnectionState.CONNECTED)
                        .type(ServerType.LOAD_BALANCER)
                        .address(serverId.getAddress())
                        .build(),
                ServerDescription.builder().address(serverId.getAddress()).state(CONNECTING).build()));
    }

    @Override
    public void resetToConnecting() {
        // no op
    }

    @Override
    public void invalidate() {
        // TODO
    }

    @Override
    public void invalidate(final ConnectionState connectionState, final Throwable reason, final int connectionGeneration,
                           final int maxWireVersion) {
        invalidate(connectionState, reason, null);
    }


    // TODO: no longer overrides base class method
    private void invalidate(final ConnectionState connectionState, final Throwable t, @Nullable final ObjectId serverId) {
        if (!isClosed()) {
            if (t instanceof MongoSocketException
                    && (!(t instanceof MongoSocketReadTimeoutException) || connectionState == BEFORE_HANDSHAKE)) {
                if (serverId != null) {
                    connectionPool.invalidate(serverId);
                }
            } else if (t instanceof MongoNotPrimaryException || t instanceof MongoNodeIsRecoveringException) {
                if (SHUTDOWN_CODES.contains(((MongoCommandException) t).getErrorCode())) {
                    if (serverId != null) {
                        connectionPool.invalidate(serverId);
                    }
                }
            }
        }
    }

    @Override
    public void close() {
        if (!closed.getAndSet(true)) {
           serverListener.serverClosed(new ServerClosedEvent(serverId));
        }
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void connect() {
        // no op
    }

    @Override
    public Connection getConnection() {
        isTrue("open", !isClosed());
        try {
            return connectionFactory.create(connectionPool.get(), new LoadBalancedServerProtocolExecutor(),
                    ClusterConnectionMode.LOAD_BALANCED);
        } catch (MongoSecurityException e) {
            // TODO: what should happen here, if anything?
            throw e;
        }
    }

    @Override
    public void getConnectionAsync(final SingleResultCallback<AsyncConnection> callback) {
        throw new UnsupportedOperationException();
    }

    // TODO: share this with DefaultServer?
    private class LoadBalancedServerProtocolExecutor implements ProtocolExecutor {
        @Override
        public <T> T execute(final LegacyProtocol<T> protocol, final InternalConnection connection) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> void executeAsync(final LegacyProtocol<T> protocol, final InternalConnection connection,
                                     final SingleResultCallback<T> callback) {
            throw new UnsupportedOperationException();
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T> T execute(final CommandProtocol<T> protocol, final InternalConnection connection, final SessionContext sessionContext) {
            try {
                protocol.sessionContext(new ClusterClockAdvancingSessionContext(sessionContext, clusterClock));
                return protocol.execute(connection);
            } catch (MongoWriteConcernWithResponseException e) {
                invalidate();
                return (T) e.getResponse();
            } catch (MongoException e) {
                invalidate(AFTER_HANDSHAKE, e, connection.getDescription().getProcessId());
                if (e instanceof MongoSocketException && sessionContext.hasSession()) {
                    sessionContext.markSessionDirty();
                }
                throw e;
            }
        }

        @Override
        public <T> void executeAsync(final CommandProtocol<T> protocol, final InternalConnection connection,
                                     final SessionContext sessionContext, final SingleResultCallback<T> callback) {
            throw new UnsupportedOperationException();
        }
    }
}
