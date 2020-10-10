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

import com.mongodb.ServerAddress;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ServerConnectionState;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerType;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.session.SessionContext;

class UnmonitoredServer implements ClusterableServer {
    private final ServerDescription description;
    private final ClusterClock clusterClock;
    private final ConnectionPool connectionPool;
    private final ConnectionFactory connectionFactory;
    private volatile boolean closed;

    public UnmonitoredServer(final ServerAddress serverAddress, final ClusterClock clusterClock, final ConnectionPool connectionPool,
                            final ConnectionFactory connectionFactory) {
        // TODO: this is copied from UnmonitoredCluster.  Should consolidate somehow
        description = ServerDescription.builder()
                .ok(true)
                .state(ServerConnectionState.CONNECTED)
                .logicalSessionTimeoutMinutes(30)  // TODO: this is a hack, but necessary to get sessions to work
                .type(ServerType.SHARD_ROUTER)
                .address(serverAddress)
                .build();
        this.clusterClock = clusterClock;
        this.connectionPool = connectionPool;
        this.connectionFactory = connectionFactory;
    }

    @Override
    public ServerDescription getDescription() {
        return description;
    }

    @Override
    public Connection getConnection() {
        return connectionFactory.create(connectionPool.get(), new UnmonitoredServerProtocolExecutor(), ClusterConnectionMode.SINGLE);
    }

    @Override
    public void getConnectionAsync(final SingleResultCallback<AsyncConnection> callback) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void resetToConnecting() {
        // no op
    }

    @Override
    public void invalidate() {
        // no op
    }

    @Override
    public void invalidate(final ConnectionState connectionState, final Throwable reason, final int connectionGeneration, final int maxWireVersion) {
        // no op
    }

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void connect() {
        // no op
    }

    private class UnmonitoredServerProtocolExecutor implements ProtocolExecutor {
        @Override
        public <T> T execute(final LegacyProtocol<T> protocol, final InternalConnection connection) {
            return protocol.execute(connection);
        }

        @Override
        public <T> void executeAsync(final LegacyProtocol<T> protocol, final InternalConnection connection,
                                     final SingleResultCallback<T> callback) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> T execute(final CommandProtocol<T> protocol, final InternalConnection connection, final SessionContext sessionContext) {
            protocol.sessionContext(new ClusterClockAdvancingSessionContext(sessionContext, clusterClock));
            try {
                return protocol.execute(connection);
            } catch (Exception e) {
                connectionPool.invalidatePuddle(connection.getInitialServerDescription()
                        .getTopologyVersion().getProcessId());
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