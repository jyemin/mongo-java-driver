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

import java.util.concurrent.atomic.AtomicBoolean;

import static com.mongodb.connection.ServerConnectionState.CONNECTING;

public class LoadBalancedServer implements ClusterableServer {
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
        // TODO
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
        throw new UnsupportedOperationException();
    }

    @Override
    public void getConnectionAsync(final SingleResultCallback<AsyncConnection> callback) {
        throw new UnsupportedOperationException();
    }
}
