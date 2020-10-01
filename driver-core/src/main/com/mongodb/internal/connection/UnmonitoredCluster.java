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

import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.connection.ClusterType;
import com.mongodb.connection.ServerConnectionState;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerType;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.selector.ServerSelector;
import org.bson.BsonTimestamp;

import static com.mongodb.assertions.Assertions.isTrue;
import static java.util.Collections.singletonList;

public class UnmonitoredCluster implements Cluster {
    private final ClusterSettings settings;
    private final ClusterDescription description;
    private final ClusterClock clusterClock = new ClusterClock();
    private final Server server;
    private volatile boolean closed;

    public UnmonitoredCluster(final ClusterSettings settings, final ClusterableServerFactory serverFactory) {
        this.settings = settings;
        description = new ClusterDescription(ClusterConnectionMode.SINGLE, ClusterType.SHARDED,
                singletonList(ServerDescription.builder()
                        .ok(true)
                        .state(ServerConnectionState.CONNECTED)
                        .logicalSessionTimeoutMinutes(30)  // TODO: this is a hack, but necessary to get sessions to work
                        .type(ServerType.SHARD_ROUTER)
                        .address(settings.getHosts().get(0))
                        .build()),
                settings, serverFactory.getSettings());
        server = serverFactory.create(settings.getHosts().get(0), null, null, clusterClock);
    }

    @Override
    public ClusterSettings getSettings() {
        isTrue("open", !isClosed());
        return settings;
    }

    @Override
    public ClusterDescription getDescription() {
        isTrue("open", !isClosed());
        return description;
    }

    @Override
    public ClusterDescription getCurrentDescription() {
        isTrue("open", !isClosed());
        return description;
    }

    @Override
    public BsonTimestamp getClusterTime() {
        isTrue("open", !isClosed());
        return clusterClock.getClusterTime();
    }

    @Override
    public Server selectServer(final ServerSelector serverSelector) {
        isTrue("open", !isClosed());
        return server;
    }

    @Override
    public void selectServerAsync(final ServerSelector serverSelector, final SingleResultCallback<Server> callback) {
        isTrue("open", !isClosed());
        callback.onResult(server, null);
    }

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }
}
