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
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.connection.ClusterType;
import com.mongodb.connection.ServerConnectionState;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerType;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.mongodb.event.ClusterClosedEvent;
import com.mongodb.event.ClusterDescriptionChangedEvent;
import com.mongodb.event.ClusterListener;
import com.mongodb.event.ClusterOpeningEvent;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.selector.ServerSelector;
import org.bson.BsonTimestamp;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.connection.ServerConnectionState.CONNECTING;
import static com.mongodb.internal.event.EventListenerHelper.createServerListener;
import static com.mongodb.internal.event.EventListenerHelper.getClusterListener;
import static java.lang.String.format;
import static java.util.Collections.singletonList;

public final class LoadBalancedCluster implements Cluster {
    private static final Logger LOGGER = Loggers.getLogger("cluster");

    private final ClusterId clusterId;
    private final ClusterSettings settings;
    private final ClusterDescription description;
    private final ClusterClock clusterClock = new ClusterClock();
    private final ClusterableServer server;
    private final ClusterListener clusterListener;
    private final AtomicBoolean closed = new AtomicBoolean();

    public LoadBalancedCluster(final ClusterId clusterId, final ClusterSettings settings, final ClusterableServerFactory serverFactory) {
        LOGGER.info(format("Cluster created with settings %s", settings.getShortDescription()));

        this.clusterId = clusterId;
        this.settings = settings;
        description = new ClusterDescription(ClusterConnectionMode.LOAD_BALANCED, ClusterType.LOAD_BALANCED,
                singletonList(ServerDescription.builder()
                        .ok(true)
                        .state(ServerConnectionState.CONNECTED)
                        .logicalSessionTimeoutMinutes(30)  // TODO: this is a hack, but necessary to get sessions to work for now
                        .type(ServerType.LOAD_BALANCER)
                        .address(settings.getHosts().get(0))
                        .build()),
                settings, serverFactory.getSettings());
        server = serverFactory.create(settings.getHosts().get(0), event -> { }, createServerListener(serverFactory.getSettings()),
                clusterClock);

        this.clusterListener = getClusterListener(settings);
        clusterListener.clusterOpening(new ClusterOpeningEvent(clusterId));

        ClusterDescription startingDescription = new ClusterDescription(settings.getMode(), ClusterType.UNKNOWN, Collections.emptyList(),
                settings, serverFactory.getSettings());
        ClusterDescription initialDescription = new ClusterDescription(settings.getMode(), ClusterType.LOAD_BALANCED,
                singletonList(ServerDescription.builder().address(settings.getHosts().get(0)).state(CONNECTING).build()),
                settings, serverFactory.getSettings());
        clusterListener.clusterDescriptionChanged(new ClusterDescriptionChangedEvent(clusterId, initialDescription, startingDescription));
        clusterListener.clusterDescriptionChanged(new ClusterDescriptionChangedEvent(clusterId, description, initialDescription));

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
    public ClusterId getClusterId() {
        return clusterId;
    }

    @Override
    public ClusterableServer getServer(final ServerAddress serverAddress) {
        return server;
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
    public ServerTuple selectServer(final ServerSelector serverSelector) {
        isTrue("open", !isClosed());
        return new ServerTuple(server, description.getServerDescriptions().get(0));
    }

    @Override
    public void selectServerAsync(final ServerSelector serverSelector, final SingleResultCallback<ServerTuple> callback) {
        isTrue("open", !isClosed());
        callback.onResult(selectServer(serverSelector), null);
    }

    @Override
    public void close() {
        if (!closed.getAndSet(true)) {
            clusterListener.clusterClosed(new ClusterClosedEvent(clusterId));
        }
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }
}
