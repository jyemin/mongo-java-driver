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

package com.mongodb.internal.cluster;

import com.mongodb.MongoCompressor;
import com.mongodb.MongoCredential;
import com.mongodb.MongoDriverInformation;
import com.mongodb.ServerApi;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.connection.ConnectionPoolSettings;
import com.mongodb.connection.ServerSettings;
import com.mongodb.connection.StreamFactory;
import com.mongodb.event.ClusterListener;
import com.mongodb.event.CommandListener;
import com.mongodb.event.ServerListener;
import com.mongodb.event.ServerMonitorListener;
import com.mongodb.internal.server.DefaultClusterableServerFactory;
import com.mongodb.internal.cluster.srv.DefaultDnsSrvRecordMonitorFactory;
import com.mongodb.internal.cluster.srv.DnsSrvRecordMonitorFactory;
import com.mongodb.internal.pool.InternalConnectionPoolSettings;
import com.mongodb.internal.server.LoadBalancedClusterableServerFactory;
import com.mongodb.lang.Nullable;

import java.util.List;

import static com.mongodb.internal.event.EventListenerHelper.NO_OP_CLUSTER_LISTENER;
import static com.mongodb.internal.event.EventListenerHelper.NO_OP_SERVER_LISTENER;
import static com.mongodb.internal.event.EventListenerHelper.NO_OP_SERVER_MONITOR_LISTENER;
import static com.mongodb.internal.event.EventListenerHelper.clusterListenerMulticaster;
import static com.mongodb.internal.event.EventListenerHelper.serverListenerMulticaster;
import static com.mongodb.internal.event.EventListenerHelper.serverMonitorListenerMulticaster;
import static java.util.Collections.singletonList;

/**
 * The default factory for cluster implementations.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class DefaultClusterFactory {

    public Cluster createCluster(final ClusterSettings originalClusterSettings, final ServerSettings originalServerSettings,
                                 final ConnectionPoolSettings connectionPoolSettings,
                                 final InternalConnectionPoolSettings internalConnectionPoolSettings,
                                 final StreamFactory streamFactory, final StreamFactory heartbeatStreamFactory,
                                 @Nullable final MongoCredential credential,
                                 final CommandListener commandListener,
                                 @Nullable final String applicationName,
                                 final MongoDriverInformation mongoDriverInformation,
                                 final List<MongoCompressor> compressorList, @Nullable final ServerApi serverApi) {

        ClusterId clusterId = new ClusterId(applicationName);
        ClusterSettings clusterSettings;
        ServerSettings serverSettings;

        if (noClusterEventListeners(originalClusterSettings, originalServerSettings)) {
            clusterSettings = ClusterSettings.builder(originalClusterSettings)
                    .clusterListenerList(singletonList(NO_OP_CLUSTER_LISTENER))
                    .build();
            serverSettings = ServerSettings.builder(originalServerSettings)
                    .serverListenerList(singletonList(NO_OP_SERVER_LISTENER))
                    .serverMonitorListenerList(singletonList(NO_OP_SERVER_MONITOR_LISTENER))
                    .build();
        } else {
            AsynchronousClusterEventListener clusterEventListener =
                    AsynchronousClusterEventListener.startNew(clusterId, getClusterListener(originalClusterSettings),
                            getServerListener(originalServerSettings), getServerMonitorListener(originalServerSettings));

            clusterSettings = ClusterSettings.builder(originalClusterSettings)
                    .clusterListenerList(singletonList(clusterEventListener))
                    .build();
            serverSettings = ServerSettings.builder(originalServerSettings)
                    .serverListenerList(singletonList(clusterEventListener))
                    .serverMonitorListenerList(singletonList(clusterEventListener))
                    .build();
        }

        DnsSrvRecordMonitorFactory dnsSrvRecordMonitorFactory = new DefaultDnsSrvRecordMonitorFactory(clusterId, serverSettings);

        if (clusterSettings.getMode() == ClusterConnectionMode.LOAD_BALANCED) {
            ClusterableServerFactory serverFactory = new LoadBalancedClusterableServerFactory(serverSettings,
                    connectionPoolSettings, internalConnectionPoolSettings, streamFactory, credential, commandListener, applicationName,
                    mongoDriverInformation != null ? mongoDriverInformation : MongoDriverInformation.builder().build(), compressorList,
                    serverApi);
            return new LoadBalancedCluster(clusterId, clusterSettings, serverFactory, dnsSrvRecordMonitorFactory);
        } else {
            ClusterableServerFactory serverFactory = new DefaultClusterableServerFactory(serverSettings,
                    connectionPoolSettings, internalConnectionPoolSettings,
                    streamFactory, heartbeatStreamFactory, credential, commandListener, applicationName,
                    mongoDriverInformation != null ? mongoDriverInformation : MongoDriverInformation.builder().build(), compressorList,
                    serverApi);

            if (clusterSettings.getMode() == ClusterConnectionMode.SINGLE) {
                return new SingleServerCluster(clusterId, clusterSettings, serverFactory);
            } else if (clusterSettings.getMode() == ClusterConnectionMode.MULTIPLE) {
                if (clusterSettings.getSrvHost() == null) {
                    return new MultiServerCluster(clusterId, clusterSettings, serverFactory);
                } else {
                    return new DnsMultiServerCluster(clusterId, clusterSettings, serverFactory, dnsSrvRecordMonitorFactory);
                }
            } else {
                throw new UnsupportedOperationException("Unsupported cluster mode: " + clusterSettings.getMode());
            }
        }
    }

    private boolean noClusterEventListeners(final ClusterSettings clusterSettings, final ServerSettings serverSettings) {
        return clusterSettings.getClusterListeners().isEmpty()
                && serverSettings.getServerListeners().isEmpty()
                && serverSettings.getServerMonitorListeners().isEmpty();
    }

    private static ClusterListener getClusterListener(final ClusterSettings clusterSettings) {
        return clusterSettings.getClusterListeners().size() == 0
                ? NO_OP_CLUSTER_LISTENER
                : clusterListenerMulticaster(clusterSettings.getClusterListeners());
    }

    private static ServerListener getServerListener(final ServerSettings serverSettings) {
        return serverSettings.getServerListeners().size() == 0
                ? NO_OP_SERVER_LISTENER
                : serverListenerMulticaster(serverSettings.getServerListeners());
    }

    private static ServerMonitorListener getServerMonitorListener(final ServerSettings serverSettings) {
        return serverSettings.getServerMonitorListeners().size() == 0
                ? NO_OP_SERVER_MONITOR_LISTENER
                : serverMonitorListenerMulticaster(serverSettings.getServerMonitorListeners());
    }
}
