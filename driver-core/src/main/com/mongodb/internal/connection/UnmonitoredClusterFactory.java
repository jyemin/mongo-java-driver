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

import com.mongodb.MongoCompressor;
import com.mongodb.MongoCredential;
import com.mongodb.MongoDriverInformation;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.connection.ConnectionPoolSettings;
import com.mongodb.connection.ServerSettings;
import com.mongodb.connection.StreamFactory;
import com.mongodb.event.CommandListener;

import java.util.List;

public final class UnmonitoredClusterFactory {

    public Cluster createCluster(final ClusterSettings clusterSettings, final ServerSettings serverSettings,
                                 final ConnectionPoolSettings connectionPoolSettings, final StreamFactory streamFactory,
                                 final StreamFactory heartbeatStreamFactory, final MongoCredential credential,
                                 final CommandListener commandListener, final String applicationName,
                                 final MongoDriverInformation mongoDriverInformation,
                                 final List<MongoCompressor> compressorList) {

        ClusterId clusterId = new ClusterId();


        ClusterableServerFactory serverFactory = new UnmonitoredClusterableServerFactory(clusterId, clusterSettings, serverSettings,
                connectionPoolSettings, streamFactory, heartbeatStreamFactory, credential, commandListener, applicationName,
                mongoDriverInformation != null ? mongoDriverInformation : MongoDriverInformation.builder().build(), compressorList);

        return new UnmonitoredCluster(clusterSettings, serverFactory);
    }
}
