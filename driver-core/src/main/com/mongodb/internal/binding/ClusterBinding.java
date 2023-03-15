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

package com.mongodb.internal.binding;

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.RequestContext;
import com.mongodb.ServerAddress;
import com.mongodb.ServerApi;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ServerDescription;
import com.mongodb.internal.connection.Cluster;
import com.mongodb.internal.connection.Connection;
import com.mongodb.internal.connection.ReadConcernAwareNoOpSessionContext;
import com.mongodb.internal.connection.Server;
import com.mongodb.internal.connection.ServerTuple;
import com.mongodb.internal.selector.ReadPreferenceServerSelector;
import com.mongodb.internal.selector.ReadPreferenceWithFallbackServerSelector;
import com.mongodb.internal.selector.ServerAddressSelector;
import com.mongodb.internal.selector.WritableServerSelector;
import com.mongodb.internal.session.ClientSessionContext;
import com.mongodb.internal.session.SessionContext;
import com.mongodb.lang.Nullable;
import com.mongodb.session.ClientSession;

import static com.mongodb.assertions.Assertions.notNull;
import static org.bson.assertions.Assertions.assertNotNull;

/**
 * A simple ReadWriteBinding implementation that supplies write connection sources bound to a possibly different primary each time, and a
 * read connection source bound to a possible different server each time.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public class ClusterBinding extends AbstractReferenceCounted implements ClusterAwareReadWriteBinding {
    private final Cluster cluster;
    private final ReadPreference readPreference;
    private final ReadConcern readConcern;
    @Nullable
    private final ServerApi serverApi;
    private final RequestContext requestContext;
    private final ClientSession session;
    private final ClientSessionContext sessionContext;
    private final boolean ownsSession;

    /**
     * Creates an instance.
     * @param cluster        a non-null Cluster which will be used to select a server to bind to
     * @param readPreference a non-null ReadPreference for read operations
     * @param readConcern    a non-null read concern
     * @param serverApi      a server API, which may be null
     * @param requestContext the request context
     */
    public ClusterBinding(final Cluster cluster, final ReadPreference readPreference, final ReadConcern readConcern,
            @Nullable final ServerApi serverApi, final RequestContext requestContext,
            final ClientSession session, final boolean ownsSession) {
        this.cluster = notNull("cluster", cluster);
        this.readPreference = notNull("readPreference", readPreference);
        this.readConcern = notNull("readConcern", readConcern);
        this.serverApi = serverApi;
        this.requestContext = notNull("requestContext", requestContext);
        this.session = notNull("session", session);
        this.sessionContext = new SyncClientSessionContext(session);
        this.ownsSession = ownsSession;
    }

    /**
     * Return the cluster.
     * @return the cluster
     */
    public Cluster getCluster() {
        return cluster;
    }

    @Override
    public ReadWriteBinding retain() {
        super.retain();
        return this;
    }

    @Override
    public int release() {
        int count = super.release();
        if (count == 0) {
            if (ownsSession) {
                session.close();
            }
        }
        return count;
    }

    @Override
    public ReadPreference getReadPreference() {
        return readPreference;
    }

    @Override
    public SessionContext getSessionContext() {
        return sessionContext;
    }

    @Override
    @Nullable
    public ServerApi getServerApi() {
        return serverApi;
    }

    @Override
    public RequestContext getRequestContext() {
        return requestContext;
    }

    @Override
    public ConnectionSource getReadConnectionSource() {
        return new ClusterBindingConnectionSource(cluster.selectServer(new ReadPreferenceServerSelector(readPreference)), readPreference);
    }

    @Override
    public ConnectionSource getReadConnectionSource(final int minWireVersion, final ReadPreference fallbackReadPreference) {
        // Assume 5.0+ for load-balanced mode
        if (cluster.getSettings().getMode() == ClusterConnectionMode.LOAD_BALANCED) {
            return getReadConnectionSource();
        } else {
            ReadPreferenceWithFallbackServerSelector readPreferenceWithFallbackServerSelector
                    = new ReadPreferenceWithFallbackServerSelector(readPreference, minWireVersion, fallbackReadPreference);
            ServerTuple serverTuple = cluster.selectServer(readPreferenceWithFallbackServerSelector);
            return new ClusterBindingConnectionSource(serverTuple, readPreferenceWithFallbackServerSelector.getAppliedReadPreference());
        }
    }

    @Override
    public ConnectionSource getWriteConnectionSource() {
        return new ClusterBindingConnectionSource(cluster.selectServer(new WritableServerSelector()), readPreference);
    }

    @Override
    public ConnectionSource getConnectionSource(final ServerAddress serverAddress) {
        return new ClusterBindingConnectionSource(cluster.selectServer(new ServerAddressSelector(serverAddress)), readPreference);
    }

    private final class ClusterBindingConnectionSource extends AbstractReferenceCounted implements ConnectionSource {
        private final Server server;
        private final ServerDescription serverDescription;
        private final ReadPreference appliedReadPreference;

        private ClusterBindingConnectionSource(final ServerTuple serverTuple, final ReadPreference appliedReadPreference) {
            this.server = serverTuple.getServer();
            this.serverDescription = serverTuple.getServerDescription();
            this.appliedReadPreference = appliedReadPreference;
            ClusterBinding.this.retain();
        }

        @Override
        public ServerDescription getServerDescription() {
            return serverDescription;
        }

        @Override
        public SessionContext getSessionContext() {
            return new ReadConcernAwareNoOpSessionContext(readConcern);
        }

        @Override
        public ServerApi getServerApi() {
            return serverApi;
        }

        @Override
        public RequestContext getRequestContext() {
            return requestContext;
        }

        @Override
        public ReadPreference getReadPreference() {
            return appliedReadPreference;
        }

        @Override
        public Connection getConnection() {
            return server.getConnection();
        }

        public ConnectionSource retain() {
            super.retain();
            ClusterBinding.this.retain();
            return this;
        }

        @Override
        public int release() {
            int count = super.release();
            ClusterBinding.this.release();
            return count;
        }
    }

    private final class SyncClientSessionContext extends ClientSessionContext {

        private final ClientSession clientSession;

        SyncClientSessionContext(final ClientSession clientSession) {
            super(clientSession);
            this.clientSession = clientSession;
        }

        @Override
        public boolean isImplicitSession() {
            return ownsSession;
        }

        @Override
        public boolean notifyMessageSent() {
            return clientSession.notifyMessageSent();
        }

        @Override
        public boolean hasActiveTransaction() {
            return clientSession.hasActiveTransaction();
        }

        @Override
        public ReadConcern getReadConcern() {
            if (clientSession.hasActiveTransaction()) {
                return assertNotNull(clientSession.getTransactionOptions().getReadConcern());
            } else if (isSnapshot()) {
                return ReadConcern.SNAPSHOT;
            } else {
                return readConcern;
            }
        }
    }
}
