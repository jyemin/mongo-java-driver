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

package com.mongodb.client.internal;

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.client.ClientSession;
import com.mongodb.connection.ClusterType;
import com.mongodb.connection.ServerDescription;
import com.mongodb.internal.binding.AbstractReferenceCounted;
import com.mongodb.internal.binding.ClusterAwareReadWriteBinding;
import com.mongodb.internal.binding.ConnectionSource;
import com.mongodb.internal.binding.ReadWriteBinding;
import com.mongodb.internal.connection.Connection;
import com.mongodb.internal.connection.Server;
import com.mongodb.internal.selector.ReadPreferenceServerSelector;
import com.mongodb.internal.session.ClientSessionContext;
import com.mongodb.internal.session.SessionContext;

import static org.bson.assertions.Assertions.notNull;

/**
 * This class is not part of the public API and may be removed or changed at any time.
 */
public class ClientSessionBinding implements ReadWriteBinding {
    private final ClusterAwareReadWriteBinding wrapped;
    private final ClientSession session;
    private final boolean ownsSession;
    private final ClientSessionContext sessionContext;

    public ClientSessionBinding(final ClientSession session, final boolean ownsSession, final ClusterAwareReadWriteBinding wrapped) {
        this.wrapped = wrapped;
        this.session = notNull("session", session);
        this.ownsSession = ownsSession;
        this.sessionContext = new SyncClientSessionContext(session);
    }

    @Override
    public ReadPreference getReadPreference() {
        return wrapped.getReadPreference();
    }

    @Override
    public int getCount() {
        return wrapped.getCount();
    }

    @Override
    public ReadWriteBinding retain() {
        wrapped.retain();
        return this;
    }

    @Override
    public void release() {
        wrapped.release();
        closeSessionIfCountIsZero();
    }

    private void closeSessionIfCountIsZero() {
        if (getCount() == 0 && ownsSession) {
            session.close();
        }
    }

    @Override
    public ConnectionSource getReadConnectionSource() {
        if (isActiveShardedTxn()) {
            return new SessionBindingConnectionSource(getPinnedConnectionSource());
        } else {
            return new SessionBindingConnectionSource(wrapped.getReadConnectionSource());
        }
    }

    public ConnectionSource getWriteConnectionSource() {
        if (isActiveShardedTxn()) {
            return new SessionBindingConnectionSource(getPinnedConnectionSource());
        } else {
            return new SessionBindingConnectionSource(wrapped.getWriteConnectionSource());
        }
    }

    @Override
    public SessionContext getSessionContext() {
        return sessionContext;
    }

    private boolean isActiveShardedTxn() {
        return session.hasActiveTransaction() && wrapped.getCluster().getDescription().getType() == ClusterType.SHARDED;
    }

    private ServerAddress pinServer() {
        ServerAddress pinnedServerAddress = session.getPinnedServerAddress();
        if (pinnedServerAddress == null) {
            Server server = wrapped.getCluster().selectServer(new ReadPreferenceServerSelector(wrapped.getReadPreference()));
            pinnedServerAddress = server.getDescription().getAddress();
            session.setPinnedServerAddress(pinnedServerAddress);
        }
        return pinnedServerAddress;
    }

    private ConnectionSource getPinnedConnectionSource() {
        TransactionContext transactionContext = (TransactionContext) session.getTransactionContext();
        if (transactionContext == null) {
            Server server = wrapped.getCluster().selectServer(new ReadPreferenceServerSelector(wrapped.getReadPreference()));
            session.setPinnedServerAddress(server.getDescription().getAddress());
            ConnectionSource connectionSource = wrapped.getConnectionSource(server.getDescription().getAddress());
            Connection connection = connectionSource.getConnection();
            transactionContext = new TransactionContext(server, connection);
            session.setTransactionContext(transactionContext);
            transactionContext.release();
        }
        return wrapped.getConnectionSource(transactionContext.server, transactionContext.connection);
    }

    private static class TransactionContext extends AbstractReferenceCounted {
        private final Server server;
        private final Connection connection;

        private TransactionContext(final Server server, final Connection connection) {
            this.server = server;
            this.connection = connection;
        }

        @Override
        public void release() {
            super.release();
            if (getCount() == 0) {
                connection.release();
            }
        }
    }

    private class SessionBindingConnectionSource implements ConnectionSource {
        private ConnectionSource wrapped;

        SessionBindingConnectionSource(final ConnectionSource wrapped) {
            this.wrapped = wrapped;
        }

        @Override
        public ServerDescription getServerDescription() {
            return wrapped.getServerDescription();
        }

        @Override
        public SessionContext getSessionContext() {
            return sessionContext;
        }

        @Override
        public Connection getConnection() {
            return wrapped.getConnection();
        }

        @Override
        @SuppressWarnings("checkstyle:methodlength")
        public ConnectionSource retain() {
            wrapped = wrapped.retain();
            return this;
        }

        @Override
        public int getCount() {
            return wrapped.getCount();
        }

        @Override
        public void release() {
            wrapped.release();
            closeSessionIfCountIsZero();
        }
    }

    private final class SyncClientSessionContext extends ClientSessionContext implements SessionContext {

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
                return clientSession.getTransactionOptions().getReadConcern();
            } else {
               return wrapped.getSessionContext().getReadConcern();
            }
        }
    }
}
