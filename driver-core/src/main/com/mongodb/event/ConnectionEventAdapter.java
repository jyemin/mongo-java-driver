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

package com.mongodb.event;

import com.mongodb.connection.ConnectionId;
import com.mongodb.connection.ConnectionPoolSettings;
import com.mongodb.connection.ServerId;

/**
 * An adapter for deprecated connection events.
 *
 * @since 4.0
 */
@SuppressWarnings("deprecation")
public final class ConnectionEventAdapter {
    /**
     * Create events for ConnectionPoolCreated and the deprecated ConnectionPoolOpened and add these events to the
     * connection pool listener.
     *
     * @param connectionPoolListener the connection pool listener
     * @param serverId the server id
     * @param settings the connection pool settings
     */
    public static void connectionPoolCreated(final ConnectionPoolListener connectionPoolListener, final ServerId serverId,
                                             final ConnectionPoolSettings settings) {
        connectionPoolListener.connectionPoolCreated(new ConnectionPoolCreatedEvent(serverId, settings));
        connectionPoolListener.connectionPoolOpened(new ConnectionPoolOpenedEvent(serverId, settings));
    }

    /**
     * Create events for ConnectionCreated and the deprecated ConnectionAdded and add these events to the
     * connection pool listener.
     *
     * @param connectionPoolListener the connection pool listener
     * @param connectionId the connection id
     */
    public static void connectionCreated(final ConnectionPoolListener connectionPoolListener, final ConnectionId connectionId) {
        connectionPoolListener.connectionAdded(new ConnectionAddedEvent(connectionId));
        connectionPoolListener.connectionCreated(new ConnectionCreatedEvent(connectionId));
    }

    /**
     * Create events for ConnectionClosed and the deprecated ConnectionRemoved and add these events to the
     * connection pool listener.
     *
     * @param connectionPoolListener the connection pool listener
     * @param connectionId the connection id
     * @param reason the reason for closing the connection
     */
    public static void connectionClosed(final ConnectionPoolListener connectionPoolListener, final ConnectionId connectionId,
                                        final ConnectionClosedEvent.Reason reason) {
        connectionPoolListener.connectionRemoved(new ConnectionRemovedEvent(connectionId, getReasonForRemoved(reason)));
        connectionPoolListener.connectionClosed(new ConnectionClosedEvent(connectionId, reason));
    }

    private static ConnectionRemovedEvent.Reason getReasonForRemoved(final ConnectionClosedEvent.Reason reason) {
        ConnectionRemovedEvent.Reason removedReason = ConnectionRemovedEvent.Reason.UNKNOWN;
        switch (reason) {
            case STALE:
                removedReason = ConnectionRemovedEvent.Reason.STALE;
                break;
            case IDLE:
                removedReason = ConnectionRemovedEvent.Reason.MAX_IDLE_TIME_EXCEEDED;
                break;
            case ERROR:
                removedReason = ConnectionRemovedEvent.Reason.ERROR;
                break;
            case POOL_CLOSED:
                removedReason = ConnectionRemovedEvent.Reason.POOL_CLOSED;
                break;
            default:
                break;
        }
        return removedReason;
    }

    private ConnectionEventAdapter() {
    }
}
