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
import com.mongodb.connection.ServerId;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * An event for checking our a connection from the pool.
 *
 * @since 3.5
 */
public final class ConnectionCheckedOutEvent {
    private final ServerId serverId;
    private final ConnectionId connectionId;

    /**
     * Construct an instance
     *
     * @param serverId the server id
     * @param connectionId the connection id
     */
    public ConnectionCheckedOutEvent(final ServerId serverId, final ConnectionId connectionId) {
        this.serverId = notNull("serverId", serverId);
        this.connectionId = notNull("connectionId", connectionId);
    }

    /**
     * Gets the server id
     *
     * @return the server id
     * @since 4.0
     */
    public ServerId getServerId() {
        return serverId;
    }

    /**
     * Gets the connection id
     *
     * @return the connection id
     */
    public ConnectionId getConnectionId() {
        return connectionId;
    }

    @Override
    public String toString() {
        return "ConnectionCheckedOutEvent{"
                       + "serverId=" + serverId
                       + " connectionId=" + connectionId
                       + '}';
    }
}
