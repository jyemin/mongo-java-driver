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

import java.util.EventListener;

/**
 * A listener for connection pool-related events.
 *
 * @since 3.5
 */
public interface ConnectionPoolListener extends EventListener {
    /**
     * Invoked when a connection pool is opened.
     *
     * @param event the event
     * @deprecated Prefer {@link #connectionPoolCreated}
     */
    @Deprecated
    default void connectionPoolOpened(ConnectionPoolOpenedEvent event) {
    }

    /**
     * Invoked when a connection pool is created.
     *
     * @param event the event
     * @since 4.0
     */
    default void connectionPoolCreated(ConnectionPoolCreatedEvent event) {
    }

    /**
     * Invoked when a connection pool is cleared.
     *
     * @param event the event
     * @since 4.0
     */
    default void connectionPoolCleared(ConnectionPoolClearedEvent event) {
    }

    /**
     * Invoked when a connection pool is closed.
     *
     * @param event the event
     */
    default void connectionPoolClosed(ConnectionPoolClosedEvent event) {
    }

    /**
     * Invoked when attempting to check out a connection from a pool.
     *
     * @param event the event
     * @since 4.0
     */
    default void connectionCheckOutStarted(ConnectionCheckOutStartedEvent event) {
    }

    /**
     * Invoked when a connection is checked out of a pool.
     *
     * @param event the event
     */
    default void connectionCheckedOut(ConnectionCheckedOutEvent event) {
    }

    /**
     * Invoked when an attempt to check out a connection from a pool fails.
     *
     * @param event the event
     * @since 4.0
     */
    default void connectionCheckOutFailed(ConnectionCheckOutFailedEvent event) {
    }

    /**
     * Invoked when a connection is checked in to a pool.
     *
     * @param event the event
     */
    default void connectionCheckedIn(ConnectionCheckedInEvent event) {
    }

    /**
     * Invoked when a connection pool's wait queue is entered.
     *
     * @param event the event
     */
    default void waitQueueEntered(ConnectionPoolWaitQueueEnteredEvent event) {
    }

    /**
     * Invoked when a connection pools wait queue is exited.
     *
     * @param event the event
     */
    default void waitQueueExited(ConnectionPoolWaitQueueExitedEvent event) {
    }

    /**
     * Invoked when a connection is added to a pool.
     *
     * @param event the event
     * @deprecated Prefer {@link #connectionCreated}
     */
    @Deprecated
    default void connectionAdded(ConnectionAddedEvent event) {
    }

    /**
     * Invoked when a connection is created.
     *
     * @param event the event
     * @since 4.0
     */
    default void connectionCreated(ConnectionCreatedEvent event) {
    }

    /**
     * Invoked when a connection is ready for use.
     *
     * @param event the event
     * @since 4.0
     */
    default void connectionReady(ConnectionReadyEvent event) {
    }

    /**
     * Invoked when a connection is removed from a pool.
     *
     * @param event the event
     * @deprecated Prefer {@link #connectionClosed}
     */
    @Deprecated
    default void connectionRemoved(ConnectionRemovedEvent event) {
    }

    /**
     * Invoked when a connection is removed from a pool.
     *
     * @param event the event
     * @since 4.0
     */
    default void connectionClosed(ConnectionClosedEvent event) {
    }
}
