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

import com.mongodb.event.ConnectionAddedEvent;
import com.mongodb.event.ConnectionCheckedInEvent;
import com.mongodb.event.ConnectionCheckedOutEvent;
import com.mongodb.event.ConnectionPoolClosedEvent;
import com.mongodb.event.ConnectionPoolListener;
import com.mongodb.event.ConnectionPoolOpenedEvent;
import com.mongodb.event.ConnectionPoolWaitQueueEnteredEvent;
import com.mongodb.event.ConnectionPoolWaitQueueExitedEvent;
import com.mongodb.event.ConnectionRemovedEvent;

import java.util.ArrayList;
import java.util.List;

class TestConnectionPoolListener implements ConnectionPoolListener {

    private final List<Object> events = new ArrayList<Object>();

    public List<Object> getEvents() {
        return events;
    }

    @Override
    public void connectionPoolOpened(final ConnectionPoolOpenedEvent event) {
        events.add(event);
    }

    @Override
    public void connectionPoolClosed(final ConnectionPoolClosedEvent event) {
        events.add(event);
    }

    @Override
    public void connectionCheckedOut(final ConnectionCheckedOutEvent event) {
        events.add(event);
    }

    @Override
    public void connectionCheckedIn(final ConnectionCheckedInEvent event) {
        events.add(event);
    }

    @Override
    public void waitQueueEntered(final ConnectionPoolWaitQueueEnteredEvent event) {
        events.add(event);
    }

    @Override
    public void waitQueueExited(final ConnectionPoolWaitQueueExitedEvent event) {
        events.add(event);
    }

    @Override
    public void connectionAdded(final ConnectionAddedEvent event) {
        events.add(event);
    }

    @Override
    public void connectionRemoved(final ConnectionRemovedEvent event) {
        events.add(event);
    }
}
