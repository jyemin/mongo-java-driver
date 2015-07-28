/*
 * Copyright 2015 MongoDB, Inc.
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
 *
 *
 */

package com.mongodb.connection;

import com.mongodb.event.CommandEvent;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandSucceededEvent;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class TestCommandListener implements CommandListener {
    private final List<CommandEvent> events = new ArrayList<CommandEvent>();
    private final int firstRequestId = RequestMessage.getCurrentGlobalId();
    @Override
    public void commandStarted(final CommandStartedEvent event) {
        events.add(new CommandStartedEvent(event.getRequestId(), event.getConnectionDescription(), event.getDatabaseName(),
                                           event.getCommandName(), event.getCommand() == null ? null : event.getCommand().clone()));
    }

    @Override
    public void commandSucceeded(final CommandSucceededEvent event) {
        events.add(new CommandSucceededEvent(event.getRequestId(), event.getConnectionDescription(), event.getCommandName(),
                                             event.getResponse() == null ? null : event.getResponse().clone(),
                                             event.getElapsedTime(TimeUnit.NANOSECONDS)));
    }

    @Override
    public void commandFailed(final CommandFailedEvent event) {
        events.add(event);
    }

    boolean eventsWereDelivered(final List<CommandEvent> expectedEvents) {
        if (expectedEvents.size() != events.size()) {
            return false;
        }
        int currentlyExpectedRequestId = firstRequestId;
        for (int i = 0; i < events.size(); i++) {
            CommandEvent actual = events.get(i);
            CommandEvent expected = expectedEvents.get(i);
            if (!actual.getClass().equals(expected.getClass())) {
                return false;
            }

            if (actual.getRequestId() != currentlyExpectedRequestId) {
                return false;
            }

            if (!(actual instanceof CommandStartedEvent)) {
                currentlyExpectedRequestId++;
            }

            if (!actual.getConnectionDescription().equals(expected.getConnectionDescription())) {
                return false;
            }

            if (!actual.getCommandName().equals(expected.getCommandName())) {
                return false;
            }

            if (actual.getClass().equals(CommandStartedEvent.class)) {
                if (!isEquivalent((CommandStartedEvent) actual, (CommandStartedEvent) expected)) {
                    return false;
                }
            } else if (actual.getClass().equals(CommandSucceededEvent.class)) {
                if (!isEquivalent((CommandSucceededEvent) actual, (CommandSucceededEvent) expected)) {
                    return false;
                }
            } else if (actual.getClass().equals(CommandFailedEvent.class)) {
                if (!isEquivalent((CommandFailedEvent) actual, (CommandFailedEvent) expected)) {
                    return false;
                }
            } else {
                throw new UnsupportedOperationException("Unsupported event type: " + actual.getClass());
            }
        }

        return true;
    }

    private boolean isEquivalent(final CommandFailedEvent actual, final CommandFailedEvent expected) {
        if (!actual.getThrowable().equals(expected.getThrowable())) {
            return false;
        }
        return true;
    }

    private boolean isEquivalent(final CommandSucceededEvent actual, final CommandSucceededEvent expected) {
        if (actual.getResponse() == null) {
            return expected.getResponse() == null;
        }
        if (!actual.getResponse().equals(expected.getResponse())) {
            return false;
        }
        return true;
    }

    private boolean isEquivalent(final CommandStartedEvent actual, final CommandStartedEvent expected) {
        if (!actual.getDatabaseName().equals(expected.getDatabaseName())) {
            return false;
        }
        if (!actual.getCommand().equals(expected.getCommand())) {
            return false;
        }
        return true;
    }
}
