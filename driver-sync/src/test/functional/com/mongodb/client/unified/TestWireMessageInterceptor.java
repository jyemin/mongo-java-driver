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

package com.mongodb.client.unified;

import com.mongodb.internal.connection.WireMessageInterceptor;
import com.mongodb.internal.connection.WireMessageInterceptors;
import com.mongodb.lang.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * Records every message a client writes. {@code observeWireMessages} selects which of them {@code expectWireMessages} sees, but
 * every recorded message is decoded and checked for structural violations.
 */
final class TestWireMessageInterceptor implements WireMessageInterceptor, AutoCloseable {
    private final String applicationName;
    private final Set<WireMessage.Source> sources;
    private final Set<WireMessage.Phase> phases;
    private final List<WireMessage> messages = new ArrayList<>();

    TestWireMessageInterceptor(final String applicationName, final Set<WireMessage.Source> sources,
            final Set<WireMessage.Phase> phases) {
        this.applicationName = requireNonNull(applicationName);
        this.sources = requireNonNull(sources);
        this.phases = requireNonNull(phases);
        WireMessageInterceptors.add(applicationName, this);
    }

    @Override
    public synchronized void intercept(final Source source, final Phase phase, final byte[] message) {
        // Every message is recorded, whatever the test observes, so that structural checks apply to all of them.
        messages.add(new WireMessage(WireMessage.Source.valueOf(source.name()), WireMessage.Phase.valueOf(phase.name()), message));
        notifyAll();
    }

    /**
     * The messages that {@code observeWireMessages} selects.
     *
     * @param source if not {@code null}, only the messages from this source are returned
     */
    synchronized List<WireMessage> getMessages(@Nullable final WireMessage.Source source) {
        return messages.stream()
                .filter(message -> sources.contains(message.getSource()) && phases.contains(message.getPhase()))
                .filter(message -> source == null || message.getSource() == source)
                .collect(Collectors.toList());
    }

    /**
     * Decodes every recorded message, selected or not, and fails if any is not well formed.
     */
    synchronized void assertAllMessagesWellFormed() {
        for (int i = 0; i < messages.size(); i++) {
            WireMessage message = messages.get(i);
            try {
                message.toDocument();
            } catch (IllegalArgumentException e) {
                throw new AssertionError("Wire message #" + i + " (" + message.getSource() + ", " + message.getPhase()
                        + ") is not well formed: " + e.getMessage(), e);
            }
        }
    }

    /**
     * Blocks until at least {@code count} messages from {@code source} have been recorded.
     *
     * @throws AssertionError if that does not happen within the timeout
     */
    synchronized void waitForMessages(final WireMessage.Source source, final int count, final long timeout, final TimeUnit unit)
            throws InterruptedException {
        long deadlineNanos = System.nanoTime() + unit.toNanos(timeout);
        while (getMessages(source).size() < count) {
            long remainingNanos = deadlineNanos - System.nanoTime();
            if (remainingNanos <= 0) {
                throw new AssertionError("Timed out waiting for " + count + " wire messages from " + source + ", recorded "
                        + getMessages(source).stream().map(message -> message.toDocument().toJson()).collect(Collectors.toList()));
            }
            TimeUnit.NANOSECONDS.timedWait(this, remainingNanos);
        }
    }

    @Override
    public void close() {
        WireMessageInterceptors.remove(applicationName);
    }
}
