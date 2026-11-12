/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mongodb.client.unified;

import com.mongodb.ServerAddress;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerType;
import com.mongodb.internal.connection.WireMessageInterceptor;
import com.mongodb.internal.connection.WireMessageInterceptors;
import com.mongodb.lang.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * Records the messages that a client writes and that {@code observeWireMessages} selects.
 *
 * <p>Whether a message is selected depends only on the kind of its connection and its phase, which the driver reports along with
 * the bytes. A message that is not selected is dropped without being decoded, and so is never checked. Every message that is
 * recorded is decoded when the test asserts, and must be well formed.</p>
 */
final class TestWireMessageInterceptor implements WireMessageInterceptor, AutoCloseable {
    private static final long POLL_NANOS = TimeUnit.MILLISECONDS.toNanos(50);

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
    public synchronized void intercept(final Source source, final Phase phase, final ServerAddress serverAddress,
            final byte[] message) {
        WireMessage.Source wireSource = WireMessage.Source.valueOf(source.name());
        WireMessage.Phase wirePhase = WireMessage.Phase.valueOf(phase.name());
        if (sources.contains(wireSource) && phases.contains(wirePhase)) {
            messages.add(new WireMessage(wireSource, wirePhase, serverAddress, message));
            notifyAll();
        }
    }

    boolean selects(final WireMessage.Source source) {
        return sources.contains(source);
    }

    /**
     * @param source if not {@code null}, only the messages from this source are returned
     */
    synchronized List<WireMessage> getMessages(@Nullable final WireMessage.Source source) {
        return messages.stream()
                .filter(message -> source == null || message.getSource() == source)
                .collect(Collectors.toList());
    }

    synchronized List<WireMessage> getMessages(final WireMessage.Source source, final ServerAddress serverAddress) {
        return messages.stream()
                .filter(message -> message.getSource() == source && message.getServerAddress().equals(serverAddress))
                .collect(Collectors.toList());
    }

    /**
     * Decodes every recorded message and fails if any is not well formed.
     */
    synchronized void assertAllMessagesWellFormed() {
        for (int i = 0; i < messages.size(); i++) {
            WireMessage message = messages.get(i);
            try {
                message.toDocument();
            } catch (IllegalArgumentException e) {
                throw new AssertionError("Wire message #" + i + " (" + message.getSource() + ", " + message.getPhase()
                        + ", " + message.getServerAddress() + ") is not well formed: " + e.getMessage(), e);
            }
        }
    }

    /**
     * Blocks until at least {@code count} messages from {@code source} have been recorded, in total.
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
            TimeUnit.NANOSECONDS.timedWait(this, Math.min(remainingNanos, POLL_NANOS));
        }
    }

    /**
     * Waits until the topology description lists at least one server and the type of every server is known, and returns their
     * addresses. A client adds the members that a server reports when it processes that server's description, so once no server
     * is unknown the description lists every server that the client monitors.
     *
     * @throws AssertionError if that does not happen within the timeout
     */
    List<ServerAddress> waitForServers(final Supplier<ClusterDescription> clusterDescription, final long timeout,
            final TimeUnit unit) throws InterruptedException {
        return waitForMessagesFromEachServer(clusterDescription, null, 0, timeout, unit);
    }

    /**
     * As {@link #waitForServers}, and then until every server has had at least {@code count} messages from {@code source}
     * written to it.
     *
     * @param source may only be {@code null} if {@code count} is 0
     */
    List<ServerAddress> waitForMessagesFromEachServer(final Supplier<ClusterDescription> clusterDescription,
            @Nullable final WireMessage.Source source, final int count, final long timeout, final TimeUnit unit)
            throws InterruptedException {
        long deadlineNanos = System.nanoTime() + unit.toNanos(timeout);
        while (true) {
            // read outside the lock: the driver takes its own locks to produce the description
            List<ServerDescription> servers = new ArrayList<>(clusterDescription.get().getServerDescriptions());
            synchronized (this) {
                boolean allKnown = !servers.isEmpty() && servers.stream().noneMatch(server -> server.getType() == ServerType.UNKNOWN);
                if (allKnown && (source == null
                        || servers.stream().allMatch(server -> getMessages(source, server.getAddress()).size() >= count))) {
                    return servers.stream().map(ServerDescription::getAddress).collect(Collectors.toList());
                }
                long remainingNanos = deadlineNanos - System.nanoTime();
                if (remainingNanos <= 0) {
                    throw new AssertionError("Timed out waiting for " + count + " wire messages from " + source + " for every server."
                            + " Servers: " + servers.stream()
                            .map(server -> server.getAddress() + " (" + server.getType() + ", "
                                    + (source == null ? 0 : getMessages(source, server.getAddress()).size()) + " messages)")
                            .collect(Collectors.toList()));
                }
                TimeUnit.NANOSECONDS.timedWait(this, Math.min(remainingNanos, POLL_NANOS));
            }
        }
    }

    @Override
    public void close() {
        WireMessageInterceptors.remove(applicationName);
    }
}
