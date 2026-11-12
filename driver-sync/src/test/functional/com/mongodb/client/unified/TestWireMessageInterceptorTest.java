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
import com.mongodb.internal.connection.WireMessageInterceptor.Phase;
import com.mongodb.internal.connection.WireMessageInterceptor.Source;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.EnumSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Selection by {@code observeWireMessages} depends only on what the driver reports about the connection, so these tests use bytes
 * that are not a message at all: if the interceptor tried to read an unselected message, it would fail.
 */
final class TestWireMessageInterceptorTest {
    private static final ServerAddress SERVER_A = new ServerAddress("a", 1);
    private static final ServerAddress SERVER_B = new ServerAddress("b", 2);
    private static final byte[] NOT_A_MESSAGE = {1, 2, 3};

    private static TestWireMessageInterceptor applicationHandshakeInterceptor(final String name) {
        return new TestWireMessageInterceptor(name, EnumSet.of(WireMessage.Source.APPLICATION), EnumSet.of(WireMessage.Phase.HANDSHAKE));
    }

    @Test
    @DisplayName("a message on a kind of connection or in a phase that is not selected is neither recorded nor checked")
    void unselectedMessagesAreNeitherRecordedNorChecked() {
        try (TestWireMessageInterceptor interceptor = applicationHandshakeInterceptor("test-unselected")) {
            interceptor.intercept(Source.MONITOR, Phase.HANDSHAKE, SERVER_A, NOT_A_MESSAGE);
            interceptor.intercept(Source.RTT, Phase.HANDSHAKE, SERVER_A, NOT_A_MESSAGE);
            interceptor.intercept(Source.APPLICATION, Phase.OPERATION, SERVER_A, NOT_A_MESSAGE);

            assertEquals(0, interceptor.getMessages(null).size());
            // would throw if any of the three had been decoded
            interceptor.assertAllMessagesWellFormed();
        }
    }

    @Test
    @DisplayName("a selected message is recorded and is checked")
    void selectedMessagesAreChecked() {
        try (TestWireMessageInterceptor interceptor = applicationHandshakeInterceptor("test-selected")) {
            interceptor.intercept(Source.APPLICATION, Phase.HANDSHAKE, SERVER_A, NOT_A_MESSAGE);

            assertEquals(1, interceptor.getMessages(null).size());
            AssertionError error = assertThrows(AssertionError.class, interceptor::assertAllMessagesWellFormed);
            assertEquals(true, error.getMessage().contains("is not well formed"), error.getMessage());
        }
    }

    @Test
    @DisplayName("messages are attributed to the server that they were written to")
    void messagesAreAttributedToServers() {
        try (TestWireMessageInterceptor interceptor = new TestWireMessageInterceptor("test-servers",
                EnumSet.of(WireMessage.Source.MONITOR), EnumSet.of(WireMessage.Phase.HANDSHAKE, WireMessage.Phase.OPERATION))) {
            interceptor.intercept(Source.MONITOR, Phase.HANDSHAKE, SERVER_A, NOT_A_MESSAGE);
            interceptor.intercept(Source.MONITOR, Phase.HANDSHAKE, SERVER_B, NOT_A_MESSAGE);
            interceptor.intercept(Source.MONITOR, Phase.OPERATION, SERVER_A, NOT_A_MESSAGE);

            assertEquals(2, interceptor.getMessages(WireMessage.Source.MONITOR, SERVER_A).size());
            assertEquals(1, interceptor.getMessages(WireMessage.Source.MONITOR, SERVER_B).size());
            assertEquals(0, interceptor.getMessages(WireMessage.Source.RTT, SERVER_A).size());
        }
    }
}
