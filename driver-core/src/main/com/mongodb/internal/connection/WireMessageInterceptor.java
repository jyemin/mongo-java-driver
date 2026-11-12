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

import com.mongodb.ServerAddress;
import com.mongodb.annotations.ThreadSafe;

/**
 * Observes the messages a connection writes to the server, as the bytes that are about to be written to the socket,
 * before any security redaction. Nothing is decoded here: the observer sees what was sent, not what the driver believes it sent.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
@FunctionalInterface
@ThreadSafe
public interface WireMessageInterceptor {

    /**
     * Where the message came from.
     */
    enum Source {
        APPLICATION,
        MONITOR,
        /** The connection a server monitor uses to measure round trip time. */
        RTT
    }

    /**
     * Whether the connection was still being established when the message was written.
     */
    enum Phase {
        /** From connect until the connection is ready, including authentication. */
        HANDSHAKE,
        OPERATION
    }

    /**
     * @param source the kind of connection that wrote the message
     * @param phase the phase of the connection
     * @param serverAddress the address of the server that the connection was opened to
     * @param message a copy of the bytes of exactly one wire protocol message, starting with its header
     */
    void intercept(Source source, Phase phase, ServerAddress serverAddress, byte[] message);
}
