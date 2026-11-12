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
import org.bson.BsonDocument;

/**
 * A message written to a server, as observed by {@code observeWireMessages}.
 *
 * <p>It holds the bytes. The decoded form is computed on demand, and decoding failures are structural violations of the
 * message, which is the point of recording the bytes.</p>
 */
final class WireMessage {
    enum Source {
        APPLICATION,
        MONITOR,
        RTT
    }

    enum Phase {
        HANDSHAKE,
        OPERATION
    }

    private final Source source;
    private final Phase phase;
    private final ServerAddress serverAddress;
    private final byte[] bytes;

    WireMessage(final Source source, final Phase phase, final ServerAddress serverAddress, final byte[] bytes) {
        this.source = source;
        this.phase = phase;
        this.serverAddress = serverAddress;
        this.bytes = bytes;
    }

    Source getSource() {
        return source;
    }

    Phase getPhase() {
        return phase;
    }

    ServerAddress getServerAddress() {
        return serverAddress;
    }

    /**
     * @throws IllegalArgumentException if the bytes are not a well formed message
     */
    BsonDocument toDocument() {
        return WireMessageDecoder.decode(bytes);
    }
}
