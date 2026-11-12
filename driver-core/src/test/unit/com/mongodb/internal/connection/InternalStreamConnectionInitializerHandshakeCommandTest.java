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
import com.mongodb.ServerApi;
import com.mongodb.ServerApiVersion;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ServerId;
import com.mongodb.connection.ServerType;
import com.mongodb.internal.TimeoutSettings;
import org.bson.BsonDocument;
import org.junit.jupiter.api.Test;

import static com.mongodb.connection.ClusterConnectionMode.LOAD_BALANCED;
import static com.mongodb.connection.ClusterConnectionMode.SINGLE;
import static com.mongodb.internal.connection.MessageHelper.buildSuccessfulReply;
import static com.mongodb.internal.connection.MessageHelper.decodeCommand;
import static com.mongodb.internal.connection.OperationContext.simpleOperationContext;
import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The initial handshake carries {@code helloOk: true} whether the command is {@code hello} or the legacy hello command. The server
 * remembers it for the connection.
 */
class InternalStreamConnectionInitializerHandshakeCommandTest {
    private final ServerId serverId = new ServerId(new ClusterId(), new ServerAddress());

    @Test
    void handshakeIsLegacyHelloWithHelloOk() {
        BsonDocument sent = handshake(SINGLE, null);

        assertEquals(BsonDocument.parse("{isMaster: 1, helloOk: true, backpressure: '2', '$db': 'admin'}"), sent);
    }

    @Test
    void handshakeWithServerApiIsHelloWithHelloOk() {
        BsonDocument sent = handshake(SINGLE, ServerApi.builder().version(ServerApiVersion.V1).build());

        assertEquals(1, sent.getInt32("hello").getValue());
        assertFalse(sent.containsKey("isMaster"));
        assertTrue(sent.getBoolean("helloOk").getValue());
        assertFalse(sent.containsKey("loadBalanced"));
    }

    @Test
    void loadBalancedHandshakeIsHelloWithLoadBalancedAndHelloOk() {
        BsonDocument sent = handshake(LOAD_BALANCED, null);

        assertEquals(1, sent.getInt32("hello").getValue());
        assertFalse(sent.containsKey("isMaster"));
        assertTrue(sent.getBoolean("loadBalanced").getValue());
        assertTrue(sent.getBoolean("helloOk").getValue());
    }

    private BsonDocument handshake(final ClusterConnectionMode clusterConnectionMode, final ServerApi serverApi) {
        TestInternalConnection connection = new TestInternalConnection(serverId, ServerType.STANDALONE);
        connection.enqueueReply(buildSuccessfulReply("{ok: 1, maxWireVersion: 3, serviceId: {$oid: '5f4e3c2b1a0f9e8d7c6b5a49'}}"));

        new InternalStreamConnectionInitializer(clusterConnectionMode, null, null, emptyList(), serverApi)
                .startHandshake(connection, simpleOperationContext(TimeoutSettings.DEFAULT));

        return decodeCommand(connection.getSent().get(0));
    }
}
