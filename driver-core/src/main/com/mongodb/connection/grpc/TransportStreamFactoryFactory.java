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

package com.mongodb.connection.grpc;

import com.mongodb.ServerAddress;
import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.SslSettings;
import com.mongodb.connection.Stream;
import com.mongodb.connection.StreamFactory;
import com.mongodb.connection.StreamFactoryFactory;
import com.mongodb.internal.connection.PowerOfTwoBufferPool;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;

/**
 * A StreamFactoryFactory for gRPC
 */
public final class TransportStreamFactoryFactory implements StreamFactoryFactory, Closeable {
    private final Map<ServerAddress, ManagedChannel> map = new HashMap<>();
    private final PowerOfTwoBufferPool bufferPool = new PowerOfTwoBufferPool();

    @Override
    public StreamFactory create(final SocketSettings socketSettings, final SslSettings sslSettings) {
        return new TransportStreamFactory();
    }

    @Override
    public void close() {
        for (ManagedChannel channel : map.values()) {
            channel.shutdown();
        }
    }

    private class TransportStreamFactory implements StreamFactory {
        @Override
        public Stream create(final ServerAddress serverAddress) {
            ManagedChannel channel;
            synchronized (TransportStreamFactoryFactory.this) {
                channel = map.get(serverAddress);
                if (channel == null) {
                    channel = ManagedChannelBuilder.forAddress(serverAddress.getHost(), serverAddress.getPort())
                            .maxInboundMessageSize(48000000)
                            .usePlaintext()
                            .build();
                    // Experimental.  Maybe need more state change notifications...
                    channel.notifyWhenStateChanged(ConnectivityState.SHUTDOWN, () -> {
                        synchronized (TransportStreamFactoryFactory.this) {
                            map.remove(serverAddress);
                        }
                    });
                    map.put(serverAddress, channel);
                }
            }
            return new TransportStream(serverAddress, bufferPool, channel);
        }
    }
}
