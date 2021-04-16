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

import com.mongodb.MongoClientException;
import com.mongodb.MongoSocketReadException;
import com.mongodb.MongoSocketWriteException;
import com.mongodb.ServerAddress;
import com.mongodb.connection.AsyncCompletionHandler;
import com.mongodb.connection.BufferProvider;
import com.mongodb.connection.Stream;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.Drainable;
import io.grpc.KnownLength;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import org.bson.ByteBuf;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executor;

import static com.mongodb.assertions.Assertions.isTrue;
import static io.grpc.stub.ClientCalls.blockingUnaryCall;

/**
 * A Stream implementation for gRPC.
 */
public final class TransportStream implements Stream {
    private final ServerAddress serverAddress;
    private final BufferProvider bufferProvider;
    private final Channel channel;
    private final String connectionId = UUID.randomUUID().toString();

    private volatile boolean isClosed = false;

    private ByteBuf header;
    private ByteBuf body;

    /**
     * The constructor.
     *
     * @param serverAddress  the server address
     * @param bufferProvider the buffer provider
     * @param channel        the channel
     */
    public TransportStream(final ServerAddress serverAddress, final BufferProvider bufferProvider, final Channel channel) {
        this.serverAddress = serverAddress;
        this.bufferProvider = bufferProvider;
        this.channel = channel;
    }

    @Override
    public ByteBuf getBuffer(final int size) {
        return bufferProvider.getBuffer(size);
    }

    @Override
    public void open() throws IOException {
        // nothing to do!
    }

    @Override
    public void openAsync(final AsyncCompletionHandler<Void> handler) {
        // nothing to do!
        handler.completed(null);
    }

    @Override
    public void write(final List<ByteBuf> buffers) throws IOException {
        MethodDescriptor.Marshaller<List<ByteBuf>> marshaller = new MessageDescriptor();
        List<ByteBuf> replyBuffers = blockingUnaryCall(channel,
                MethodDescriptor.newBuilder(marshaller, marshaller)
                        .setFullMethodName("OP_MSG")
                        .setType(MethodDescriptor.MethodType.UNARY)
                        .build(),
                CallOptions.DEFAULT
                        .withCallCredentials(new MongoCallCredentials()),
                buffers);
        header = replyBuffers.get(0);
        body = replyBuffers.get(1);
    }

    @Override
    public ByteBuf read(final int numBytes) throws IOException {
        ByteBuf retVal;
        if (header != null) {
            isTrue("Reading whole header", numBytes == header.remaining());
            retVal = header;
            header = null;
        } else if (body != null) {
            isTrue("Reading whole body", numBytes == body.remaining());
            retVal = body;
            body = null;
        } else {
            throw new MongoClientException("Oops");
        }

        return retVal;
    }

    @Override
    public ByteBuf read(int numBytes, int additionalTimeout) throws IOException {
        return read(numBytes);
    }


    @Override
    public void writeAsync(final List<ByteBuf> buffers, final AsyncCompletionHandler<Void> handler) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void readAsync(final int numBytes, final AsyncCompletionHandler<ByteBuf> handler) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ServerAddress getAddress() {
        return serverAddress;
    }

    @Override
    public void close() {
        isClosed = true;
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    private class MongoCallCredentials extends io.grpc.CallCredentials {
        @Override
        public void applyRequestMetadata(final RequestInfo requestInfo, final Executor appExecutor, final MetadataApplier applier) {
            appExecutor.execute(() -> {
                Metadata headers = new Metadata();
                Metadata.Key<String> jwtKey = Metadata.Key.of("lcid", Metadata.ASCII_STRING_MARSHALLER);
                headers.put(jwtKey, connectionId);
                applier.apply(headers);
            });
        }

        @Override
        public void thisUsesUnstableApi() {
        }
    }

    private final class MessageDescriptor implements MethodDescriptor.Marshaller<List<ByteBuf>> {
        @Override
        public InputStream stream(final List<ByteBuf> buffers) {
            return new ByteBufDrainableInputStream(buffers);
        }

        @Override
        public List<ByteBuf> parse(final InputStream stream) {
            org.bson.ByteBuf header = bufferProvider.getBuffer(16);
            try {
                //noinspection ResultOfMethodCallIgnored
                stream.read(header.array());
                int remaining = header.getInt(0);

                org.bson.ByteBuf body = bufferProvider.getBuffer(remaining);
                //noinspection ResultOfMethodCallIgnored
                stream.read(body.array());
                List<ByteBuf> retVal = new ArrayList<>(2);
                retVal.add(header);
                retVal.add(body);
                return retVal;
            } catch (IOException e) {
                throw new MongoSocketReadException("", serverAddress, e);
            }
        }
    }

    private final class ByteBufDrainableInputStream extends InputStream implements KnownLength, Drainable {

        private final List<ByteBuf> buffers;

        public ByteBufDrainableInputStream(List<ByteBuf> buffers) {
            this.buffers = buffers;
        }

        @Override
        public int drainTo(final OutputStream target) {
            try {
                for (ByteBuf cur: buffers) {
                    // This is a bit sketchy.  Assumes a 0 offset into the backing array.
                    target.write(cur.array(), cur.position(), cur.remaining());
                }
            } catch (IOException e) {
                throw new MongoSocketWriteException("This shouldn't really happen", serverAddress, e);
            }
            return available();
        }

        @Override
        public int read() {
            // TODO
            throw new UnsupportedOperationException("I guess this is needed after all");
        }

        public int available() {
            int available = 0;
            for (ByteBuf cur : buffers) {
                available += cur.remaining();
            }
            return available;
        }
    }
}
