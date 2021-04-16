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

import com.google.protobuf.ByteString;
import com.mongodb.MongoException;
import com.mongodb.MongoInternalException;
import com.mongodb.MongoInterruptedException;
import com.mongodb.ServerAddress;
import com.mongodb.connection.AsyncCompletionHandler;
import com.mongodb.connection.BufferProvider;
import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.Stream;
import com.mongodb.internal.connection.TransportGrpc;
import com.mongodb.internal.connection.TransportGrpc.TransportBlockingStub;
import com.mongodb.internal.connection.TransportGrpc.TransportStub;
import com.mongodb.internal.connection.TransportOuterClass.Message;
import io.grpc.Channel;
import io.grpc.Metadata;
import io.grpc.stub.StreamObserver;
import org.bson.ByteBuf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

/**
 * A Stream implementation for gRPC.
 */
public final class TransportStream implements Stream {
    private final ServerAddress serverAddress;
    private final BufferProvider bufferProvider;
    private final TransportStub transportStub;
    private final TransportBlockingStub transportBlockingStub;
    private SocketSettings socketSettings;
    private final String connectionId = UUID.randomUUID().toString();

    private volatile boolean isClosed = false;

    private ByteBuffer nextResponse;
    private Throwable nextError;
    private PendingReader pendingReader;

    /**
     * The constructor.
     *
     * @param serverAddress  the server address
     * @param bufferProvider the buffer provider
     * @param channel        the channel
     * @param socketSettings the socket settings
     */
    public TransportStream(final ServerAddress serverAddress, final BufferProvider bufferProvider, final Channel channel,
                           final SocketSettings socketSettings) {
        this.serverAddress = serverAddress;
        this.bufferProvider = bufferProvider;
        this.transportStub = TransportGrpc.newStub(channel).withCallCredentials(new MongoCallCredentials());
        this.transportBlockingStub = TransportGrpc.newBlockingStub(channel).withCallCredentials(new MongoCallCredentials());
        this.socketSettings = socketSettings;
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
        Message message = Message.newBuilder()
                .setPayload(ByteString.copyFrom(toByteStrings(buffers)))
                .build();
        Message replyMessage = transportBlockingStub.sendMessage(message);
        nextResponse = replyMessage.getPayload().asReadOnlyByteBuffer();
    }

    @Override
    public ByteBuf read(final int numBytes) throws IOException {
        ByteBuf readBytes = bufferProvider.getBuffer(numBytes);
        nextResponse.get(readBytes.array(), 0, numBytes);
        if (!nextResponse.hasRemaining()) {
            nextResponse = null;
        }
        return readBytes;
    }

    @Override
    public ByteBuf read(int numBytes, int additionalTimeout) throws IOException {
        return read(numBytes);
    }


    @Override
    public void writeAsync(final List<ByteBuf> buffers, final AsyncCompletionHandler<Void> handler) {
        Message message = Message.newBuilder()
                .setPayload(ByteString.copyFrom(toByteStrings(buffers)))
                .build();
        transportStub
                .sendMessage(
                        message,
                        new StreamObserver<Message>() {
                            @Override
                            public void onNext(final Message value) {
                                synchronized (TransportStream.this) {
                                    nextResponse = value.getPayload().asReadOnlyByteBuffer();
                                }
                            }

                            @Override
                            public void onError(final Throwable t) {
                                synchronized (TransportStream.this) {
                                    nextError = t;
                                    complete();
                                }
                            }

                            @Override
                            public void onCompleted() {
                                complete();
                            }

                            private void complete() {
                                synchronized (TransportStream.this) {
                                    if (pendingReader != null) {
                                        PendingReader localPendingReader = pendingReader;
                                        pendingReader = null;
                                        readAsync(localPendingReader.numBytes, localPendingReader.handler);
                                    }
                                }
                            }
                        });
        handler.completed(null);
    }

    @Override
    public void readAsync(final int numBytes, final AsyncCompletionHandler<ByteBuf> handler) {
        ByteBuf readBytes = null;
        Throwable localNextError = null;
        synchronized (this) {
            if (nextError != null) {
                localNextError = nextError;
                nextError = null;
            } else if (nextResponse != null) {
                readBytes = bufferProvider.getBuffer(numBytes);
                nextResponse.get(readBytes.array(), 0, numBytes);
                if (!nextResponse.hasRemaining()) {
                    nextResponse = null;
                }
            } else {
                pendingReader = new PendingReader(numBytes, handler);
            }
        }
        if (localNextError != null) {
            handler.failed(localNextError);
        } else if (readBytes != null) {
            handler.completed(readBytes);
        }
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

    private Iterable<ByteString> toByteStrings(final List<ByteBuf> buffers) {
        return buffers.stream()
                .map(byteBuf -> ByteString.copyFrom(byteBuf.asNIO()))
                .collect(Collectors.toList());
    }

    private static final class FutureAsyncCompletionHandler<T> implements AsyncCompletionHandler<T> {
        private final CountDownLatch latch = new CountDownLatch(1);
        private volatile T t;
        private volatile Throwable throwable;

        FutureAsyncCompletionHandler() {
        }

        @Override
        public void completed(final T t) {
            this.t = t;
            latch.countDown();
        }

        @Override
        public void failed(final Throwable t) {
            this.throwable = t;
            latch.countDown();
        }

        public T get() throws IOException {
            try {
                latch.await();
                if (throwable != null) {
                    if (throwable instanceof IOException) {
                        throw (IOException) throwable;
                    } else if (throwable instanceof MongoException) {
                        throw (MongoException) throwable;
                    } else {
                        throw new MongoInternalException("Exception thrown from Netty Stream", throwable);
                    }
                }
                return t;
            } catch (InterruptedException e) {
                throw new MongoInterruptedException("Interrupted", e);
            }
        }
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

    private static final class PendingReader {
        private final int numBytes;
        private final AsyncCompletionHandler<ByteBuf> handler;

        public PendingReader(int numBytes, AsyncCompletionHandler<ByteBuf> handler) {
            this.numBytes = numBytes;
            this.handler = handler;
        }
    }
}
