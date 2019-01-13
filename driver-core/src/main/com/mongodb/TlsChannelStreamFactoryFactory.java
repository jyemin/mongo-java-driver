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

package com.mongodb;

import com.mongodb.connection.AsyncCompletionHandler;
import com.mongodb.connection.BufferProvider;
import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.SslSettings;
import com.mongodb.connection.Stream;
import com.mongodb.connection.StreamFactory;
import com.mongodb.connection.StreamFactoryFactory;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.mongodb.internal.connection.ConcurrentLinkedDeque;
import com.mongodb.internal.connection.PowerOfTwoBufferPool;
import com.mongodb.internal.connection.tlschannel.BufferAllocator;
import com.mongodb.internal.connection.tlschannel.ClientTlsChannel;
import com.mongodb.internal.connection.tlschannel.TlsChannel;
import com.mongodb.internal.connection.tlschannel.async.AsynchronousTlsChannel;
import com.mongodb.internal.connection.tlschannel.async.AsynchronousTlsChannelGroup;
import org.bson.ByteBuf;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import java.io.Closeable;
import java.io.IOException;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;
import java.nio.channels.InterruptedByTimeoutException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.internal.connection.SslHelper.enableHostNameVerification;
import static com.mongodb.internal.connection.SslHelper.enableSni;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * POC
 *
 * @since 3.10
 */
public class TlsChannelStreamFactoryFactory implements StreamFactoryFactory, Closeable {

    private static final Logger LOGGER = Loggers.getLogger("connection.tls");

    private final SelectorMonitor selectorMonitor;
    private final AsynchronousTlsChannelGroup group;
    private final PowerOfTwoBufferPool bufferPool = new PowerOfTwoBufferPool();

    /**
     * POC
     */
    public TlsChannelStreamFactoryFactory() {
        group = new AsynchronousTlsChannelGroup();
        selectorMonitor = new SelectorMonitor();
        selectorMonitor.start();
    }

    @Override
    public StreamFactory create(final SocketSettings socketSettings, final SslSettings sslSettings) {
        return new StreamFactory() {
            @Override
            public Stream create(final ServerAddress serverAddress) {
                return new TlsChannelStream(serverAddress, socketSettings, sslSettings, bufferPool, group, selectorMonitor);
            }
        };
    }

    @Override
    public void close() {
        selectorMonitor.close();
        group.shutdown();
    }

    private static class SelectorMonitor implements Closeable {

        private static final class Pair {
            private final SocketChannel socketChannel;
            private final Runnable attachment;

            private Pair(final SocketChannel socketChannel, final Runnable attachment) {
                this.socketChannel = socketChannel;
                this.attachment = attachment;
            }
        }

        private final Selector selector;
        private volatile boolean isClosed;
        private final ConcurrentLinkedDeque<Pair> pendingRegistrations = new ConcurrentLinkedDeque<Pair>();

        SelectorMonitor() {
            try {
                this.selector = Selector.open();
            } catch (IOException e) {
                throw new MongoClientException("Exception opening Selector", e);
            }
        }

        void start() {
            Thread selectorThread = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        while (!isClosed) {
                            try {
                                selector.select();

                                for (SelectionKey selectionKey : selector.selectedKeys()) {
                                    selectionKey.cancel();
                                    Runnable runnable = (Runnable) selectionKey.attachment();
                                    runnable.run();
                                }

                                for (Iterator<Pair> iter = pendingRegistrations.iterator(); iter.hasNext();) {
                                    Pair pendingRegistration = iter.next();
                                    pendingRegistration.socketChannel.register(selector, SelectionKey.OP_CONNECT,
                                            pendingRegistration.attachment);
                                    iter.remove();
                                }
                            } catch (IOException e) {
                                LOGGER.warn("Exception in selector loop", e);
                            } catch (RuntimeException e) {
                                LOGGER.warn("Exception in selector loop", e);
                            }
                        }
                    } finally {
                        try {
                            selector.close();
                        } catch (IOException e) {
                            // ignore
                        }
                    }
                }
            });
            selectorThread.setDaemon(true);
            selectorThread.start();
        }

        void register(final SocketChannel channel, final Runnable attachment) {
            pendingRegistrations.add(new Pair(channel, attachment));
            selector.wakeup();
        }

        @Override
        public void close() {
            isClosed = true;
            selector.wakeup();
        }
    }

    private static class TlsChannelStream implements Stream {

        private final ServerAddress serverAddress;
        private final SocketSettings settings;
        private final SslSettings sslSettings;
        private final BufferProvider bufferProvider;
        private final AsynchronousTlsChannelGroup group;
        private final SelectorMonitor selectorMonitor;
        private volatile AsynchronousTlsChannel channel;
        private volatile boolean isClosed;

        TlsChannelStream(final ServerAddress serverAddress, final SocketSettings settings, final SslSettings sslSettings,
                         final BufferProvider bufferProvider, final AsynchronousTlsChannelGroup group,
                         final SelectorMonitor selectorMonitor) {
            this.serverAddress = serverAddress;
            this.settings = settings;
            this.sslSettings = sslSettings;
            this.bufferProvider = bufferProvider;
            this.group = group;
            this.selectorMonitor = selectorMonitor;
        }

        @Override
        public void openAsync(final AsyncCompletionHandler<Void> handler) {
            isTrue("unopened", channel == null);
            try {
                final SocketChannel socketChannel = SocketChannel.open();
                socketChannel.configureBlocking(false);

                socketChannel.setOption(StandardSocketOptions.TCP_NODELAY, true);
                socketChannel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
                if (settings.getReceiveBufferSize() > 0) {
                    socketChannel.setOption(StandardSocketOptions.SO_RCVBUF, settings.getReceiveBufferSize());
                }
                if (settings.getSendBufferSize() > 0) {
                    socketChannel.setOption(StandardSocketOptions.SO_SNDBUF, settings.getSendBufferSize());
                }

                socketChannel.connect(serverAddress.getSocketAddress());

                selectorMonitor.register(socketChannel, new Runnable() {
                    @Override
                    public void run() {
                        try {
                            if (!socketChannel.finishConnect()) {
                                throw new MongoSocketOpenException("Failed to finish connect", serverAddress);
                            }

                            SSLEngine sslEngine = getSslContext().createSSLEngine(serverAddress.getHost(), serverAddress.getPort());
                            sslEngine.setUseClientMode(true);

                            SSLParameters sslParameters = sslEngine.getSSLParameters();
                            enableSni(serverAddress.getHost(), sslParameters);

                            if (!sslSettings.isInvalidHostNameAllowed()) {
                                enableHostNameVerification(sslParameters);
                            }
                            sslEngine.setSSLParameters(sslParameters);

                            BufferAllocator bufferAllocator = new BufferProviderAllocator();

                            TlsChannel tlsChannel = ClientTlsChannel.newBuilder(socketChannel, sslEngine)
                                    .withEncryptedBufferAllocator(bufferAllocator)
                                    .withPlainBufferAllocator(bufferAllocator)
                                    .build();

                            // build asynchronous channel, based in the TLS channel and associated with the global group.
                            channel = new AsynchronousTlsChannel(group, tlsChannel, socketChannel);

                            handler.completed(null);
                        } catch (IOException e) {
                            handler.failed(new MongoSocketOpenException("Exception opening socket", serverAddress, e));
                        } catch (Throwable t) {
                            handler.failed(t);
                        }
                    }
                });
            } catch (IOException e) {
                handler.failed(new MongoSocketOpenException("Exception opening socket", serverAddress, e));
            } catch (Throwable t) {
                handler.failed(t);
            }
        }

        @Override
        public void writeAsync(final List<ByteBuf> buffers, final AsyncCompletionHandler<Void> handler) {
            final AsyncWritableByteChannelAdapter byteChannel = new AsyncWritableByteChannelAdapter();
            final Iterator<ByteBuf> iter = buffers.iterator();
            pipeOneBuffer(byteChannel, iter.next(), new AsyncCompletionHandler<Void>() {
                @Override
                public void completed(final Void t) {
                    if (iter.hasNext()) {
                        pipeOneBuffer(byteChannel, iter.next(), this);
                    } else {
                        handler.completed(null);
                    }
                }

                @Override
                public void failed(final Throwable t) {
                    handler.failed(t);
                }
            });
        }

        @Override
        public void readAsync(final int numBytes, final AsyncCompletionHandler<ByteBuf> handler) {
            ByteBuf buffer = bufferProvider.getBuffer(numBytes);
            channel.read(buffer.asNIO(), settings.getReadTimeout(MILLISECONDS), MILLISECONDS, null,
                    new BasicCompletionHandler(buffer, handler));
        }

        @Override
        public void open() throws IOException {
            FutureAsyncCompletionHandler<Void> handler = new FutureAsyncCompletionHandler<Void>();
            openAsync(handler);
            handler.getOpen();
        }

        @Override
        public void write(final List<ByteBuf> buffers) throws IOException {
            FutureAsyncCompletionHandler<Void> handler = new FutureAsyncCompletionHandler<Void>();
            writeAsync(buffers, handler);
            handler.getWrite();
        }

        @Override
        public ByteBuf read(final int numBytes) throws IOException {
            FutureAsyncCompletionHandler<ByteBuf> handler = new FutureAsyncCompletionHandler<ByteBuf>();
            readAsync(numBytes, handler);
            return handler.getRead();
        }

        @Override
        public ServerAddress getAddress() {
            return serverAddress;
        }

        @Override
        public void close() {
            try {
                if (channel != null) {
                    channel.close();
                }
            } catch (IOException e) {
                // ignore
            } finally {
                channel = null;
                isClosed = true;
            }
        }

        @Override
        public boolean isClosed() {
            return isClosed;
        }

        @Override
        public ByteBuf getBuffer(final int size) {
            return bufferProvider.getBuffer(size);
        }

        private SSLContext getSslContext() {
            try {
                return (sslSettings.getContext() == null) ? SSLContext.getDefault() : sslSettings.getContext();
            } catch (NoSuchAlgorithmException e) {
                throw new MongoClientException("Unable to create default SSLContext", e);
            }
        }

        private void pipeOneBuffer(final AsyncWritableByteChannelAdapter byteChannel, final ByteBuf byteBuffer,
                                   final AsyncCompletionHandler<Void> outerHandler) {
            byteChannel.write(byteBuffer.asNIO(), new AsyncCompletionHandler<Void>() {
                @Override
                public void completed(final Void t) {
                    if (byteBuffer.hasRemaining()) {
                        byteChannel.write(byteBuffer.asNIO(), this);
                    } else {
                        outerHandler.completed(null);
                    }
                }

                @Override
                public void failed(final Throwable t) {
                    outerHandler.failed(t);
                }
            });
        }

        private class AsyncWritableByteChannelAdapter {
            void write(final ByteBuffer src, final AsyncCompletionHandler<Void> handler) {
                channel.write(src, null, new AsyncWritableByteChannelAdapter.WriteCompletionHandler(handler));
            }

            private class WriteCompletionHandler extends BaseCompletionHandler<Void, Integer, Object> {

                WriteCompletionHandler(final AsyncCompletionHandler<Void> handler) {
                    super(handler);
                }

                @Override
                public void completed(final Integer result, final Object attachment) {
                    AsyncCompletionHandler<Void> localHandler = getHandlerAndClear();
                    localHandler.completed(null);
                }

                @Override
                public void failed(final Throwable exc, final Object attachment) {
                    AsyncCompletionHandler<Void> localHandler = getHandlerAndClear();
                    localHandler.failed(exc);
                }
            }
        }

        private final class BasicCompletionHandler extends BaseCompletionHandler<ByteBuf, Integer, Void> {
            private final AtomicReference<ByteBuf> byteBufReference;

            private BasicCompletionHandler(final ByteBuf dst, final AsyncCompletionHandler<ByteBuf> handler) {
                super(handler);
                this.byteBufReference = new AtomicReference<ByteBuf>(dst);
            }

            @Override
            public void completed(final Integer result, final Void attachment) {
                AsyncCompletionHandler<ByteBuf> localHandler = getHandlerAndClear();
                ByteBuf localByteBuf = byteBufReference.getAndSet(null);
                if (result == -1) {
                    localByteBuf.release();
                    localHandler.failed(new MongoSocketReadException("Prematurely reached end of stream", serverAddress));
                } else if (!localByteBuf.hasRemaining()) {
                    localByteBuf.flip();
                    localHandler.completed(localByteBuf);
                } else {
                    channel.read(localByteBuf.asNIO(), settings.getReadTimeout(MILLISECONDS), MILLISECONDS, null,
                            new BasicCompletionHandler(localByteBuf, localHandler));
                }
            }

            @Override
            public void failed(final Throwable t, final Void attachment) {
                AsyncCompletionHandler<ByteBuf> localHandler = getHandlerAndClear();
                ByteBuf localByteBuf = byteBufReference.getAndSet(null);
                localByteBuf.release();
                if (t instanceof InterruptedByTimeoutException) {
                    localHandler.failed(new MongoSocketReadTimeoutException("Timeout while receiving message", serverAddress, t));
                } else {
                    localHandler.failed(t);
                }
            }
        }

        // Private base class for all CompletionHandler implementors that ensures the upstream handler is
        // set to null before it is used.  This is to work around an observed issue with implementations of
        // AsynchronousSocketChannel that fail to clear references to handlers stored in instance fields of
        // the class.
        private abstract static class BaseCompletionHandler<T, V, A> implements CompletionHandler<V, A> {
            private final AtomicReference<AsyncCompletionHandler<T>> handlerReference;

            BaseCompletionHandler(final AsyncCompletionHandler<T> handler) {
                this.handlerReference = new AtomicReference<AsyncCompletionHandler<T>>(handler);
            }

            AsyncCompletionHandler<T> getHandlerAndClear() {
                return handlerReference.getAndSet(null);
            }
        }

        static class FutureAsyncCompletionHandler<T> implements AsyncCompletionHandler<T> {
            private final CountDownLatch latch = new CountDownLatch(1);
            private volatile T result;
            private volatile Throwable error;

            @Override
            public void completed(final T result) {
                this.result = result;
                latch.countDown();
            }

            @Override
            public void failed(final Throwable t) {
                this.error = t;
                latch.countDown();
            }

            void getOpen() throws IOException {
                get("Opening");
            }

            void getWrite() throws IOException {
                get("Writing to");
            }

            T getRead() throws IOException {
                return get("Reading from");
            }

            private T get(final String prefix) throws IOException {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    throw new MongoInterruptedException(prefix + " the AsynchronousSocketChannelStream failed", e);

                }
                if (error != null) {
                    if (error instanceof IOException) {
                        throw (IOException) error;
                    } else if (error instanceof MongoException) {
                        throw (MongoException) error;
                    } else {
                        throw new MongoInternalException(prefix + " the TlsChannelStream failed", error);
                    }
                }
                return result;
            }

        }

        private class BufferProviderAllocator implements BufferAllocator {
            @Override
            public ByteBuf allocate(final int size) {
                return bufferProvider.getBuffer(size);
            }

            @Override
            public void free(final ByteBuf buffer) {
                buffer.release();
            }
        }
    }
}

