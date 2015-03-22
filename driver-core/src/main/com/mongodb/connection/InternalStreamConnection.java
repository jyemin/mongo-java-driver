/*
 * Copyright 2013-2015 MongoDB, Inc.
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

package com.mongodb.connection;

import com.mongodb.MongoException;
import com.mongodb.MongoInternalException;
import com.mongodb.MongoInterruptedException;
import com.mongodb.MongoSocketClosedException;
import com.mongodb.MongoSocketReadException;
import com.mongodb.MongoSocketReadTimeoutException;
import com.mongodb.MongoSocketWriteException;
import com.mongodb.ServerAddress;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.mongodb.event.ConnectionEvent;
import com.mongodb.event.ConnectionListener;
import com.mongodb.event.ConnectionMessageReceivedEvent;
import com.mongodb.event.ConnectionMessagesSentEvent;
import org.bson.ByteBuf;
import org.bson.io.ByteBufferBsonInput;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;
import java.nio.channels.ClosedByInterruptException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.connection.ReplyHeader.REPLY_HEADER_LENGTH;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static java.lang.String.format;

// This class is a bit strange currently.  It supports both concurrent synchronous and asynchronous send and receive, but for simplicity is
// designed to only handle concurrent synchronous OR concurrent asynchronous requests at any given time.  This works because
// Server#getConnection returns instances of the synchronous Connection class, which Server#getConnectionAsync returns instances of the
// asynchronous AsyncConnection class, so at any given time a client's view of a connection is either solely synchronous or solely
// asynchronous.
class InternalStreamConnection implements InternalConnection {
    private final ServerId serverId;
    private final StreamFactory streamFactory;
    private final InternalConnectionInitializer connectionInitializer;
    private final ConnectionListener connectionListener;

    private final LinkedList<SendMessageRequest> writeQueue = new LinkedList<SendMessageRequest>();
    private final ConcurrentHashMap<Integer, SingleResultCallback<ResponseBuffers>> readQueue =
        new ConcurrentHashMap<Integer, SingleResultCallback<ResponseBuffers>>();
    private final ConcurrentMap<Integer, ReceiveMessageResponse> messages = new ConcurrentHashMap<Integer, ReceiveMessageResponse>();

    private final AtomicBoolean isClosed = new AtomicBoolean();
    private final AtomicBoolean opened = new AtomicBoolean();
    private final Lock writerLock = new ReentrantLock(false);
    private final Lock readerLock = new ReentrantLock(false);

    private boolean isWriting;
    private boolean isReading;

    private final AtomicReference<CountDownLatch> readingPhase = new AtomicReference<CountDownLatch>(new CountDownLatch(1));

    private volatile MongoException exceptionThatPrecededStreamClosing;

    private volatile ConnectionDescription description;
    private volatile Stream stream;

    static final Logger LOGGER = Loggers.getLogger("connection");

    InternalStreamConnection(final ServerId serverId, final StreamFactory streamFactory,
                             final InternalConnectionInitializer connectionInitializer,
                             final ConnectionListener connectionListener) {
        this.serverId = notNull("serverId", serverId);
        this.streamFactory = notNull("streamFactory", streamFactory);
        this.connectionInitializer = notNull("connectionInitializer", connectionInitializer);
        this.connectionListener = new ErrorHandlingConnectionListener(notNull("connectionListener", connectionListener));
        description = new ConnectionDescription(serverId);
    }

    @Override
    public ConnectionDescription getDescription() {
        return description;
    }

    @Override
    public void open() {
        isTrue("Open already called", stream == null);
        stream = streamFactory.create(serverId.getAddress());
        try {
            stream.open();
            description = connectionInitializer.initialize(this);
            opened.set(true);

            connectionListener.connectionOpened(new ConnectionEvent(getId()));
            LOGGER.info(format("Opened connection [%s] to %s", getId(), serverId.getAddress()));
        } catch (Throwable t) {
            close();
            if (t instanceof MongoException) {
                throw (MongoException) t;
            } else {
                throw new MongoException(t.toString(), t);
            }
        }
    }

    @Override
    public void openAsync(final SingleResultCallback<Void> callback) {
        isTrue("Open already called", stream == null, callback);
        try {
            stream = streamFactory.create(serverId.getAddress());
        } catch (Throwable t) {
            callback.onResult(null, t);
            return;
        }
        stream.openAsync(new AsyncCompletionHandler<Void>() {
            @Override
            public void completed(final Void aVoid) {
                connectionInitializer.initializeAsync(InternalStreamConnection.this, new SingleResultCallback<ConnectionDescription>() {
                    @Override
                    public void onResult(final ConnectionDescription result, final Throwable t) {
                        if (t != null) {
                            close();
                            callback.onResult(null, t);
                        } else {
                            description = result;
                            opened.set(true);
                            connectionListener.connectionOpened(new ConnectionEvent(getId()));
                            if (LOGGER.isInfoEnabled()) {
                                LOGGER.info(format("Opened connection [%s] to %s", getId(), serverId.getAddress()));
                            }
                            callback.onResult(null, null);
                        }
                    }
                });
            }

            @Override
            public void failed(final Throwable t) {
                callback.onResult(null, t);
            }
        });
    }

    @Override
    public void close() {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(String.format("Closing connection %s", getId()));
        }
        if (stream != null) {
            stream.close();
        }
        isClosed.set(true);
        connectionListener.connectionClosed(new ConnectionEvent(getId()));
    }

    @Override
    public boolean opened() {
        return opened.get();
    }

    @Override
    public boolean isClosed() {
        return isClosed.get();
    }

    @Override
    public void sendMessage(final List<ByteBuf> byteBuffers, final int lastRequestId) {
        notNull("stream is open", stream);

        if (isClosed()) {
            throw new MongoSocketClosedException("Cannot write to a closed stream", getServerAddress());
        }

        writerLock.lock();
        try {
            stream.write(byteBuffers);
            connectionListener.messagesSent(new ConnectionMessagesSentEvent(getId(), lastRequestId, getTotalRemaining(byteBuffers)));
        } catch (Exception e) {
            close();
            throw translateWriteException(e);
        } finally {
            writerLock.unlock();
        }
    }

    @Override
    public ResponseBuffers receiveMessage(final int responseTo) {
        notNull("stream is open", stream);
        if (isClosed()) {
            throw new MongoSocketClosedException("Cannot read from a closed stream", getServerAddress());
        }

        CountDownLatch localLatch = new CountDownLatch(1);
        readerLock.lock();
        try {
            ResponseBuffers responseBuffers = receiveResponseBuffers();
            messages.put(responseBuffers.getReplyHeader().getResponseTo(), new ReceiveMessageResponse(responseBuffers, null));
        } catch (Throwable t) {
            exceptionThatPrecededStreamClosing = translateReadException(t);
            close();
        } finally {
            readerLock.unlock();
        }

        readingPhase.getAndSet(localLatch).countDown();

        while (true) {
            if (isClosed()) {
                if (exceptionThatPrecededStreamClosing != null) {
                    throw exceptionThatPrecededStreamClosing;
                } else {
                    throw new MongoSocketClosedException("Socket has been closed", getServerAddress());
                }
            }
            ReceiveMessageResponse myResponse = messages.get(responseTo);
            if (myResponse != null) {
                connectionListener.messageReceived(new ConnectionMessageReceivedEvent(getId(),
                                                                                      myResponse.getResult()
                                                                                                .getReplyHeader()
                                                                                                .getResponseTo(),
                                                                                      myResponse.getResult()
                                                                                                .getReplyHeader()
                                                                                                .getMessageLength()));
                return myResponse.getResult();
            }

            try {
                localLatch.await();
            } catch (InterruptedException e) {
                throw new MongoInterruptedException("Interrupted while reading from stream", e);
            }

            localLatch = readingPhase.get();
        }
    }

    @Override
    public void sendMessageAsync(final List<ByteBuf> byteBuffers, final int lastRequestId, final SingleResultCallback<Void> callback) {
        notNull("stream is open", stream, callback);

        if (isClosed()) {
            callback.onResult(null, new MongoSocketClosedException("Can not read from a closed socket", getServerAddress()));
        }

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Queuing send message: %s. ([%s])", lastRequestId, getId()));
        }

        SendMessageRequest sendMessageRequest = new SendMessageRequest(byteBuffers, lastRequestId, errorHandlingCallback(callback, LOGGER));

        boolean mustWrite = false;
        writerLock.lock();
        try {
            if (isWriting) {
                writeQueue.add(sendMessageRequest);
            } else {
                isWriting = true;
                mustWrite = true;
            }

        } finally {
            writerLock.unlock();
        }

        if (mustWrite) {
            writeAsync(sendMessageRequest);
        }
    }

    private void writeAsync(final SendMessageRequest request) {
        stream.writeAsync(request.getByteBuffers(), new AsyncCompletionHandler<Void>() {
            @Override
            public void completed(final Void v) {
                SendMessageRequest nextMessage = null;
                writerLock.lock();
                try {
                    nextMessage = writeQueue.poll();
                    if (nextMessage == null) {
                        isWriting = false;
                    }
                } finally {
                    writerLock.unlock();
                }

                connectionListener.messagesSent(new ConnectionMessagesSentEvent(getId(), request.getMessageId(),
                                                                                getTotalRemaining(request.getByteBuffers())));
                request.getCallback().onResult(null, null);

                if (nextMessage != null) {
                    writeAsync(nextMessage);
                }
            }

            @Override
            public void failed(final Throwable t) {
                close();
                request.getCallback().onResult(null, translateWriteException(t));
            }
        });
    }

    @Override
    public void receiveMessageAsync(final int responseTo, final SingleResultCallback<ResponseBuffers> callback) {
        isTrue("stream is open", stream != null, callback);

        if (isClosed()) {
            callback.onResult(null, new MongoSocketClosedException("Can not read from a closed socket", getServerAddress()));
        }

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Queuing read message: %s. ([%s])", responseTo, getId()));
        }

        ReceiveMessageResponse response = null;
        readerLock.lock();
        boolean mustRead = false;
        try {
            response = messages.get(responseTo);

            if (response == null) {
                readQueue.put(responseTo, callback);
            }

            if (!readQueue.isEmpty() && !isReading) {
                isReading = true;
                mustRead = true;
            }
        } finally {
            readerLock.unlock();
        }

        executeCallbackAndReceiveResponse(callback, response == null ? null : response.getResult(), mustRead);
    }

    private void executeCallbackAndReceiveResponse(final SingleResultCallback<ResponseBuffers> callback, final ResponseBuffers result,
                                                   final boolean mustRead) {
        if (callback != null && result != null) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(String.format("Executing callback for %s on %s", result.getReplyHeader().getResponseTo(), getId()));
            }
            callback.onResult(result, null);
        }

        if (mustRead) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(String.format("Start receiving response on %s", getId()));
            }
            receiveResponseAsync();
        }
    }

    private class ResponseBuffersCallback implements SingleResultCallback<ResponseBuffers> {
        @Override
        public void onResult(final ResponseBuffers result, final Throwable t) {
            if (t != null) {
                failAllQueuedReads(t);
            } else {
                SingleResultCallback<ResponseBuffers> callback = null;
                boolean mustRead = false;
                readerLock.lock();
                try {
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace(String.format("Read response to message %s on %s", result.getReplyHeader().getResponseTo(), getId()));
                    }

                    callback = readQueue.remove(result.getReplyHeader().getResponseTo());

                    if (readQueue.isEmpty()) {
                        isReading = false;
                    } else {
                        mustRead = true;
                    }

                    if (callback == null) {
                        messages.put(result.getReplyHeader().getResponseTo(), new ReceiveMessageResponse(result, null));
                    }
                } finally {
                    readerLock.unlock();
                }

                executeCallbackAndReceiveResponse(callback, result, mustRead);
            }
        }
    }

    private ConnectionId getId() {
        return description.getConnectionId();
    }

    private ServerAddress getServerAddress() {
        return description.getServerAddress();
    }

    private void receiveResponseAsync() {
        readAsync(REPLY_HEADER_LENGTH,
                  errorHandlingCallback(new ResponseHeaderCallback(new ResponseBuffersCallback()), LOGGER));
    }

    private void readAsync(final int numBytes, final SingleResultCallback<ByteBuf> callback) {
        if (isClosed()) {
            callback.onResult(null, new MongoSocketClosedException("Cannot read from a closed stream", getServerAddress()));
        } else {
            try {
                stream.readAsync(numBytes, new AsyncCompletionHandler<ByteBuf>() {
                    @Override
                    public void completed(final ByteBuf buffer) {
                        callback.onResult(buffer, null);
                    }

                    @Override
                    public void failed(final Throwable t) {
                        close();
                        callback.onResult(null, translateReadException(t));
                    }
                });
            } catch (Exception e) {
                callback.onResult(null, translateReadException(e));
            }
        }
    }

    private MongoException translateWriteException(final Throwable e) {
        if (e instanceof MongoException) {
            return (MongoException) e;
        } else if (e instanceof IOException) {
            return new MongoSocketWriteException("Exception sending message", getServerAddress(), e);
        } else if (e instanceof InterruptedException) {
            return new MongoInternalException("Thread interrupted exception", e);
        } else {
            return new MongoInternalException("Unexpected exception", e);
        }
    }

    private MongoException translateReadException(final Throwable e) {
        if (e instanceof MongoException) {
            return (MongoException) e;
        } else if (e instanceof SocketTimeoutException) {
            return new MongoSocketReadTimeoutException("Timeout while receiving message", getServerAddress(), e);
        } else if (e instanceof InterruptedIOException) {
            return new MongoInterruptedException("Interrupted while receiving message", (InterruptedIOException) e);
        } else if (e instanceof ClosedByInterruptException) {
            return new MongoInterruptedException("Interrupted while receiving message", (ClosedByInterruptException) e);
        } else if (e instanceof IOException) {
            return new MongoSocketReadException("Exception receiving message", getServerAddress(), e);
        } else if (e instanceof RuntimeException) {
            return new MongoInternalException("Unexpected runtime exception", e);
        } else if (e instanceof InterruptedException) {
            return new MongoInternalException("Interrupted exception", e);
        } else {
            return new MongoInternalException("Unexpected exception", e);
        }
    }

    private ResponseBuffers receiveResponseBuffers() throws IOException {
        ByteBuf headerByteBuffer = stream.read(REPLY_HEADER_LENGTH);
        ReplyHeader replyHeader;
        ByteBufferBsonInput headerInputBuffer = new ByteBufferBsonInput(headerByteBuffer);
        try {
            replyHeader = new ReplyHeader(headerInputBuffer);
        } finally {
            headerInputBuffer.close();
        }

        ByteBuf bodyByteBuffer = null;

        if (replyHeader.getNumberReturned() > 0) {
            bodyByteBuffer = stream.read(replyHeader.getMessageLength() - REPLY_HEADER_LENGTH);
        }
        return new ResponseBuffers(replyHeader, bodyByteBuffer);
    }

    @Override
    public ByteBuf getBuffer(final int size) {
        notNull("open", stream);
        return stream.getBuffer(size);
    }

    private class ResponseHeaderCallback implements SingleResultCallback<ByteBuf> {
        private final SingleResultCallback<ResponseBuffers> callback;

        public ResponseHeaderCallback(final SingleResultCallback<ResponseBuffers> callback) {
            this.callback = callback;
        }

        @Override
        public void onResult(final ByteBuf result, final Throwable t) {
            if (t != null) {
                callback.onResult(null, t);
            } else {
                ReplyHeader replyHeader;
                ByteBufferBsonInput headerInputBuffer = new ByteBufferBsonInput(result);
                try {
                    replyHeader = new ReplyHeader(headerInputBuffer);
                } finally {
                    headerInputBuffer.close();
                }

                if (replyHeader.getMessageLength() == REPLY_HEADER_LENGTH) {
                    onSuccess(new ResponseBuffers(replyHeader, null));
                } else {
                    readAsync(replyHeader.getMessageLength() - REPLY_HEADER_LENGTH,
                              new ResponseBodyCallback(replyHeader));
                }
            }
        }

        private void onSuccess(final ResponseBuffers responseBuffers) {

            if (responseBuffers == null) {
                callback.onResult(null, new MongoException("Unexpected empty response buffers"));
                return;
            }

            connectionListener.messageReceived(new ConnectionMessageReceivedEvent(getId(),
                                                                                  responseBuffers.getReplyHeader().getResponseTo(),
                                                                                  responseBuffers.getReplyHeader().getMessageLength()));

            try {
                callback.onResult(responseBuffers, null);
            } catch (Throwable t) {
                LOGGER.warn("Exception calling callback", t);
            }
        }

        private class ResponseBodyCallback implements SingleResultCallback<ByteBuf> {
            private final ReplyHeader replyHeader;

            public ResponseBodyCallback(final ReplyHeader replyHeader) {
                this.replyHeader = replyHeader;
            }

            @Override
            public void onResult(final ByteBuf result, final Throwable t) {
                if (t != null) {
                    try {
                        callback.onResult(new ResponseBuffers(replyHeader, result), t);
                    } catch (Throwable tr) {
                        LOGGER.warn("Exception calling callback", tr);
                    }
                } else {
                    onSuccess(new ResponseBuffers(replyHeader, result));
                }
            }
        }
    }

    private int getTotalRemaining(final List<ByteBuf> byteBuffers) {
        int messageSize = 0;
        for (final ByteBuf cur : byteBuffers) {
            messageSize += cur.remaining();
        }
        return messageSize;
    }

    private void failAllQueuedReads(final Throwable t) {
        close();
        Iterator<Map.Entry<Integer, SingleResultCallback<ResponseBuffers>>> it = readQueue.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Integer, SingleResultCallback<ResponseBuffers>> pairs = it.next();
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(format("Processing unknown failed message: %s. ([%s] %s)", pairs.getKey(), getId(), serverId));
            }
            SingleResultCallback<ResponseBuffers> callback = pairs.getValue();
            it.remove();
            try {
                callback.onResult(null, t);
            } catch (Throwable tr) {
                LOGGER.warn("Exception calling callback", tr);
            }
        }
    }

    private static class SendMessageRequest {
        private final SingleResultCallback<Void> callback;
        private final List<ByteBuf> byteBuffers;
        private final int messageId;

        SendMessageRequest(final List<ByteBuf> byteBuffers, final int messageId, final SingleResultCallback<Void> callback) {
            this.byteBuffers = byteBuffers;
            this.messageId = messageId;
            this.callback = callback;
        }

        public SingleResultCallback<Void> getCallback() {
            return callback;
        }

        public List<ByteBuf> getByteBuffers() {
            return byteBuffers;
        }

        public int getMessageId() {
            return messageId;
        }
    }


    private static class ReceiveMessageResponse {
        private final ResponseBuffers result;
        private final MongoException e;

        public ReceiveMessageResponse(final ResponseBuffers result, final MongoException e) {
            this.result = result;
            this.e = e;
        }

        public ResponseBuffers getResult() {
            return result;
        }

        public MongoException getError() {
            return e;
        }

        public boolean hasError() {
            return e != null;
        }
    }

    private static class ErrorHandlingConnectionListener implements ConnectionListener {

        private final ConnectionListener wrapped;

        public ErrorHandlingConnectionListener(final ConnectionListener wrapped) {
            this.wrapped = wrapped;
        }

        @Override
        public void connectionOpened(final ConnectionEvent event) {
            try {
                wrapped.connectionOpened(event);
            } catch (Throwable t) {
                LOGGER.warn("Exception when trying to signal connectionOpened to the connectionListener", t);
            }
       }

        @Override
        public void connectionClosed(final ConnectionEvent event) {
            try {
                wrapped.connectionClosed(event);
            } catch (Throwable t) {
                LOGGER.warn("Exception when trying to signal connectionOpened to the connectionListener", t);
            }
        }

        @Override
        public void messagesSent(final ConnectionMessagesSentEvent event) {
            try {
                wrapped.messagesSent(event);
            } catch (Throwable t) {
                LOGGER.warn("Exception when trying to signal connectionOpened to the connectionListener", t);
            }
        }

        @Override
        public void messageReceived(final ConnectionMessageReceivedEvent event) {
            try {
                wrapped.messageReceived(event);
            } catch (Throwable t) {
                LOGGER.warn("Exception when trying to signal connectionOpened to the connectionListener", t);
            }
        }
    }
}
