/*
 * Copyright 2013-2016 MongoDB, Inc.
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

import com.mongodb.MongoClientException;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoCompressor;
import com.mongodb.MongoException;
import com.mongodb.MongoInternalException;
import com.mongodb.MongoInterruptedException;
import com.mongodb.MongoSocketClosedException;
import com.mongodb.MongoSocketReadException;
import com.mongodb.MongoSocketReadTimeoutException;
import com.mongodb.MongoSocketWriteException;
import com.mongodb.ServerAddress;
import com.mongodb.annotations.NotThreadSafe;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.mongodb.event.CommandListener;
import com.mongodb.session.SessionContext;
import org.bson.BsonBinaryReader;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonValue;
import org.bson.ByteBuf;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.Decoder;
import org.bson.codecs.RawBsonDocumentCodec;
import org.bson.io.ByteBufferBsonInput;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;
import java.nio.channels.ClosedByInterruptException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.connection.MessageHeader.MESSAGE_HEADER_LENGTH;
import static com.mongodb.connection.OpCode.OP_COMPRESSED;
import static com.mongodb.connection.ProtocolHelper.getClusterTime;
import static com.mongodb.connection.ProtocolHelper.getCommandFailureException;
import static com.mongodb.connection.ProtocolHelper.getMessageSettings;
import static com.mongodb.connection.ProtocolHelper.getOperationTime;
import static com.mongodb.connection.ProtocolHelper.isCommandOk;
import static com.mongodb.connection.ProtocolHelper.sendCommandFailedEvent;
import static com.mongodb.connection.ProtocolHelper.sendCommandStartedEvent;
import static com.mongodb.connection.ProtocolHelper.sendCommandSucceededEvent;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static java.lang.String.format;
import static java.util.Arrays.asList;

@NotThreadSafe
class InternalStreamConnection implements InternalConnection {

    private static final Set<String> SECURITY_SENSITIVE_COMMANDS = new HashSet<String>(asList(
            "authenticate",
            "saslStart",
            "saslContinue",
            "getnonce",
            "createUser",
            "updateUser",
            "copydbgetnonce",
            "copydbsaslstart",
            "copydb"));

    private static final Logger LOGGER = Loggers.getLogger("connection");

    private final ServerId serverId;
    private final StreamFactory streamFactory;
    private final InternalConnectionInitializer connectionInitializer;

    private volatile ConnectionDescription description;
    private volatile Stream stream;

    private final AtomicBoolean isClosed = new AtomicBoolean();
    private final AtomicBoolean opened = new AtomicBoolean();

    private final List<MongoCompressor> compressorList;
    private final CommandListener commandListener;
    private volatile Compressor sendCompressor;
    private volatile Map<Byte, Compressor> compressorMap;

    InternalStreamConnection(final ServerId serverId, final StreamFactory streamFactory,
                             final List<MongoCompressor> compressorList, final CommandListener commandListener,
                             final InternalConnectionInitializer connectionInitializer) {
        this.serverId = notNull("serverId", serverId);
        this.streamFactory = notNull("streamFactory", streamFactory);
        this.compressorList = notNull("compressorList", compressorList);
        this.compressorMap = createCompressorMap(compressorList);
        this.commandListener = commandListener;
        this.connectionInitializer = notNull("connectionInitializer", connectionInitializer);
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
            sendCompressor = findSendCompressor(description);
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
                            sendCompressor = findSendCompressor(description);
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

    private Map<Byte, Compressor> createCompressorMap(final List<MongoCompressor> compressorList) {
        Map<Byte, Compressor> compressorMap = new HashMap<Byte, Compressor>(this.compressorList.size());

        for (MongoCompressor mongoCompressor : compressorList) {
            Compressor compressor = createCompressor(mongoCompressor);
            compressorMap.put(compressor.getId(), compressor);
        }
        return compressorMap;
    }

    private Compressor findSendCompressor(final ConnectionDescription description) {
        if (description.getCompressors().isEmpty()) {
            return null;
        }

        String firstCompressorName = description.getCompressors().get(0);

        for (Compressor compressor : compressorMap.values()) {
            if (compressor.getName().equals(firstCompressorName)) {
                return compressor;
            }
        }

        throw new MongoInternalException("Unexpected compressor negotiated: " + firstCompressorName);
    }

    private Compressor createCompressor(final MongoCompressor mongoCompressor) {
        if (mongoCompressor.getName().equals("zlib")) {
            return new ZlibCompressor(mongoCompressor);
        } else if (mongoCompressor.getName().equals("snappy")) {
            return new SnappyCompressor();
        } else {
            throw new MongoClientException("Unsupported compressor " + mongoCompressor.getName());
        }
    }

    @Override
    public void close() {
        // All but the first call is a no-op
        if (!isClosed.getAndSet(true)) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(String.format("Closing connection %s", getId()));
            }
            if (stream != null) {
                stream.close();
            }
        }
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
    public <T> T sendAndReceive(final CommandMessage message, final Decoder<T> decoder, final SessionContext sessionContext) {
        ByteBufferBsonOutput bsonOutput = new ByteBufferBsonOutput(this);
        LazyCommandDocument lazyCommandDocument = new LazyCommandDocument(message, bsonOutput);
        CommandEventSender commandEventSender = new CommandEventSender(message, lazyCommandDocument);

        try {
            message.encode(bsonOutput, sessionContext);
            commandEventSender.sendStartedEvent();
        } catch (RuntimeException e) {
            bsonOutput.close();
            throw e;
        }

        try {
            sendCommandMessage(message, lazyCommandDocument, bsonOutput, sessionContext);
            if (message.isResponseExpected()) {
                return receiveCommandMessageResponse(message, decoder, commandEventSender, sessionContext);
            } else {
                commandEventSender.sendSucceededEventForOneWayCommand();
                return null;
            }
        } catch (RuntimeException e) {
            commandEventSender.sendFailedEvent(e);
            throw e;
        }
    }

    private void sendCommandMessage(final CommandMessage message, final LazyCommandDocument lazyCommandDocument,
                                    final ByteBufferBsonOutput bsonOutput, final SessionContext sessionContext) {
        try {
            if (sendCompressor == null || SECURITY_SENSITIVE_COMMANDS.contains(lazyCommandDocument.getName())) {
                sendMessage(bsonOutput.getByteBuffers(), message.getId());
            } else {
                CompressedMessage compressedMessage = new CompressedMessage(message.getOpCode(), bsonOutput.getByteBuffers(),
                        sendCompressor,
                        getMessageSettings(description));
                ByteBufferBsonOutput compressedBsonOutput = new ByteBufferBsonOutput(this);
                compressedMessage.encode(compressedBsonOutput, sessionContext);
                try {
                    sendMessage(compressedBsonOutput.getByteBuffers(), message.getId());
                } finally {
                    compressedBsonOutput.close();
                }
            }
        } finally {
            bsonOutput.close();
        }
    }

    private <T> T receiveCommandMessageResponse(final CommandMessage message, final Decoder<T> decoder,
                                                final CommandEventSender commandEventSender, final SessionContext sessionContext) {
        ResponseBuffers responseBuffers = receiveMessage(message.getId());
        try {
            updateSessionContext(sessionContext, responseBuffers);

            if (!isCommandOk(responseBuffers)) {
                throw getCommandFailureException(getResponseDocument(responseBuffers, message.getId(), new BsonDocumentCodec()),
                        description.getServerAddress());
            }

            commandEventSender.sendSucceededEvent(responseBuffers);

            return new ReplyMessage<T>(responseBuffers, decoder, message.getId()).getDocuments().get(0);
        } finally {
            responseBuffers.close();
        }
    }

    @Override
    public <T> void sendAndReceiveAsync(final CommandMessage message, final Decoder<T> decoder, final SessionContext sessionContext,
                                        final SingleResultCallback<T> callback) {
        notNull("stream is open", stream, callback);

        if (isClosed()) {
            callback.onResult(null, new MongoSocketClosedException("Can not read from a closed socket", getServerAddress()));
            return;
        }

        ByteBufferBsonOutput bsonOutput = new ByteBufferBsonOutput(this);
        ByteBufferBsonOutput compressedBsonOutput = new ByteBufferBsonOutput(this);

        try {
            message.encode(bsonOutput, sessionContext);
            LazyCommandDocument lazyCommandDocument = new LazyCommandDocument(message, bsonOutput);
            CommandEventSender commandEventSender = new CommandEventSender(message, lazyCommandDocument);
            commandEventSender.sendStartedEvent();

            if (sendCompressor == null || SECURITY_SENSITIVE_COMMANDS.contains(lazyCommandDocument.getName())) {
                sendCommandMessageAsync(message.getId(), decoder, sessionContext, callback, bsonOutput, commandEventSender,
                        message.isResponseExpected());
            } else {
                CompressedMessage compressedMessage = new CompressedMessage(message.getOpCode(), bsonOutput.getByteBuffers(),
                        sendCompressor,
                        getMessageSettings(description));
                compressedMessage.encode(compressedBsonOutput, sessionContext);
                bsonOutput.close();
                sendCommandMessageAsync(message.getId(), decoder, sessionContext, callback, compressedBsonOutput, commandEventSender,
                        message.isResponseExpected());
            }
        } catch (Throwable t) {
            bsonOutput.close();
            compressedBsonOutput.close();
            callback.onResult(null, t);
        }
    }

    private <T> void sendCommandMessageAsync(final int messageId, final Decoder<T> decoder, final SessionContext sessionContext,
                                             final SingleResultCallback<T> callback, final ByteBufferBsonOutput bsonOutput,
                                             final CommandEventSender commandEventSender, final boolean responseExpected) {
        sendMessageAsync(bsonOutput.getByteBuffers(), messageId, new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final Throwable t) {
                bsonOutput.close();
                if (t != null) {
                    commandEventSender.sendFailedEvent(t);
                    callback.onResult(null, t);
                } else if (!responseExpected) {
                    commandEventSender.sendSucceededEventForOneWayCommand();
                    callback.onResult(null, null);
                } else {
                    readAsync(MESSAGE_HEADER_LENGTH, new MessageHeaderCallback(new SingleResultCallback<ResponseBuffers>() {
                        @Override
                        public void onResult(final ResponseBuffers responseBuffers, final Throwable t) {
                            if (t != null) {
                                commandEventSender.sendFailedEvent(t);
                                callback.onResult(null, t);
                                return;
                            }
                            try {
                                updateSessionContext(sessionContext, responseBuffers);
                                boolean commandOk =
                                        isCommandOk(new BsonBinaryReader(new ByteBufferBsonInput(responseBuffers.getBodyByteBuffer())));
                                responseBuffers.reset();
                                if (!commandOk) {
                                    MongoException commandFailureException = getCommandFailureException(getResponseDocument(responseBuffers,
                                            messageId, new BsonDocumentCodec()), description.getServerAddress());
                                    commandEventSender.sendFailedEvent(commandFailureException);
                                    throw commandFailureException;
                                }
                                commandEventSender.sendSucceededEvent(responseBuffers);
                                T result = new ReplyMessage<T>(responseBuffers, decoder, messageId).getDocuments().get(0);

                                callback.onResult(result, null);
                            } catch (Throwable localThrowable) {
                                callback.onResult(null, localThrowable);
                            } finally {
                                responseBuffers.close();
                            }
                        }
                    }));
                }
            }
        });
    }

    @Override
    public void sendMessage(final List<ByteBuf> byteBuffers, final int lastRequestId) {
        notNull("stream is open", stream);

        if (isClosed()) {
            throw new MongoSocketClosedException("Cannot write to a closed stream", getServerAddress());
        }

        try {
            stream.write(byteBuffers);
        } catch (Exception e) {
            close();
            throw translateWriteException(e);
        }
    }

    @Override
    public ResponseBuffers receiveMessage(final int responseTo) {
        notNull("stream is open", stream);
        if (isClosed()) {
            throw new MongoSocketClosedException("Cannot read from a closed stream", getServerAddress());
        }

        try {
            return receiveResponseBuffers();
        } catch (Throwable t) {
            close();
            throw translateReadException(t);
        }
    }

    @Override
    public void sendMessageAsync(final List<ByteBuf> byteBuffers, final int lastRequestId, final SingleResultCallback<Void> callback) {
        notNull("stream is open", stream, callback);

        if (isClosed()) {
            callback.onResult(null, new MongoSocketClosedException("Can not read from a closed socket", getServerAddress()));
            return;
        }

        writeAsync(byteBuffers, errorHandlingCallback(callback, LOGGER));
    }

    private void writeAsync(final List<ByteBuf> byteBuffers, final SingleResultCallback<Void> callback) {
        stream.writeAsync(byteBuffers, new AsyncCompletionHandler<Void>() {
            @Override
            public void completed(final Void v) {
                callback.onResult(null, null);
            }

            @Override
            public void failed(final Throwable t) {
                close();
                callback.onResult(null, translateWriteException(t));
            }
        });
    }

    @Override
    public void receiveMessageAsync(final int responseTo, final SingleResultCallback<ResponseBuffers> callback) {
        isTrue("stream is open", stream != null, callback);

        if (isClosed()) {
            callback.onResult(null, new MongoSocketClosedException("Can not read from a closed socket", getServerAddress()));
            return;
        }

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(String.format("Start receiving response on %s", getId()));
        }
        readAsync(MESSAGE_HEADER_LENGTH, new MessageHeaderCallback(new SingleResultCallback<ResponseBuffers>() {
            @Override
            public void onResult(final ResponseBuffers result, final Throwable t) {
                if (t != null) {
                    close();
                    callback.onResult(null, t);
                } else {
                    callback.onResult(result, null);
                }
            }
        }));
    }

    private void readAsync(final int numBytes, final SingleResultCallback<ByteBuf> callback) {
        if (isClosed()) {
            callback.onResult(null, new MongoSocketClosedException("Cannot read from a closed stream", getServerAddress()));
            return;
        }

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

    private ConnectionId getId() {
        return description.getConnectionId();
    }

    private ServerAddress getServerAddress() {
        return description.getServerAddress();
    }

    private void updateSessionContext(final SessionContext sessionContext, final ResponseBuffers responseBuffers) {
        sessionContext.advanceOperationTime(getOperationTime(responseBuffers));
        sessionContext.advanceClusterTime(getClusterTime(responseBuffers));
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
        ByteBuf messageHeaderBuffer = stream.read(MESSAGE_HEADER_LENGTH);
        MessageHeader messageHeader;
        try {
            messageHeader = new MessageHeader(messageHeaderBuffer, description.getMaxMessageSize());
        } finally {
            messageHeaderBuffer.release();
        }

        ByteBuf messageBuffer = stream.read(messageHeader.getMessageLength() - MESSAGE_HEADER_LENGTH);

        if (messageHeader.getOpCode() == OP_COMPRESSED.getValue()) {
            CompressedHeader compressedHeader = new CompressedHeader(messageBuffer, messageHeader);

            Compressor compressor = getCompressor(compressedHeader);

            ByteBuf buffer = getBuffer(compressedHeader.getUncompressedSize());
            compressor.uncompress(messageBuffer, buffer);

            buffer.flip();
            return new ResponseBuffers(new ReplyHeader(buffer, compressedHeader), buffer);
        } else {
            return new ResponseBuffers(new ReplyHeader(messageBuffer, messageHeader), messageBuffer);
        }
    }

    private Compressor getCompressor(final CompressedHeader compressedHeader) {
        Compressor compressor = compressorMap.get(compressedHeader.getCompressorId());
        if (compressor == null) {
            throw new MongoClientException("Unsupported compressor with identifier " + compressedHeader.getCompressorId());
        }
        return compressor;
    }

    @Override
    public ByteBuf getBuffer(final int size) {
        notNull("open", stream);
        return stream.getBuffer(size);
    }

    private static <T extends BsonDocument> T getResponseDocument(final ResponseBuffers responseBuffers,
                                                                  final int messageId, final Decoder<T> decoder) {
        ReplyMessage<T> replyMessage = new ReplyMessage<T>(responseBuffers, decoder, messageId);
        responseBuffers.reset();
        return replyMessage.getDocuments().get(0);
    }

    private class MessageHeaderCallback implements SingleResultCallback<ByteBuf> {
        private final SingleResultCallback<ResponseBuffers> callback;

        MessageHeaderCallback(final SingleResultCallback<ResponseBuffers> callback) {
            this.callback = callback;
        }

        @Override
        public void onResult(final ByteBuf result, final Throwable t) {
            if (t != null) {
                callback.onResult(null, t);
                return;
            }
            try {
                MessageHeader messageHeader = new MessageHeader(result, description.getMaxMessageSize());
                readAsync(messageHeader.getMessageLength() - MESSAGE_HEADER_LENGTH, new MessageCallback(messageHeader));
            } catch (Throwable localThrowable) {
                callback.onResult(null, localThrowable);
            } finally {
                if (result != null) {
                    result.release();
                }
            }
        }

        private class MessageCallback implements SingleResultCallback<ByteBuf> {
            private final MessageHeader messageHeader;

            MessageCallback(final MessageHeader messageHeader) {
                this.messageHeader = messageHeader;
            }

            @Override
            public void onResult(final ByteBuf result, final Throwable t) {
                if (t != null) {
                    callback.onResult(null, t);
                    return;
                }
                try {
                    ReplyHeader replyHeader;
                    ByteBuf responseBuffer;
                    if (messageHeader.getOpCode() == OP_COMPRESSED.getValue()) {
                        try {
                            CompressedHeader compressedHeader = new CompressedHeader(result, messageHeader);
                            Compressor compressor = getCompressor(compressedHeader);
                            ByteBuf buffer = getBuffer(compressedHeader.getUncompressedSize());
                            compressor.uncompress(result, buffer);

                            buffer.flip();
                            replyHeader = new ReplyHeader(buffer, compressedHeader);
                            responseBuffer = buffer;
                        } finally {
                            result.release();
                        }
                    } else {
                        replyHeader = new ReplyHeader(result, messageHeader);
                        responseBuffer = result;
                    }
                    callback.onResult(new ResponseBuffers(replyHeader, responseBuffer), null);
                } catch (Throwable localThrowable) {
                    callback.onResult(null, localThrowable);
                }
            }
        }
    }

    private static final Logger COMMAND_LOGGER = Loggers.getLogger("protocol.command");

    private class CommandEventSender {
        private final long startTimeNanos;
        private final CommandMessage message;
        private final LazyCommandDocument lazyCommandDocument;

        CommandEventSender(final CommandMessage message, final LazyCommandDocument lazyCommandDocument) {
            this.startTimeNanos = System.nanoTime();
            this.message = message;
            this.lazyCommandDocument = lazyCommandDocument;
        }

        public void sendStartedEvent() {
            if (sendRequired()) {
                BsonDocument commandDocumentForEvent = (SECURITY_SENSITIVE_COMMANDS.contains(lazyCommandDocument.getName()))
                        ? new BsonDocument() : lazyCommandDocument.getDocument();

                if (loggingRequired()) {
                    COMMAND_LOGGER.debug(
                            format("Sending command {%s : %s, ...} with request id %d to database %s on connection [%s] to server %s",
                                    lazyCommandDocument.getName(), lazyCommandDocument.getFirstValue(), message.getId(),
                                    message.getNamespace().getDatabaseName(), description.getConnectionId(),
                                    description.getServerAddress()));
                }

                if (eventRequired()) {
                    sendCommandStartedEvent(message, message.getNamespace().getDatabaseName(),
                            lazyCommandDocument.getName(), commandDocumentForEvent, getDescription(), commandListener);
                }
            }
        }

        public void sendFailedEvent(final Throwable t) {
            if (sendRequired()) {
                Throwable commandEventException = t;
                if (t instanceof MongoCommandException && (SECURITY_SENSITIVE_COMMANDS.contains(lazyCommandDocument.getName()))) {
                    commandEventException = new MongoCommandException(new BsonDocument(), description.getServerAddress());
                }
                long elapsedTimeNanos = System.nanoTime() - startTimeNanos;

                if (loggingRequired()) {
                    COMMAND_LOGGER.debug(
                            format("Execution of command with request id %d failed to complete successfully in %.2f ms on connection [%s] "
                                            + "to server %s",
                                    message.getId(), nanosToMillis(elapsedTimeNanos), description.getConnectionId(),
                                    description.getServerAddress()),
                            commandEventException);
                }

                if (eventRequired()) {
                    sendCommandFailedEvent(message, lazyCommandDocument.getName(), description, elapsedTimeNanos, commandEventException,
                            commandListener);
                }
            }
        }

        public void sendSucceededEvent(final ResponseBuffers responseBuffers) {
            if (sendRequired()) {
                BsonDocument responseDocumentForEvent = (SECURITY_SENSITIVE_COMMANDS.contains(lazyCommandDocument.getName()))
                        ? new BsonDocument()
                        : getResponseDocument(responseBuffers, message.getId(),
                        new RawBsonDocumentCodec());
                long elapsedTimeNanos = System.nanoTime() - startTimeNanos;

                if (loggingRequired()) {
                    COMMAND_LOGGER.debug(
                            format("Execution of command with request id %d completed successfully in %.2f ms on connection [%s] "
                                            + "to server %s",
                                    message.getId(), nanosToMillis(elapsedTimeNanos), description.getConnectionId(),
                                    description.getServerAddress()));
                }

                if (eventRequired()) {
                    sendCommandSucceededEvent(message, lazyCommandDocument.getName(), responseDocumentForEvent, description,
                            elapsedTimeNanos, commandListener);
                }
            }
        }

        public void sendSucceededEventForOneWayCommand() {
            if (sendRequired()) {
                BsonDocument responseDocumentForEvent = new BsonDocument("ok", new BsonInt32(1));
                long elapsedTimeNanos = System.nanoTime() - startTimeNanos;

                if (loggingRequired()) {
                    COMMAND_LOGGER.debug(
                            format("Execution of one-way command with request id %d completed successfully in %.2f ms on connection [%s] "
                                            + "to server %s",
                                    message.getId(), nanosToMillis(elapsedTimeNanos), description.getConnectionId(),
                                    description.getServerAddress()));
                }

                if (eventRequired()) {
                    sendCommandSucceededEvent(message, lazyCommandDocument.getName(), responseDocumentForEvent, description,
                            elapsedTimeNanos, commandListener);
                }
            }
        }

        private boolean sendRequired() {
            return (eventRequired() || loggingRequired()) && opened();
        }

        private boolean loggingRequired() {
            return COMMAND_LOGGER.isDebugEnabled();
        }

        private boolean eventRequired() {
            return commandListener != null;
        }

        private double nanosToMillis(final long elapsedTimeNanos) {
            return elapsedTimeNanos / 1000000.0;
        }
    }

    // Lazily determine the command document and command name, since they're only needed if either a command listener or compression
    // is enabled
    private static final class LazyCommandDocument {
        private final CommandMessage commandMessage;
        private final ByteBufferBsonOutput bsonOutput;
        private BsonDocument commandDocument;
        private String commandName;
        private BsonValue firstValue;

        private LazyCommandDocument(final CommandMessage commandMessage, final ByteBufferBsonOutput bsonOutput) {
            this.commandMessage = commandMessage;
            this.bsonOutput = bsonOutput;
        }

        public String getName() {
            if (commandName == null) {
                commandName = getDocument().getFirstKey();
            }
            return commandName;
        }

        public BsonValue getFirstValue() {
            if (firstValue == null) {
                firstValue = getDocument().getFirstValue();
            }
            return firstValue;
        }

        private BsonDocument getDocument() {
            if (commandDocument == null) {
                commandDocument = commandMessage.getCommandDocument(bsonOutput);
            }
            return commandDocument;
        }
    }
}
