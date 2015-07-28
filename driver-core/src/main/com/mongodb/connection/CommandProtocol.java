/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

import com.mongodb.MongoNamespace;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.mongodb.event.CommandListener;
import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.FieldNameValidator;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.Decoder;
import org.bson.codecs.DecoderContext;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.connection.ByteBufBsonDocument.createOne;
import static com.mongodb.connection.ProtocolHelper.getCommandFailureException;
import static com.mongodb.connection.ProtocolHelper.sendCommandFailedEvent;
import static com.mongodb.connection.ProtocolHelper.sendCommandStartedEvent;
import static com.mongodb.connection.ProtocolHelper.sendCommandSucceededEvent;
import static java.lang.String.format;

/**
 * A protocol for executing a command against a MongoDB server using the OP_QUERY wire protocol message.
 *
 * @param <T> the type returned from execution
 * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
 */
class CommandProtocol<T> implements Protocol<T> {

    public static final Logger LOGGER = Loggers.getLogger("protocol.command");

    private final MongoNamespace namespace;
    private final BsonDocument command;
    private final Decoder<T> commandResultDecoder;
    private final FieldNameValidator fieldNameValidator;
    private boolean slaveOk;
    private CommandListener commandListener;

    /**
     * Construct an instance.
     * @param database             the database
     * @param command              the command
     * @param fieldNameValidator   the field name validator to apply tot the command
     * @param commandResultDecoder the decoder to use to decode the command result
     */
    public CommandProtocol(final String database, final BsonDocument command, final FieldNameValidator fieldNameValidator,
                           final Decoder<T> commandResultDecoder) {
        notNull("database", database);
        this.namespace = new MongoNamespace(database, MongoNamespace.COMMAND_COLLECTION_NAME);
        this.command = notNull("command", command);
        this.commandResultDecoder = notNull("commandResultDecoder", commandResultDecoder);
        this.fieldNameValidator = notNull("fieldNameValidator", fieldNameValidator);
    }

    public boolean isSlaveOk() {
        return slaveOk;
    }

    public CommandProtocol<T> slaveOk(final boolean slaveOk) {
        this.slaveOk = slaveOk;
        return this;
    }


    @Override
    public T execute(final InternalConnection connection) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Sending command {%s : %s} to database %s on connection [%s] to server %s",
                                getCommandName(), command.values().iterator().next(),
                                namespace.getDatabaseName(), connection.getDescription().getConnectionId(),
                                connection.getDescription().getServerAddress()));
        }
        long startTimeNanos = System.nanoTime();
        CommandMessage commandMessage = null;
        try {
            commandMessage = sendMessage(connection);
            ResponseBuffers responseBuffers = connection.receiveMessage(commandMessage.getId());
            ReplyMessage<BsonDocument> replyMessage;
            try {
                 replyMessage = new ReplyMessage<BsonDocument>(responseBuffers, new BsonDocumentCodec(), commandMessage.getId());
            } finally {
                responseBuffers.close();
            }

            BsonDocument response = replyMessage.getDocuments().get(0);
            if (!ProtocolHelper.isCommandOk(response)) {
                throw getCommandFailureException(response, connection.getDescription().getServerAddress());
            }

            T retval = commandResultDecoder.decode(new BsonDocumentReader(response), DecoderContext.builder().build());
            if (commandListener != null) {
                sendCommandSucceededEvent(commandMessage, getCommandName(), response, connection.getDescription(), startTimeNanos,
                                          commandListener);
            }
            LOGGER.debug("Command execution completed");
            return retval;
        } catch (RuntimeException e) {
            if (commandListener != null) {
                sendCommandFailedEvent(commandMessage, getCommandName(), connection.getDescription(), startTimeNanos, e, commandListener);
            }
            throw e;
        }
    }

    @Override
    public void executeAsync(final InternalConnection connection, final SingleResultCallback<T> callback) {
        try {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(format("Asynchronously sending command {%s : %s} to database %s on connection [%s] to server %s",
                                    getCommandName(), command.values().iterator().next(),
                                    namespace.getDatabaseName(), connection.getDescription().getConnectionId(),
                                    connection.getDescription().getServerAddress()));
            }
            ByteBufferBsonOutput bsonOutput = new ByteBufferBsonOutput(connection);
            CommandMessage message = new CommandMessage(namespace.getFullName(), command, slaveOk, fieldNameValidator,
                                                        ProtocolHelper.getMessageSettings(connection.getDescription()));
            ProtocolHelper.encodeMessage(message, bsonOutput);

            SingleResultCallback<ResponseBuffers> receiveCallback = new CommandResultCallback<T>(callback, commandResultDecoder,
                                                                                                 message.getId(),
                                                                                                 connection.getDescription()
                                                                                                           .getServerAddress());
            connection.sendMessageAsync(bsonOutput.getByteBuffers(), message.getId(),
                                        new SendMessageCallback<T>(connection, bsonOutput, message.getId(), callback, receiveCallback
                                        ));
        } catch (Throwable t) {
            callback.onResult(null, t);
        }
    }

    @Override
    public void setCommandListener(final CommandListener commandListener) {
        this.commandListener = commandListener;
    }

    private String getCommandName() {
        return command.keySet().iterator().next();
    }

    private CommandMessage sendMessage(final InternalConnection connection) {
        ByteBufferBsonOutput bsonOutput = new ByteBufferBsonOutput(connection);
        try {
            CommandMessage message = new CommandMessage(namespace.getFullName(), command, slaveOk, fieldNameValidator,
                                                        ProtocolHelper.getMessageSettings(connection.getDescription()));
            int documentPosition = message.encodeWithMetadata(bsonOutput).getFirstDocumentPosition();
            if (commandListener != null) {
                ByteBufBsonDocument rawDocument = createOne(bsonOutput, documentPosition);
                sendCommandStartedEvent(message, namespace.getDatabaseName(), rawDocument.getFirstKey(),
                                        rawDocument, connection.getDescription(), commandListener);
            }

            connection.sendMessage(bsonOutput.getByteBuffers(), message.getId());
            return message;
        } finally {
            bsonOutput.close();
        }
    }
}
