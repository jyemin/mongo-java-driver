/*
 * Copyright 2017 MongoDB, Inc.
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

package com.mongodb.connection;

import com.mongodb.MongoCommandException;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.mongodb.event.CommandListener;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.codecs.RawBsonDocumentCodec;

import java.util.Set;

import static com.mongodb.connection.ProtocolHelper.sendCommandFailedEvent;
import static com.mongodb.connection.ProtocolHelper.sendCommandStartedEvent;
import static com.mongodb.connection.ProtocolHelper.sendCommandSucceededEvent;
import static java.lang.String.format;

class LoggingCommandEventSender implements CommandEventSender {
    private static final Logger LOGGER = Loggers.getLogger("protocol.command");

    private final Set<String> securitySensitiveCommands;
    private final ConnectionDescription description;
    private final CommandListener commandListener;
    private final long startTimeNanos;
    private final CommandMessage message;
    private final LazyCommandDocument lazyCommandDocument;

    static boolean isRequired(final CommandListener commandListener) {
        return commandListener != null || LOGGER.isDebugEnabled();
    }

    LoggingCommandEventSender(final Set<String> securitySensitiveCommands, final ConnectionDescription description,
                              final CommandListener commandListener, final CommandMessage message,
                              final LazyCommandDocument lazyCommandDocument) {
        this.securitySensitiveCommands = securitySensitiveCommands;
        this.description = description;
        this.commandListener = commandListener;
        this.startTimeNanos = System.nanoTime();
        this.message = message;
        this.lazyCommandDocument = lazyCommandDocument;
    }

    @Override
    public void sendStartedEvent() {
        if (loggingRequired()) {
            LOGGER.debug(
                    format("Sending command {%s : %s, ...} with request id %d to database %s on connection [%s] to server %s",
                            lazyCommandDocument.getName(), lazyCommandDocument.getFirstValue(), message.getId(),
                            message.getNamespace().getDatabaseName(), description.getConnectionId(), description.getServerAddress()));
        }

        if (eventRequired()) {
            BsonDocument commandDocumentForEvent = (securitySensitiveCommands.contains(lazyCommandDocument.getName()))
                    ? new BsonDocument() : lazyCommandDocument.getDocument();

            sendCommandStartedEvent(message, message.getNamespace().getDatabaseName(),
                    lazyCommandDocument.getName(), commandDocumentForEvent, description, commandListener);
        }
    }

    @Override
    public void sendFailedEvent(final Throwable t) {
        Throwable commandEventException = t;
        if (t instanceof MongoCommandException && (securitySensitiveCommands.contains(lazyCommandDocument.getName()))) {
            commandEventException = new MongoCommandException(new BsonDocument(), description.getServerAddress());
        }
        long elapsedTimeNanos = System.nanoTime() - startTimeNanos;

        if (loggingRequired()) {
            LOGGER.debug(
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

    @Override
    public void sendSucceededEvent(final ResponseBuffers responseBuffers) {
        long elapsedTimeNanos = System.nanoTime() - startTimeNanos;

        if (loggingRequired()) {
            LOGGER.debug(
                    format("Execution of command with request id %d completed successfully in %.2f ms on connection [%s] to server %s",
                            message.getId(), nanosToMillis(elapsedTimeNanos), description.getConnectionId(),
                            description.getServerAddress()));
        }

        if (eventRequired()) {
            BsonDocument responseDocumentForEvent = (securitySensitiveCommands.contains(lazyCommandDocument.getName()))
                    ? new BsonDocument()
                    : responseBuffers.getResponseDocument(message.getId(), new RawBsonDocumentCodec());
            sendCommandSucceededEvent(message, lazyCommandDocument.getName(), responseDocumentForEvent, description,
                    elapsedTimeNanos, commandListener);
        }
    }

    @Override
    public void sendSucceededEventForOneWayCommand() {
        long elapsedTimeNanos = System.nanoTime() - startTimeNanos;

        if (loggingRequired()) {
            LOGGER.debug(
                    format("Execution of one-way command with request id %d completed successfully in %.2f ms on connection [%s] "
                                    + "to server %s",
                            message.getId(), nanosToMillis(elapsedTimeNanos), description.getConnectionId(),
                            description.getServerAddress()));
        }

        if (eventRequired()) {
            BsonDocument responseDocumentForEvent = new BsonDocument("ok", new BsonInt32(1));
            sendCommandSucceededEvent(message, lazyCommandDocument.getName(), responseDocumentForEvent, description,
                    elapsedTimeNanos, commandListener);
        }
    }

    private boolean loggingRequired() {
        return LOGGER.isDebugEnabled();
    }

    private boolean eventRequired() {
        return commandListener != null;
    }

    private double nanosToMillis(final long elapsedTimeNanos) {
        return elapsedTimeNanos / 1000000.0;
    }
}
