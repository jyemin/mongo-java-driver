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

import com.mongodb.MongoClientException;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadPreference;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.internal.session.SessionContext;
import com.mongodb.internal.timeout.Deadline;
import com.mongodb.internal.validator.MappedFieldNameValidator;
import org.bson.BsonArray;
import org.bson.BsonBinaryWriter;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonElement;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonWriter;
import org.bson.FieldNameValidator;
import org.bson.codecs.EncoderContext;
import org.bson.io.BsonOutput;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.mongodb.ReadPreference.primary;
import static com.mongodb.ReadPreference.primaryPreferred;
import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.connection.ClusterConnectionMode.MULTIPLE;
import static com.mongodb.connection.ClusterConnectionMode.SINGLE;
import static com.mongodb.connection.ServerType.SHARD_ROUTER;
import static com.mongodb.connection.ServerType.STANDALONE;
import static com.mongodb.internal.connection.BsonWriterHelper.writePayload;
import static com.mongodb.internal.connection.ReadConcernHelper.getReadConcernDocument;
import static com.mongodb.internal.operation.ServerVersionHelper.FOUR_DOT_TWO_WIRE_VERSION;
import static com.mongodb.internal.operation.ServerVersionHelper.FOUR_DOT_ZERO_WIRE_VERSION;
import static com.mongodb.internal.operation.ServerVersionHelper.THREE_DOT_SIX_WIRE_VERSION;

/**
 * A command message that uses OP_MSG or OP_QUERY to send the command.
 */
public final class CommandMessage extends RequestMessage {
    private final MongoNamespace namespace;
    private final BsonDocument command;
    private final FieldNameValidator commandFieldNameValidator;
    private final ReadPreference readPreference;
    private final boolean exhaustAllowed;
    private final SplittablePayload payload;
    private final FieldNameValidator payloadFieldNameValidator;
    private final boolean responseExpected;
    private final ClusterConnectionMode clusterConnectionMode;
    private final Deadline deadline;

    CommandMessage(final MongoNamespace namespace, final BsonDocument command, final FieldNameValidator commandFieldNameValidator,
                   final ReadPreference readPreference, final MessageSettings settings) {
        this(namespace, command, commandFieldNameValidator, readPreference, settings, true, null, null,
                MULTIPLE, Deadline.infinite());
    }

    CommandMessage(final MongoNamespace namespace, final BsonDocument command, final FieldNameValidator commandFieldNameValidator,
                   final ReadPreference readPreference, final MessageSettings settings, final boolean exhaustAllowed) {
        this(namespace, command, commandFieldNameValidator, readPreference, settings, true, exhaustAllowed, null, null,
                MULTIPLE, Deadline.infinite());
    }

    CommandMessage(final MongoNamespace namespace, final BsonDocument command, final FieldNameValidator commandFieldNameValidator,
                   final ReadPreference readPreference, final MessageSettings settings, final boolean responseExpected,
                   final SplittablePayload payload, final FieldNameValidator payloadFieldNameValidator,
                   final ClusterConnectionMode clusterConnectionMode, final Deadline deadline) {
        this(namespace, command, commandFieldNameValidator, readPreference, settings, responseExpected, false, payload,
                payloadFieldNameValidator, clusterConnectionMode, deadline);
    }

    CommandMessage(final MongoNamespace namespace, final BsonDocument command, final FieldNameValidator commandFieldNameValidator,
                   final ReadPreference readPreference, final MessageSettings settings,
                   final boolean responseExpected, final boolean exhaustAllowed,
                   final SplittablePayload payload, final FieldNameValidator payloadFieldNameValidator,
                   final ClusterConnectionMode clusterConnectionMode,
                   final Deadline deadline) {
        super(namespace.getFullName(), getOpCode(settings), settings);
        this.namespace = namespace;
        this.command = command;
        this.commandFieldNameValidator = commandFieldNameValidator;
        this.readPreference = readPreference;
        this.responseExpected = responseExpected;
        this.exhaustAllowed = exhaustAllowed;
        this.payload = payload;
        this.payloadFieldNameValidator = payloadFieldNameValidator;
        this.clusterConnectionMode = clusterConnectionMode;
        this.deadline = deadline;
    }

    BsonDocument getCommandDocument(final ByteBufferBsonOutput bsonOutput) {
        ByteBufBsonDocument byteBufBsonDocument = ByteBufBsonDocument.createOne(bsonOutput,
                getEncodingMetadata().getFirstDocumentPosition());
        BsonDocument commandBsonDocument;

        if (useOpMsg() && containsPayload()) {
            commandBsonDocument = byteBufBsonDocument.toBsonDocument();

            int payloadStartPosition = getEncodingMetadata().getFirstDocumentPosition()
                    + byteBufBsonDocument.getSizeInBytes()
                    + 1 // payload type
                    + 4 // payload size
                    + payload.getPayloadName().getBytes(StandardCharsets.UTF_8).length + 1;  // null-terminated UTF-8 payload name
            commandBsonDocument.append(payload.getPayloadName(),
                    new BsonArray(ByteBufBsonDocument.createList(bsonOutput, payloadStartPosition)));
        } else {
            commandBsonDocument = byteBufBsonDocument;
        }

        if (commandBsonDocument.containsKey("$query")) {
            commandBsonDocument = commandBsonDocument.getDocument("$query");
        }
        return commandBsonDocument;
    }

    boolean containsPayload() {
        return payload != null;
    }

    boolean isResponseExpected() {
        isTrue("The message must be encoded before determining if a response is expected", getEncodingMetadata() != null);
        return !useOpMsg() || requireOpMsgResponse();
    }

    MongoNamespace getNamespace() {
        return namespace;
    }

    @Override
    protected EncodingMetadata encodeMessageBodyWithMetadata(final BsonOutput bsonOutput, final SessionContext sessionContext) {
        int messageStartPosition = bsonOutput.getPosition() - MESSAGE_PROLOGUE_LENGTH;
        int commandStartPosition;
        if (useOpMsg()) {
            int flagPosition = bsonOutput.getPosition();
            bsonOutput.writeInt32(0);   // flag bits
            bsonOutput.writeByte(0);    // payload type
            commandStartPosition = bsonOutput.getPosition();

            addDocument(getCommandToEncode(), bsonOutput, commandFieldNameValidator, getExtraElements(sessionContext));

            if (payload != null) {
                bsonOutput.writeByte(1);          // payload type
                int payloadBsonOutputStartPosition = bsonOutput.getPosition();
                bsonOutput.writeInt32(0);         // size
                bsonOutput.writeCString(payload.getPayloadName());
                writePayload(new BsonBinaryWriter(bsonOutput, payloadFieldNameValidator), bsonOutput, getSettings(),
                        messageStartPosition, payload, getSettings().getMaxDocumentSize());

                int payloadBsonOutputLength = bsonOutput.getPosition() - payloadBsonOutputStartPosition;
                bsonOutput.writeInt32(payloadBsonOutputStartPosition, payloadBsonOutputLength);
            }

            // Write the flag bits
            bsonOutput.writeInt32(flagPosition, getOpMsgFlagBits());
        } else {
            bsonOutput.writeInt32(getOpQueryFlagBits());
            bsonOutput.writeCString(namespace.getFullName());
            bsonOutput.writeInt32(0);
            bsonOutput.writeInt32(-1);

            commandStartPosition = bsonOutput.getPosition();

            if (payload == null) {
                addDocument(getCommandToEncode(), bsonOutput, commandFieldNameValidator, null);
            } else {
                addDocumentWithPayload(bsonOutput, messageStartPosition);
            }
        }
        return new EncodingMetadata(commandStartPosition);
    }

    private FieldNameValidator getPayloadArrayFieldNameValidator() {
        Map<String, FieldNameValidator> rootMap = new HashMap<String, FieldNameValidator>();
        rootMap.put(payload.getPayloadName(), payloadFieldNameValidator);
        return new MappedFieldNameValidator(commandFieldNameValidator, rootMap);
    }

    private void addDocumentWithPayload(final BsonOutput bsonOutput, final int messageStartPosition) {
        BsonBinaryWriter bsonBinaryWriter = new BsonBinaryWriter(bsonOutput, getPayloadArrayFieldNameValidator());
        BsonWriter bsonWriter = new SplittablePayloadBsonWriter(bsonBinaryWriter, bsonOutput, messageStartPosition, getSettings(), payload);
        BsonDocument commandToEncode = getCommandToEncode();
        getCodec(commandToEncode).encode(bsonWriter, commandToEncode, EncoderContext.builder().build());
    }

    private int getOpMsgFlagBits() {
        return getOpMsgResponseExpectedFlagBit();
    }

    private int getOpMsgResponseExpectedFlagBit() {
        int flagBits = 0;
        if (!requireOpMsgResponse()) {
            flagBits = 1 << 1;
        }
        if (exhaustAllowed) {
            flagBits |= 1 << 16;
        }
        return flagBits;
    }

    private boolean requireOpMsgResponse() {
        if (responseExpected) {
            return true;
        } else {
            return payload != null && payload.hasAnotherSplit();
        }
    }

    private int getOpQueryFlagBits() {
        return getOpQuerySlaveOkFlagBit();
    }

    private int getOpQuerySlaveOkFlagBit() {
        if (isSlaveOk()) {
            return 1 << 2;
        } else {
            return 0;
        }
    }

    private boolean isSlaveOk() {
        return (readPreference != null && readPreference.isSlaveOk()) || isDirectConnectionToReplicaSetMember();
    }

    private boolean isDirectConnectionToReplicaSetMember() {
        return clusterConnectionMode == SINGLE
                && getSettings().getServerType() != SHARD_ROUTER
                && getSettings().getServerType() != STANDALONE;
    }

    private boolean useOpMsg() {
        return getOpCode().equals(OpCode.OP_MSG);
    }

    private BsonDocument getCommandToEncode() {
        BsonDocument commandToEncode = command;
        if (!useOpMsg() && readPreference != null && !readPreference.equals(primary())) {
            commandToEncode = new BsonDocument("$query", command).append("$readPreference", readPreference.toDocument());
        }
        return commandToEncode;
    }

    private List<BsonElement> getExtraElements(final SessionContext sessionContext) {
        List<BsonElement> extraElements = new ArrayList<BsonElement>();
        extraElements.add(new BsonElement("$db", new BsonString(new MongoNamespace(getCollectionName()).getDatabaseName())));
        if (sessionContext.getClusterTime() != null) {
            extraElements.add(new BsonElement("$clusterTime", sessionContext.getClusterTime()));
        }
        if (sessionContext.hasSession() && responseExpected) {
            extraElements.add(new BsonElement("lsid", sessionContext.getSessionId()));
        }
        boolean firstMessageInTransaction = sessionContext.notifyMessageSent();
        if (sessionContext.hasActiveTransaction()) {
            checkServerVersionForTransactionSupport();
            extraElements.add(new BsonElement("txnNumber", new BsonInt64(sessionContext.getTransactionNumber())));
            if (firstMessageInTransaction) {
                extraElements.add(new BsonElement("startTransaction", BsonBoolean.TRUE));
                addReadConcernDocument(extraElements, sessionContext);
            }
            extraElements.add(new BsonElement("autocommit", BsonBoolean.FALSE));
        }
        if (readPreference != null) {
            if (!readPreference.equals(primary())) {
                extraElements.add(new BsonElement("$readPreference", readPreference.toDocument()));
            } else if (isDirectConnectionToReplicaSetMember()) {
                extraElements.add(new BsonElement("$readPreference", primaryPreferred().toDocument()));
            }
        }
        if (!deadline.isInfinite()) {
            long timeRemaining = deadline.getTimeRemaining(TimeUnit.MILLISECONDS);
            if (timeRemaining > 0) {
                // TODO: is it safe to always add this? Seems like we will need some cooperation with Operation implementations
                // TODO: or at least operation creators, to not set maxTimeMS if timeout is non-infinite
                // TODO: deal with RTT somehow.  Where to get it from?
                extraElements.add(new BsonElement("maxTimeMS", new BsonInt64(timeRemaining)));
            }
        }
        return extraElements;
    }

    private void checkServerVersionForTransactionSupport() {
        int wireVersion = getSettings().getMaxWireVersion();
        if (wireVersion < FOUR_DOT_ZERO_WIRE_VERSION
                || (wireVersion < FOUR_DOT_TWO_WIRE_VERSION && getSettings().getServerType() == SHARD_ROUTER)) {
            throw new MongoClientException("Transactions are not supported by the MongoDB cluster to which this client is connected.");
        }
    }


    private void addReadConcernDocument(final List<BsonElement> extraElements, final SessionContext sessionContext) {
        BsonDocument readConcernDocument = getReadConcernDocument(sessionContext);
        if (!readConcernDocument.isEmpty()) {
            extraElements.add(new BsonElement("readConcern", readConcernDocument));
        }
    }

    private static OpCode getOpCode(final MessageSettings settings) {
        return isServerVersionAtLeastThreeDotSix(settings) ? OpCode.OP_MSG : OpCode.OP_QUERY;
    }

    private static boolean isServerVersionAtLeastThreeDotSix(final MessageSettings settings) {
          return settings.getMaxWireVersion() >= THREE_DOT_SIX_WIRE_VERSION;
    }

}
