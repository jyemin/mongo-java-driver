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

package com.mongodb.client.internal;

import com.mongodb.MongoException;
import com.mongodb.MongoInternalException;
import com.mongodb.MongoNamespace;
import com.mongodb.client.model.vault.DataKeyOptions;
import com.mongodb.client.model.vault.EncryptOptions;
import com.mongodb.crypt.capi.MongoCrypt;
import com.mongodb.crypt.capi.MongoCryptContext;
import com.mongodb.crypt.capi.MongoDataKeyOptions;
import com.mongodb.crypt.capi.MongoExplicitEncryptOptions;
import com.mongodb.crypt.capi.MongoKeyDecryptor;
import com.mongodb.lang.Nullable;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.crypt.capi.MongoCryptContext.State;

class Crypt implements Closeable {

    private final Map<String, BsonDocument> namespaceToLocalSchemaDocumentMap;
    private final MongoCrypt mongoCrypt;
    private final CollectionInfoRetriever collectionInfoRetriever;
    private final CommandMarker commandMarker;
    private final KeyRetriever keyRetriever;
    private final KeyManagementService keyManagementService;

    /**
     * Create an instance to use for explicit encryption and decryption, and data key creation.
     *
     * @param mongoCrypt the mongoCrypt wrapper
     * @param keyRetriever the key retriever
     * @param keyManagementService the key management service
     */
    Crypt(final MongoCrypt mongoCrypt, final KeyRetriever keyRetriever, final KeyManagementService keyManagementService) {
        this(mongoCrypt, null, null, keyRetriever, keyManagementService, null);
    }

    /**
     * Create an instance to use for auto-encryption and auto-decryption.
     *
     * @param mongoCrypt the mongoCrypt wrapper
     * @param keyRetriever the key retriever
     * @param keyManagementService the key management service
     * @param collectionInfoRetriever the collection info retriever
     * @param commandMarker the command marker
     * @param namespaceToLocalSchemaDocumentMap the map of namespace to local schema
     */
    Crypt(final MongoCrypt mongoCrypt, @Nullable final CollectionInfoRetriever collectionInfoRetriever,
          @Nullable final CommandMarker commandMarker, final KeyRetriever keyRetriever, final KeyManagementService keyManagementService,
          @Nullable final Map<String, BsonDocument> namespaceToLocalSchemaDocumentMap) {
        this.namespaceToLocalSchemaDocumentMap = namespaceToLocalSchemaDocumentMap;
        this.mongoCrypt = mongoCrypt;
        this.collectionInfoRetriever = collectionInfoRetriever;
        this.commandMarker = commandMarker;
        this.keyRetriever = keyRetriever;
        this.keyManagementService = keyManagementService;
    }

    /**
     * Encrypt the given command
     *
     * @param namespace the namespace
     * @param command   the unencrypted command
     * @return the encyrpted command
     */
    public RawBsonDocument encrypt(final MongoNamespace namespace, final RawBsonDocument command) {
        notNull("databaseName", namespace);
        notNull("command", command);

        MongoCryptContext encryptionContext = mongoCrypt.createEncryptionContext(namespace.getFullName(),
                namespaceToLocalSchemaDocumentMap.get(namespace.getFullName()));

        try {
            return executeStateMachine(encryptionContext, namespace.getDatabaseName(), command);
        } finally {
            encryptionContext.close();
        }
    }

    /**
     * Decrypt the given command response
     *
     * @param commandResponse the encrypted command response
     * @return the decrypted command response
     */
    RawBsonDocument decrypt(final RawBsonDocument commandResponse) {
        notNull("commandResponse", commandResponse);

        MongoCryptContext decryptionContext = mongoCrypt.createDecryptionContext(commandResponse);

        try {
            return executeStateMachine(decryptionContext, null, commandResponse);
        } finally {
            decryptionContext.close();
        }
    }

    /**
     * Create a data key.
     *
     * @param kmsProvider the KMS provider to create the data key for
     * @param options     the data key options
     * @return the document representing the data key to be added to the key vault
     */
    BsonDocument createDataKey(final String kmsProvider, final DataKeyOptions options) {
        notNull("kmsProvider", kmsProvider);
        notNull("options", options);

        MongoCryptContext dataKeyCreationContext = mongoCrypt.createDataKeyContext(kmsProvider,
                MongoDataKeyOptions.builder()
                        .keyAltNames(options.getKeyAltNames())
                        .masterKey(options.getMasterKey())
                        .build());

        try {
            return executeStateMachine(dataKeyCreationContext, null, null);
        } finally {
            dataKeyCreationContext.close();
        }
    }

    /**
     * Encrypt the given value with the given options
     *
     * @param value the value to encrypt
     * @param options the options
     * @return the encrypted value
     */
    BsonBinary encryptExplicitly(final BsonValue value, final EncryptOptions options) {
        notNull("value", value);
        notNull("options", options);

        MongoCryptContext encryptionContext = mongoCrypt.createExplicitEncryptionContext(
                new BsonDocument("v", value), MongoExplicitEncryptOptions.builder()
                        .keyId(options.getKeyId())
                        .algorithm(options.getAlgorithm())
                        .initializationVector(options.getInitializationVector())
                        .build());

        try {
            return executeStateMachine(encryptionContext, null, null).getBinary("v");
        } finally {
            encryptionContext.close();
        }
    }

    /**
     * Decrypt the given encrypted value.
     *
     * @param value the encrypted value
     * @return the decrypted value
     */
    BsonValue decryptExplicitly(final BsonBinary value) {
        notNull("value", value);

        MongoCryptContext decryptionContext = mongoCrypt.createExplicitDecryptionContext(new BsonDocument("v", value));

        try {
            return executeStateMachine(decryptionContext, null, null).get("v");
        } finally {
            decryptionContext.close();
        }
    }

    @Override
    public void close() {
        mongoCrypt.close();
    }

    private RawBsonDocument executeStateMachine(final MongoCryptContext cryptContext, final String databaseName,
                                                final RawBsonDocument defaultResponse) {
        while (true) {
            State state = cryptContext.getState();
            switch (state) {
                case NEED_MONGO_COLLINFO:
                    BsonDocument collectionInfo = collectionInfoRetriever.filter(databaseName, cryptContext.getMongoOperation());
                    if (collectionInfo != null) {
                        cryptContext.addMongoOperationResult(collectionInfo);
                    }
                    cryptContext.completeMongoOperation();
                    break;
                case NEED_MONGO_MARKINGS:
                    BsonDocument markedCommand = commandMarker.mark(databaseName, cryptContext.getMongoOperation(), defaultResponse);
                    cryptContext.addMongoOperationResult(markedCommand);
                    cryptContext.completeMongoOperation();
                    break;
                case NEED_MONGO_KEYS:
                    fetchKeys(cryptContext);
                    break;
                case NEED_KMS:
                    decryptKeys(cryptContext);
                    break;
                case READY:
                    return (RawBsonDocument) cryptContext.finish();
                case NOTHING_TO_DO:
                    return defaultResponse;
                default:
                    throw new MongoInternalException("Unsupported encryptor state + " + state);
            }
        }
    }

    private void fetchKeys(final MongoCryptContext keyBroker) {
        Iterator<BsonDocument> iterator = keyRetriever.find(keyBroker.getMongoOperation());
        while (iterator.hasNext()) {
            keyBroker.addMongoOperationResult(iterator.next());
        }
        keyBroker.completeMongoOperation();
    }

    private void decryptKeys(final MongoCryptContext cryptContext) {
        MongoKeyDecryptor keyDecryptor = cryptContext.nextKeyDecryptor();
        while (keyDecryptor != null) {
            decryptKey(keyDecryptor);
            keyDecryptor = cryptContext.nextKeyDecryptor();
        }
        cryptContext.completeKeyDecryptors();
    }

    private void decryptKey(final MongoKeyDecryptor keyDecryptor) {
        InputStream inputStream = keyManagementService.stream(keyDecryptor.getHostName(), keyDecryptor.getMessage());
        try {
            byte[] bytes = new byte[4096];

            int bytesNeeded = keyDecryptor.bytesNeeded();

            while (bytesNeeded > 0) {
                int bytesRead = inputStream.read(bytes, 0, bytesNeeded);
                keyDecryptor.feed(ByteBuffer.wrap(bytes, 0, bytesRead));
                bytesNeeded = keyDecryptor.bytesNeeded();
            }
        } catch (IOException e) {
            throw new MongoException("Exception decrypting key", e);  // TODO: change exception type
        } finally {
            try {
                inputStream.close();
            } catch (IOException e) {
                // ignore
            }
        }
    }
}
