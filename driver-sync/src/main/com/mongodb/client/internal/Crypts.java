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

import com.mongodb.AutoEncryptionOptions;
import com.mongodb.KeyVaultEncryptionOptions;
import com.mongodb.MongoClientException;
import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.crypt.capi.MongoAwsKmsProviderOptions;
import com.mongodb.crypt.capi.MongoCryptOptions;
import com.mongodb.crypt.capi.MongoCrypts;
import com.mongodb.crypt.capi.MongoLocalKmsProviderOptions;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;

import javax.net.ssl.SSLContext;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

class Crypts {

    @Nullable
    static Crypt createCrypt(final MongoClient keyVaultClient, final MongoClient collectionInfoClient,
                             final AutoEncryptionOptions options) {
        // TODO: this has to be closed
        // TODO: make this configurable, but default according to platform
        MongoClient mongocryptdClient =
                MongoClients.create("mongodb://%2Ftmp%2Fmongocryptd.sock/?serverSelectionTimeoutMS=1000");

        return new CryptImpl(
                MongoCrypts.create(createMongoCryptOptions(options.getKmsProviders())),
                new CollectionInfoRetrieverImpl(collectionInfoClient),
                new CommandMarkerImpl(mongocryptdClient, "mongocryptd"),
                createKeyRetriever(keyVaultClient, options.getKeyVaultNamespace()),
                createtKeyManagementService(),
                options.getNamespaceToLocalSchemaDocumentMap());
    }

    static Crypt create(final MongoClient keyVaultClient, final KeyVaultEncryptionOptions options) {
        return new CryptImpl(
                MongoCrypts.create(createMongoCryptOptions(options.getKmsProviders())),
                createKeyRetriever(keyVaultClient, options.getKeyVaultNamespace()), createtKeyManagementService());
    }

    private static MongoCryptOptions createMongoCryptOptions(final Map<String, Map<String, Object>> kmsProviders) {
        MongoCryptOptions.Builder mongoCryptOptionsBuilder = MongoCryptOptions.builder();

        for (Map.Entry<String, Map<String, Object>> entry : kmsProviders.entrySet()) {
            if (entry.getKey().equals("aws")) {
                mongoCryptOptionsBuilder.awsKmsProviderOptions(
                        MongoAwsKmsProviderOptions.builder()
                                .accessKeyId((String) entry.getValue().get("accessKeyId"))
                                .secretAccessKey((String) entry.getValue().get("secretAccessKey"))
                                .build()
                );
            } else if (entry.getKey().equals("local")) {
                mongoCryptOptionsBuilder.localKmsProviderOptions(
                        MongoLocalKmsProviderOptions.builder()
                                .localMasterKey(ByteBuffer.wrap((byte[]) entry.getValue().get("key")))
                                .build()
                );
            } else {
                throw new MongoClientException("Unrecognized KMS provider key: " + entry.getKey());
            }
        }
        return mongoCryptOptionsBuilder.build();
    }

    private static KeyRetrieverImpl createKeyRetriever(final MongoClient keyVaultClient, final String keyVaultNamespaceString) {
        MongoNamespace namespace = new MongoNamespace(keyVaultNamespaceString);
        return new KeyRetrieverImpl(keyVaultClient.getDatabase(namespace.getDatabaseName())
                .getCollection(namespace.getCollectionName(), BsonDocument.class));
    }

    private static KeyManagementServiceImpl createtKeyManagementService() {
        return new KeyManagementServiceImpl(getSslContext(), 443, 10000);
    }

    private static SSLContext getSslContext() {
        SSLContext sslContext;
        try {
            sslContext = SSLContext.getDefault();
        } catch (NoSuchAlgorithmException e) {
            throw new MongoClientException("Unable to create default SSLContext", e);
        }
        return sslContext;
    }
}
