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

import com.mongodb.KeyVaultEncryptionOptions;
import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.vault.DataKeyOptions;
import com.mongodb.client.vault.EncryptOptions;
import com.mongodb.client.vault.KeyVault;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonValue;

import java.io.Closeable;

public class KeyVaultImpl implements KeyVault, Closeable {
    private final Crypt crypt;
    private final KeyVaultEncryptionOptions options;
    private final MongoClient keyVaultClient;

    public KeyVaultImpl(final KeyVaultEncryptionOptions options) {
        keyVaultClient = MongoClients.create(options.getKeyVaultMongoClientSettings());
        this.crypt = Crypts.create(keyVaultClient, options);
        this.options = options;
    }

    @Override
    public BsonBinary createDataKey(final String kmsProvider) {
        return createDataKey(kmsProvider, new DataKeyOptions());
    }

    @Override
    public BsonBinary createDataKey(final String kmsProvider, final DataKeyOptions dataKeyOptions) {
        BsonDocument dataKeyDocument = crypt.createDataKey(kmsProvider, dataKeyOptions);

        MongoNamespace namespace = new MongoNamespace(options.getKeyVaultNamespace());
        keyVaultClient.getDatabase(namespace.getDatabaseName()).getCollection(namespace.getCollectionName(), BsonDocument.class)
                .insertOne(dataKeyDocument);
        return dataKeyDocument.getBinary("_id");
    }

    @Override
    public BsonBinary encrypt(final BsonValue value, final EncryptOptions options) {
        return crypt.encryptExplicitly(value, options);
    }

    @Override
    public BsonValue decrypt(final BsonBinary value) {
        return crypt.decryptExplicitly(value);
    }

    @Override
    public void close() {
        keyVaultClient.close();
    }
}
