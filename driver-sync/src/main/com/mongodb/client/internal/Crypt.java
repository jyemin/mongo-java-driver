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

import com.mongodb.MongoNamespace;
import com.mongodb.client.model.vault.DataKeyOptions;
import com.mongodb.client.model.vault.EncryptOptions;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;

import java.io.Closeable;

/**
 * This class is NOT part of the public API.
 */
public interface Crypt extends Closeable {

    /**
     * Encrypt the given command
     *
     * @param namespace the namespace
     * @param command      the unencrypted command
     * @return the encyrpted command
     */
    RawBsonDocument encrypt(MongoNamespace namespace, RawBsonDocument command);

    /**
     * Decrypt the given command response
     *
     * @param commandResponse the encrypted command response
     * @return the decrypted command response
     */
    RawBsonDocument decrypt(RawBsonDocument commandResponse);

    BsonDocument createDataKey(String kmsProvider, DataKeyOptions dataKeyOptions);

    BsonBinary encryptExplicitly(BsonValue value, EncryptOptions options);

    BsonValue decryptExplicitly(BsonBinary value);

    @Override
    void close();
}
