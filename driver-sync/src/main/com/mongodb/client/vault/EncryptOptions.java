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

// TODO: move to core
package com.mongodb.client.vault;

import org.bson.BsonBinary;
import org.bson.BsonValue;

import java.util.Arrays;

public class EncryptOptions {
    // Optional: Identifies a key vault document by the UUID '_id'.
    private BsonBinary keyId;

    // Optional: Identifies a key vault document by 'keyAltName'.
    private BsonValue keyAltName;

    // Required: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic" or "AEAD_AES_256_CBC_HMAC_SHA_512-Randomized"
    private final String algorithm;

    // Optional: Only applicable for Deterministic encryption.
    private byte[] initializationVector;

    public EncryptOptions(final String algorithm) {
        this.algorithm = algorithm;
    }


    public BsonBinary getKeyId() {
        return keyId;
    }

    public BsonValue getKeyAltName() {
        return keyAltName;
    }

    public String getAlgorithm() {
        return algorithm;
    }

    public byte[] getInitializationVector() {
        return initializationVector;
    }

    public EncryptOptions keyId(final BsonBinary keyId) {
        this.keyId = keyId;
        return this;
    }

    public EncryptOptions keyAltName(final BsonValue keyAltName) {
        this.keyAltName = keyAltName;
        return this;
    }



    public EncryptOptions initializationVector(final byte[] initializationVector) {
        this.initializationVector = initializationVector;
        return this;
    }

    @Override
    public String toString() {
        return "EncryptOptions{" +
                "keyId=" + keyId +
                ", keyAltName=" + keyAltName +
                ", algorithm='" + algorithm + '\'' +
                ", initializationVector=" + Arrays.toString(initializationVector) +
                '}';
    }
}
