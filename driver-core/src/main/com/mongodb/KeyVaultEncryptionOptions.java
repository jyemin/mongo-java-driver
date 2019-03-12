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

package com.mongodb;

import java.util.Map;

// TODO: consider Settings suffix, to be consistent with MongoClientSettings

/**
 * The key vault encryption options
 *
 * @since 3.11
 */
public class KeyVaultEncryptionOptions {
    private final MongoClientSettings keyVaultMongoClientSettings;
    private final String keyVaultNamespace;
    private final Map<String, Map<String, Object>> kmsProviders;

    /**
     * Construct an instance.
     *
     * @param keyVaultMongoClientSettings key vault client settings
     * @param keyVaultNamespace           key vault namespace
     * @param kmsProviders                map of KMS provider properties
     */
    public KeyVaultEncryptionOptions(final MongoClientSettings keyVaultMongoClientSettings,
                                     final String keyVaultNamespace,
                                     final Map<String, Map<String, Object>> kmsProviders) {
        this.keyVaultMongoClientSettings = keyVaultMongoClientSettings;
        this.keyVaultNamespace = keyVaultNamespace;
        this.kmsProviders = kmsProviders;
    }

    /**
     * Gets the key vault client settings
     *
     * @return key vault client settings
     */
    public MongoClientSettings getKeyVaultMongoClientSettings() {
        return keyVaultMongoClientSettings;
    }

    /**
     * Gets the key vault namespace
     *
     * @return key vault namespace
     */
    public String getKeyVaultNamespace() {
        return keyVaultNamespace;
    }

    /**
     * Gets the map of KMS provider properties
     *
     * @return map of KMS provider properties
     */
    public Map<String, Map<String, Object>> getKmsProviders() {
        return kmsProviders;
    }
}
