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

import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;

import java.util.Map;

/**
 * The auto-encryption options.
 *
 * @since 3.11
 */
public class AutoEncryptionOptions {
    private final MongoClientSettings keyVaultMongoClientSettings;
    private final String keyVaultNamespace;
    private final Map<String, Map<String, Object>> kmsProviders;
    private final Map<String, BsonDocument> namespaceToLocalSchemaDocumentMap;
    private final Map<String, Object> extraOptions;

    /**
     * Construct an instance.
     *
     * @param keyVaultMongoClientSettings       key vault client settings
     * @param keyVaultNamespace                 key value namespace
     * @param kmsProviders                      map of KMS provider properties
     * @param namespaceToLocalSchemaDocumentMap map of namespace to local JSON schema
     * @param extraOptions                      extra options
     */
    public AutoEncryptionOptions(@Nullable final MongoClientSettings keyVaultMongoClientSettings,
                                 final String keyVaultNamespace,
                                 final Map<String, Map<String, Object>> kmsProviders,
                                 final Map<String, BsonDocument> namespaceToLocalSchemaDocumentMap,
                                 final Map<String, Object> extraOptions) {
        this.keyVaultMongoClientSettings = keyVaultMongoClientSettings;
        this.keyVaultNamespace = keyVaultNamespace;
        this.kmsProviders = kmsProviders;
        this.namespaceToLocalSchemaDocumentMap = namespaceToLocalSchemaDocumentMap;
        this.extraOptions = extraOptions;
    }

    /**
     * Gets the key vault settings.
     *
     * @return the key vault settings
     */
    @Nullable
    public MongoClientSettings getKeyVaultMongoClientSettings() {
        return keyVaultMongoClientSettings;
    }

    /**
     * Gets the key vault namespace.
     *
     * @return the key vault namespace
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

    /**
     * Gets the map of namespace to local JSON schema
     *
     * @return map of namespace to local JSON schema
     */
    public Map<String, BsonDocument> getNamespaceToLocalSchemaDocumentMap() {
        return namespaceToLocalSchemaDocumentMap;
    }

    /**
     * Gets the extra options.
     *
     * @return the extra options
     */
    public Map<String, Object> getExtraOptions() {
        return extraOptions;
    }
}
