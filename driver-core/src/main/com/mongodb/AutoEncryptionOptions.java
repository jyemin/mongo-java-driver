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
 *
 * @since 3.11
 */
public class AutoEncryptionOptions {
    private final MongoClientSettings keyVaultMongoClientSettings;
    private final String keyVaultNamespace;
    private final Map<String, Map<String, Object>> kmsProviders;
    private final Map<String, BsonDocument> namespaceToLocalSchemaDocumentMap;
    private final Map<String, Object> extraOptions;

    public AutoEncryptionOptions(@Nullable final MongoClientSettings keyVaultMongoClientSettings,
                                 @Nullable final String keyVaultNamespace,
                                 final Map<String, Map<String, Object>> kmsProviders,
                                 final Map<String, BsonDocument> namespaceToLocalSchemaDocumentMap,
                                 final Map<String, Object> extraOptions) {
        this.keyVaultMongoClientSettings = keyVaultMongoClientSettings;
        this.keyVaultNamespace = keyVaultNamespace;
        this.kmsProviders = kmsProviders;
        this.namespaceToLocalSchemaDocumentMap = namespaceToLocalSchemaDocumentMap;
        this.extraOptions = extraOptions;       
    }

    @Nullable
    public MongoClientSettings getKeyVaultMongoClientSettings() {
        return keyVaultMongoClientSettings;
    }

    @Nullable
    public String getKeyVaultNamespace() {
        return keyVaultNamespace;
    }

    public Map<String, Map<String, Object>> getKmsProviders() {
        return kmsProviders;
    }

    public Map<String, BsonDocument> getNamespaceToLocalSchemaDocumentMap() {
        return namespaceToLocalSchemaDocumentMap;
    }

    public Map<String, Object> getExtraOptions() {
        return extraOptions;
    }
}
