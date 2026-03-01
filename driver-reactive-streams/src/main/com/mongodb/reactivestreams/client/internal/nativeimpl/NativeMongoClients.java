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
package com.mongodb.reactivestreams.client.internal.nativeimpl;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.reactivestreams.client.MongoClient;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * A factory for {@link MongoClient} instances backed by the native Rust implementation.
 *
 * <p>This class mirrors the functionality of {@link com.mongodb.reactivestreams.client.MongoClients} but creates
 * native client instances that use the Rust FFI layer for MongoDB operations.</p>
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 *
 * @see NativeMongoClient
 * @see com.mongodb.reactivestreams.client.MongoClients
 */
public final class NativeMongoClients {

    /**
     * Creates a new client with the default connection string "mongodb://localhost".
     *
     * @return the client
     */
    public static MongoClient create() {
        return create(new ConnectionString("mongodb://localhost"));
    }

    /**
     * Create a new client with the given client settings.
     *
     * @param settings the settings
     * @return the client
     */
    public static MongoClient create(final MongoClientSettings settings) {
        notNull("settings", settings);
        return new NativeMongoClient(settings);
    }

    /**
     * Create a new client with the given connection string.
     *
     * @param connectionString the connection string
     * @return the client
     */
    public static MongoClient create(final String connectionString) {
        return create(new ConnectionString(connectionString));
    }

    /**
     * Create a new client with the given connection string.
     *
     * @param connectionString the connection string
     * @return the client
     */
    public static MongoClient create(final ConnectionString connectionString) {
        return create(MongoClientSettings.builder().applyConnectionString(connectionString).build());
    }

    private NativeMongoClients() {
    }
}

