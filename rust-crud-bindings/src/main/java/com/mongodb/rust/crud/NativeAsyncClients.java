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
package com.mongodb.rust.crud;

import com.mongodb.MongoClientSettings;

/**
 * Factory methods for creating {@link NativeAsyncClient} instances.
 */
public final class NativeAsyncClients {

    private static final String FFM_ASYNC_CLIENT_CLASS = "com.mongodb.rust.crud.internal.FfmAsyncClient";

    private NativeAsyncClients() {
        // static factory class
    }

    /**
     * Creates a new async client with the given settings.
     *
     * @param settings the client settings
     * @return a new NativeAsyncClient
     */
    public static NativeAsyncClient create(MongoClientSettings settings) {
        try {
            Class<?> ffmClientClass = Class.forName(FFM_ASYNC_CLIENT_CLASS);
            return (NativeAsyncClient) ffmClientClass
                    .getConstructor(MongoClientSettings.class)
                    .newInstance(settings);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create NativeAsyncClient via FFM. " +
                    "Ensure Java 23+ is being used at runtime.", e);
        }
    }
}

