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
import com.mongodb.rust.crud.internal.DefaultNativeSyncClient;

/**
 * Factory methods for creating {@link NativeSyncClient} instances.
 */
public final class NativeSyncClients {

    private NativeSyncClients() {
        // static factory class
    }

    /**
     * Creates a new sync client with the given settings.
     *
     * @param settings the client settings
     * @return a new NativeSyncClient
     */
    public static NativeSyncClient create(MongoClientSettings settings) {
        return new DefaultNativeSyncClient(NativeAsyncClients.create(settings));
    }

    /**
     * Creates a new sync client wrapping the given async client.
     *
     * @param asyncClient the async client to wrap
     * @return a new NativeSyncClient
     */
    public static NativeSyncClient create(NativeAsyncClient asyncClient) {
        return new DefaultNativeSyncClient(asyncClient);
    }
}

