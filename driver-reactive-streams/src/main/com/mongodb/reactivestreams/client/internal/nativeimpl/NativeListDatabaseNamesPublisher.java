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

import com.mongodb.lang.Nullable;
import com.mongodb.rust.crud.NativeAsyncClient;
import com.mongodb.rust.crud.NativeAsyncClientSession;
import com.mongodb.rust.crud.NativeAsyncCursor;
import com.mongodb.rust.crud.SingleResultCallback;
import com.mongodb.client.model.ListDatabasesOptions;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.concurrent.TimeUnit;

/**
 * Native implementation of list database names as a Publisher.
 */
final class NativeListDatabaseNamesPublisher extends NativeCursorPublisher<String> {

    private final ListDatabasesOptions options = new ListDatabasesOptions();

    NativeListDatabaseNamesPublisher(NativeAsyncClient nativeClient, @Nullable NativeAsyncClientSession session, CodecRegistry codecRegistry) {
        super(nativeClient, session, codecRegistry);
    }

    @Override
    protected void executeAsync(SingleResultCallback<NativeAsyncCursor<String>> callback) {
        options.nameOnly(true);
        if (getBatchSize() != null) {
            options.batchSize(getBatchSize());
        }
        getNativeClient().listDatabaseNames(options, getSession(), callback);
    }

    public NativeListDatabaseNamesPublisher authorizedDatabasesOnly(boolean authorizedDatabasesOnly) {
        options.authorizedDatabasesOnly(authorizedDatabasesOnly);
        return this;
    }

    public NativeListDatabaseNamesPublisher maxTime(long maxTime, TimeUnit timeUnit) {
        options.maxTime(maxTime, timeUnit);
        return this;
    }
}

