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
import com.mongodb.reactivestreams.client.ListCollectionNamesPublisher;
import com.mongodb.rust.crud.NativeAsyncClient;
import com.mongodb.rust.crud.NativeAsyncClientSession;
import com.mongodb.rust.crud.NativeAsyncCursor;
import com.mongodb.rust.crud.NativeOperationContext;
import com.mongodb.rust.crud.SingleResultCallback;
import com.mongodb.client.model.ListCollectionsOptions;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.concurrent.TimeUnit;

/**
 * Native implementation of ListCollectionNamesPublisher.
 */
final class NativeListCollectionNamesPublisher extends NativeCursorPublisher<String> implements ListCollectionNamesPublisher {

    private final String databaseName;
    private final ListCollectionsOptions options = new ListCollectionsOptions();

    NativeListCollectionNamesPublisher(NativeAsyncClient nativeClient, @Nullable NativeAsyncClientSession session,
                                       NativeOperationContext operationContext, String databaseName, CodecRegistry codecRegistry) {
        super(nativeClient, session, operationContext, codecRegistry);
        this.databaseName = databaseName;
    }

    @Override
    protected void executeAsync(SingleResultCallback<NativeAsyncCursor<String>> callback) {
        if (getBatchSize() != null) {
            options.batchSize(getBatchSize());
        }
        getNativeClient().listCollectionNames(databaseName, options, getOperationContext(), getSession(), callback);
    }

    @Override public ListCollectionNamesPublisher maxTime(long maxTime, TimeUnit timeUnit) { options.maxTime(maxTime, timeUnit); return this; }
    @Override public ListCollectionNamesPublisher batchSize(int batchSize) { setBatchSize(batchSize); options.batchSize(batchSize); return this; }
    @Override public ListCollectionNamesPublisher authorizedCollections(boolean auth) { options.authorizedCollections(auth); return this; }
    @Override public ListCollectionNamesPublisher filter(@Nullable Bson filter) { options.filter(filter); return this; }

    @Override
    public ListCollectionNamesPublisher comment(@Nullable BsonValue comment) {
        options.comment(comment);
        return this;
    }

    @Override
    public ListCollectionNamesPublisher comment(@Nullable String comment) {
        options.comment(comment != null ? new BsonString(comment) : null);
        return this;
    }
}

