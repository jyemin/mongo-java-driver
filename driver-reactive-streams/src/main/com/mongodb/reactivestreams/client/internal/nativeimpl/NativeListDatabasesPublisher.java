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
import com.mongodb.reactivestreams.client.ListDatabasesPublisher;
import com.mongodb.rust.crud.NativeAsyncClient;
import com.mongodb.rust.crud.NativeAsyncClientSession;
import com.mongodb.rust.crud.NativeAsyncCursor;
import com.mongodb.rust.crud.SingleResultCallback;
import com.mongodb.client.model.ListDatabasesOptions;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.concurrent.TimeUnit;

/**
 * Native implementation of ListDatabasesPublisher.
 */
final class NativeListDatabasesPublisher<TResult> extends NativeCursorPublisher<TResult> implements ListDatabasesPublisher<TResult> {

    private final Class<TResult> resultClass;
    private final ListDatabasesOptions options = new ListDatabasesOptions();

    NativeListDatabasesPublisher(NativeAsyncClient nativeClient, @Nullable NativeAsyncClientSession session,
                                 Class<TResult> resultClass, CodecRegistry codecRegistry) {
        super(nativeClient, session, codecRegistry);
        this.resultClass = resultClass;
    }

    @Override
    protected void executeAsync(SingleResultCallback<NativeAsyncCursor<TResult>> callback) {
        if (getBatchSize() != null) {
            options.batchSize(getBatchSize());
        }
        getNativeClient().listDatabases(options, getCodecRegistry().get(resultClass), getSession(), callback);
    }

    @Override public ListDatabasesPublisher<TResult> maxTime(long maxTime, TimeUnit timeUnit) { options.maxTime(maxTime, timeUnit); return this; }
    @Override public ListDatabasesPublisher<TResult> nameOnly(@Nullable Boolean nameOnly) { options.nameOnly(nameOnly); return this; }
    @Override public ListDatabasesPublisher<TResult> authorizedDatabasesOnly(@Nullable Boolean auth) { options.authorizedDatabasesOnly(auth); return this; }
    @Override public ListDatabasesPublisher<TResult> batchSize(int batchSize) { setBatchSize(batchSize); options.batchSize(batchSize); return this; }
    @Override public ListDatabasesPublisher<TResult> timeoutMode(com.mongodb.client.cursor.TimeoutMode timeoutMode) { return this; }
    @Override public ListDatabasesPublisher<TResult> filter(@Nullable Bson filter) { options.filter(filter); return this; }

    @Override
    public ListDatabasesPublisher<TResult> comment(@Nullable BsonValue comment) {
        options.comment(comment);
        return this;
    }

    @Override
    public ListDatabasesPublisher<TResult> comment(@Nullable String comment) {
        options.comment(comment != null ? new BsonString(comment) : null);
        return this;
    }
}

