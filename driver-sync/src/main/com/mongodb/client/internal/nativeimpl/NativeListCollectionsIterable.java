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
package com.mongodb.client.internal.nativeimpl;

import com.mongodb.client.ListCollectionsIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.cursor.TimeoutMode;
import com.mongodb.lang.Nullable;
import com.mongodb.rust.crud.NativeSyncClient;
import com.mongodb.rust.crud.NativeSyncClientSession;
import com.mongodb.rust.crud.NativeSyncCursor;
import com.mongodb.client.model.ListCollectionsOptions;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.concurrent.TimeUnit;

/**
 * Native implementation of ListCollectionsIterable using Rust FFI.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class NativeListCollectionsIterable<TResult>
        extends NativeMongoIterableBase<TResult>
        implements ListCollectionsIterable<TResult> {

    private final String databaseName;
    private final ListCollectionsOptions options = new ListCollectionsOptions();

    public NativeListCollectionsIterable(NativeSyncClient nativeClient,
                                         @Nullable NativeSyncClientSession nativeSession,
                                         String databaseName,
                                         Class<TResult> resultClass,
                                         CodecRegistry codecRegistry) {
        super(nativeClient, nativeSession, resultClass, codecRegistry);
        this.databaseName = databaseName;
    }

    @Override
    public ListCollectionsIterable<TResult> filter(@Nullable Bson filter) {
        options.filter(filter);
        return this;
    }

    @Override
    public ListCollectionsIterable<TResult> maxTime(long maxTime, TimeUnit timeUnit) {
        options.maxTime(maxTime, timeUnit);
        return this;
    }

    @Override
    public ListCollectionsIterable<TResult> batchSize(int batchSize) {
        options.batchSize(batchSize);
        return this;
    }

    @Override
    public ListCollectionsIterable<TResult> comment(@Nullable String comment) {
        options.comment(comment != null ? new BsonString(comment) : null);
        return this;
    }

    @Override
    public ListCollectionsIterable<TResult> comment(@Nullable BsonValue comment) {
        options.comment(comment);
        return this;
    }

    @Override
    public ListCollectionsIterable<TResult> timeoutMode(TimeoutMode timeoutMode) {
        // TODO: Implement timeout mode
        return this;
    }

    @Override
    public MongoCursor<TResult> cursor() {
        NativeSyncCursor<TResult> nativeCursor = getNativeClient().listCollections(
                databaseName, options, getCodec(), getNativeSession());
        return new NativeMongoCursor<>(nativeCursor);
    }
}

