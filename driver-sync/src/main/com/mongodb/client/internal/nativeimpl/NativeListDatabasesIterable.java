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

import com.mongodb.client.ListDatabasesIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.cursor.TimeoutMode;
import com.mongodb.lang.Nullable;
import com.mongodb.rust.crud.NativeOperationContext;
import com.mongodb.rust.crud.NativeSyncClient;
import com.mongodb.rust.crud.NativeSyncClientSession;
import com.mongodb.rust.crud.NativeSyncCursor;
import com.mongodb.client.model.ListDatabasesOptions;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.concurrent.TimeUnit;

/**
 * Native implementation of ListDatabasesIterable.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class NativeListDatabasesIterable<TResult>
        extends NativeMongoIterableBase<TResult>
        implements ListDatabasesIterable<TResult> {

    private final ListDatabasesOptions options = new ListDatabasesOptions();

    public NativeListDatabasesIterable(NativeSyncClient nativeClient,
                                       @Nullable NativeSyncClientSession nativeSession,
                                       NativeOperationContext operationContext,
                                       Class<TResult> resultClass,
                                       CodecRegistry codecRegistry) {
        super(nativeClient, nativeSession, operationContext, resultClass, codecRegistry);
    }

    @Override
    public ListDatabasesIterable<TResult> maxTime(long maxTime, TimeUnit timeUnit) {
        options.maxTime(maxTime, timeUnit);
        return this;
    }

    @Override
    public ListDatabasesIterable<TResult> batchSize(int batchSize) {
        options.batchSize(batchSize);
        return this;
    }

    @Override
    public ListDatabasesIterable<TResult> filter(@Nullable Bson filter) {
        options.filter(filter);
        return this;
    }

    @Override
    public ListDatabasesIterable<TResult> nameOnly(@Nullable Boolean nameOnly) {
        options.nameOnly(nameOnly);
        return this;
    }

    @Override
    public ListDatabasesIterable<TResult> authorizedDatabasesOnly(@Nullable Boolean authorizedDatabasesOnly) {
        options.authorizedDatabasesOnly(authorizedDatabasesOnly);
        return this;
    }

    @Override
    public ListDatabasesIterable<TResult> comment(@Nullable String comment) {
        options.comment(comment != null ? new BsonString(comment) : null);
        return this;
    }

    @Override
    public ListDatabasesIterable<TResult> comment(@Nullable BsonValue comment) {
        options.comment(comment);
        return this;
    }

    @Override
    public ListDatabasesIterable<TResult> timeoutMode(TimeoutMode timeoutMode) {
        // TODO: Implement timeout mode when ListDatabasesOptions supports it
        return this;
    }

    @Override
    public MongoCursor<TResult> cursor() {
        NativeSyncCursor<TResult> nativeCursor = getNativeClient().listDatabases(options, getCodec(), getOperationContext(), getNativeSession());
        return new NativeMongoCursor<>(nativeCursor);
    }
}

