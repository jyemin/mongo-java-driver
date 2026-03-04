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

import com.mongodb.client.ListCollectionNamesIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.lang.Nullable;
import com.mongodb.rust.crud.NativeOperationContext;
import com.mongodb.rust.crud.NativeSyncClient;
import com.mongodb.rust.crud.NativeSyncClientSession;
import com.mongodb.rust.crud.NativeSyncCursor;
import com.mongodb.client.model.ListCollectionsOptions;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.conversions.Bson;

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * Native implementation of ListCollectionNamesIterable using Rust FFI.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class NativeListCollectionNamesIterable
        extends NativeMongoIterableBase<String>
        implements ListCollectionNamesIterable {

    private final String databaseName;
    private final ListCollectionsOptions options = new ListCollectionsOptions();

    public NativeListCollectionNamesIterable(NativeSyncClient nativeClient,
                                             @Nullable NativeSyncClientSession nativeSession,
                                             NativeOperationContext operationContext,
                                             String databaseName) {
        super(nativeClient, nativeSession, operationContext, String.class, null);  // No codec registry needed for String
        this.databaseName = notNull("databaseName", databaseName);
    }

    @Override
    public ListCollectionNamesIterable filter(@Nullable Bson filter) {
        options.filter(filter);
        return this;
    }

    @Override
    public ListCollectionNamesIterable maxTime(long maxTime, TimeUnit timeUnit) {
        options.maxTime(maxTime, timeUnit);
        return this;
    }

    @Override
    public ListCollectionNamesIterable batchSize(int batchSize) {
        options.batchSize(batchSize);
        return this;
    }

    @Override
    public ListCollectionNamesIterable comment(@Nullable String comment) {
        options.comment(comment != null ? new BsonString(comment) : null);
        return this;
    }

    @Override
    public ListCollectionNamesIterable comment(@Nullable BsonValue comment) {
        options.comment(comment);
        return this;
    }

    @Override
    public ListCollectionNamesIterable authorizedCollections(boolean authorizedCollections) {
        options.authorizedCollections(authorizedCollections);
        return this;
    }

    @Override
    public MongoCursor<String> cursor() {
        NativeSyncCursor<String> nativeCursor = getNativeClient().listCollectionNames(
                databaseName, options, getOperationContext(), getNativeSession());
        return new NativeMongoCursor<>(nativeCursor);
    }
}

