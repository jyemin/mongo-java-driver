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

import com.mongodb.MongoNamespace;
import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.cursor.TimeoutMode;
import com.mongodb.lang.Nullable;
import com.mongodb.rust.crud.NativeOperationContext;
import com.mongodb.rust.crud.NativeSyncClient;
import com.mongodb.rust.crud.NativeSyncClientSession;
import com.mongodb.rust.crud.NativeSyncCursor;
import com.mongodb.client.model.ListIndexesOptions;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * Native implementation of ListIndexesIterable using Rust FFI.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class NativeListIndexesIterable<TResult>
        extends NativeMongoIterableBase<TResult>
        implements ListIndexesIterable<TResult> {

    private final MongoNamespace namespace;
    private final ListIndexesOptions options = new ListIndexesOptions();

    public NativeListIndexesIterable(NativeSyncClient nativeClient,
                                     @Nullable NativeSyncClientSession nativeSession,
                                     NativeOperationContext operationContext,
                                     MongoNamespace namespace,
                                     Class<TResult> resultClass,
                                     CodecRegistry codecRegistry) {
        super(nativeClient, nativeSession, operationContext, resultClass, codecRegistry);
        this.namespace = notNull("namespace", namespace);
    }

    @Override
    public ListIndexesIterable<TResult> maxTime(long maxTime, TimeUnit timeUnit) {
        options.maxTime(maxTime, timeUnit);
        return this;
    }

    @Override
    public ListIndexesIterable<TResult> batchSize(int batchSize) {
        options.batchSize(batchSize);
        return this;
    }

    @Override
    public ListIndexesIterable<TResult> comment(@Nullable String comment) {
        options.comment(comment != null ? new BsonString(comment) : null);
        return this;
    }

    @Override
    public ListIndexesIterable<TResult> comment(@Nullable BsonValue comment) {
        options.comment(comment);
        return this;
    }

    @Override
    public ListIndexesIterable<TResult> timeoutMode(TimeoutMode timeoutMode) {
        // TODO: Implement timeout mode
        return this;
    }

    @Override
    public MongoCursor<TResult> cursor() {
        NativeSyncCursor<TResult> nativeCursor = getNativeClient().listIndexes(
                namespace, options, getCodec(), getOperationContext(), getNativeSession());
        return new NativeMongoCursor<>(nativeCursor);
    }
}

