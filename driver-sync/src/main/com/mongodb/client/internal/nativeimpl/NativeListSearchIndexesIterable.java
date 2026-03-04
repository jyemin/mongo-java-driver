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

import com.mongodb.ExplainVerbosity;
import com.mongodb.MongoNamespace;
import com.mongodb.client.ListSearchIndexesIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.cursor.TimeoutMode;
import com.mongodb.client.model.Collation;
import com.mongodb.lang.Nullable;
import com.mongodb.rust.crud.NativeOperationContext;
import com.mongodb.rust.crud.NativeSyncClient;
import com.mongodb.rust.crud.NativeSyncClientSession;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.concurrent.TimeUnit;

/**
 * Native implementation of ListSearchIndexesIterable using Rust FFI.
 *
 * <p>Note: Search indexes are an Atlas-only feature and are not yet supported in the native client.</p>
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class NativeListSearchIndexesIterable<TResult>
        extends NativeMongoIterableBase<TResult>
        implements ListSearchIndexesIterable<TResult> {

    public NativeListSearchIndexesIterable(NativeSyncClient nativeClient,
                                           @Nullable NativeSyncClientSession nativeSession,
                                           NativeOperationContext operationContext,
                                           MongoNamespace namespace,
                                           Class<TResult> resultClass,
                                           CodecRegistry codecRegistry) {
        super(nativeClient, nativeSession, operationContext, resultClass, codecRegistry);
    }

    @Override
    public ListSearchIndexesIterable<TResult> name(String indexName) {
        return this;
    }

    @Override
    public ListSearchIndexesIterable<TResult> allowDiskUse(@Nullable Boolean allowDiskUse) {
        return this;
    }

    @Override
    public ListSearchIndexesIterable<TResult> batchSize(int batchSize) {
        return this;
    }

    @Override
    public ListSearchIndexesIterable<TResult> maxTime(long maxTime, TimeUnit timeUnit) {
        return this;
    }

    @Override
    public ListSearchIndexesIterable<TResult> collation(@Nullable Collation collation) {
        return this;
    }

    @Override
    public ListSearchIndexesIterable<TResult> comment(@Nullable String comment) {
        return this;
    }

    @Override
    public ListSearchIndexesIterable<TResult> comment(@Nullable BsonValue comment) {
        return this;
    }

    @Override
    public ListSearchIndexesIterable<TResult> timeoutMode(TimeoutMode timeoutMode) {
        return this;
    }

    @Override
    public Document explain() {
        throw new UnsupportedOperationException("listSearchIndexes not yet implemented in native client");
    }

    @Override
    public Document explain(ExplainVerbosity verbosity) {
        throw new UnsupportedOperationException("listSearchIndexes not yet implemented in native client");
    }

    @Override
    public <E> E explain(Class<E> explainResultClass) {
        throw new UnsupportedOperationException("listSearchIndexes not yet implemented in native client");
    }

    @Override
    public <E> E explain(Class<E> explainResultClass, ExplainVerbosity verbosity) {
        throw new UnsupportedOperationException("listSearchIndexes not yet implemented in native client");
    }

    @Override
    public MongoCursor<TResult> cursor() {
        throw new UnsupportedOperationException("listSearchIndexes not yet implemented in native client");
    }
}

