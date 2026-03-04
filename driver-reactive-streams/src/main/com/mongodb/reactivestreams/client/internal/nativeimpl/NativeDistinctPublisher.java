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

import com.mongodb.MongoNamespace;
import com.mongodb.client.model.Collation;
import com.mongodb.client.model.DistinctOptions;
import com.mongodb.lang.Nullable;
import com.mongodb.reactivestreams.client.DistinctPublisher;
import com.mongodb.rust.crud.NativeAsyncClient;
import com.mongodb.rust.crud.NativeAsyncClientSession;
import com.mongodb.rust.crud.NativeAsyncCursor;
import com.mongodb.rust.crud.NativeOperationContext;
import com.mongodb.rust.crud.SingleResultCallback;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWrapper;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.concurrent.TimeUnit;

/**
 * Native implementation of DistinctPublisher.
 */
final class NativeDistinctPublisher<TResult> extends NativeCursorPublisher<TResult> implements DistinctPublisher<TResult> {

    private final MongoNamespace namespace;
    private final String fieldName;
    private final Class<TResult> resultClass;
    private final DistinctOptions options = new DistinctOptions();
    private BsonDocument filter = new BsonDocument();

    NativeDistinctPublisher(NativeAsyncClient nativeClient, @Nullable NativeAsyncClientSession session,
                            NativeOperationContext operationContext, MongoNamespace namespace, String fieldName,
                            Class<TResult> resultClass, CodecRegistry codecRegistry) {
        super(nativeClient, session, operationContext, codecRegistry);
        this.namespace = namespace;
        this.fieldName = fieldName;
        this.resultClass = resultClass;
    }

    @Override
    protected void executeAsync(SingleResultCallback<NativeAsyncCursor<TResult>> callback) {
        if (getBatchSize() != null) {
            options.batchSize(getBatchSize());
        }
        getNativeClient().distinct(namespace, fieldName, filter, options, getCodecRegistry().get(resultClass), getOperationContext(), getSession(), callback);
    }

    @Override
    public DistinctPublisher<TResult> filter(@Nullable Bson filter) {
        this.filter = filter != null ? BsonDocumentWrapper.asBsonDocument(filter, getCodecRegistry()) : new BsonDocument();
        return this;
    }
    @Override public DistinctPublisher<TResult> maxTime(long maxTime, TimeUnit timeUnit) { options.maxTime(maxTime, timeUnit); return this; }
    @Override public DistinctPublisher<TResult> collation(@Nullable Collation collation) { options.collation(collation); return this; }
    @Override public DistinctPublisher<TResult> batchSize(int batchSize) { setBatchSize(batchSize); options.batchSize(batchSize); return this; }
    @Override public DistinctPublisher<TResult> comment(@Nullable String comment) { options.comment(comment != null ? new BsonString(comment) : null); return this; }
    @Override public DistinctPublisher<TResult> comment(@Nullable BsonValue comment) { options.comment(comment); return this; }
    @Override public DistinctPublisher<TResult> hint(@Nullable Bson hint) { options.hint(hint); return this; }
    @Override public DistinctPublisher<TResult> hintString(@Nullable String hint) { options.hintString(hint); return this; }
    @Override public DistinctPublisher<TResult> timeoutMode(com.mongodb.client.cursor.TimeoutMode timeoutMode) { return this; }
}

