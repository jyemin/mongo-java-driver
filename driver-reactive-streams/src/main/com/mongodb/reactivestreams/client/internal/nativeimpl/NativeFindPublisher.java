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

import com.mongodb.CursorType;
import com.mongodb.ExplainVerbosity;
import com.mongodb.MongoNamespace;
import com.mongodb.client.model.Collation;
import com.mongodb.client.model.FindOptions;
import com.mongodb.lang.Nullable;
import com.mongodb.reactivestreams.client.FindPublisher;
import com.mongodb.rust.crud.NativeAsyncClient;
import com.mongodb.rust.crud.NativeAsyncClientSession;
import com.mongodb.rust.crud.NativeAsyncCursor;
import com.mongodb.rust.crud.NativeOperationContext;
import com.mongodb.rust.crud.SingleResultCallback;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWrapper;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.reactivestreams.Publisher;

import java.util.concurrent.TimeUnit;

/**
 * Native implementation of FindPublisher.
 */
final class NativeFindPublisher<TResult> extends NativeCursorPublisher<TResult> implements FindPublisher<TResult> {

    private final MongoNamespace namespace;
    private final Class<TResult> resultClass;
    private final FindOptions options = new FindOptions();
    private BsonDocument filter = new BsonDocument();

    NativeFindPublisher(NativeAsyncClient nativeClient, @Nullable NativeAsyncClientSession session,
                        NativeOperationContext operationContext, MongoNamespace namespace, Class<TResult> resultClass,
                        CodecRegistry codecRegistry) {
        super(nativeClient, session, operationContext, codecRegistry);
        this.namespace = namespace;
        this.resultClass = resultClass;
    }

    @Override
    protected void executeAsync(SingleResultCallback<NativeAsyncCursor<TResult>> callback) {
        if (getBatchSize() != null) {
            options.batchSize(getBatchSize());
        }
        getNativeClient().find(namespace, filter, options, getCodecRegistry().get(resultClass), getOperationContext(), getSession(), callback);
    }

    @Override public FindPublisher<TResult> filter(@Nullable Bson filter) {
        this.filter = filter != null ? BsonDocumentWrapper.asBsonDocument(filter, getCodecRegistry()) : new BsonDocument();
        return this;
    }
    @Override public FindPublisher<TResult> limit(int limit) { options.limit(limit); return this; }
    @Override public FindPublisher<TResult> skip(int skip) { options.skip(skip); return this; }
    @Override public FindPublisher<TResult> maxTime(long maxTime, TimeUnit timeUnit) { options.maxTime(maxTime, timeUnit); return this; }
    @Override public FindPublisher<TResult> maxAwaitTime(long maxAwaitTime, TimeUnit timeUnit) { options.maxAwaitTime(maxAwaitTime, timeUnit); return this; }
    @Override public FindPublisher<TResult> projection(@Nullable Bson projection) { options.projection(projection); return this; }
    @Override public FindPublisher<TResult> sort(@Nullable Bson sort) { options.sort(sort); return this; }
    @Override public FindPublisher<TResult> noCursorTimeout(boolean noCursorTimeout) { options.noCursorTimeout(noCursorTimeout); return this; }
    @Override public FindPublisher<TResult> partial(boolean partial) { options.partial(partial); return this; }
    @Override public FindPublisher<TResult> cursorType(CursorType cursorType) { options.cursorType(cursorType); return this; }
    @Override public FindPublisher<TResult> collation(@Nullable Collation collation) { options.collation(collation); return this; }
    @Override public FindPublisher<TResult> comment(@Nullable String comment) { options.comment(comment); return this; }
    @Override public FindPublisher<TResult> comment(@Nullable BsonValue comment) { options.comment(comment); return this; }
    @Override public FindPublisher<TResult> hint(@Nullable Bson hint) { options.hint(hint); return this; }
    @Override public FindPublisher<TResult> hintString(@Nullable String hint) { options.hintString(hint); return this; }
    @Override public FindPublisher<TResult> let(@Nullable Bson let) { options.let(let); return this; }
    @Override public FindPublisher<TResult> max(@Nullable Bson max) { options.max(max); return this; }
    @Override public FindPublisher<TResult> min(@Nullable Bson min) { options.min(min); return this; }
    @Override public FindPublisher<TResult> returnKey(boolean returnKey) { options.returnKey(returnKey); return this; }
    @Override public FindPublisher<TResult> showRecordId(boolean showRecordId) { options.showRecordId(showRecordId); return this; }
    @Override public FindPublisher<TResult> batchSize(int batchSize) { setBatchSize(batchSize); options.batchSize(batchSize); return this; }
    @Override public FindPublisher<TResult> allowDiskUse(@Nullable Boolean allowDiskUse) { options.allowDiskUse(allowDiskUse); return this; }
    @Override public FindPublisher<TResult> timeoutMode(com.mongodb.client.cursor.TimeoutMode timeoutMode) { return this; }

    @Override public Publisher<Document> explain() { return explain(Document.class); }
    @Override public Publisher<Document> explain(ExplainVerbosity verbosity) { return explain(Document.class, verbosity); }
    @Override public <E> Publisher<E> explain(Class<E> explainResultClass) { return explain(explainResultClass, null); }
    @Override public <E> Publisher<E> explain(Class<E> explainResultClass, @Nullable ExplainVerbosity verbosity) {
        throw new UnsupportedOperationException("explain not yet implemented");
    }

    @Override
    public Publisher<TResult> first() {
        // Use limit(-1) and batchSize(0) to match standard driver behavior.
        // limit(-1) tells MongoDB to return 1 document and automatically close the cursor.
        // The native client consumes options synchronously during subscribe(), so we can
        // restore immediately after.
        return subscriber -> {
            long origLimit = options.getLimit();
            int origBatchSize = options.getBatchSize();
            options.limit(-1);
            options.batchSize(0);
            try {
                super.first().subscribe(subscriber);
            } finally {
                options.limit(origLimit);
                options.batchSize(origBatchSize);
            }
        };
    }
}

