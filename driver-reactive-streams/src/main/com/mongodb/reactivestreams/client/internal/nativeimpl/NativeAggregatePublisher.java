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

import com.mongodb.ExplainVerbosity;
import com.mongodb.MongoNamespace;
import com.mongodb.client.model.AggregateOptions;
import com.mongodb.client.model.Collation;
import com.mongodb.lang.Nullable;
import com.mongodb.reactivestreams.client.AggregatePublisher;
import com.mongodb.rust.crud.NativeAsyncClient;
import com.mongodb.rust.crud.NativeAsyncClientSession;
import com.mongodb.rust.crud.NativeAsyncCursor;
import com.mongodb.rust.crud.SingleResultCallback;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWrapper;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.reactivestreams.Publisher;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Native implementation of AggregatePublisher.
 */
final class NativeAggregatePublisher<TResult> extends NativeCursorPublisher<TResult> implements AggregatePublisher<TResult> {

    private final String databaseName;
    @Nullable private final String collectionName;
    private final List<BsonDocument> pipeline;
    private final Class<TResult> resultClass;
    private final AggregateOptions options = new AggregateOptions();
    @Nullable private Boolean bypassDocumentValidation;

    NativeAggregatePublisher(NativeAsyncClient nativeClient, @Nullable NativeAsyncClientSession session,
                             String databaseName, @Nullable String collectionName,
                             List<? extends Bson> pipeline, Class<TResult> resultClass, CodecRegistry codecRegistry) {
        super(nativeClient, session, codecRegistry);
        this.databaseName = databaseName;
        this.collectionName = collectionName;
        this.resultClass = resultClass;
        this.pipeline = new ArrayList<>();
        for (Bson stage : pipeline) {
            this.pipeline.add(BsonDocumentWrapper.asBsonDocument(stage, codecRegistry));
        }
    }

    @Override
    protected void executeAsync(SingleResultCallback<NativeAsyncCursor<TResult>> callback) {
        if (getBatchSize() != null) {
            options.batchSize(getBatchSize());
        }
        if (collectionName != null) {
            MongoNamespace ns = new MongoNamespace(databaseName, collectionName);
            getNativeClient().aggregate(ns, pipeline, options, bypassDocumentValidation, getCodecRegistry().get(resultClass), getSession(), callback);
        } else {
            getNativeClient().aggregateDatabase(databaseName, pipeline, options, bypassDocumentValidation, getCodecRegistry().get(resultClass), getSession(), callback);
        }
    }

    @Override public AggregatePublisher<TResult> allowDiskUse(@Nullable Boolean allowDiskUse) { options.allowDiskUse(allowDiskUse); return this; }
    @Override public AggregatePublisher<TResult> maxTime(long maxTime, TimeUnit timeUnit) { options.maxTime(maxTime, timeUnit); return this; }
    @Override public AggregatePublisher<TResult> maxAwaitTime(long maxAwaitTime, TimeUnit timeUnit) { /* TODO: Support when options support it */ return this; }
    @Override public AggregatePublisher<TResult> bypassDocumentValidation(@Nullable Boolean bypass) { this.bypassDocumentValidation = bypass; return this; }
    @Override public AggregatePublisher<TResult> collation(@Nullable Collation collation) { options.collation(collation); return this; }
    @Override public AggregatePublisher<TResult> comment(@Nullable String comment) { options.comment(comment); return this; }
    @Override public AggregatePublisher<TResult> comment(@Nullable BsonValue comment) { options.comment(comment); return this; }
    @Override public AggregatePublisher<TResult> hint(@Nullable Bson hint) { options.hint(hint); return this; }
    @Override public AggregatePublisher<TResult> hintString(@Nullable String hint) { options.hintString(hint); return this; }
    @Override public AggregatePublisher<TResult> let(@Nullable Bson let) { options.let(let); return this; }
    @Override public AggregatePublisher<TResult> batchSize(int batchSize) { setBatchSize(batchSize); options.batchSize(batchSize); return this; }
    @Override public AggregatePublisher<TResult> timeoutMode(com.mongodb.client.cursor.TimeoutMode timeoutMode) { return this; }

    @Override public Publisher<Void> toCollection() { throw new UnsupportedOperationException("toCollection not yet implemented"); }
    @Override public Publisher<Document> explain() { return explain(Document.class); }
    @Override public Publisher<Document> explain(ExplainVerbosity verbosity) { return explain(Document.class, verbosity); }
    @Override public <E> Publisher<E> explain(Class<E> explainResultClass) { return explain(explainResultClass, null); }
    @Override public <E> Publisher<E> explain(Class<E> explainResultClass, @Nullable ExplainVerbosity verbosity) {
        throw new UnsupportedOperationException("explain not yet implemented");
    }
}

