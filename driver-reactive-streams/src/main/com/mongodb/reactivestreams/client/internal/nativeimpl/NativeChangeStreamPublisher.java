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
import com.mongodb.client.model.ChangeStreamOptions;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.FullDocumentBeforeChange;
import com.mongodb.lang.Nullable;
import com.mongodb.reactivestreams.client.ChangeStreamPublisher;
import com.mongodb.rust.crud.NativeAsyncClient;
import com.mongodb.rust.crud.NativeAsyncClientSession;
import com.mongodb.rust.crud.NativeAsyncCursor;
import com.mongodb.rust.crud.NativeOperationContext;
import com.mongodb.rust.crud.SingleResultCallback;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWrapper;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.reactivestreams.Publisher;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Native implementation of ChangeStreamPublisher.
 */
final class NativeChangeStreamPublisher<TResult> implements ChangeStreamPublisher<TResult> {

    enum WatchLevel { CLIENT, DATABASE, COLLECTION }

    private final NativeAsyncClient nativeClient;
    @Nullable private final NativeAsyncClientSession session;
    private final NativeOperationContext operationContext;
    private final List<BsonDocument> pipeline;
    private final Class<TResult> resultClass;
    private final CodecRegistry codecRegistry;
    private final WatchLevel watchLevel;
    @Nullable private final String databaseName;
    @Nullable private final String collectionName;
    private final ChangeStreamOptions options = new ChangeStreamOptions();

    NativeChangeStreamPublisher(NativeAsyncClient nativeClient, @Nullable NativeAsyncClientSession session,
                                NativeOperationContext operationContext, List<? extends Bson> pipeline, Class<TResult> resultClass,
                                CodecRegistry codecRegistry, WatchLevel watchLevel, @Nullable String databaseName,
                                @Nullable String collectionName) {
        this.nativeClient = nativeClient;
        this.session = session;
        this.operationContext = operationContext;
        this.resultClass = resultClass;
        this.codecRegistry = codecRegistry;
        this.watchLevel = watchLevel;
        this.databaseName = databaseName;
        this.collectionName = collectionName;
        this.pipeline = new ArrayList<>();
        for (Bson stage : pipeline) {
            this.pipeline.add(BsonDocumentWrapper.asBsonDocument(stage, codecRegistry));
        }
    }

    @SuppressWarnings("unchecked")
    private reactor.core.publisher.Flux<ChangeStreamDocument<TResult>> toFlux() {
        return Publishers.cursorToFlux(
            Publishers.toMono(callback -> {
                switch (watchLevel) {
                    case COLLECTION:
                        MongoNamespace ns = new MongoNamespace(databaseName, collectionName);
                        nativeClient.watchCollection(ns, pipeline, options,
                                ChangeStreamDocument.createCodec(resultClass, codecRegistry), operationContext, session,
                                (SingleResultCallback) callback);
                        break;
                    case DATABASE:
                        nativeClient.watchDatabase(databaseName, pipeline, options,
                                ChangeStreamDocument.createCodec(resultClass, codecRegistry), operationContext, session,
                                (SingleResultCallback) callback);
                        break;
                    case CLIENT:
                    default:
                        nativeClient.watchClient(pipeline, options,
                                ChangeStreamDocument.createCodec(resultClass, codecRegistry), operationContext, session,
                                (SingleResultCallback) callback);
                        break;
                }
            })
        );
    }

    @Override
    public void subscribe(org.reactivestreams.Subscriber<? super ChangeStreamDocument<TResult>> subscriber) {
        toFlux().subscribe(subscriber);
    }

    @Override public ChangeStreamPublisher<TResult> fullDocument(FullDocument fullDocument) { options.fullDocument(fullDocument); return this; }
    @Override public ChangeStreamPublisher<TResult> fullDocumentBeforeChange(FullDocumentBeforeChange fd) { options.fullDocumentBeforeChange(fd); return this; }
    @Override public ChangeStreamPublisher<TResult> resumeAfter(BsonDocument resumeToken) { options.resumeAfter(resumeToken); return this; }
    @Override public ChangeStreamPublisher<TResult> startAfter(BsonDocument startAfter) { options.startAfter(startAfter); return this; }
    @Override public ChangeStreamPublisher<TResult> startAtOperationTime(BsonTimestamp ts) { options.startAtOperationTime(ts); return this; }
    @Override public ChangeStreamPublisher<TResult> maxAwaitTime(long maxAwaitTime, TimeUnit timeUnit) { options.maxAwaitTime(maxAwaitTime, timeUnit); return this; }
    @Override public ChangeStreamPublisher<TResult> collation(@Nullable Collation collation) { options.collation(collation); return this; }
    @Override public ChangeStreamPublisher<TResult> batchSize(int batchSize) { options.batchSize(batchSize); return this; }
    @Override public ChangeStreamPublisher<TResult> comment(@Nullable String comment) { options.comment(comment != null ? new BsonString(comment) : null); return this; }
    @Override public ChangeStreamPublisher<TResult> comment(@Nullable BsonValue comment) { options.comment(comment); return this; }
    @Override public ChangeStreamPublisher<TResult> showExpandedEvents(boolean show) { /* TODO: Add when ChangeStreamOptions supports it */ return this; }

    @Override
    public Publisher<ChangeStreamDocument<TResult>> first() {
        return toFlux().next();
    }

    @Override
    public <TDocument> Publisher<TDocument> withDocumentClass(Class<TDocument> clazz) {
        // TODO: Implement withDocumentClass for native change streams
        throw new UnsupportedOperationException("withDocumentClass not yet implemented for native change streams");
    }
}

