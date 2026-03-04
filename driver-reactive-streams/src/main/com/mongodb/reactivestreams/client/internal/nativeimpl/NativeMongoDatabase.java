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

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.CreateViewOptions;
import com.mongodb.lang.Nullable;
import com.mongodb.reactivestreams.client.AggregatePublisher;
import com.mongodb.reactivestreams.client.ChangeStreamPublisher;
import com.mongodb.reactivestreams.client.ClientSession;
import com.mongodb.reactivestreams.client.ListCollectionNamesPublisher;
import com.mongodb.reactivestreams.client.ListCollectionsPublisher;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import com.mongodb.rust.crud.NativeAsyncClient;
import com.mongodb.rust.crud.NativeAsyncClientSession;
import com.mongodb.rust.crud.NativeOperationContext;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWrapper;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.reactivestreams.Publisher;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * Native implementation of MongoDatabase using Rust FFI.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class NativeMongoDatabase implements MongoDatabase {

    private final NativeAsyncClient nativeClient;
    private final String databaseName;
    private final CodecRegistry codecRegistry;
    private final NativeOperationContext operationContext;

    NativeMongoDatabase(NativeAsyncClient nativeClient, String databaseName, CodecRegistry codecRegistry,
                        ReadPreference readPreference, WriteConcern writeConcern, ReadConcern readConcern) {
        this.nativeClient = notNull("nativeClient", nativeClient);
        this.databaseName = notNull("databaseName", databaseName);
        this.codecRegistry = notNull("codecRegistry", codecRegistry);
        this.operationContext = NativeOperationContext.builder()
                .readPreference(readPreference)
                .writeConcern(writeConcern)
                .readConcern(readConcern)
                .build();
    }

    @Override
    public String getName() {
        return databaseName;
    }

    @Override
    public CodecRegistry getCodecRegistry() {
        return codecRegistry;
    }

    @Override
    public ReadPreference getReadPreference() {
        return operationContext.getReadPreference();
    }

    @Override
    public WriteConcern getWriteConcern() {
        return operationContext.getWriteConcern();
    }

    @Override
    public ReadConcern getReadConcern() {
        return operationContext.getReadConcern();
    }

    @Override
    @Nullable
    public Long getTimeout(TimeUnit timeUnit) {
        return null;
    }

    @Override
    public MongoDatabase withCodecRegistry(CodecRegistry codecRegistry) {
        return new NativeMongoDatabase(nativeClient, databaseName, codecRegistry, getReadPreference(), getWriteConcern(), getReadConcern());
    }

    @Override
    public MongoDatabase withReadPreference(ReadPreference readPreference) {
        return new NativeMongoDatabase(nativeClient, databaseName, codecRegistry, readPreference, getWriteConcern(), getReadConcern());
    }

    @Override
    public MongoDatabase withWriteConcern(WriteConcern writeConcern) {
        return new NativeMongoDatabase(nativeClient, databaseName, codecRegistry, getReadPreference(), writeConcern, getReadConcern());
    }

    @Override
    public MongoDatabase withReadConcern(ReadConcern readConcern) {
        return new NativeMongoDatabase(nativeClient, databaseName, codecRegistry, getReadPreference(), getWriteConcern(), readConcern);
    }

    @Override
    public MongoDatabase withTimeout(long timeout, TimeUnit timeUnit) {
        return this;
    }

    @Override
    public MongoCollection<Document> getCollection(String collectionName) {
        return getCollection(collectionName, Document.class);
    }

    @Override
    public <TDocument> MongoCollection<TDocument> getCollection(String collectionName, Class<TDocument> documentClass) {
        return new NativeMongoCollection<>(nativeClient, databaseName, collectionName, documentClass, codecRegistry,
                getReadPreference(), getWriteConcern(), getReadConcern());
    }

    // ==================== Command Operations ====================

    @Override
    public Publisher<Document> runCommand(Bson command) {
        return runCommandInternal(null, command, Document.class);
    }

    @Override
    public Publisher<Document> runCommand(Bson command, ReadPreference readPreference) {
        return runCommandInternal(null, command, Document.class);
    }

    @Override
    public <TResult> Publisher<TResult> runCommand(Bson command, Class<TResult> resultClass) {
        return runCommandInternal(null, command, resultClass);
    }

    @Override
    public <TResult> Publisher<TResult> runCommand(Bson command, ReadPreference readPreference, Class<TResult> resultClass) {
        return runCommandInternal(null, command, resultClass);
    }

    @Override
    public Publisher<Document> runCommand(ClientSession clientSession, Bson command) {
        return runCommandInternal(clientSession, command, Document.class);
    }

    @Override
    public Publisher<Document> runCommand(ClientSession clientSession, Bson command, ReadPreference readPreference) {
        return runCommandInternal(clientSession, command, Document.class);
    }

    @Override
    public <TResult> Publisher<TResult> runCommand(ClientSession clientSession, Bson command, Class<TResult> resultClass) {
        return runCommandInternal(clientSession, command, resultClass);
    }

    @Override
    public <TResult> Publisher<TResult> runCommand(ClientSession clientSession, Bson command, ReadPreference readPreference, Class<TResult> resultClass) {
        return runCommandInternal(clientSession, command, resultClass);
    }

    private <TResult> Publisher<TResult> runCommandInternal(@Nullable ClientSession clientSession, Bson command, Class<TResult> resultClass) {
        BsonDocument commandDoc = BsonDocumentWrapper.asBsonDocument(command, codecRegistry);
        return Publishers.toMono(callback ->
            nativeClient.runCommand(databaseName, commandDoc, codecRegistry.get(resultClass), operationContext, getNativeSession(clientSession), callback));
    }

    // ==================== Drop Operations ====================

    @Override
    public Publisher<Void> drop() {
        return dropInternal(null);
    }

    @Override
    public Publisher<Void> drop(ClientSession clientSession) {
        return dropInternal(clientSession);
    }

    private Publisher<Void> dropInternal(@Nullable ClientSession clientSession) {
        return Publishers.toMonoVoid(callback ->
            nativeClient.dropDatabase(databaseName, operationContext, getNativeSession(clientSession), callback));
    }

    // ==================== Collection List Operations ====================

    @Override
    public ListCollectionNamesPublisher listCollectionNames() {
        return new NativeListCollectionNamesPublisher(nativeClient, null, operationContext, databaseName, codecRegistry);
    }

    @Override
    public ListCollectionNamesPublisher listCollectionNames(ClientSession clientSession) {
        return new NativeListCollectionNamesPublisher(nativeClient, getNativeSession(clientSession), operationContext, databaseName, codecRegistry);
    }

    @Override
    public ListCollectionsPublisher<Document> listCollections() {
        return listCollections(Document.class);
    }

    @Override
    public <TResult> ListCollectionsPublisher<TResult> listCollections(Class<TResult> resultClass) {
        return new NativeListCollectionsPublisher<>(nativeClient, null, operationContext, databaseName, resultClass, codecRegistry);
    }

    @Override
    public ListCollectionsPublisher<Document> listCollections(ClientSession clientSession) {
        return listCollections(clientSession, Document.class);
    }

    @Override
    public <TResult> ListCollectionsPublisher<TResult> listCollections(ClientSession clientSession, Class<TResult> resultClass) {
        return new NativeListCollectionsPublisher<>(nativeClient, getNativeSession(clientSession), operationContext, databaseName, resultClass, codecRegistry);
    }

    // ==================== Create Collection/View ====================

    @Override
    public Publisher<Void> createCollection(String collectionName) {
        return createCollection(collectionName, new CreateCollectionOptions());
    }

    @Override
    public Publisher<Void> createCollection(String collectionName, CreateCollectionOptions options) {
        return createCollectionInternal(null, collectionName, options);
    }

    @Override
    public Publisher<Void> createCollection(ClientSession clientSession, String collectionName) {
        return createCollection(clientSession, collectionName, new CreateCollectionOptions());
    }

    @Override
    public Publisher<Void> createCollection(ClientSession clientSession, String collectionName, CreateCollectionOptions options) {
        return createCollectionInternal(clientSession, collectionName, options);
    }

    private Publisher<Void> createCollectionInternal(@Nullable ClientSession clientSession, String collectionName, CreateCollectionOptions options) {
        return Publishers.toMonoVoid(callback ->
            nativeClient.createCollection(databaseName, collectionName, options, operationContext, getNativeSession(clientSession), callback));
    }

    @Override
    public Publisher<Void> createView(String viewName, String viewOn, List<? extends Bson> pipeline) {
        return createView(viewName, viewOn, pipeline, new CreateViewOptions());
    }

    @Override
    public Publisher<Void> createView(String viewName, String viewOn, List<? extends Bson> pipeline, CreateViewOptions createViewOptions) {
        throw new UnsupportedOperationException("createView not yet implemented");
    }

    @Override
    public Publisher<Void> createView(ClientSession clientSession, String viewName, String viewOn, List<? extends Bson> pipeline) {
        return createView(clientSession, viewName, viewOn, pipeline, new CreateViewOptions());
    }

    @Override
    public Publisher<Void> createView(ClientSession clientSession, String viewName, String viewOn, List<? extends Bson> pipeline, CreateViewOptions createViewOptions) {
        throw new UnsupportedOperationException("createView not yet implemented");
    }

    // ==================== Aggregate Operations ====================

    @Override
    public AggregatePublisher<Document> aggregate(List<? extends Bson> pipeline) {
        return aggregate(pipeline, Document.class);
    }

    @Override
    public <TResult> AggregatePublisher<TResult> aggregate(List<? extends Bson> pipeline, Class<TResult> resultClass) {
        return new NativeAggregatePublisher<>(nativeClient, null, operationContext, databaseName, null, pipeline, resultClass, codecRegistry);
    }

    @Override
    public AggregatePublisher<Document> aggregate(ClientSession clientSession, List<? extends Bson> pipeline) {
        return aggregate(clientSession, pipeline, Document.class);
    }

    @Override
    public <TResult> AggregatePublisher<TResult> aggregate(ClientSession clientSession, List<? extends Bson> pipeline, Class<TResult> resultClass) {
        return new NativeAggregatePublisher<>(nativeClient, getNativeSession(clientSession), operationContext, databaseName, null, pipeline, resultClass, codecRegistry);
    }

    // ==================== Watch Operations ====================

    @Override
    public ChangeStreamPublisher<Document> watch() {
        return watch(java.util.Collections.emptyList(), Document.class);
    }

    @Override
    public <TResult> ChangeStreamPublisher<TResult> watch(Class<TResult> resultClass) {
        return watch(java.util.Collections.emptyList(), resultClass);
    }

    @Override
    public ChangeStreamPublisher<Document> watch(List<? extends Bson> pipeline) {
        return watch(pipeline, Document.class);
    }

    @Override
    public <TResult> ChangeStreamPublisher<TResult> watch(List<? extends Bson> pipeline, Class<TResult> resultClass) {
        return new NativeChangeStreamPublisher<>(nativeClient, null, operationContext, pipeline, resultClass, codecRegistry,
                NativeChangeStreamPublisher.WatchLevel.DATABASE, databaseName, null);
    }

    @Override
    public ChangeStreamPublisher<Document> watch(ClientSession clientSession) {
        return watch(clientSession, java.util.Collections.emptyList(), Document.class);
    }

    @Override
    public <TResult> ChangeStreamPublisher<TResult> watch(ClientSession clientSession, Class<TResult> resultClass) {
        return watch(clientSession, java.util.Collections.emptyList(), resultClass);
    }

    @Override
    public ChangeStreamPublisher<Document> watch(ClientSession clientSession, List<? extends Bson> pipeline) {
        return watch(clientSession, pipeline, Document.class);
    }

    @Override
    public <TResult> ChangeStreamPublisher<TResult> watch(ClientSession clientSession, List<? extends Bson> pipeline, Class<TResult> resultClass) {
        return new NativeChangeStreamPublisher<>(nativeClient, getNativeSession(clientSession), operationContext, pipeline, resultClass, codecRegistry,
                NativeChangeStreamPublisher.WatchLevel.DATABASE, databaseName, null);
    }

    // ==================== Helper Methods ====================

    @Nullable
    private NativeAsyncClientSession getNativeSession(@Nullable ClientSession clientSession) {
        if (clientSession == null) {
            return null;
        }
        if (!(clientSession instanceof NativeClientSession)) {
            throw new IllegalArgumentException("ClientSession must be a NativeClientSession");
        }
        return ((NativeClientSession) clientSession).getNativeSession();
    }
}
