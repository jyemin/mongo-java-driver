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

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.ClientSession;
import com.mongodb.client.ListCollectionNamesIterable;
import com.mongodb.client.ListCollectionsIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.CreateViewOptions;
import com.mongodb.lang.Nullable;
import com.mongodb.rust.crud.NativeSyncClient;
import com.mongodb.rust.crud.NativeSyncClientSession;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWrapper;
import org.bson.Document;
import org.bson.codecs.Codec;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Native implementation of MongoDatabase using Rust FFI via NativeSyncClient.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class NativeMongoDatabase implements MongoDatabase {

    private final NativeSyncClient nativeClient;
    private final String name;
    private final CodecRegistry codecRegistry;
    private final ReadPreference readPreference;
    private final WriteConcern writeConcern;
    private final ReadConcern readConcern;

    public NativeMongoDatabase(NativeSyncClient nativeClient, String name, CodecRegistry codecRegistry,
                               ReadPreference readPreference, WriteConcern writeConcern, ReadConcern readConcern) {
        this.nativeClient = nativeClient;
        this.name = name;
        this.codecRegistry = codecRegistry;
        this.readPreference = readPreference;
        this.writeConcern = writeConcern;
        this.readConcern = readConcern;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public CodecRegistry getCodecRegistry() {
        return codecRegistry;
    }

    @Override
    public ReadPreference getReadPreference() {
        return readPreference;
    }

    @Override
    public WriteConcern getWriteConcern() {
        return writeConcern;
    }

    @Override
    public ReadConcern getReadConcern() {
        return readConcern;
    }

    @Override
    @Nullable
    public Long getTimeout(TimeUnit timeUnit) {
        return null;
    }

    @Override
    public MongoDatabase withCodecRegistry(CodecRegistry codecRegistry) {
        return new NativeMongoDatabase(nativeClient, name, codecRegistry, readPreference, writeConcern, readConcern);
    }

    @Override
    public MongoDatabase withReadPreference(ReadPreference readPreference) {
        return new NativeMongoDatabase(nativeClient, name, codecRegistry, readPreference, writeConcern, readConcern);
    }

    @Override
    public MongoDatabase withWriteConcern(WriteConcern writeConcern) {
        return new NativeMongoDatabase(nativeClient, name, codecRegistry, readPreference, writeConcern, readConcern);
    }

    @Override
    public MongoDatabase withReadConcern(ReadConcern readConcern) {
        return new NativeMongoDatabase(nativeClient, name, codecRegistry, readPreference, writeConcern, readConcern);
    }

    @Override
    public MongoDatabase withTimeout(long timeout, TimeUnit timeUnit) {
        // TODO: Implement timeout
        return this;
    }

    @Override
    public MongoCollection<Document> getCollection(String collectionName) {
        return getCollection(collectionName, Document.class);
    }

    @Override
    public <TDocument> MongoCollection<TDocument> getCollection(String collectionName, Class<TDocument> documentClass) {
        // TODO: Create NativeMongoCollection
        throw new UnsupportedOperationException("getCollection not yet implemented - NativeMongoCollection needed");
    }

    @Override
    public Document runCommand(Bson command) {
        return runCommand(command, Document.class);
    }

    @Override
    public Document runCommand(Bson command, ReadPreference readPreference) {
        return runCommand(command, readPreference, Document.class);
    }

    @Override
    public <TResult> TResult runCommand(Bson command, Class<TResult> resultClass) {
        return runCommandInternal(null, command, resultClass);
    }

    @Override
    public <TResult> TResult runCommand(Bson command, ReadPreference readPreference, Class<TResult> resultClass) {
        // Note: ReadPreference is ignored - native client doesn't support it yet
        return runCommandInternal(null, command, resultClass);
    }

    @Override
    public Document runCommand(ClientSession clientSession, Bson command) {
        return runCommand(clientSession, command, Document.class);
    }

    @Override
    public Document runCommand(ClientSession clientSession, Bson command, ReadPreference readPreference) {
        return runCommand(clientSession, command, readPreference, Document.class);
    }

    @Override
    public <TResult> TResult runCommand(ClientSession clientSession, Bson command, Class<TResult> resultClass) {
        return runCommandInternal(clientSession, command, resultClass);
    }

    @Override
    public <TResult> TResult runCommand(ClientSession clientSession, Bson command, ReadPreference readPreference, Class<TResult> resultClass) {
        // Note: ReadPreference is ignored - native client doesn't support it yet
        return runCommandInternal(clientSession, command, resultClass);
    }

    private <TResult> TResult runCommandInternal(@Nullable ClientSession clientSession, Bson command, Class<TResult> resultClass) {
        BsonDocument commandDoc = BsonDocumentWrapper.asBsonDocument(command, codecRegistry);
        Codec<TResult> codec = codecRegistry.get(resultClass);
        NativeSyncClientSession nativeSession = getNativeSession(clientSession);
        return nativeClient.runCommand(name, commandDoc, codec, nativeSession);
    }

    @Nullable
    private NativeSyncClientSession getNativeSession(@Nullable ClientSession clientSession) {
        if (clientSession == null) {
            return null;
        }
        if (!(clientSession instanceof NativeClientSession)) {
            throw new IllegalArgumentException("ClientSession must be a NativeClientSession");
        }
        return ((NativeClientSession) clientSession).getNativeSession();
    }

    @Override
    public void drop() {
        dropInternal(null);
    }

    @Override
    public void drop(ClientSession clientSession) {
        dropInternal(clientSession);
    }

    private void dropInternal(@Nullable ClientSession clientSession) {
        nativeClient.dropDatabase(name, getNativeSession(clientSession));
    }

    @Override
    public ListCollectionNamesIterable listCollectionNames() {
        return listCollectionNamesInternal(null);
    }

    @Override
    public ListCollectionNamesIterable listCollectionNames(ClientSession clientSession) {
        return listCollectionNamesInternal(clientSession);
    }

    private ListCollectionNamesIterable listCollectionNamesInternal(@Nullable ClientSession clientSession) {
        return new NativeListCollectionNamesIterable(nativeClient, getNativeSession(clientSession), name);
    }

    @Override
    public ListCollectionsIterable<Document> listCollections() {
        return listCollections(Document.class);
    }

    @Override
    public <TResult> ListCollectionsIterable<TResult> listCollections(Class<TResult> resultClass) {
        return listCollectionsInternal(null, resultClass);
    }

    @Override
    public ListCollectionsIterable<Document> listCollections(ClientSession clientSession) {
        return listCollections(clientSession, Document.class);
    }

    @Override
    public <TResult> ListCollectionsIterable<TResult> listCollections(ClientSession clientSession, Class<TResult> resultClass) {
        return listCollectionsInternal(clientSession, resultClass);
    }

    private <TResult> ListCollectionsIterable<TResult> listCollectionsInternal(@Nullable ClientSession clientSession,
                                                                                Class<TResult> resultClass) {
        return new NativeListCollectionsIterable<>(nativeClient, getNativeSession(clientSession), name, resultClass, codecRegistry);
    }

    @Override
    public void createCollection(String collectionName) {
        createCollection(collectionName, new CreateCollectionOptions());
    }

    @Override
    public void createCollection(String collectionName, CreateCollectionOptions options) {
        createCollectionInternal(null, collectionName, options);
    }

    @Override
    public void createCollection(ClientSession clientSession, String collectionName) {
        createCollection(clientSession, collectionName, new CreateCollectionOptions());
    }

    @Override
    public void createCollection(ClientSession clientSession, String collectionName, CreateCollectionOptions options) {
        createCollectionInternal(clientSession, collectionName, options);
    }

    private void createCollectionInternal(@Nullable ClientSession clientSession, String collectionName, CreateCollectionOptions options) {
        nativeClient.createCollection(name, collectionName, options, getNativeSession(clientSession));
    }

    @Override
    public void createView(String viewName, String viewOn, List<? extends Bson> pipeline) {
        throw new UnsupportedOperationException("createView not yet implemented");
    }

    @Override
    public void createView(String viewName, String viewOn, List<? extends Bson> pipeline, CreateViewOptions options) {
        throw new UnsupportedOperationException("createView not yet implemented");
    }

    @Override
    public void createView(ClientSession clientSession, String viewName, String viewOn, List<? extends Bson> pipeline) {
        throw new UnsupportedOperationException("createView not yet implemented");
    }

    @Override
    public void createView(ClientSession clientSession, String viewName, String viewOn, List<? extends Bson> pipeline, CreateViewOptions options) {
        throw new UnsupportedOperationException("createView not yet implemented");
    }

    @Override
    public ChangeStreamIterable<Document> watch() {
        return watch(Collections.emptyList(), Document.class);
    }

    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(Class<TResult> resultClass) {
        return watch(Collections.emptyList(), resultClass);
    }

    @Override
    public ChangeStreamIterable<Document> watch(List<? extends Bson> pipeline) {
        return watch(pipeline, Document.class);
    }

    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(List<? extends Bson> pipeline, Class<TResult> resultClass) {
        return new NativeChangeStreamIterable<>(nativeClient, null, pipeline, resultClass, codecRegistry,
                NativeChangeStreamIterable.WatchLevel.DATABASE, name, null);
    }

    @Override
    public ChangeStreamIterable<Document> watch(ClientSession clientSession) {
        return watch(clientSession, Collections.emptyList(), Document.class);
    }

    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(ClientSession clientSession, Class<TResult> resultClass) {
        return watch(clientSession, Collections.emptyList(), resultClass);
    }

    @Override
    public ChangeStreamIterable<Document> watch(ClientSession clientSession, List<? extends Bson> pipeline) {
        return watch(clientSession, pipeline, Document.class);
    }

    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(ClientSession clientSession, List<? extends Bson> pipeline, Class<TResult> resultClass) {
        return new NativeChangeStreamIterable<>(nativeClient, getNativeSession(clientSession), pipeline, resultClass, codecRegistry,
                NativeChangeStreamIterable.WatchLevel.DATABASE, name, null);
    }

    @Override
    public AggregateIterable<Document> aggregate(List<? extends Bson> pipeline) {
        return aggregate(pipeline, Document.class);
    }

    @Override
    public <TResult> AggregateIterable<TResult> aggregate(List<? extends Bson> pipeline, Class<TResult> resultClass) {
        return aggregateInternal(null, pipeline, resultClass);
    }

    @Override
    public AggregateIterable<Document> aggregate(ClientSession clientSession, List<? extends Bson> pipeline) {
        return aggregate(clientSession, pipeline, Document.class);
    }

    @Override
    public <TResult> AggregateIterable<TResult> aggregate(ClientSession clientSession, List<? extends Bson> pipeline, Class<TResult> resultClass) {
        return aggregateInternal(clientSession, pipeline, resultClass);
    }

    private <TResult> AggregateIterable<TResult> aggregateInternal(@Nullable ClientSession clientSession,
                                                                    List<? extends Bson> pipeline,
                                                                    Class<TResult> resultClass) {
        return new NativeAggregateIterable<>(nativeClient, getNativeSession(clientSession), name, pipeline, resultClass, codecRegistry);
    }
}

