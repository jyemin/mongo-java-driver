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

import com.mongodb.ClientSessionOptions;
import com.mongodb.MongoClientSettings;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.model.bulk.ClientBulkWriteOptions;
import com.mongodb.client.model.bulk.ClientBulkWriteResult;
import com.mongodb.client.model.bulk.ClientNamespacedWriteModel;
import com.mongodb.lang.Nullable;
import com.mongodb.reactivestreams.client.ChangeStreamPublisher;
import com.mongodb.reactivestreams.client.ClientSession;
import com.mongodb.reactivestreams.client.ListDatabasesPublisher;
import com.mongodb.reactivestreams.client.MongoCluster;
import com.mongodb.reactivestreams.client.MongoDatabase;
import com.mongodb.rust.crud.NativeAsyncClient;
import com.mongodb.rust.crud.NativeOperationContext;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.reactivestreams.Publisher;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * Native implementation of MongoCluster.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
final class NativeMongoCluster implements MongoCluster {

    private final NativeAsyncClient nativeClient;
    private final MongoClientSettings settings;
    private final CodecRegistry codecRegistry;
    private final ReadPreference readPreference;
    private final WriteConcern writeConcern;
    private final ReadConcern readConcern;

    NativeMongoCluster(NativeAsyncClient nativeClient, MongoClientSettings settings, CodecRegistry codecRegistry,
                       ReadPreference readPreference, WriteConcern writeConcern, ReadConcern readConcern) {
        this.nativeClient = notNull("nativeClient", nativeClient);
        this.settings = notNull("settings", settings);
        this.codecRegistry = notNull("codecRegistry", codecRegistry);
        this.readPreference = notNull("readPreference", readPreference);
        this.writeConcern = notNull("writeConcern", writeConcern);
        this.readConcern = notNull("readConcern", readConcern);
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
    public MongoCluster withCodecRegistry(CodecRegistry codecRegistry) {
        return new NativeMongoCluster(nativeClient, settings, codecRegistry, readPreference, writeConcern, readConcern);
    }

    @Override
    public MongoCluster withReadPreference(ReadPreference readPreference) {
        return new NativeMongoCluster(nativeClient, settings, codecRegistry, readPreference, writeConcern, readConcern);
    }

    @Override
    public MongoCluster withWriteConcern(WriteConcern writeConcern) {
        return new NativeMongoCluster(nativeClient, settings, codecRegistry, readPreference, writeConcern, readConcern);
    }

    @Override
    public MongoCluster withReadConcern(ReadConcern readConcern) {
        return new NativeMongoCluster(nativeClient, settings, codecRegistry, readPreference, writeConcern, readConcern);
    }

    @Override
    public MongoCluster withTimeout(long timeout, TimeUnit timeUnit) {
        return this;
    }

    @Override
    public MongoDatabase getDatabase(String databaseName) {
        return new NativeMongoDatabase(nativeClient, databaseName, codecRegistry, readPreference, writeConcern, readConcern);
    }

    @Override
    public Publisher<ClientSession> startSession() {
        return startSession(ClientSessionOptions.builder().build());
    }

    @Override
    public Publisher<ClientSession> startSession(ClientSessionOptions options) {
        throw new UnsupportedOperationException("startSession on MongoCluster not yet implemented");
    }

    @Override
    public Publisher<String> listDatabaseNames() {
        NativeOperationContext opCtx = NativeOperationContext.builder()
                .readPreference(readPreference).writeConcern(writeConcern).readConcern(readConcern).build();
        return new NativeListDatabaseNamesPublisher(nativeClient, null, opCtx, codecRegistry);
    }

    @Override
    public Publisher<String> listDatabaseNames(ClientSession clientSession) {
        NativeOperationContext opCtx = NativeOperationContext.builder()
                .readPreference(readPreference).writeConcern(writeConcern).readConcern(readConcern).build();
        return new NativeListDatabaseNamesPublisher(nativeClient, getNativeSession(clientSession), opCtx, codecRegistry);
    }

    @Override
    public ListDatabasesPublisher<Document> listDatabases() {
        return listDatabases(Document.class);
    }

    @Override
    public <TResult> ListDatabasesPublisher<TResult> listDatabases(Class<TResult> resultClass) {
        NativeOperationContext opCtx = NativeOperationContext.builder()
                .readPreference(readPreference).writeConcern(writeConcern).readConcern(readConcern).build();
        return new NativeListDatabasesPublisher<>(nativeClient, null, opCtx, resultClass, codecRegistry);
    }

    @Override
    public ListDatabasesPublisher<Document> listDatabases(ClientSession clientSession) {
        return listDatabases(clientSession, Document.class);
    }

    @Override
    public <TResult> ListDatabasesPublisher<TResult> listDatabases(ClientSession clientSession, Class<TResult> resultClass) {
        NativeOperationContext opCtx = NativeOperationContext.builder()
                .readPreference(readPreference).writeConcern(writeConcern).readConcern(readConcern).build();
        return new NativeListDatabasesPublisher<>(nativeClient, getNativeSession(clientSession), opCtx, resultClass, codecRegistry);
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
        NativeOperationContext opCtx = NativeOperationContext.builder()
                .readPreference(readPreference).writeConcern(writeConcern).readConcern(readConcern).build();
        return new NativeChangeStreamPublisher<>(nativeClient, null, opCtx, pipeline, resultClass, codecRegistry,
                NativeChangeStreamPublisher.WatchLevel.CLIENT, null, null);
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
        NativeOperationContext opCtx = NativeOperationContext.builder()
                .readPreference(readPreference).writeConcern(writeConcern).readConcern(readConcern).build();
        return new NativeChangeStreamPublisher<>(nativeClient, getNativeSession(clientSession), opCtx, pipeline, resultClass, codecRegistry,
                NativeChangeStreamPublisher.WatchLevel.CLIENT, null, null);
    }

    // ==================== Bulk Write Operations ====================

    @Override
    public Publisher<ClientBulkWriteResult> bulkWrite(List<? extends ClientNamespacedWriteModel> models) {
        throw new UnsupportedOperationException("Client bulk write not yet implemented");
    }

    @Override
    public Publisher<ClientBulkWriteResult> bulkWrite(List<? extends ClientNamespacedWriteModel> models, ClientBulkWriteOptions options) {
        throw new UnsupportedOperationException("Client bulk write not yet implemented");
    }

    @Override
    public Publisher<ClientBulkWriteResult> bulkWrite(ClientSession clientSession, List<? extends ClientNamespacedWriteModel> models) {
        throw new UnsupportedOperationException("Client bulk write not yet implemented");
    }

    @Override
    public Publisher<ClientBulkWriteResult> bulkWrite(ClientSession clientSession, List<? extends ClientNamespacedWriteModel> models, ClientBulkWriteOptions options) {
        throw new UnsupportedOperationException("Client bulk write not yet implemented");
    }

    // ==================== Helper Methods ====================

    @Nullable
    private com.mongodb.rust.crud.NativeAsyncClientSession getNativeSession(@Nullable ClientSession clientSession) {
        if (clientSession == null) {
            return null;
        }
        if (!(clientSession instanceof NativeClientSession)) {
            throw new IllegalArgumentException("ClientSession must be a NativeClientSession");
        }
        return ((NativeClientSession) clientSession).getNativeSession();
    }
}
