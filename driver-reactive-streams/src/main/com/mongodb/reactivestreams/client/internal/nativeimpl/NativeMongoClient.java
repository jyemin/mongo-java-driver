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
import com.mongodb.MongoDriverInformation;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.model.bulk.ClientBulkWriteOptions;
import com.mongodb.client.model.bulk.ClientBulkWriteResult;
import com.mongodb.client.model.bulk.ClientNamespacedWriteModel;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.lang.Nullable;
import com.mongodb.reactivestreams.client.ChangeStreamPublisher;
import com.mongodb.reactivestreams.client.ClientSession;
import com.mongodb.reactivestreams.client.ListDatabasesPublisher;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoDatabase;
import com.mongodb.rust.crud.NativeAsyncClient;
import com.mongodb.rust.crud.NativeAsyncClients;
import com.mongodb.rust.crud.NativeOperationContext;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * Native implementation of MongoClient using Rust FFI.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class NativeMongoClient implements MongoClient {

    private final MongoClientSettings settings;
    private final NativeAsyncClient nativeClient;
    private final CodecRegistry codecRegistry;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public NativeMongoClient(MongoClientSettings settings) {
        this.settings = notNull("settings", settings);
        this.nativeClient = NativeAsyncClients.create(settings);
        this.codecRegistry = settings.getCodecRegistry();
    }

    NativeAsyncClient getNativeClient() {
        return nativeClient;
    }

    // ==================== MongoCluster Implementation ====================

    @Override
    public CodecRegistry getCodecRegistry() {
        return codecRegistry;
    }

    @Override
    public ReadPreference getReadPreference() {
        return settings.getReadPreference();
    }

    @Override
    public WriteConcern getWriteConcern() {
        return settings.getWriteConcern();
    }

    @Override
    public ReadConcern getReadConcern() {
        return settings.getReadConcern();
    }

    @Override
    @Nullable
    public Long getTimeout(TimeUnit timeUnit) {
        // TODO: Implement timeout support
        return null;
    }

    @Override
    public com.mongodb.reactivestreams.client.MongoCluster withCodecRegistry(CodecRegistry codecRegistry) {
        return new NativeMongoCluster(nativeClient, settings, codecRegistry, getReadPreference(), getWriteConcern(), getReadConcern());
    }

    @Override
    public com.mongodb.reactivestreams.client.MongoCluster withReadPreference(ReadPreference readPreference) {
        return new NativeMongoCluster(nativeClient, settings, codecRegistry, readPreference, getWriteConcern(), getReadConcern());
    }

    @Override
    public com.mongodb.reactivestreams.client.MongoCluster withWriteConcern(WriteConcern writeConcern) {
        return new NativeMongoCluster(nativeClient, settings, codecRegistry, getReadPreference(), writeConcern, getReadConcern());
    }

    @Override
    public com.mongodb.reactivestreams.client.MongoCluster withReadConcern(ReadConcern readConcern) {
        return new NativeMongoCluster(nativeClient, settings, codecRegistry, getReadPreference(), getWriteConcern(), readConcern);
    }

    @Override
    public com.mongodb.reactivestreams.client.MongoCluster withTimeout(long timeout, TimeUnit timeUnit) {
        // TODO: Implement timeout support
        return this;
    }

    @Override
    public MongoDatabase getDatabase(String databaseName) {
        return new NativeMongoDatabase(nativeClient, databaseName, codecRegistry, getReadPreference(), getWriteConcern(), getReadConcern());
    }

    // ==================== Session Operations ====================

    @Override
    public Publisher<ClientSession> startSession() {
        return startSession(ClientSessionOptions.builder().build());
    }

    @Override
    public Publisher<ClientSession> startSession(ClientSessionOptions options) {
        return Publishers.toMono(callback ->
            nativeClient.startSession(options, (session, error) -> {
                if (error != null) {
                    callback.onResult(null, error);
                } else {
                    callback.onResult(new NativeClientSession(session, this, options), null);
                }
            })
        );
    }

    // ==================== Database Operations ====================

    @Override
    public Publisher<String> listDatabaseNames() {
        return listDatabaseNames(null);
    }

    @Override
    public Publisher<String> listDatabaseNames(ClientSession clientSession) {
        NativeOperationContext opCtx = NativeOperationContext.builder()
                .readPreference(getReadPreference()).writeConcern(getWriteConcern()).readConcern(getReadConcern()).build();
        return new NativeListDatabaseNamesPublisher(nativeClient, getNativeSession(clientSession), opCtx, codecRegistry);
    }

    @Override
    public ListDatabasesPublisher<Document> listDatabases() {
        return listDatabases(Document.class);
    }

    @Override
    public <TResult> ListDatabasesPublisher<TResult> listDatabases(Class<TResult> resultClass) {
        return listDatabasesInternal(null, resultClass);
    }

    @Override
    public ListDatabasesPublisher<Document> listDatabases(ClientSession clientSession) {
        return listDatabases(clientSession, Document.class);
    }

    @Override
    public <TResult> ListDatabasesPublisher<TResult> listDatabases(ClientSession clientSession, Class<TResult> resultClass) {
        return listDatabasesInternal(clientSession, resultClass);
    }

    private <TResult> ListDatabasesPublisher<TResult> listDatabasesInternal(@Nullable ClientSession clientSession, Class<TResult> resultClass) {
        NativeOperationContext opCtx = NativeOperationContext.builder()
                .readPreference(getReadPreference()).writeConcern(getWriteConcern()).readConcern(getReadConcern()).build();
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
        return watchInternal(null, pipeline, resultClass);
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
        return watchInternal(clientSession, pipeline, resultClass);
    }

    private <TResult> ChangeStreamPublisher<TResult> watchInternal(@Nullable ClientSession clientSession,
                                                                    List<? extends Bson> pipeline,
                                                                    Class<TResult> resultClass) {
        NativeOperationContext opCtx = NativeOperationContext.builder()
                .readPreference(getReadPreference()).writeConcern(getWriteConcern()).readConcern(getReadConcern()).build();
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

    // ==================== MongoClient Implementation ====================

    @Override
    public ClusterDescription getClusterDescription() {
        // TODO: Implement cluster description
        throw new UnsupportedOperationException("getClusterDescription not yet implemented");
    }

    @Override
    public void appendMetadata(MongoDriverInformation mongoDriverInformation) {
        // TODO: Implement metadata appending
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            nativeClient.close();
        }
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
