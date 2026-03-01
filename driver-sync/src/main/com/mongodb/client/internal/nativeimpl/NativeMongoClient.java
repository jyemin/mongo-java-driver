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

import com.mongodb.ClientSessionOptions;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoDriverInformation;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.ClientSession;
import com.mongodb.client.ListDatabasesIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCluster;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.bulk.ClientBulkWriteOptions;
import com.mongodb.client.model.bulk.ClientBulkWriteResult;
import com.mongodb.client.model.bulk.ClientNamespacedWriteModel;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.lang.Nullable;
import com.mongodb.rust.crud.NativeSyncClient;
import com.mongodb.rust.crud.NativeSyncClients;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.mongodb.assertions.Assertions.notNull;
import static org.bson.codecs.configuration.CodecRegistries.withUuidRepresentation;

/**
 * Native implementation of MongoClient using Rust FFI via NativeSyncClient.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class NativeMongoClient implements MongoClient {

    private final NativeSyncClient nativeClient;
    private final AtomicBoolean closed;
    private final boolean ownsClient;

    // Settings that can be overridden via with*() methods
    private final CodecRegistry codecRegistry;
    private final ReadPreference readPreference;
    private final WriteConcern writeConcern;
    private final ReadConcern readConcern;
    @Nullable
    private final Long timeoutMs;

    /**
     * Creates a new NativeMongoClient that owns the underlying native client.
     */
    public NativeMongoClient(MongoClientSettings settings) {
        this(NativeSyncClients.create(notNull("settings", settings)),
                new AtomicBoolean(false),
                true,
                withUuidRepresentation(settings.getCodecRegistry(), settings.getUuidRepresentation()),
                settings.getReadPreference(),
                settings.getWriteConcern(),
                settings.getReadConcern(),
                settings.getTimeout(TimeUnit.MILLISECONDS));
    }

    /**
     * Private constructor for creating views with overridden settings.
     */
    private NativeMongoClient(NativeSyncClient nativeClient,
                              AtomicBoolean closed,
                              boolean ownsClient,
                              CodecRegistry codecRegistry,
                              ReadPreference readPreference,
                              WriteConcern writeConcern,
                              ReadConcern readConcern,
                              @Nullable Long timeoutMs) {
        this.nativeClient = nativeClient;
        this.closed = closed;
        this.ownsClient = ownsClient;
        this.codecRegistry = codecRegistry;
        this.readPreference = readPreference;
        this.writeConcern = writeConcern;
        this.readConcern = readConcern;
        this.timeoutMs = timeoutMs;
    }

    @Override
    public void close() {
        if (ownsClient && !closed.getAndSet(true)) {
            nativeClient.close();
        }
    }

    @Override
    public ClusterDescription getClusterDescription() {
        // TODO: Implement cluster description from native client
        throw new UnsupportedOperationException("getClusterDescription not yet implemented");
    }

    @Override
    public void appendMetadata(MongoDriverInformation mongoDriverInformation) {
        // TODO: Implement metadata append
        throw new UnsupportedOperationException("appendMetadata not yet implemented");
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
        if (timeoutMs == null) {
            return null;
        }
        return timeUnit.convert(timeoutMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public MongoCluster withCodecRegistry(CodecRegistry codecRegistry) {
        return new NativeMongoClient(nativeClient, closed, false,
                notNull("codecRegistry", codecRegistry), readPreference, writeConcern, readConcern, timeoutMs);
    }

    @Override
    public MongoCluster withReadPreference(ReadPreference readPreference) {
        return new NativeMongoClient(nativeClient, closed, false,
                codecRegistry, notNull("readPreference", readPreference), writeConcern, readConcern, timeoutMs);
    }

    @Override
    public MongoCluster withWriteConcern(WriteConcern writeConcern) {
        return new NativeMongoClient(nativeClient, closed, false,
                codecRegistry, readPreference, notNull("writeConcern", writeConcern), readConcern, timeoutMs);
    }

    @Override
    public MongoCluster withReadConcern(ReadConcern readConcern) {
        return new NativeMongoClient(nativeClient, closed, false,
                codecRegistry, readPreference, writeConcern, notNull("readConcern", readConcern), timeoutMs);
    }

    @Override
    public MongoCluster withTimeout(long timeout, TimeUnit timeUnit) {
        return new NativeMongoClient(nativeClient, closed, false,
                codecRegistry, readPreference, writeConcern, readConcern,
                TimeUnit.MILLISECONDS.convert(timeout, timeUnit));
    }

    @Override
    public MongoDatabase getDatabase(String databaseName) {
        return new NativeMongoDatabase(nativeClient, databaseName, codecRegistry,
                readPreference, writeConcern, readConcern);
    }

    @Override
    public ClientSession startSession() {
        return startSession(ClientSessionOptions.builder().build());
    }

    @Override
    public ClientSession startSession(ClientSessionOptions options) {
        return new NativeClientSession(nativeClient.startSession(options), this);
    }

    @Override
    public MongoIterable<String> listDatabaseNames() {
        return listDatabases().map(doc -> doc.getString("name"));
    }

    @Override
    public MongoIterable<String> listDatabaseNames(ClientSession clientSession) {
        return listDatabases(clientSession).map(doc -> doc.getString("name"));
    }

    @Override
    public ListDatabasesIterable<Document> listDatabases() {
        return listDatabases(Document.class);
    }

    @Override
    public ListDatabasesIterable<Document> listDatabases(ClientSession clientSession) {
        return listDatabases(clientSession, Document.class);
    }

    @Override
    public <TResult> ListDatabasesIterable<TResult> listDatabases(Class<TResult> resultClass) {
        return new NativeListDatabasesIterable<>(nativeClient, null, resultClass, codecRegistry);
    }

    @Override
    public <TResult> ListDatabasesIterable<TResult> listDatabases(ClientSession clientSession, Class<TResult> resultClass) {
        NativeClientSession nativeSession = getNativeSession(clientSession);
        return new NativeListDatabasesIterable<>(nativeClient, nativeSession.getNativeSession(), resultClass, codecRegistry);
    }

    @Override
    public ChangeStreamIterable<Document> watch() {
        return watch(Collections.emptyList());
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
                NativeChangeStreamIterable.WatchLevel.CLIENT, null, null);
    }

    @Override
    public ChangeStreamIterable<Document> watch(ClientSession clientSession) {
        return watch(clientSession, Collections.emptyList());
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
        NativeClientSession nativeSession = getNativeSession(clientSession);
        return new NativeChangeStreamIterable<>(nativeClient, nativeSession.getNativeSession(), pipeline, resultClass, codecRegistry,
                NativeChangeStreamIterable.WatchLevel.CLIENT, null, null);
    }

    @Override
    public ClientBulkWriteResult bulkWrite(List<? extends ClientNamespacedWriteModel> models) {
        return bulkWrite(models, ClientBulkWriteOptions.clientBulkWriteOptions());
    }

    @Override
    public ClientBulkWriteResult bulkWrite(List<? extends ClientNamespacedWriteModel> models, ClientBulkWriteOptions options) {
        // Client-level bulk write (MongoDB 8.0+) is not yet supported by the native client
        throw new UnsupportedOperationException("Client-level bulkWrite is not yet supported by NativeMongoClient");
    }

    @Override
    public ClientBulkWriteResult bulkWrite(ClientSession clientSession, List<? extends ClientNamespacedWriteModel> models) {
        return bulkWrite(clientSession, models, ClientBulkWriteOptions.clientBulkWriteOptions());
    }

    @Override
    public ClientBulkWriteResult bulkWrite(ClientSession clientSession, List<? extends ClientNamespacedWriteModel> models, ClientBulkWriteOptions options) {
        // Client-level bulk write (MongoDB 8.0+) is not yet supported by the native client
        throw new UnsupportedOperationException("Client-level bulkWrite is not yet supported by NativeMongoClient");
    }

    // Package-private accessor for NativeMongoDatabase
    NativeSyncClient getNativeClient() {
        return nativeClient;
    }

    /**
     * Extracts the NativeClientSession from a ClientSession.
     * Throws if the session is not a NativeClientSession.
     */
    private NativeClientSession getNativeSession(ClientSession clientSession) {
        if (clientSession instanceof NativeClientSession) {
            return (NativeClientSession) clientSession;
        }
        throw new IllegalArgumentException("ClientSession must be created by this NativeMongoClient");
    }
}

