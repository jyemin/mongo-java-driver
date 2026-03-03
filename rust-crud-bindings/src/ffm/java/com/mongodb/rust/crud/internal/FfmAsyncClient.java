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

package com.mongodb.rust.crud.internal;

import com.mongodb.ClientSessionOptions;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoNamespace;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.AggregateOptions;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.ChangeStreamOptions;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.CreateIndexOptions;
import com.mongodb.client.model.DeleteOptions;
import com.mongodb.client.model.DistinctOptions;
import com.mongodb.client.model.DropCollectionOptions;
import com.mongodb.client.model.DropIndexOptions;
import com.mongodb.client.model.EstimatedDocumentCountOptions;
import com.mongodb.client.model.FindOneAndDeleteOptions;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.FindOptions;
import com.mongodb.client.model.IndexModel;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.InsertOneOptions;
import com.mongodb.client.model.RenameCollectionOptions;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.model.ListCollectionsOptions;
import com.mongodb.client.model.ListDatabasesOptions;
import com.mongodb.client.model.ListIndexesOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.internal.rust.crud.ffi.ClientCallback;
import com.mongodb.internal.rust.crud.ffi.ClientOptions;
import com.mongodb.internal.rust.crud.ffi.ClientResult;
import com.mongodb.internal.rust.crud.ffi.MongoDbFfi;
import com.mongodb.internal.rust.crud.ffi.VoidCallback;
import com.mongodb.lang.Nullable;
import com.mongodb.rust.crud.NativeAsyncChangeStream;
import com.mongodb.rust.crud.NativeAsyncCursor;
import com.mongodb.rust.crud.NativeAsyncClient;
import com.mongodb.rust.crud.NativeAsyncClientSession;
import com.mongodb.rust.crud.SingleResultCallback;
import org.bson.BsonDocument;
import org.bson.codecs.Decoder;
import org.bson.codecs.DecoderContext;
import org.bson.conversions.Bson;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * FFM implementation of NativeAsyncClient.
 */
public final class FfmAsyncClient implements NativeAsyncClient {

    private final MemorySegment clientPtr;
    private final Arena clientArena;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public FfmAsyncClient(MongoClientSettings settings) {
        this(buildConnectionString(settings));
    }

    public FfmAsyncClient(String connectionString) {
        this.clientArena = Arena.ofShared();
        this.clientPtr = createClientSync(connectionString);
    }

    private static String buildConnectionString(MongoClientSettings settings) {
        StringBuilder sb = new StringBuilder("mongodb://");

        // Credentials (username:password@)
        var credential = settings.getCredential();
        if (credential != null && credential.getUserName() != null) {
            sb.append(urlEncode(credential.getUserName()));
            if (credential.getPassword() != null) {
                sb.append(":").append(urlEncode(new String(credential.getPassword())));
            }
            sb.append("@");
        }

        // Hosts
        var hosts = settings.getClusterSettings().getHosts();
        for (int i = 0; i < hosts.size(); i++) {
            if (i > 0) sb.append(",");
            sb.append(hosts.get(i).getHost()).append(":").append(hosts.get(i).getPort());
        }

        // Auth database
        String authDb = null;
        if (credential != null && credential.getSource() != null) {
            authDb = credential.getSource();
        }
        if (authDb != null) {
            sb.append("/").append(urlEncode(authDb));
        }

        // Options
        List<String> options = new ArrayList<>();

        // Auth mechanism
        if (credential != null && credential.getAuthenticationMechanism() != null) {
            options.add("authMechanism=" + credential.getAuthenticationMechanism().getMechanismName());
        }

        // TLS/SSL
        var sslSettings = settings.getSslSettings();
        if (sslSettings.isEnabled()) {
            options.add("tls=true");
            if (sslSettings.isInvalidHostNameAllowed()) {
                options.add("tlsAllowInvalidHostnames=true");
            }
        }

        // Cluster settings
        var clusterSettings = settings.getClusterSettings();
        if (clusterSettings.getRequiredReplicaSetName() != null) {
            options.add("replicaSet=" + urlEncode(clusterSettings.getRequiredReplicaSetName()));
        }
        if (clusterSettings.getMode() == com.mongodb.connection.ClusterConnectionMode.SINGLE) {
            options.add("directConnection=true");
        }

        // Connection pool settings
        var poolSettings = settings.getConnectionPoolSettings();
        if (poolSettings.getMaxSize() != 100) { // default is 100
            options.add("maxPoolSize=" + poolSettings.getMaxSize());
        }
        if (poolSettings.getMinSize() != 0) { // default is 0
            options.add("minPoolSize=" + poolSettings.getMinSize());
        }
        long maxIdleTimeMs = poolSettings.getMaxConnectionIdleTime(java.util.concurrent.TimeUnit.MILLISECONDS);
        if (maxIdleTimeMs > 0) {
            options.add("maxIdleTimeMS=" + maxIdleTimeMs);
        }

        // Socket settings
        var socketSettings = settings.getSocketSettings();
        long connectTimeoutMs = socketSettings.getConnectTimeout(java.util.concurrent.TimeUnit.MILLISECONDS);
        if (connectTimeoutMs != 10000) { // default is 10s
            options.add("connectTimeoutMS=" + connectTimeoutMs);
        }
        // Note: socketTimeoutMS is not supported by the Rust driver

        // Server selection timeout
        long serverSelectionTimeoutMs = clusterSettings.getServerSelectionTimeout(java.util.concurrent.TimeUnit.MILLISECONDS);
        if (serverSelectionTimeoutMs != 30000) { // default is 30s
            options.add("serverSelectionTimeoutMS=" + serverSelectionTimeoutMs);
        }

        // Read preference
        var readPreference = settings.getReadPreference();
        if (readPreference != null && !readPreference.getName().equals("primary")) {
            options.add("readPreference=" + readPreference.getName());
        }

        // Write concern
        var writeConcern = settings.getWriteConcern();
        if (writeConcern != null) {
            if (writeConcern.getWObject() instanceof Number) {
                options.add("w=" + ((Number) writeConcern.getWObject()).intValue());
            } else if (writeConcern.getWObject() instanceof String) {
                options.add("w=" + urlEncode((String) writeConcern.getWObject()));
            }
            if (writeConcern.getWTimeout(java.util.concurrent.TimeUnit.MILLISECONDS) != null) {
                options.add("wTimeoutMS=" + writeConcern.getWTimeout(java.util.concurrent.TimeUnit.MILLISECONDS));
            }
            if (writeConcern.getJournal() != null) {
                options.add("journal=" + writeConcern.getJournal());
            }
        }

        // Retry settings
        if (!settings.getRetryWrites()) {
            options.add("retryWrites=false");
        }
        if (!settings.getRetryReads()) {
            options.add("retryReads=false");
        }

        // Application name
        if (settings.getApplicationName() != null) {
            options.add("appName=" + urlEncode(settings.getApplicationName()));
        }

        // Append options
        if (!options.isEmpty()) {
            if (authDb == null) {
                sb.append("/");
            }
            sb.append("?");
            sb.append(String.join("&", options));
        }

        return sb.toString();
    }

    private static String urlEncode(String value) {
        try {
            return java.net.URLEncoder.encode(value, java.nio.charset.StandardCharsets.UTF_8);
        } catch (Exception e) {
            return value;
        }
    }

    private MemorySegment createClientSync(String connectionString) {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<MemorySegment> resultRef = new AtomicReference<>();
        AtomicReference<Throwable> errorRef = new AtomicReference<>();

        MemorySegment options = ClientOptions.allocate(clientArena);
        MemorySegment connStr = clientArena.allocateFrom(connectionString);
        ClientOptions.connection_string(options, connStr);

        MemorySegment callback = ClientCallback.allocate((userdata, result, error) -> {
            try {
                if (!error.equals(MemorySegment.NULL)) {
                    errorRef.set(ErrorConverter.toException(error));
                } else {
                    MemorySegment clientPtr = ClientResult.client(result);
                    resultRef.set(clientPtr);
                }
            } finally {
                latch.countDown();
            }
        }, clientArena);

        MongoDbFfi.mongo_client_new(options, MemorySegment.NULL, callback);

        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while creating client", e);
        }

        if (errorRef.get() != null) {
            Throwable t = errorRef.get();
            if (t instanceof RuntimeException) {
                throw (RuntimeException) t;
            }
            throw new RuntimeException("Failed to create client", t);
        }

        return resultRef.get();
    }

    MemorySegment getClientPtr() {
        return clientPtr;
    }

    // ==================== Session ====================

    @Override
    public void startSession(ClientSessionOptions options, SingleResultCallback<NativeAsyncClientSession> callback) {
        Arena arena = Arena.ofShared();
        try {
            MemorySegment opts = OptionsMarshaller.toSessionOptions(arena, options);
            MemorySegment cb = CallbackBridge.createSessionCallback(arena, (sessionPtr, error) -> {
                arena.close();
                if (error != null) {
                    callback.completeExceptionally(error);
                } else {
                    callback.complete(new FfmAsyncClientSession(clientPtr, sessionPtr, options));
                }
            });
            MongoDbFfi.mongo_session_start(clientPtr, opts, MemorySegment.NULL, cb);
        } catch (Throwable t) {
            arena.close();
            callback.completeExceptionally(t);
        }
    }

    // ==================== CRUD Operations ====================

    @Override
    public void insertOne(MongoNamespace namespace,
                          BsonDocument document,
                          InsertOneOptions options,
                          @Nullable NativeAsyncClientSession session,
                          SingleResultCallback<InsertOneResult> callback) {
        
        // Use auto arena that stays alive until callback completes
        Arena arena = Arena.ofShared();
        try {
            MemorySegment ctx = createOperationContext(arena, session);
            MemorySegment dbName = arena.allocateFrom(namespace.getDatabaseName());
            MemorySegment collName = arena.allocateFrom(namespace.getCollectionName());
            MemorySegment docBson = BsonMarshaller.toBsonStruct(arena, document);
            MemorySegment opts = OptionsMarshaller.toInsertOneOptions(arena, options);
            MemorySegment cb = CallbackBridge.createInsertOneCallback(arena, wrapCallback(callback, arena));

            MongoDbFfi.mongo_insert_one(
                clientPtr, ctx, dbName, collName, docBson, opts, MemorySegment.NULL, cb
            );
        } catch (Throwable t) {
            arena.close();
            callback.completeExceptionally(t);
        }
    }

    @Override
    public void insertMany(MongoNamespace namespace,
                           List<BsonDocument> documents,
                           InsertManyOptions options,
                           @Nullable NativeAsyncClientSession session,
                           SingleResultCallback<InsertManyResult> callback) {
        
        Arena arena = Arena.ofShared();
        try {
            MemorySegment ctx = createOperationContext(arena, session);
            MemorySegment dbName = arena.allocateFrom(namespace.getDatabaseName());
            MemorySegment collName = arena.allocateFrom(namespace.getCollectionName());
            MemorySegment batch = BsonMarshaller.toBsonBatch(arena, documents);
            MemorySegment opts = OptionsMarshaller.toInsertManyOptions(arena, options);
            MemorySegment cb = CallbackBridge.createInsertManyCallback(arena, wrapCallback(callback, arena));

            MongoDbFfi.mongo_insert_many(
                clientPtr, ctx, dbName, collName, batch, opts, MemorySegment.NULL, cb
            );
        } catch (Throwable t) {
            arena.close();
            callback.completeExceptionally(t);
        }
    }

    @Override
    public void updateOne(MongoNamespace namespace,
                          Bson filter,
                          Bson update,
                          UpdateOptions options,
                          @Nullable NativeAsyncClientSession session,
                          SingleResultCallback<UpdateResult> callback) {
        
        Arena arena = Arena.ofShared();
        try {
            MemorySegment ctx = createOperationContext(arena, session);
            MemorySegment dbName = arena.allocateFrom(namespace.getDatabaseName());
            MemorySegment collName = arena.allocateFrom(namespace.getCollectionName());
            MemorySegment filterBson = BsonMarshaller.toBsonStruct(arena, filter);
            MemorySegment updateBson = BsonMarshaller.toBsonStruct(arena, update);
            MemorySegment opts = OptionsMarshaller.toUpdateOptions(arena, options);
            MemorySegment cb = CallbackBridge.createUpdateCallback(arena, wrapCallback(callback, arena));

            MongoDbFfi.mongo_update_one(
                clientPtr, ctx, dbName, collName, filterBson, updateBson, opts, MemorySegment.NULL, cb
            );
        } catch (Throwable t) {
            arena.close();
            callback.completeExceptionally(t);
        }
    }

    @Override
    public void updateMany(MongoNamespace namespace,
                           Bson filter,
                           Bson update,
                           UpdateOptions options,
                           @Nullable NativeAsyncClientSession session,
                           SingleResultCallback<UpdateResult> callback) {
        
        Arena arena = Arena.ofShared();
        try {
            MemorySegment ctx = createOperationContext(arena, session);
            MemorySegment dbName = arena.allocateFrom(namespace.getDatabaseName());
            MemorySegment collName = arena.allocateFrom(namespace.getCollectionName());
            MemorySegment filterBson = BsonMarshaller.toBsonStruct(arena, filter);
            MemorySegment updateBson = BsonMarshaller.toBsonStruct(arena, update);
            MemorySegment opts = OptionsMarshaller.toUpdateOptions(arena, options);
            MemorySegment cb = CallbackBridge.createUpdateCallback(arena, wrapCallback(callback, arena));

            MongoDbFfi.mongo_update_many(
                clientPtr, ctx, dbName, collName, filterBson, updateBson, opts, MemorySegment.NULL, cb
            );
        } catch (Throwable t) {
            arena.close();
            callback.completeExceptionally(t);
        }
    }

    @Override
    public void replaceOne(MongoNamespace namespace,
                           Bson filter,
                           BsonDocument replacement,
                           ReplaceOptions options,
                           @Nullable NativeAsyncClientSession session,
                           SingleResultCallback<UpdateResult> callback) {

        Arena arena = Arena.ofShared();
        try {
            MemorySegment ctx = createOperationContext(arena, session);
            MemorySegment dbName = arena.allocateFrom(namespace.getDatabaseName());
            MemorySegment collName = arena.allocateFrom(namespace.getCollectionName());
            MemorySegment filterBson = BsonMarshaller.toBsonStruct(arena, filter);
            MemorySegment replacementBson = BsonMarshaller.toBsonStruct(arena, replacement);
            MemorySegment opts = OptionsMarshaller.toReplaceOptions(arena, options);
            MemorySegment cb = CallbackBridge.createUpdateCallback(arena, wrapCallback(callback, arena));

            MongoDbFfi.mongo_replace_one(
                clientPtr, ctx, dbName, collName, filterBson, replacementBson, opts, MemorySegment.NULL, cb
            );
        } catch (Throwable t) {
            arena.close();
            callback.completeExceptionally(t);
        }
    }

    @Override
    public void deleteOne(MongoNamespace namespace,
                          Bson filter,
                          DeleteOptions options,
                          @Nullable NativeAsyncClientSession session,
                          SingleResultCallback<DeleteResult> callback) {

        Arena arena = Arena.ofShared();
        try {
            MemorySegment ctx = createOperationContext(arena, session);
            MemorySegment dbName = arena.allocateFrom(namespace.getDatabaseName());
            MemorySegment collName = arena.allocateFrom(namespace.getCollectionName());
            MemorySegment filterBson = BsonMarshaller.toBsonStruct(arena, filter);
            MemorySegment opts = OptionsMarshaller.toDeleteOptions(arena, options);
            MemorySegment cb = CallbackBridge.createDeleteCallback(arena, wrapCallback(callback, arena));

            MongoDbFfi.mongo_delete_one(
                clientPtr, ctx, dbName, collName, filterBson, opts, MemorySegment.NULL, cb
            );
        } catch (Throwable t) {
            arena.close();
            callback.completeExceptionally(t);
        }
    }

    @Override
    public void deleteMany(MongoNamespace namespace,
                           Bson filter,
                           DeleteOptions options,
                           @Nullable NativeAsyncClientSession session,
                           SingleResultCallback<DeleteResult> callback) {

        Arena arena = Arena.ofShared();
        try {
            MemorySegment ctx = createOperationContext(arena, session);
            MemorySegment dbName = arena.allocateFrom(namespace.getDatabaseName());
            MemorySegment collName = arena.allocateFrom(namespace.getCollectionName());
            MemorySegment filterBson = BsonMarshaller.toBsonStruct(arena, filter);
            MemorySegment opts = OptionsMarshaller.toDeleteOptions(arena, options);
            MemorySegment cb = CallbackBridge.createDeleteCallback(arena, wrapCallback(callback, arena));

            MongoDbFfi.mongo_delete_many(
                clientPtr, ctx, dbName, collName, filterBson, opts, MemorySegment.NULL, cb
            );
        } catch (Throwable t) {
            arena.close();
            callback.completeExceptionally(t);
        }
    }

    @Override
    public <T> void findOne(MongoNamespace namespace,
                            Bson filter,
                            FindOptions options,
                            Decoder<T> decoder,
                            @Nullable NativeAsyncClientSession session,
                            SingleResultCallback<T> callback) {

        Arena arena = Arena.ofShared();
        try {
            MemorySegment ctx = createOperationContext(arena, session);
            MemorySegment dbName = arena.allocateFrom(namespace.getDatabaseName());
            MemorySegment collName = arena.allocateFrom(namespace.getCollectionName());
            MemorySegment filterBson = BsonMarshaller.toBsonStruct(arena, filter);
            MemorySegment opts = OptionsMarshaller.toFindOneOptions(arena, options);
            MemorySegment cb = CallbackBridge.createFindOneCallback(arena, wrapDecodingCallback(callback, decoder, arena));

            MongoDbFfi.mongo_find_one(
                clientPtr, ctx, dbName, collName, filterBson, opts, MemorySegment.NULL, cb
            );
        } catch (Throwable t) {
            arena.close();
            callback.completeExceptionally(t);
        }
    }

    @Override
    public <T> void find(MongoNamespace namespace,
                         Bson filter,
                         FindOptions options,
                         Decoder<T> decoder,
                         @Nullable NativeAsyncClientSession session,
                         SingleResultCallback<NativeAsyncCursor<T>> callback) {

        Arena arena = Arena.ofShared();
        try {
            MemorySegment ctx = createOperationContext(arena, session);
            MemorySegment dbName = arena.allocateFrom(namespace.getDatabaseName());
            MemorySegment collName = arena.allocateFrom(namespace.getCollectionName());
            MemorySegment filterBson = BsonMarshaller.toBsonStruct(arena, filter);
            MemorySegment opts = OptionsMarshaller.toFindOptions(arena, options);

            // Wrap to convert CursorHandle to AsyncCursor
            SingleResultCallback<CursorHandle> cursorCallback = (handle, error) -> {
                arena.close();
                if (error != null) {
                    callback.completeExceptionally(error);
                } else {
                    callback.complete(new FfmAsyncCursor<>(clientPtr, handle, decoder));
                }
            };
            MemorySegment cb = CallbackBridge.createCursorResultCallback(arena, cursorCallback);

            MongoDbFfi.mongo_find(
                clientPtr, ctx, dbName, collName, filterBson, opts, MemorySegment.NULL, cb
            );
        } catch (Throwable t) {
            arena.close();
            callback.completeExceptionally(t);
        }
    }

    @Override
    public <T> void findOneAndUpdate(MongoNamespace namespace,
                                      Bson filter,
                                      Bson update,
                                      FindOneAndUpdateOptions options,
                                      Decoder<T> decoder,
                                      @Nullable NativeAsyncClientSession session,
                                      SingleResultCallback<T> callback) {

        Arena arena = Arena.ofShared();
        try {
            MemorySegment ctx = createOperationContext(arena, session);
            MemorySegment dbName = arena.allocateFrom(namespace.getDatabaseName());
            MemorySegment collName = arena.allocateFrom(namespace.getCollectionName());
            MemorySegment filterBson = BsonMarshaller.toBsonStruct(arena, filter);
            MemorySegment updateBson = BsonMarshaller.toBsonStruct(arena, update);
            MemorySegment opts = OptionsMarshaller.toFindOneAndUpdateOptions(arena, options);
            MemorySegment cb = CallbackBridge.createFindOneCallback(arena, wrapDecodingCallback(callback, decoder, arena));

            MongoDbFfi.mongo_find_one_and_update(
                clientPtr, ctx, dbName, collName, filterBson, updateBson, opts, MemorySegment.NULL, cb
            );
        } catch (Throwable t) {
            arena.close();
            callback.completeExceptionally(t);
        }
    }

    @Override
    public <T> void findOneAndReplace(MongoNamespace namespace,
                                       Bson filter,
                                       BsonDocument replacement,
                                       FindOneAndReplaceOptions options,
                                       Decoder<T> decoder,
                                       @Nullable NativeAsyncClientSession session,
                                       SingleResultCallback<T> callback) {

        Arena arena = Arena.ofShared();
        try {
            MemorySegment ctx = createOperationContext(arena, session);
            MemorySegment dbName = arena.allocateFrom(namespace.getDatabaseName());
            MemorySegment collName = arena.allocateFrom(namespace.getCollectionName());
            MemorySegment filterBson = BsonMarshaller.toBsonStruct(arena, filter);
            MemorySegment replacementBson = BsonMarshaller.toBsonStruct(arena, replacement);
            MemorySegment opts = OptionsMarshaller.toFindOneAndReplaceOptions(arena, options);
            MemorySegment cb = CallbackBridge.createFindOneCallback(arena, wrapDecodingCallback(callback, decoder, arena));

            MongoDbFfi.mongo_find_one_and_replace(
                clientPtr, ctx, dbName, collName, filterBson, replacementBson, opts, MemorySegment.NULL, cb
            );
        } catch (Throwable t) {
            arena.close();
            callback.completeExceptionally(t);
        }
    }

    @Override
    public <T> void findOneAndDelete(MongoNamespace namespace,
                                      Bson filter,
                                      FindOneAndDeleteOptions options,
                                      Decoder<T> decoder,
                                      @Nullable NativeAsyncClientSession session,
                                      SingleResultCallback<T> callback) {

        Arena arena = Arena.ofShared();
        try {
            MemorySegment ctx = createOperationContext(arena, session);
            MemorySegment dbName = arena.allocateFrom(namespace.getDatabaseName());
            MemorySegment collName = arena.allocateFrom(namespace.getCollectionName());
            MemorySegment filterBson = BsonMarshaller.toBsonStruct(arena, filter);
            MemorySegment opts = OptionsMarshaller.toFindOneAndDeleteOptions(arena, options);
            MemorySegment cb = CallbackBridge.createFindOneCallback(arena, wrapDecodingCallback(callback, decoder, arena));

            MongoDbFfi.mongo_find_one_and_delete(
                clientPtr, ctx, dbName, collName, filterBson, opts, MemorySegment.NULL, cb
            );
        } catch (Throwable t) {
            arena.close();
            callback.completeExceptionally(t);
        }
    }

    // ==================== Aggregate Operations ====================

    @Override
    public <T> void aggregate(MongoNamespace namespace,
                              List<BsonDocument> pipeline,
                              AggregateOptions options,
                              @Nullable Boolean bypassDocumentValidation,
                              Decoder<T> decoder,
                              @Nullable NativeAsyncClientSession session,
                              SingleResultCallback<NativeAsyncCursor<T>> callback) {

        Arena arena = Arena.ofShared();
        try {
            MemorySegment ctx = createOperationContext(arena, session);
            MemorySegment dbName = arena.allocateFrom(namespace.getDatabaseName());
            MemorySegment collName = arena.allocateFrom(namespace.getCollectionName());
            MemorySegment pipelineBatch = BsonMarshaller.toBsonBatch(arena, pipeline);
            MemorySegment opts = OptionsMarshaller.toAggregateOptions(arena, options, bypassDocumentValidation);

            SingleResultCallback<CursorHandle> cursorCallback = (handle, error) -> {
                arena.close();
                if (error != null) {
                    callback.completeExceptionally(error);
                } else {
                    callback.complete(new FfmAsyncCursor<>(clientPtr, handle, decoder));
                }
            };
            MemorySegment cb = CallbackBridge.createCursorResultCallback(arena, cursorCallback);

            MongoDbFfi.mongo_aggregate(
                clientPtr, ctx, dbName, collName, pipelineBatch, opts, MemorySegment.NULL, cb
            );
        } catch (Throwable t) {
            arena.close();
            callback.completeExceptionally(t);
        }
    }

    @Override
    public <T> void aggregateDatabase(String databaseName,
                                       List<BsonDocument> pipeline,
                                       AggregateOptions options,
                                       @Nullable Boolean bypassDocumentValidation,
                                       Decoder<T> decoder,
                                       @Nullable NativeAsyncClientSession session,
                                       SingleResultCallback<NativeAsyncCursor<T>> callback) {

        Arena arena = Arena.ofShared();
        try {
            MemorySegment ctx = createOperationContext(arena, session);
            MemorySegment dbName = arena.allocateFrom(databaseName);
            MemorySegment pipelineBatch = BsonMarshaller.toBsonBatch(arena, pipeline);
            MemorySegment opts = OptionsMarshaller.toAggregateOptions(arena, options, bypassDocumentValidation);

            SingleResultCallback<CursorHandle> cursorCallback = (handle, error) -> {
                arena.close();
                if (error != null) {
                    callback.completeExceptionally(error);
                } else {
                    callback.complete(new FfmAsyncCursor<>(clientPtr, handle, decoder));
                }
            };
            MemorySegment cb = CallbackBridge.createCursorResultCallback(arena, cursorCallback);

            MongoDbFfi.mongo_aggregate_database(
                clientPtr, ctx, dbName, pipelineBatch, opts, MemorySegment.NULL, cb
            );
        } catch (Throwable t) {
            arena.close();
            callback.completeExceptionally(t);
        }
    }

    // ==================== Count Operations ====================

    @Override
    public void countDocuments(MongoNamespace namespace,
                                Bson filter,
                                CountOptions options,
                                @Nullable NativeAsyncClientSession session,
                                SingleResultCallback<Long> callback) {

        Arena arena = Arena.ofShared();
        try {
            MemorySegment ctx = createOperationContext(arena, session);
            MemorySegment dbName = arena.allocateFrom(namespace.getDatabaseName());
            MemorySegment collName = arena.allocateFrom(namespace.getCollectionName());
            MemorySegment filterBson = BsonMarshaller.toBsonStruct(arena, filter);
            MemorySegment opts = OptionsMarshaller.toCountOptions(arena, options);
            MemorySegment cb = CallbackBridge.createCountCallback(arena, wrapCallback(callback, arena));

            MongoDbFfi.mongo_count_documents(
                clientPtr, ctx, dbName, collName, filterBson, opts, MemorySegment.NULL, cb
            );
        } catch (Throwable t) {
            arena.close();
            callback.completeExceptionally(t);
        }
    }

    @Override
    public void estimatedDocumentCount(MongoNamespace namespace,
                                        EstimatedDocumentCountOptions options,
                                        SingleResultCallback<Long> callback) {

        Arena arena = Arena.ofShared();
        try {
            MemorySegment ctx = OptionsMarshaller.createOperationContext(arena, null);
            MemorySegment dbName = arena.allocateFrom(namespace.getDatabaseName());
            MemorySegment collName = arena.allocateFrom(namespace.getCollectionName());
            MemorySegment opts = OptionsMarshaller.toEstimatedDocumentCountOptions(arena, options);
            MemorySegment cb = CallbackBridge.createCountCallback(arena, wrapCallback(callback, arena));

            MongoDbFfi.mongo_estimated_document_count(
                clientPtr, ctx, dbName, collName, opts, MemorySegment.NULL, cb
            );
        } catch (Throwable t) {
            arena.close();
            callback.completeExceptionally(t);
        }
    }

    @Override
    public <T> void distinct(MongoNamespace namespace,
                              String fieldName,
                              Bson filter,
                              DistinctOptions options,
                              Decoder<T> decoder,
                              @Nullable NativeAsyncClientSession session,
                              SingleResultCallback<NativeAsyncCursor<T>> callback) {

        Arena arena = Arena.ofShared();
        try {
            MemorySegment ctx = createOperationContext(arena, session);
            MemorySegment dbName = arena.allocateFrom(namespace.getDatabaseName());
            MemorySegment collName = arena.allocateFrom(namespace.getCollectionName());
            MemorySegment field = arena.allocateFrom(fieldName);
            MemorySegment filterBson = BsonMarshaller.toBsonStruct(arena, filter);
            MemorySegment opts = OptionsMarshaller.toDistinctOptions(arena, options);

            SingleResultCallback<CursorHandle> cursorCallback = (handle, error) -> {
                arena.close();
                if (error != null) {
                    callback.completeExceptionally(error);
                } else {
                    callback.complete(new FfmAsyncCursor<>(clientPtr, handle, decoder));
                }
            };
            MemorySegment cb = CallbackBridge.createCursorResultCallback(arena, cursorCallback);

            MongoDbFfi.mongo_distinct(
                clientPtr, ctx, dbName, collName, field, filterBson, opts, MemorySegment.NULL, cb
            );
        } catch (Throwable t) {
            arena.close();
            callback.completeExceptionally(t);
        }
    }

    // ==================== Index Operations ====================

    @Override
    public void createIndex(MongoNamespace namespace,
                             Bson keys,
                             CreateIndexOptions options,
                             @Nullable NativeAsyncClientSession session,
                             SingleResultCallback<String> callback) {

        Arena arena = Arena.ofShared();
        try {
            MemorySegment ctx = createOperationContext(arena, session);
            MemorySegment dbName = arena.allocateFrom(namespace.getDatabaseName());
            MemorySegment collName = arena.allocateFrom(namespace.getCollectionName());
            MemorySegment keysBson = BsonMarshaller.toBsonStruct(arena, keys);
            MemorySegment opts = OptionsMarshaller.toCreateIndexOptions(arena, options);
            MemorySegment cb = CallbackBridge.createStringCallback(arena, wrapCallback(callback, arena));

            MongoDbFfi.mongo_create_index(
                clientPtr, ctx, dbName, collName, keysBson, opts, MemorySegment.NULL, cb
            );
        } catch (Throwable t) {
            arena.close();
            callback.completeExceptionally(t);
        }
    }

    @Override
    public void createIndexes(MongoNamespace namespace,
                               List<IndexModel> indexes,
                               CreateIndexOptions options,
                               @Nullable NativeAsyncClientSession session,
                               SingleResultCallback<List<String>> callback) {

        Arena arena = Arena.ofShared();
        try {
            MemorySegment ctx = createOperationContext(arena, session);
            MemorySegment dbName = arena.allocateFrom(namespace.getDatabaseName());
            MemorySegment collName = arena.allocateFrom(namespace.getCollectionName());
            OptionsMarshaller.IndexModelArray indexArray = OptionsMarshaller.toIndexModelArray(arena, indexes);
            MemorySegment opts = OptionsMarshaller.toCreateIndexOptions(arena, options);
            MemorySegment cb = CallbackBridge.createStringListCallback(arena, wrapCallback(callback, arena));

            MongoDbFfi.mongo_create_indexes(
                clientPtr, ctx, dbName, collName, indexArray.array, indexArray.length,
                opts, MemorySegment.NULL, cb
            );
        } catch (Throwable t) {
            arena.close();
            callback.completeExceptionally(t);
        }
    }

    @Override
    public void dropIndex(MongoNamespace namespace,
                           String indexName,
                           DropIndexOptions options,
                           @Nullable NativeAsyncClientSession session,
                           SingleResultCallback<Void> callback) {

        Arena arena = Arena.ofShared();
        try {
            MemorySegment ctx = createOperationContext(arena, session);
            MemorySegment dbName = arena.allocateFrom(namespace.getDatabaseName());
            MemorySegment collName = arena.allocateFrom(namespace.getCollectionName());
            MemorySegment name = arena.allocateFrom(indexName);
            MemorySegment opts = OptionsMarshaller.toDropIndexOptions(arena, options);
            MemorySegment cb = CallbackBridge.createVoidCallback(arena, wrapCallback(callback, arena));

            MongoDbFfi.mongo_drop_index(
                clientPtr, ctx, dbName, collName, name, opts, MemorySegment.NULL, cb
            );
        } catch (Throwable t) {
            arena.close();
            callback.completeExceptionally(t);
        }
    }

    @Override
    public void dropIndex(MongoNamespace namespace,
                           Bson keys,
                           DropIndexOptions options,
                           @Nullable NativeAsyncClientSession session,
                           SingleResultCallback<Void> callback) {
        // Convert keys to index name and delegate
        // For simplicity, serialize keys to JSON as the index name pattern
        String indexName = keys.toBsonDocument().toJson();
        dropIndex(namespace, indexName, options, session, callback);
    }

    @Override
    public <T> void listIndexes(MongoNamespace namespace,
                                 ListIndexesOptions options,
                                 Decoder<T> decoder,
                                 @Nullable NativeAsyncClientSession session,
                                 SingleResultCallback<NativeAsyncCursor<T>> callback) {

        Arena arena = Arena.ofShared();
        try {
            MemorySegment ctx = createOperationContext(arena, session);
            MemorySegment dbName = arena.allocateFrom(namespace.getDatabaseName());
            MemorySegment collName = arena.allocateFrom(namespace.getCollectionName());
            MemorySegment opts = OptionsMarshaller.toListIndexesOptions(arena, options);

            SingleResultCallback<CursorHandle> cursorCallback = (handle, error) -> {
                arena.close();
                if (error != null) {
                    callback.completeExceptionally(error);
                } else {
                    callback.complete(new FfmAsyncCursor<>(clientPtr, handle, decoder));
                }
            };
            MemorySegment cb = CallbackBridge.createCursorResultCallback(arena, cursorCallback);

            MongoDbFfi.mongo_list_indexes(
                clientPtr, ctx, dbName, collName, opts, MemorySegment.NULL, cb
            );
        } catch (Throwable t) {
            arena.close();
            callback.completeExceptionally(t);
        }
    }

    // ==================== Collection Admin Operations ====================

    @Override
    public void createCollection(String databaseName,
                                  String collectionName,
                                  CreateCollectionOptions options,
                                  @Nullable NativeAsyncClientSession session,
                                  SingleResultCallback<Void> callback) {

        Arena arena = Arena.ofShared();
        try {
            MemorySegment ctx = createOperationContext(arena, session);
            MemorySegment dbName = arena.allocateFrom(databaseName);
            MemorySegment collName = arena.allocateFrom(collectionName);
            MemorySegment opts = OptionsMarshaller.toCreateCollectionOptions(arena, options);
            MemorySegment cb = CallbackBridge.createVoidCallback(arena, wrapCallback(callback, arena));

            MongoDbFfi.mongo_create_collection(
                clientPtr, ctx, dbName, collName, opts, MemorySegment.NULL, cb
            );
        } catch (Throwable t) {
            arena.close();
            callback.completeExceptionally(t);
        }
    }

    @Override
    public void dropCollection(MongoNamespace namespace,
                                DropCollectionOptions options,
                                @Nullable NativeAsyncClientSession session,
                                SingleResultCallback<Void> callback) {

        Arena arena = Arena.ofShared();
        try {
            MemorySegment ctx = createOperationContext(arena, session);
            MemorySegment dbName = arena.allocateFrom(namespace.getDatabaseName());
            MemorySegment collName = arena.allocateFrom(namespace.getCollectionName());
            MemorySegment opts = OptionsMarshaller.toDropCollectionOptions(arena, options);
            MemorySegment cb = CallbackBridge.createVoidCallback(arena, wrapCallback(callback, arena));

            MongoDbFfi.mongo_drop_collection(
                clientPtr, ctx, dbName, collName, opts, MemorySegment.NULL, cb
            );
        } catch (Throwable t) {
            arena.close();
            callback.completeExceptionally(t);
        }
    }

    @Override
    public void renameCollection(MongoNamespace namespace,
                                  MongoNamespace newNamespace,
                                  RenameCollectionOptions options,
                                  @Nullable NativeAsyncClientSession session,
                                  SingleResultCallback<Void> callback) {

        // FFI only supports renaming within the same database
        if (!namespace.getDatabaseName().equals(newNamespace.getDatabaseName())) {
            callback.completeExceptionally(new IllegalArgumentException(
                "Cross-database rename not supported. Source: " + namespace.getDatabaseName()
                + ", Target: " + newNamespace.getDatabaseName()));
            return;
        }

        Arena arena = Arena.ofShared();
        try {
            MemorySegment ctx = createOperationContext(arena, session);
            MemorySegment dbName = arena.allocateFrom(namespace.getDatabaseName());
            MemorySegment collName = arena.allocateFrom(namespace.getCollectionName());
            MemorySegment newCollName = arena.allocateFrom(newNamespace.getCollectionName());
            boolean dropTarget = options != null && options.isDropTarget();
            MemorySegment cb = CallbackBridge.createVoidCallback(arena, wrapCallback(callback, arena));

            MongoDbFfi.mongo_rename_collection(
                clientPtr, ctx, dbName, collName, newCollName, dropTarget, MemorySegment.NULL, cb
            );
        } catch (Throwable t) {
            arena.close();
            callback.completeExceptionally(t);
        }
    }

    @Override
    public <T> void listCollections(String databaseName,
                                     ListCollectionsOptions options,
                                     Decoder<T> decoder,
                                     @Nullable NativeAsyncClientSession session,
                                     SingleResultCallback<NativeAsyncCursor<T>> callback) {

        Arena arena = Arena.ofShared();
        try {
            MemorySegment ctx = createOperationContext(arena, session);
            MemorySegment dbName = arena.allocateFrom(databaseName);
            Bson filter = options.getFilter();
            MemorySegment filterBson = filter != null ? BsonMarshaller.toBsonStruct(arena, filter) : MemorySegment.NULL;
            MemorySegment opts = OptionsMarshaller.toListCollectionsOptions(arena, options);

            SingleResultCallback<CursorHandle> cursorCallback = (handle, error) -> {
                arena.close();
                if (error != null) {
                    callback.completeExceptionally(error);
                } else {
                    callback.complete(new FfmAsyncCursor<>(clientPtr, handle, decoder));
                }
            };
            MemorySegment cb = CallbackBridge.createCursorResultCallback(arena, cursorCallback);

            MongoDbFfi.mongo_list_collections(
                clientPtr, ctx, dbName, filterBson, opts, MemorySegment.NULL, cb
            );
        } catch (Throwable t) {
            arena.close();
            callback.completeExceptionally(t);
        }
    }

    @Override
    public void listCollectionNames(String databaseName,
                                     ListCollectionsOptions options,
                                     @Nullable NativeAsyncClientSession session,
                                     SingleResultCallback<NativeAsyncCursor<String>> callback) {

        // Note: The FFI returns a list of names, not a cursor. We wrap it in a simple cursor.
        Arena arena = Arena.ofShared();
        try {
            MemorySegment ctx = createOperationContext(arena, session);
            MemorySegment dbName = arena.allocateFrom(databaseName);
            Bson filter = options.getFilter();
            MemorySegment filterBson = filter != null ? BsonMarshaller.toBsonStruct(arena, filter) : MemorySegment.NULL;
            MemorySegment opts = OptionsMarshaller.toListCollectionsOptions(arena, options);

            SingleResultCallback<List<String>> listCallback = (names, error) -> {
                arena.close();
                if (error != null) {
                    callback.completeExceptionally(error);
                } else {
                    callback.complete(new ListBackedCursor<>(names));
                }
            };
            MemorySegment cb = CallbackBridge.createListCollectionNamesCallback(arena, listCallback);

            MongoDbFfi.mongo_list_collection_names(
                clientPtr, ctx, dbName, filterBson, opts, MemorySegment.NULL, cb
            );
        } catch (Throwable t) {
            arena.close();
            callback.completeExceptionally(t);
        }
    }

    // ==================== Database Admin Operations ====================

    @Override
    public void dropDatabase(String databaseName,
                              @Nullable NativeAsyncClientSession session,
                              SingleResultCallback<Void> callback) {

        Arena arena = Arena.ofShared();
        try {
            MemorySegment ctx = createOperationContext(arena, session);
            MemorySegment dbName = arena.allocateFrom(databaseName);
            MemorySegment cb = CallbackBridge.createVoidCallback(arena, wrapCallback(callback, arena));

            MongoDbFfi.mongo_drop_database(
                clientPtr, ctx, dbName, MemorySegment.NULL, MemorySegment.NULL, cb
            );
        } catch (Throwable t) {
            arena.close();
            callback.completeExceptionally(t);
        }
    }

    @Override
    public <T> void listDatabases(ListDatabasesOptions options,
                                   Decoder<T> decoder,
                                   @Nullable NativeAsyncClientSession session,
                                   SingleResultCallback<NativeAsyncCursor<T>> callback) {

        // Note: FFI returns a single document with databases array, not a cursor
        Arena arena = Arena.ofShared();
        try {
            MemorySegment ctx = createOperationContext(arena, session);
            MemorySegment opts = OptionsMarshaller.toListDatabasesOptions(arena, options);

            SingleResultCallback<BsonDocument> docCallback = (doc, error) -> {
                arena.close();
                if (error != null) {
                    callback.completeExceptionally(error);
                } else {
                    List<T> databases = ResultConverter.decodeArray(doc, "databases", decoder);
                    callback.complete(new ListBackedCursor<>(databases));
                }
            };
            MemorySegment cb = CallbackBridge.createListDatabasesCallback(arena, docCallback);

            MongoDbFfi.mongo_list_databases(
                clientPtr, ctx, opts, MemorySegment.NULL, cb
            );
        } catch (Throwable t) {
            arena.close();
            callback.completeExceptionally(t);
        }
    }

    @Override
    public void listDatabaseNames(ListDatabasesOptions options,
                                   @Nullable NativeAsyncClientSession session,
                                   SingleResultCallback<NativeAsyncCursor<String>> callback) {

        Arena arena = Arena.ofShared();
        try {
            MemorySegment ctx = createOperationContext(arena, session);
            // For listDatabaseNames, force nameOnly=true for efficiency
            ListDatabasesOptions effectiveOptions = new ListDatabasesOptions()
                .filter(options.getFilter())
                .nameOnly(true)  // Always true for names-only
                .authorizedDatabasesOnly(options.getAuthorizedDatabasesOnly())
                .comment(options.getComment());
            MemorySegment opts = OptionsMarshaller.toListDatabasesOptions(arena, effectiveOptions);

            SingleResultCallback<List<String>> listCallback = (names, error) -> {
                arena.close();
                if (error != null) {
                    callback.completeExceptionally(error);
                } else {
                    callback.complete(new ListBackedCursor<>(names));
                }
            };
            MemorySegment cb = CallbackBridge.createListDatabaseNamesCallback(arena, listCallback);

            MongoDbFfi.mongo_list_database_names(
                clientPtr, ctx, opts, MemorySegment.NULL, cb
            );
        } catch (Throwable t) {
            arena.close();
            callback.completeExceptionally(t);
        }
    }

    // ==================== Command Operations ====================

    @Override
    public <T> void runCommand(String databaseName,
                                BsonDocument command,
                                Decoder<T> decoder,
                                @Nullable NativeAsyncClientSession session,
                                SingleResultCallback<T> callback) {

        Arena arena = Arena.ofShared();
        try {
            MemorySegment ctx = createOperationContext(arena, session);
            MemorySegment dbName = arena.allocateFrom(databaseName);
            MemorySegment commandBson = BsonMarshaller.toBsonStruct(arena, command);
            MemorySegment cb = CallbackBridge.createCommandCallback(arena,
                wrapDecodingCallback(callback, decoder, arena));

            MongoDbFfi.mongo_run_command(
                clientPtr, ctx, dbName, commandBson, MemorySegment.NULL, MemorySegment.NULL, cb
            );
        } catch (Throwable t) {
            arena.close();
            callback.completeExceptionally(t);
        }
    }

    @Override
    public <T> void runCursorCommand(String databaseName,
                                      BsonDocument command,
                                      Decoder<T> decoder,
                                      @Nullable NativeAsyncClientSession session,
                                      SingleResultCallback<NativeAsyncCursor<T>> callback) {

        Arena arena = Arena.ofShared();
        try {
            MemorySegment ctx = createOperationContext(arena, session);
            MemorySegment dbName = arena.allocateFrom(databaseName);
            MemorySegment commandBson = BsonMarshaller.toBsonStruct(arena, command);

            SingleResultCallback<CursorHandle> cursorCallback = (handle, error) -> {
                arena.close();
                if (error != null) {
                    callback.completeExceptionally(error);
                } else {
                    callback.complete(new FfmAsyncCursor<>(clientPtr, handle, decoder));
                }
            };
            MemorySegment cb = CallbackBridge.createCursorResultCallback(arena, cursorCallback);

            MongoDbFfi.mongo_run_cursor_command(
                clientPtr, ctx, dbName, commandBson, MemorySegment.NULL, MemorySegment.NULL, cb
            );
        } catch (Throwable t) {
            arena.close();
            callback.completeExceptionally(t);
        }
    }

    private <T> SingleResultCallback<BsonDocument> wrapDecodingCallback(
            SingleResultCallback<T> callback, Decoder<T> decoder, Arena arena) {
        return (result, error) -> {
            arena.close();
            if (error != null) {
                callback.completeExceptionally(error);
            } else if (result == null) {
                callback.complete(null);
            } else {
                try {
                    T decoded = decoder.decode(result.asBsonReader(), DecoderContext.builder().build());
                    callback.complete(decoded);
                } catch (Throwable t) {
                    callback.completeExceptionally(t);
                }
            }
        };
    }

    // ==================== Change Stream Operations ====================

    @Override
    public <T> void watchCollection(MongoNamespace namespace,
                                     List<BsonDocument> pipeline,
                                     ChangeStreamOptions options,
                                     Decoder<T> decoder,
                                     @Nullable NativeAsyncClientSession session,
                                     SingleResultCallback<NativeAsyncChangeStream<T>> callback) {

        Arena arena = Arena.ofShared();
        try {
            MemorySegment ctx = createOperationContext(arena, session);
            MemorySegment dbName = arena.allocateFrom(namespace.getDatabaseName());
            MemorySegment collName = arena.allocateFrom(namespace.getCollectionName());
            MemorySegment pipelineBatch = BsonMarshaller.toBsonBatch(arena, pipeline);
            MemorySegment opts = OptionsMarshaller.toChangeStreamOptions(arena, options);
            MemorySegment cb = CallbackBridge.createChangeStreamCallback(arena,
                wrapChangeStreamCallback(callback, decoder, arena));

            MongoDbFfi.mongo_watch_collection(
                clientPtr, ctx, dbName, collName, pipelineBatch, opts, MemorySegment.NULL, cb
            );
        } catch (Throwable t) {
            arena.close();
            callback.completeExceptionally(t);
        }
    }

    @Override
    public <T> void watchDatabase(String databaseName,
                                   List<BsonDocument> pipeline,
                                   ChangeStreamOptions options,
                                   Decoder<T> decoder,
                                   @Nullable NativeAsyncClientSession session,
                                   SingleResultCallback<NativeAsyncChangeStream<T>> callback) {

        Arena arena = Arena.ofShared();
        try {
            MemorySegment ctx = createOperationContext(arena, session);
            MemorySegment dbName = arena.allocateFrom(databaseName);
            MemorySegment pipelineBatch = BsonMarshaller.toBsonBatch(arena, pipeline);
            MemorySegment opts = OptionsMarshaller.toChangeStreamOptions(arena, options);
            MemorySegment cb = CallbackBridge.createChangeStreamCallback(arena,
                wrapChangeStreamCallback(callback, decoder, arena));

            MongoDbFfi.mongo_watch_database(
                clientPtr, ctx, dbName, pipelineBatch, opts, MemorySegment.NULL, cb
            );
        } catch (Throwable t) {
            arena.close();
            callback.completeExceptionally(t);
        }
    }

    @Override
    public <T> void watchClient(List<BsonDocument> pipeline,
                                 ChangeStreamOptions options,
                                 Decoder<T> decoder,
                                 @Nullable NativeAsyncClientSession session,
                                 SingleResultCallback<NativeAsyncChangeStream<T>> callback) {

        Arena arena = Arena.ofShared();
        try {
            MemorySegment ctx = createOperationContext(arena, session);
            MemorySegment pipelineBatch = BsonMarshaller.toBsonBatch(arena, pipeline);
            MemorySegment opts = OptionsMarshaller.toChangeStreamOptions(arena, options);
            MemorySegment cb = CallbackBridge.createChangeStreamCallback(arena,
                wrapChangeStreamCallback(callback, decoder, arena));

            MongoDbFfi.mongo_watch_client(
                clientPtr, ctx, pipelineBatch, opts, MemorySegment.NULL, cb
            );
        } catch (Throwable t) {
            arena.close();
            callback.completeExceptionally(t);
        }
    }

    private <T> SingleResultCallback<MemorySegment> wrapChangeStreamCallback(
            SingleResultCallback<NativeAsyncChangeStream<T>> callback, Decoder<T> decoder, Arena arena) {
        return (changeStreamPtr, error) -> {
            arena.close();
            if (error != null) {
                callback.completeExceptionally(error);
            } else {
                callback.complete(new FfmAsyncChangeStream<>(clientPtr, changeStreamPtr, decoder));
            }
        };
    }

    // ==================== Bulk Write Operations ====================

    @Override
    public void bulkWrite(MongoNamespace namespace,
                           List<? extends WriteModel<BsonDocument>> requests,
                           BulkWriteOptions options,
                           @Nullable NativeAsyncClientSession session,
                           SingleResultCallback<BulkWriteResult> callback) {

        Arena arena = Arena.ofShared();
        try {
            MemorySegment ctx = createOperationContext(arena, session);
            MemorySegment dbName = arena.allocateFrom(namespace.getDatabaseName());
            MemorySegment collName = arena.allocateFrom(namespace.getCollectionName());
            OptionsMarshaller.WriteModelArray modelArray = OptionsMarshaller.toWriteModelArray(arena, requests);
            MemorySegment opts = OptionsMarshaller.toBulkWriteOptions(arena, options);
            MemorySegment cb = CallbackBridge.createBulkWriteCallback(arena, wrapCallback(callback, arena));

            MongoDbFfi.mongo_bulk_write(
                clientPtr, ctx, dbName, collName, modelArray.array, modelArray.length,
                opts, MemorySegment.NULL, cb
            );
        } catch (Throwable t) {
            arena.close();
            callback.completeExceptionally(t);
        }
    }

    /**
     * Wraps a callback to close the arena when the callback is invoked.
     */
    private <T> SingleResultCallback<T> wrapCallback(SingleResultCallback<T> callback, Arena arena) {
        return (result, error) -> {
            try {
                callback.onResult(result, error);
            } finally {
                arena.close();
            }
        };
    }

    /**
     * Creates an OperationContext, extracting the session pointer if present.
     *
     * <p>TODO: Pass ReadPreference, WriteConcern, and ReadConcern through the OperationContext.
     * Currently these are only set at client creation via connection string. To support
     * per-collection or per-operation concerns, add concern parameters to NativeAsyncClient
     * methods and wire them through here. See {@link OptionsMarshaller#createOperationContext}
     * for the full version that accepts concerns.</p>
     */
    private MemorySegment createOperationContext(Arena arena, @Nullable NativeAsyncClientSession session) {
        MemorySegment sessionPtr = MemorySegment.NULL;
        if (session != null) {
            sessionPtr = ((FfmAsyncClientSession) session).getSessionPtr();
        }
        return OptionsMarshaller.createOperationContextWithSessionPtr(arena, sessionPtr);
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            CountDownLatch latch = new CountDownLatch(1);
            AtomicReference<Throwable> errorRef = new AtomicReference<>();

            Arena arena = Arena.ofShared();
            MemorySegment callback = VoidCallback.allocate((userdata, error) -> {
                try {
                    if (!error.equals(MemorySegment.NULL)) {
                        errorRef.set(ErrorConverter.toException(error));
                    }
                } finally {
                    latch.countDown();
                }
            }, arena);

            MongoDbFfi.mongo_client_destroy(clientPtr, MemorySegment.NULL, callback);

            try {
                latch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            arena.close();
            clientArena.close();

            if (errorRef.get() != null) {
                Throwable t = errorRef.get();
                if (t instanceof RuntimeException) {
                    throw (RuntimeException) t;
                }
                throw new RuntimeException("Failed to close client", t);
            }
        }
    }
}

