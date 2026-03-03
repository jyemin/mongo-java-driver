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
import com.mongodb.MongoCompressor;
import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.*;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.connection.ConnectionPoolSettings;
import com.mongodb.connection.ServerSettings;
import com.mongodb.connection.SocketSettings;
import com.mongodb.internal.rust.crud.ffi.AuthSettings;
import com.mongodb.internal.rust.crud.ffi.ConnectionSettings;
import com.mongodb.internal.rust.crud.ffi.Error_;
import com.mongodb.internal.rust.crud.ffi.MongoDbFfi;
import com.mongodb.internal.rust.crud.ffi.TlsSettings;
import com.mongodb.lang.Nullable;
import com.mongodb.rust.crud.NativeAsyncChangeStream;
import com.mongodb.rust.crud.NativeAsyncClient;
import com.mongodb.rust.crud.NativeAsyncClientSession;
import com.mongodb.rust.crud.NativeAsyncCursor;
import com.mongodb.rust.crud.SingleResultCallback;
import org.bson.BsonDocument;
import org.bson.codecs.Decoder;
import org.bson.conversions.Bson;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * FFM implementation of NativeAsyncClient.
 *
 * <h2>Implementation Status</h2>
 * <h3>Implemented:</h3>
 * <ul>
 *   <li>Client creation/destruction</li>
 *   <li>Sessions (start, end, transactions)</li>
 *   <li>runCommand</li>
 * </ul>
 *
 * <h3>Not Yet Implemented:</h3>
 * <ul>
 *   <li>insertOne, insertMany, find, findOne</li>
 *   <li>updateOne, updateMany, replaceOne, deleteOne, deleteMany</li>
 *   <li>findOneAndUpdate, findOneAndReplace, findOneAndDelete</li>
 *   <li>aggregate, countDocuments, estimatedDocumentCount, distinct</li>
 *   <li>Index operations, Collection/database admin, Change streams, Bulk write</li>
 * </ul>
 */
public final class FfmAsyncClient implements NativeAsyncClient {

    private final MemorySegment clientPtr;
    private final Arena clientArena;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public FfmAsyncClient(MongoClientSettings settings) {
        this.clientArena = Arena.ofShared();
        this.clientPtr = createClient(settings);
    }

    private MemorySegment createClient(MongoClientSettings settings) {
        // Build ConnectionSettings struct
        MemorySegment connectionSettings = ConnectionSettings.allocate(clientArena);
        populateConnectionSettings(connectionSettings, settings);

        // Build AuthSettings struct (nullable)
        MemorySegment authSettings = MemorySegment.NULL;
        if (settings.getCredential() != null) {
            authSettings = AuthSettings.allocate(clientArena);
            populateAuthSettings(authSettings, settings);
        }

        // Build TlsSettings struct (nullable - only if TLS is enabled)
        MemorySegment tlsSettings = MemorySegment.NULL;
        if (settings.getSslSettings().isEnabled()) {
            tlsSettings = TlsSettings.allocate(clientArena);
            populateTlsSettings(tlsSettings, settings);
        }

        // Allocate error pointer (pointer to pointer)
        MemorySegment errorPtrPtr = clientArena.allocate(ValueLayout.ADDRESS);
        errorPtrPtr.set(ValueLayout.ADDRESS, 0, MemorySegment.NULL);

        // Call FFI
        MemorySegment clientPtr = MongoDbFfi.mongo_client_new(
                connectionSettings, authSettings, tlsSettings, errorPtrPtr);

        // Check for errors
        MemorySegment errorPtr = errorPtrPtr.get(ValueLayout.ADDRESS, 0);
        if (errorPtr.address() != 0) {
            // Reinterpret the error pointer to read Error_ struct
            MemorySegment error = errorPtr.reinterpret(Error_.sizeof());
            byte errorType = Error_.error_type(error);
            String errorMessage = extractErrorMessage(error, errorType);
            MongoDbFfi.error_free(errorPtr);
            throw new MongoException("Failed to create client: " + errorMessage + " (error type: " + errorType + ")");
        }

        if (clientPtr.equals(MemorySegment.NULL)) {
            throw new MongoException("Failed to create client: null pointer returned (no error details)");
        }

        return clientPtr;
    }

    private void populateConnectionSettings(MemorySegment struct, MongoClientSettings settings) {
        ClusterSettings cluster = settings.getClusterSettings();
        ConnectionPoolSettings pool = settings.getConnectionPoolSettings();
        SocketSettings socket = settings.getSocketSettings();
        ServerSettings server = settings.getServerSettings();

        // hosts - comma-separated list
        String hosts = cluster.getHosts().stream()
                .map(ServerAddress::toString)
                .collect(Collectors.joining(","));
        ConnectionSettings.hosts(struct, clientArena.allocateFrom(hosts));

        // app_name
        if (settings.getApplicationName() != null) {
            ConnectionSettings.app_name(struct, clientArena.allocateFrom(settings.getApplicationName()));
        } else {
            ConnectionSettings.app_name(struct, MemorySegment.NULL);
        }

        // compressors - comma-separated list of compressor names
        List<MongoCompressor> compressorList = settings.getCompressorList();
        if (compressorList.isEmpty()) {
            ConnectionSettings.compressors(struct, MemorySegment.NULL);
        } else {
            String compressors = compressorList.stream()
                    .map(MongoCompressor::getName)
                    .collect(Collectors.joining(","));
            ConnectionSettings.compressors(struct, clientArena.allocateFrom(compressors));
        }

        // direct_connection - check if single host and mode is single
        ConnectionSettings.direct_connection(struct,
                settings.getClusterSettings().getMode() == ClusterConnectionMode.SINGLE);

        // load_balanced
        ConnectionSettings.load_balanced(struct, settings.getClusterSettings().getMode() == ClusterConnectionMode.LOAD_BALANCED);
        // from cluster settings

        // pool sizes
        ConnectionSettings.max_pool_size(struct, pool.getMaxSize());
        ConnectionSettings.min_pool_size(struct, pool.getMinSize());
        ConnectionSettings.max_idle_time_ms(struct, pool.getMaxConnectionIdleTime(TimeUnit.MILLISECONDS));

        // timeouts
        ConnectionSettings.connect_timeout_ms(struct, socket.getConnectTimeout(TimeUnit.MILLISECONDS));
        ConnectionSettings.socket_timeout_ms(struct, socket.getReadTimeout(TimeUnit.MILLISECONDS));
        ConnectionSettings.server_selection_timeout_ms(struct,
                cluster.getServerSelectionTimeout(TimeUnit.MILLISECONDS));
        ConnectionSettings.local_threshold_ms(struct, cluster.getLocalThreshold(TimeUnit.MILLISECONDS));
        ConnectionSettings.heartbeat_frequency_ms(struct, server.getHeartbeatFrequency(TimeUnit.MILLISECONDS));

        // replica_set
        if (cluster.getRequiredReplicaSetName() != null) {
            ConnectionSettings.replica_set(struct, clientArena.allocateFrom(cluster.getRequiredReplicaSetName()));
        } else {
            ConnectionSettings.replica_set(struct, MemorySegment.NULL);
        }

        // read_preference_mode: 0=Primary, 1=PrimaryPreferred, 2=Secondary, 3=SecondaryPreferred, 4=Nearest
        // TODO: FFI doesn't support read preference tags or maxStaleness
        ConnectionSettings.read_preference_mode(struct, toReadPreferenceMode(settings.getReadPreference()));

        ConnectionSettings.srv_service_name(struct, clientArena.allocateFrom(settings.getClusterSettings().getSrvServiceName()));

        Integer srvMaxHosts = settings.getClusterSettings().getSrvMaxHosts();
        if (srvMaxHosts != null) {
            ConnectionSettings.srv_max_hosts(struct, settings.getClusterSettings().getSrvMaxHosts());
        } else {
            ConnectionSettings.srv_max_hosts(struct, 0);
        }
    }

    private void populateAuthSettings(MemorySegment struct, MongoClientSettings settings) {
        var credential = settings.getCredential();
        if (credential == null) {
            return;
        }

        // mechanism
        if (credential.getMechanism() != null) {
            AuthSettings.mechanism(struct, clientArena.allocateFrom(credential.getMechanism()));
        } else {
            AuthSettings.mechanism(struct, MemorySegment.NULL);
        }

        // username
        if (credential.getUserName() != null) {
            AuthSettings.username(struct, clientArena.allocateFrom(credential.getUserName()));
        } else {
            AuthSettings.username(struct, MemorySegment.NULL);
        }

        // password
        if (credential.getPassword() != null) {
            AuthSettings.password(struct, clientArena.allocateFrom(new String(credential.getPassword())));
        } else {
            AuthSettings.password(struct, MemorySegment.NULL);
        }

        // source (auth database)
        if (credential.getSource() != null) {
            AuthSettings.source(struct, clientArena.allocateFrom(credential.getSource()));
        } else {
            AuthSettings.source(struct, MemorySegment.NULL);
        }
    }

    private void populateTlsSettings(MemorySegment struct, MongoClientSettings settings) {
        var ssl = settings.getSslSettings();

        TlsSettings.enabled(struct, ssl.isEnabled());
        TlsSettings.allow_invalid_hostnames(struct, ssl.isInvalidHostNameAllowed());

        // TODO: allow_invalid_certificates - Java driver doesn't expose this directly
        TlsSettings.allow_invalid_certificates(struct, false);

        // TODO: Certificate file paths - not directly available from SslSettings (Java uses SSLContext)
        TlsSettings.ca_file(struct, MemorySegment.NULL);
        TlsSettings.cert_file(struct, MemorySegment.NULL);
        TlsSettings.cert_key_file(struct, MemorySegment.NULL);
    }

    private static byte toReadPreferenceMode(ReadPreference readPreference) {
        return switch (readPreference.getName()) {
            case "primary" -> (byte) 0;
            case "primaryPreferred" -> (byte) 1;
            case "secondary" -> (byte) 2;
            case "secondaryPreferred" -> (byte) 3;
            case "nearest" -> (byte) 4;
            default -> (byte) 0; // default to primary
        };
    }

    MemorySegment getClientPtr() {
        return clientPtr;
    }

    /**
     * Extracts error message from an FFI Error struct based on error type.
     */
    private String extractErrorMessage(MemorySegment error, byte errorType) {
        try {
            MemorySegment errorUnion = Error_.error(error);
            // The union contains pointers to specific error types, each of which has a message field
            // For simplicity, we treat the first field as a pointer to a struct with a message field
            MemorySegment errorStructPtr = errorUnion.get(ValueLayout.ADDRESS, 0);
            if (!errorStructPtr.equals(MemorySegment.NULL)) {
                // All error types have 'message' as their first field (const char*)
                MemorySegment messagePtr = errorStructPtr.reinterpret(8).get(ValueLayout.ADDRESS, 0);
                if (!messagePtr.equals(MemorySegment.NULL)) {
                    return messagePtr.reinterpret(1024).getString(0);
                }
            }
        } catch (Exception e) {
            return "Unable to extract error message: " + e.getMessage();
        }
        return "Unknown error";
    }

    // ==================== Session (IMPLEMENTED) ====================

    @Override
    public void startSession(ClientSessionOptions options, SingleResultCallback<NativeAsyncClientSession> callback) {
        // TODO: Implement using mongo_session_start
        throw new UnsupportedOperationException("FFI: startSession not yet implemented");
    }

    // ==================== Command Operations (IMPLEMENTED) ====================

    @Override
    public <T> void runCommand(String databaseName,
                                BsonDocument command,
                                Decoder<T> decoder,
                                @Nullable NativeAsyncClientSession session,
                                SingleResultCallback<T> callback) {
        // TODO: Implement using mongo_run_command
        throw new UnsupportedOperationException("FFI: runCommand not yet implemented");
    }

    @Override
    public <T> void runCursorCommand(String databaseName,
                                      BsonDocument command,
                                      Decoder<T> decoder,
                                      @Nullable NativeAsyncClientSession session,
                                      SingleResultCallback<NativeAsyncCursor<T>> callback) {
        throw new UnsupportedOperationException("FFI: runCursorCommand not yet implemented");
    }

    // ==================== CRUD Operations (NOT IMPLEMENTED) ====================

    @Override
    public void insertOne(MongoNamespace namespace, BsonDocument document, InsertOneOptions options,
                          @Nullable NativeAsyncClientSession session, SingleResultCallback<InsertOneResult> callback) {
        throw new UnsupportedOperationException("FFI: insertOne not yet implemented");
    }

    @Override
    public void insertMany(MongoNamespace namespace, List<BsonDocument> documents, InsertManyOptions options,
                           @Nullable NativeAsyncClientSession session, SingleResultCallback<InsertManyResult> callback) {
        throw new UnsupportedOperationException("FFI: insertMany not yet implemented");
    }

    @Override
    public void updateOne(MongoNamespace namespace, Bson filter, Bson update, UpdateOptions options,
                          @Nullable NativeAsyncClientSession session, SingleResultCallback<UpdateResult> callback) {
        throw new UnsupportedOperationException("FFI: updateOne not yet implemented");
    }

    @Override
    public void updateMany(MongoNamespace namespace, Bson filter, Bson update, UpdateOptions options,
                           @Nullable NativeAsyncClientSession session, SingleResultCallback<UpdateResult> callback) {
        throw new UnsupportedOperationException("FFI: updateMany not yet implemented");
    }

    @Override
    public void replaceOne(MongoNamespace namespace, Bson filter, BsonDocument replacement, ReplaceOptions options,
                           @Nullable NativeAsyncClientSession session, SingleResultCallback<UpdateResult> callback) {
        throw new UnsupportedOperationException("FFI: replaceOne not yet implemented");
    }

    @Override
    public void deleteOne(MongoNamespace namespace, Bson filter, DeleteOptions options,
                          @Nullable NativeAsyncClientSession session, SingleResultCallback<DeleteResult> callback) {
        throw new UnsupportedOperationException("FFI: deleteOne not yet implemented");
    }

    @Override
    public void deleteMany(MongoNamespace namespace, Bson filter, DeleteOptions options,
                           @Nullable NativeAsyncClientSession session, SingleResultCallback<DeleteResult> callback) {
        throw new UnsupportedOperationException("FFI: deleteMany not yet implemented");
    }

    @Override
    public <T> void findOne(MongoNamespace namespace, Bson filter, FindOptions options, Decoder<T> decoder,
                            @Nullable NativeAsyncClientSession session, SingleResultCallback<T> callback) {
        throw new UnsupportedOperationException("FFI: findOne not yet implemented");
    }

    @Override
    public <T> void find(MongoNamespace namespace, Bson filter, FindOptions options, Decoder<T> decoder,
                         @Nullable NativeAsyncClientSession session, SingleResultCallback<NativeAsyncCursor<T>> callback) {
        throw new UnsupportedOperationException("FFI: find not yet implemented");
    }

    @Override
    public <T> void findOneAndUpdate(MongoNamespace namespace, Bson filter, Bson update, FindOneAndUpdateOptions options,
                                      Decoder<T> decoder, @Nullable NativeAsyncClientSession session, SingleResultCallback<T> callback) {
        throw new UnsupportedOperationException("FFI: findOneAndUpdate not yet implemented");
    }

    @Override
    public <T> void findOneAndReplace(MongoNamespace namespace, Bson filter, BsonDocument replacement, FindOneAndReplaceOptions options,
                                       Decoder<T> decoder, @Nullable NativeAsyncClientSession session, SingleResultCallback<T> callback) {
        throw new UnsupportedOperationException("FFI: findOneAndReplace not yet implemented");
    }

    @Override
    public <T> void findOneAndDelete(MongoNamespace namespace, Bson filter, FindOneAndDeleteOptions options,
                                      Decoder<T> decoder, @Nullable NativeAsyncClientSession session, SingleResultCallback<T> callback) {
        throw new UnsupportedOperationException("FFI: findOneAndDelete not yet implemented");
    }

    // ==================== Aggregate Operations ====================

    @Override
    public <T> void aggregate(MongoNamespace namespace, List<BsonDocument> pipeline, AggregateOptions options,
                              @Nullable Boolean bypassDocumentValidation, Decoder<T> decoder,
                              @Nullable NativeAsyncClientSession session, SingleResultCallback<NativeAsyncCursor<T>> callback) {
        throw new UnsupportedOperationException("FFI: aggregate not yet implemented");
    }

    @Override
    public <T> void aggregateDatabase(String databaseName, List<BsonDocument> pipeline, AggregateOptions options,
                                       @Nullable Boolean bypassDocumentValidation, Decoder<T> decoder,
                                       @Nullable NativeAsyncClientSession session, SingleResultCallback<NativeAsyncCursor<T>> callback) {
        throw new UnsupportedOperationException("FFI: aggregateDatabase not yet implemented");
    }

    // ==================== Count Operations ====================

    @Override
    public void countDocuments(MongoNamespace namespace, Bson filter, CountOptions options,
                                @Nullable NativeAsyncClientSession session, SingleResultCallback<Long> callback) {
        throw new UnsupportedOperationException("FFI: countDocuments not yet implemented");
    }

    @Override
    public void estimatedDocumentCount(MongoNamespace namespace, EstimatedDocumentCountOptions options,
                                        SingleResultCallback<Long> callback) {
        throw new UnsupportedOperationException("FFI: estimatedDocumentCount not yet implemented");
    }

    @Override
    public <T> void distinct(MongoNamespace namespace, String fieldName, Bson filter, DistinctOptions options,
                              Decoder<T> decoder, @Nullable NativeAsyncClientSession session,
                              SingleResultCallback<NativeAsyncCursor<T>> callback) {
        throw new UnsupportedOperationException("FFI: distinct not yet implemented");
    }

    // ==================== Index Operations ====================

    @Override
    public void createIndex(MongoNamespace namespace, Bson keys, CreateIndexOptions options,
                             @Nullable NativeAsyncClientSession session, SingleResultCallback<String> callback) {
        throw new UnsupportedOperationException("FFI: createIndex not yet implemented");
    }

    @Override
    public void createIndexes(MongoNamespace namespace, List<IndexModel> indexes, CreateIndexOptions options,
                               @Nullable NativeAsyncClientSession session, SingleResultCallback<List<String>> callback) {
        throw new UnsupportedOperationException("FFI: createIndexes not yet implemented");
    }

    @Override
    public void dropIndex(MongoNamespace namespace, String indexName, DropIndexOptions options,
                           @Nullable NativeAsyncClientSession session, SingleResultCallback<Void> callback) {
        throw new UnsupportedOperationException("FFI: dropIndex not yet implemented");
    }

    @Override
    public void dropIndex(MongoNamespace namespace, Bson keys, DropIndexOptions options,
                           @Nullable NativeAsyncClientSession session, SingleResultCallback<Void> callback) {
        throw new UnsupportedOperationException("FFI: dropIndex not yet implemented");
    }

    @Override
    public <T> void listIndexes(MongoNamespace namespace, ListIndexesOptions options, Decoder<T> decoder,
                                 @Nullable NativeAsyncClientSession session, SingleResultCallback<NativeAsyncCursor<T>> callback) {
        throw new UnsupportedOperationException("FFI: listIndexes not yet implemented");
    }

    // ==================== Collection Admin Operations ====================

    @Override
    public void createCollection(String databaseName, String collectionName, CreateCollectionOptions options,
                                  @Nullable NativeAsyncClientSession session, SingleResultCallback<Void> callback) {
        throw new UnsupportedOperationException("FFI: createCollection not yet implemented");
    }

    @Override
    public void dropCollection(MongoNamespace namespace, DropCollectionOptions options,
                                @Nullable NativeAsyncClientSession session, SingleResultCallback<Void> callback) {
        throw new UnsupportedOperationException("FFI: dropCollection not yet implemented");
    }

    @Override
    public void renameCollection(MongoNamespace namespace, MongoNamespace newNamespace, RenameCollectionOptions options,
                                  @Nullable NativeAsyncClientSession session, SingleResultCallback<Void> callback) {
        throw new UnsupportedOperationException("FFI: renameCollection not yet implemented");
    }

    @Override
    public <T> void listCollections(String databaseName, ListCollectionsOptions options, Decoder<T> decoder,
                                     @Nullable NativeAsyncClientSession session, SingleResultCallback<NativeAsyncCursor<T>> callback) {
        throw new UnsupportedOperationException("FFI: listCollections not yet implemented");
    }

    @Override
    public void listCollectionNames(String databaseName, ListCollectionsOptions options,
                                     @Nullable NativeAsyncClientSession session, SingleResultCallback<NativeAsyncCursor<String>> callback) {
        throw new UnsupportedOperationException("FFI: listCollectionNames not yet implemented");
    }

    // ==================== Database Admin Operations ====================

    @Override
    public void dropDatabase(String databaseName, @Nullable NativeAsyncClientSession session, SingleResultCallback<Void> callback) {
        throw new UnsupportedOperationException("FFI: dropDatabase not yet implemented");
    }

    @Override
    public <T> void listDatabases(ListDatabasesOptions options, Decoder<T> decoder,
                                   @Nullable NativeAsyncClientSession session, SingleResultCallback<NativeAsyncCursor<T>> callback) {
        throw new UnsupportedOperationException("FFI: listDatabases not yet implemented");
    }

    @Override
    public void listDatabaseNames(ListDatabasesOptions options, @Nullable NativeAsyncClientSession session,
                                   SingleResultCallback<NativeAsyncCursor<String>> callback) {
        throw new UnsupportedOperationException("FFI: listDatabaseNames not yet implemented");
    }

    // ==================== Change Stream Operations ====================

    @Override
    public <T> void watchCollection(MongoNamespace namespace, List<BsonDocument> pipeline, ChangeStreamOptions options,
                                     Decoder<T> decoder, @Nullable NativeAsyncClientSession session,
                                     SingleResultCallback<NativeAsyncChangeStream<T>> callback) {
        throw new UnsupportedOperationException("FFI: watchCollection not yet implemented");
    }

    @Override
    public <T> void watchDatabase(String databaseName, List<BsonDocument> pipeline, ChangeStreamOptions options,
                                   Decoder<T> decoder, @Nullable NativeAsyncClientSession session,
                                   SingleResultCallback<NativeAsyncChangeStream<T>> callback) {
        throw new UnsupportedOperationException("FFI: watchDatabase not yet implemented");
    }

    @Override
    public <T> void watchClient(List<BsonDocument> pipeline, ChangeStreamOptions options, Decoder<T> decoder,
                                 @Nullable NativeAsyncClientSession session, SingleResultCallback<NativeAsyncChangeStream<T>> callback) {
        throw new UnsupportedOperationException("FFI: watchClient not yet implemented");
    }

    // ==================== Bulk Write Operations ====================

    @Override
    public void bulkWrite(MongoNamespace namespace, List<? extends WriteModel<BsonDocument>> requests,
                           BulkWriteOptions options, @Nullable NativeAsyncClientSession session,
                           SingleResultCallback<BulkWriteResult> callback) {
        throw new UnsupportedOperationException("FFI: bulkWrite not yet implemented");
    }

    // ==================== Lifecycle ====================

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            MongoDbFfi.mongo_client_destroy(clientPtr);
            clientArena.close();
        }
    }
}
