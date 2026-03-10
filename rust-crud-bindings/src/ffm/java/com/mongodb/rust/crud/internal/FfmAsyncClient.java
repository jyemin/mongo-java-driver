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
import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
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
import com.mongodb.internal.rust.crud.ffi.MongoDbFfi;
import com.mongodb.internal.rust.crud.ffi.OperationContext;
import com.mongodb.internal.rust.crud.ffi.RunCommandCallback;
import com.mongodb.internal.rust.crud.ffi.SessionOptions;
import com.mongodb.internal.rust.crud.ffi.TlsSettings;
import com.mongodb.lang.Nullable;
import com.mongodb.rust.crud.NativeAsyncChangeStream;
import com.mongodb.rust.crud.NativeAsyncClient;
import com.mongodb.rust.crud.NativeAsyncClientSession;
import com.mongodb.rust.crud.NativeAsyncCursor;
import com.mongodb.rust.crud.NativeOperationContext;
import com.mongodb.rust.crud.SingleResultCallback;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.codecs.Decoder;
import org.bson.conversions.Bson;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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

    // Cache FFI handles to avoid repeated allocations and leaks
    private final ConcurrentHashMap<ReadPreference, MemorySegment> readPreferenceCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<WriteConcern, MemorySegment> writeConcernCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<ReadConcern, MemorySegment> readConcernCache = new ConcurrentHashMap<>();

    // Shared callback stubs - allocated once per client, reused across all operations
    private final MemorySegment findCallbackStub;
    private final MemorySegment runCommandCallbackStub;
    private final MemorySegment insertOneCallbackStub;
    private final MemorySegment insertManyCallbackStub;
    private final MemorySegment dropCallbackStub;

    public FfmAsyncClient(MongoClientSettings settings) {
        this.clientArena = Arena.ofShared();
        this.clientPtr = createClient(settings);

        // Initialize shared callback stubs - all use the same dispatch pattern
        this.findCallbackStub = com.mongodb.internal.rust.crud.ffi.FindCallback.allocate(
                (userdata, result, error) -> CallbackRegistry.dispatch(userdata, result, error),
                clientArena);
        this.runCommandCallbackStub = RunCommandCallback.allocate(
                (userdata, result, error) -> CallbackRegistry.dispatch(userdata, result, error),
                clientArena);
        this.insertOneCallbackStub = com.mongodb.internal.rust.crud.ffi.InsertOneCallback.allocate(
                (userdata, result, error) -> CallbackRegistry.dispatch(userdata, result, error),
                clientArena);
        this.insertManyCallbackStub = com.mongodb.internal.rust.crud.ffi.InsertManyCallback.allocate(
                (userdata, result, error) -> CallbackRegistry.dispatch(userdata, result, error),
                clientArena);
        this.dropCallbackStub = com.mongodb.internal.rust.crud.ffi.DropCallback.allocate(
                (userdata, error) -> CallbackRegistry.dispatchVoid(userdata, error),
                clientArena);
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
            MongoException exception = FfmErrorMapper.toException(errorPtr);
            MongoDbFfi.error_free(errorPtr);
            throw exception;
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
        // Note: Client-level settings only use mode; tags/maxStaleness are set per-operation via OperationContext
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

    static byte toReadPreferenceMode(ReadPreference readPreference) {
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

    // ==================== Session (IMPLEMENTED) ====================

    @Override
    public void startSession(ClientSessionOptions options, SingleResultCallback<NativeAsyncClientSession> callback) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment sessionOptions = toSessionOptionsFFI(arena, options);
            MemorySegment errorPtrPtr = arena.allocate(ValueLayout.ADDRESS);
            errorPtrPtr.set(ValueLayout.ADDRESS, 0, MemorySegment.NULL);

            MemorySegment sessionPtr = MongoDbFfi.mongo_session_start(clientPtr, sessionOptions, errorPtrPtr);

            MemorySegment errorPtr = errorPtrPtr.get(ValueLayout.ADDRESS, 0);
            if (errorPtr.address() != 0) {
                callback.onResult(null, FfmErrorMapper.toException(errorPtr));
                return;
            }

            if (sessionPtr.address() == 0) {
                callback.onResult(null, new MongoException("Failed to start session: null pointer returned"));
                return;
            }

            FfmAsyncClientSession session = new FfmAsyncClientSession(clientPtr, sessionPtr, options);
            callback.onResult(session, null);
        } catch (Exception e) {
            callback.onResult(null, e);
        }
    }

    @Nullable
    private MemorySegment toSessionOptionsFFI(Arena arena, ClientSessionOptions options) {
        MemorySegment struct = SessionOptions.allocate(arena);

        // causal_consistency: -1 = not set, 0 = false, 1 = true
        if (options.isCausallyConsistent() != null) {
            SessionOptions.causal_consistency(struct, (byte) (options.isCausallyConsistent() ? 1 : 0));
        } else {
            SessionOptions.causal_consistency(struct, (byte) -1);
        }

        // snapshot: -1 = not set, 0 = false, 1 = true
        if (options.isSnapshot() != null) {
            SessionOptions.snapshot(struct, (byte) (options.isSnapshot() ? 1 : 0));
        } else {
            SessionOptions.snapshot(struct, (byte) -1);
        }

        // default_transaction_options
        com.mongodb.TransactionOptions defaultTxnOptions = options.getDefaultTransactionOptions();
        if (defaultTxnOptions != null) {
            SessionOptions.default_transaction_options(struct, FfmAsyncClientSession.toTransactionOptionsFFI(arena, defaultTxnOptions));
        } else {
            SessionOptions.default_transaction_options(struct, MemorySegment.NULL);
        }

        return struct;
    }

    // ==================== Command Operations (IMPLEMENTED) ====================

    // Set to true to enable timing instrumentation (adds overhead)
    static final boolean TIMING_ENABLED = false;

    // RunCommand timing instrumentation
    private static long rcArenaTime, rcMarshalTime, rcRegistryTime, rcFfiDispatchTime, rcCount;

    public static void printRunCommandTimings() {
        if (!TIMING_ENABLED || rcCount == 0) return;
        System.out.printf("RunCommand timings (avg over %d calls):%n", rcCount);
        System.out.printf("  BEFORE FFI CALL:%n");
        System.out.printf("    Arena create:    %.4f ms%n", rcArenaTime / 1_000_000.0 / rcCount);
        System.out.printf("    Marshal:         %.4f ms%n", rcMarshalTime / 1_000_000.0 / rcCount);
        System.out.printf("    Registry:        %.4f ms%n", rcRegistryTime / 1_000_000.0 / rcCount);
        System.out.printf("    FFI dispatch:    %.4f ms%n", rcFfiDispatchTime / 1_000_000.0 / rcCount);
        double total = (rcArenaTime + rcMarshalTime + rcRegistryTime + rcFfiDispatchTime) / 1_000_000.0 / rcCount;
        System.out.printf("  TOTAL INSTRUMENTED: %.4f ms%n", total);
    }

    // InsertOne timing instrumentation
    private static long insertArenaTime, insertMarshalTime, insertRegistryTime, insertFfiTime, insertCount;

    public static void printInsertOneTimings() {
        if (!TIMING_ENABLED || insertCount == 0) return;
        System.out.printf("InsertOne timings (avg over %d calls):%n", insertCount);
        System.out.printf("  Arena create:    %.4f ms%n", insertArenaTime / 1_000_000.0 / insertCount);
        System.out.printf("  Marshal:         %.4f ms%n", insertMarshalTime / 1_000_000.0 / insertCount);
        System.out.printf("  Registry:        %.4f ms%n", insertRegistryTime / 1_000_000.0 / insertCount);
        System.out.printf("  FFI dispatch:    %.4f ms%n", insertFfiTime / 1_000_000.0 / insertCount);
        double total = (insertArenaTime + insertMarshalTime + insertRegistryTime + insertFfiTime) / 1_000_000.0 / insertCount;
        System.out.printf("  TOTAL JAVA SIDE: %.4f ms%n", total);
    }

    static {
        // Print timing info on shutdown
        if (TIMING_ENABLED) {
            Runtime.getRuntime().addShutdownHook(new Thread(FfmAsyncClient::printInsertOneTimings));
        }
    }

    @Override
    public <T> void runCommand(String databaseName,
                                BsonDocument command,
                                Decoder<T> decoder,
                                NativeOperationContext context,
                                @Nullable NativeAsyncClientSession session,
                                SingleResultCallback<T> callback) {
        long t0 = TIMING_ENABLED ? System.nanoTime() : 0;
        Arena arena = Arena.ofAuto();
        long t1 = TIMING_ENABLED ? System.nanoTime() : 0;
        if (TIMING_ENABLED) rcArenaTime += (t1 - t0);

        try {
            MemorySegment dbName = arena.allocateFrom(databaseName);
            MemorySegment commandBson = BsonMarshaller.toBsonStruct(arena, command);
            MemorySegment operationContext = buildOperationContext(arena, context, session);
            long t2 = TIMING_ENABLED ? System.nanoTime() : 0;
            if (TIMING_ENABLED) rcMarshalTime += (t2 - t1);

            long opId = CallbackRegistry.register(new PendingOperation<>(
                    callback,
                    (result) -> {
                        MemorySegment data = com.mongodb.internal.rust.crud.ffi.Bson.data(result);
                        long len = com.mongodb.internal.rust.crud.ffi.Bson.len(result);
                        return BsonMarshaller.decode(data, len, decoder);
                    },
                    arena
            ));
            long t3 = TIMING_ENABLED ? System.nanoTime() : 0;
            if (TIMING_ENABLED) rcRegistryTime += (t3 - t2);

            MongoDbFfi.mongo_run_command(
                    clientPtr,
                    operationContext,
                    dbName,
                    commandBson,
                    runCommandCallbackStub,
                    CallbackRegistry.toUserdata(opId));
            if (TIMING_ENABLED) {
                long t4 = System.nanoTime();
                rcFfiDispatchTime += (t4 - t3);
                rcCount++;
            }
        } catch (Exception e) {
            callback.onResult(null, e);
        }
    }

    @Override
    public <T> void runCursorCommand(String databaseName,
                                      BsonDocument command,
                                      Decoder<T> decoder,
                                      NativeOperationContext context,
                                      @Nullable NativeAsyncClientSession session,
                                      SingleResultCallback<NativeAsyncCursor<T>> callback) {
        throw new UnsupportedOperationException("FFI: runCursorCommand not yet implemented");
    }

    // ==================== CRUD Operations ====================

    @Override
    public void insertOne(MongoNamespace namespace, BsonDocument document, InsertOneOptions options,
                          NativeOperationContext context, @Nullable NativeAsyncClientSession session, SingleResultCallback<InsertOneResult> callback) {
        long t0 = TIMING_ENABLED ? System.nanoTime() : 0;
        Arena arena = Arena.ofAuto();
        long t1 = TIMING_ENABLED ? System.nanoTime() : 0;
        if (TIMING_ENABLED) insertArenaTime += (t1 - t0);

        try {
            MemorySegment dbName = arena.allocateFrom(namespace.getDatabaseName());
            MemorySegment collName = arena.allocateFrom(namespace.getCollectionName());
            MemorySegment documentBson = BsonMarshaller.toBsonStructForInsert(arena, document);
            MemorySegment operationContext = buildOperationContext(arena, context, session);

            // bypass_document_validation: -1 = not set, 0 = false, 1 = true
            byte bypassDocValidation = options.getBypassDocumentValidation() == null
                    ? (byte) -1
                    : (byte) (options.getBypassDocumentValidation() ? 1 : 0);

            // comment (nullable)
            MemorySegment comment = options.getComment() != null
                    ? BsonMarshaller.toBsonValueStruct(arena, options.getComment())
                    : MemorySegment.NULL;

            long t2 = TIMING_ENABLED ? System.nanoTime() : 0;
            if (TIMING_ENABLED) insertMarshalTime += (t2 - t1);

            long opId = CallbackRegistry.register(new PendingOperation<>(
                    callback,
                    (result) -> {
                        MemorySegment insertedIdSegment = com.mongodb.internal.rust.crud.ffi.InsertOneResult.inserted_id(result);
                        org.bson.BsonValue insertedId = BsonMarshaller.fromBsonValueStruct(insertedIdSegment);
                        return InsertOneResult.acknowledged(insertedId);
                    },
                    arena
            ));

            long t3 = TIMING_ENABLED ? System.nanoTime() : 0;
            if (TIMING_ENABLED) insertRegistryTime += (t3 - t2);

            MongoDbFfi.mongo_insert_one(
                    clientPtr,
                    operationContext,
                    dbName,
                    collName,
                    documentBson,
                    bypassDocValidation,
                    comment,
                    insertOneCallbackStub,
                    CallbackRegistry.toUserdata(opId));

            if (TIMING_ENABLED) {
                long t4 = System.nanoTime();
                insertFfiTime += (t4 - t3);
                insertCount++;
            }
        } catch (Exception e) {
            callback.onResult(null, e);
        }
    }

    @Override
    public void insertMany(MongoNamespace namespace, List<BsonDocument> documents, InsertManyOptions options,
                           NativeOperationContext context, @Nullable NativeAsyncClientSession session, SingleResultCallback<InsertManyResult> callback) {
        Arena arena = Arena.ofAuto();
        try {
            MemorySegment dbName = arena.allocateFrom(namespace.getDatabaseName());
            MemorySegment collName = arena.allocateFrom(namespace.getCollectionName());
            MemorySegment documentsArray = BsonMarshaller.toDocumentBsonArrayStructForInsert(arena, documents);
            MemorySegment operationContext = buildOperationContext(arena, context, session);

            // bypass_document_validation: -1 = not set, 0 = false, 1 = true
            byte bypassDocValidation = options.getBypassDocumentValidation() == null
                    ? (byte) -1
                    : (byte) (options.getBypassDocumentValidation() ? 1 : 0);

            // ordered (default true)
            boolean ordered = options.isOrdered();

            // comment (nullable)
            MemorySegment comment = options.getComment() != null
                    ? BsonMarshaller.toBsonValueStruct(arena, options.getComment())
                    : MemorySegment.NULL;

            long opId = CallbackRegistry.register(new PendingOperation<>(
                    callback,
                    this::parseInsertManyResult,
                    arena
            ));

            MongoDbFfi.mongo_insert_many(
                    clientPtr,
                    operationContext,
                    dbName,
                    collName,
                    documentsArray,
                    bypassDocValidation,
                    ordered,
                    comment,
                    insertManyCallbackStub,
                    CallbackRegistry.toUserdata(opId));
        } catch (Exception e) {
            callback.onResult(null, e);
        }
    }

    private InsertManyResult parseInsertManyResult(MemorySegment result) {
        MemorySegment insertedIdsPtr = com.mongodb.internal.rust.crud.ffi.InsertManyResult.inserted_ids(result);
        long insertedIdsLen = com.mongodb.internal.rust.crud.ffi.InsertManyResult.inserted_ids_len(result);

        Map<Integer, BsonValue> insertedIds = new HashMap<>();

        if (insertedIdsPtr.address() != 0 && insertedIdsLen > 0) {
            // Reinterpret to access array of InsertedId structs
            long structSize = com.mongodb.internal.rust.crud.ffi.InsertedId.sizeof();
            MemorySegment insertedIdsArray = insertedIdsPtr.reinterpret(structSize * insertedIdsLen);

            for (int i = 0; i < insertedIdsLen; i++) {
                MemorySegment insertedId = com.mongodb.internal.rust.crud.ffi.InsertedId.asSlice(insertedIdsArray, i);
                long index = com.mongodb.internal.rust.crud.ffi.InsertedId.index(insertedId);
                MemorySegment idValue = com.mongodb.internal.rust.crud.ffi.InsertedId.id(insertedId);
                BsonValue bsonId = BsonMarshaller.fromBsonValueStruct(idValue);
                insertedIds.put((int) index, bsonId);
            }
        }

        return InsertManyResult.acknowledged(insertedIds);
    }

    @Override
    public void updateOne(MongoNamespace namespace, Bson filter, Bson update, UpdateOptions options,
                          NativeOperationContext context, @Nullable NativeAsyncClientSession session, SingleResultCallback<UpdateResult> callback) {
        throw new UnsupportedOperationException("FFI: updateOne not yet implemented");
    }

    @Override
    public void updateMany(MongoNamespace namespace, Bson filter, Bson update, UpdateOptions options,
                           NativeOperationContext context, @Nullable NativeAsyncClientSession session, SingleResultCallback<UpdateResult> callback) {
        throw new UnsupportedOperationException("FFI: updateMany not yet implemented");
    }

    @Override
    public void replaceOne(MongoNamespace namespace, Bson filter, BsonDocument replacement, ReplaceOptions options,
                           NativeOperationContext context, @Nullable NativeAsyncClientSession session, SingleResultCallback<UpdateResult> callback) {
        throw new UnsupportedOperationException("FFI: replaceOne not yet implemented");
    }

    @Override
    public void deleteOne(MongoNamespace namespace, Bson filter, DeleteOptions options,
                          NativeOperationContext context, @Nullable NativeAsyncClientSession session, SingleResultCallback<DeleteResult> callback) {
        throw new UnsupportedOperationException("FFI: deleteOne not yet implemented");
    }

    @Override
    public void deleteMany(MongoNamespace namespace, Bson filter, DeleteOptions options,
                           NativeOperationContext context, @Nullable NativeAsyncClientSession session, SingleResultCallback<DeleteResult> callback) {
        throw new UnsupportedOperationException("FFI: deleteMany not yet implemented");
    }

    @Override
    public <T> void findOne(MongoNamespace namespace, Bson filter, FindOptions options, Decoder<T> decoder,
                            NativeOperationContext context, @Nullable NativeAsyncClientSession session, SingleResultCallback<T> callback) {
        throw new UnsupportedOperationException("FFI: findOne not yet implemented");
    }

    // Timing instrumentation for find
    private static long arenaTime, marshalTime, findOptionsTime, registryTime, ffiDispatchTime, findCount;

    public static void printTimings() {
        if (!TIMING_ENABLED || findCount == 0) return;
        System.out.printf("Find timings (avg over %d calls):%n", findCount);
        System.out.printf("  BEFORE FFI CALL:%n");
        System.out.printf("    Arena create:    %.4f ms%n", arenaTime / 1_000_000.0 / findCount);
        System.out.printf("    Marshal (other): %.4f ms%n", marshalTime / 1_000_000.0 / findCount);
        System.out.printf("    FindOptions:     %.4f ms%n", findOptionsTime / 1_000_000.0 / findCount);
        System.out.printf("    Registry:        %.4f ms%n", registryTime / 1_000_000.0 / findCount);
        System.out.printf("    FFI dispatch:    %.4f ms%n", ffiDispatchTime / 1_000_000.0 / findCount);
        double total = (arenaTime + marshalTime + findOptionsTime + registryTime + ffiDispatchTime) / 1_000_000.0 / findCount;
        System.out.printf("  TOTAL INSTRUMENTED: %.4f ms%n", total);
    }

    @Override
    public <T> void find(MongoNamespace namespace, Bson filter, FindOptions options, Decoder<T> decoder,
                         NativeOperationContext context, @Nullable NativeAsyncClientSession session, SingleResultCallback<NativeAsyncCursor<T>> callback) {
        long t0 = TIMING_ENABLED ? System.nanoTime() : 0;
        Arena arena = Arena.ofAuto();  // GC-managed, no explicit close needed
        long t1 = TIMING_ENABLED ? System.nanoTime() : 0;
        if (TIMING_ENABLED) arenaTime += (t1 - t0);

        try {
            MemorySegment dbName = arena.allocateFrom(namespace.getDatabaseName());
            MemorySegment collName = arena.allocateFrom(namespace.getCollectionName());
            MemorySegment filterBson = BsonMarshaller.toBsonStruct(arena, filter.toBsonDocument());
            MemorySegment operationContext = buildOperationContext(arena, context, session);
            long tOpts0 = TIMING_ENABLED ? System.nanoTime() : 0;
            MemorySegment findOptions = buildFindOptions(arena, options);
            long tOpts1 = TIMING_ENABLED ? System.nanoTime() : 0;
            if (TIMING_ENABLED) findOptionsTime += (tOpts1 - tOpts0);
            long t2 = TIMING_ENABLED ? System.nanoTime() : 0;
            if (TIMING_ENABLED) marshalTime += (t2 - t1) - (tOpts1 - tOpts0);

            MemorySegment sessionPtr = session != null
                    ? ((FfmAsyncClientSession) session).getSessionPtr()
                    : MemorySegment.NULL;

            // Capture context for the result decoder
            final MemorySegment capturedClientPtr = clientPtr;

            // Register pending operation with result decoder
            PendingOperation<NativeAsyncCursor<T>> pendingOp = new PendingOperation<>(
                    callback,
                    (result) -> FfmAsyncCursor.fromCursorResult(capturedClientPtr, result, sessionPtr, decoder),
                    arena
            );
            long opId = CallbackRegistry.register(pendingOp);
            long t3 = TIMING_ENABLED ? System.nanoTime() : 0;
            if (TIMING_ENABLED) registryTime += (t3 - t2);

            MongoDbFfi.mongo_find(
                    clientPtr,
                    operationContext,
                    dbName,
                    collName,
                    filterBson,
                    findOptions,
                    findCallbackStub,
                    CallbackRegistry.toUserdata(opId));
            if (TIMING_ENABLED) {
                long t4 = System.nanoTime();
                ffiDispatchTime += (t4 - t3);
                pendingOp.setFfiDispatchEndTime(t4);
                findCount++;
            }
        } catch (Exception e) {
            callback.onResult(null, e);
        }
    }

    private MemorySegment buildFindOptions(Arena arena, FindOptions options) {
        MemorySegment opts = com.mongodb.internal.rust.crud.ffi.FindOptions.allocate(arena);

        // Tri-state booleans: -1 = not set, 0 = false, 1 = true
        com.mongodb.internal.rust.crud.ffi.FindOptions.allow_disk_use(opts,
                options.getAllowDiskUse() == null ? (byte) -1 : (byte) (options.getAllowDiskUse() ? 1 : 0));
        com.mongodb.internal.rust.crud.ffi.FindOptions.allow_partial_results(opts,
                (byte) (options.isPartial() ? 1 : 0));
        // batchSize: Java defaults to 0 when not set, but FFI uses -1 for "not set"
        int batchSize = options.getBatchSize();
        com.mongodb.internal.rust.crud.ffi.FindOptions.batch_size(opts, batchSize);

        // Comment (nullable Bson)
        com.mongodb.internal.rust.crud.ffi.FindOptions.comment(opts,
                options.getComment() != null
                        ? BsonMarshaller.toBsonValueStruct(arena, options.getComment())
                        : MemorySegment.NULL);

        // Cursor type: 0 = NonTailable, 1 = Tailable, 2 = TailableAwait
        byte cursorType = 0;
        if (options.getCursorType() != null) {
            switch (options.getCursorType()) {
                case NonTailable: cursorType = 0; break;
                case Tailable: cursorType = 1; break;
                case TailableAwait: cursorType = 2; break;
            }
        }
        com.mongodb.internal.rust.crud.ffi.FindOptions.cursor_type(opts, cursorType);

        // Hint (either string name or Bson keys)
        com.mongodb.internal.rust.crud.ffi.FindOptions.hint_name(opts,
                options.getHintString() != null ? arena.allocateFrom(options.getHintString()) : MemorySegment.NULL);
        com.mongodb.internal.rust.crud.ffi.FindOptions.hint_keys(opts,
                options.getHint() != null
                        ? BsonMarshaller.toBsonStruct(arena, options.getHint().toBsonDocument())
                        : MemorySegment.NULL);

        long limit = options.getLimit();
        com.mongodb.internal.rust.crud.ffi.FindOptions.limit(opts, limit);
        long skip = options.getSkip();
        com.mongodb.internal.rust.crud.ffi.FindOptions.skip(opts, skip);
        // maxAwaitTimeMs, maxTimeMs: pass through (0 = no timeout)
        com.mongodb.internal.rust.crud.ffi.FindOptions.max_await_time_ms(opts, options.getMaxAwaitTimeMS());
        com.mongodb.internal.rust.crud.ffi.FindOptions.max_time_ms(opts, options.getMaxTimeMS());

        // Bson options (nullable)
        com.mongodb.internal.rust.crud.ffi.FindOptions.max(opts,
                options.getMax() != null
                        ? BsonMarshaller.toBsonStruct(arena, options.getMax().toBsonDocument())
                        : MemorySegment.NULL);
        com.mongodb.internal.rust.crud.ffi.FindOptions.min(opts,
                options.getMin() != null
                        ? BsonMarshaller.toBsonStruct(arena, options.getMin().toBsonDocument())
                        : MemorySegment.NULL);

        com.mongodb.internal.rust.crud.ffi.FindOptions.no_cursor_timeout(opts,
                (byte) (options.isNoCursorTimeout() ? 1 : 0));

        com.mongodb.internal.rust.crud.ffi.FindOptions.projection(opts,
                options.getProjection() != null
                        ? BsonMarshaller.toBsonStruct(arena, options.getProjection().toBsonDocument())
                        : MemorySegment.NULL);

        com.mongodb.internal.rust.crud.ffi.FindOptions.return_key(opts,
                (byte) (options.isReturnKey() ? 1 : 0));
        com.mongodb.internal.rust.crud.ffi.FindOptions.show_record_id(opts,
                (byte) (options.isShowRecordId() ? 1 : 0));

        com.mongodb.internal.rust.crud.ffi.FindOptions.sort(opts,
                options.getSort() != null
                        ? BsonMarshaller.toBsonStruct(arena, options.getSort().toBsonDocument())
                        : MemorySegment.NULL);

        com.mongodb.internal.rust.crud.ffi.FindOptions.collation(opts,
                options.getCollation() != null
                        ? BsonMarshaller.toBsonStruct(arena, options.getCollation().asDocument())
                        : MemorySegment.NULL);

        com.mongodb.internal.rust.crud.ffi.FindOptions.let_vars(opts,
                options.getLet() != null
                        ? BsonMarshaller.toBsonStruct(arena, options.getLet().toBsonDocument())
                        : MemorySegment.NULL);

        return opts;
    }

    @Override
    public <T> void findOneAndUpdate(MongoNamespace namespace, Bson filter, Bson update, FindOneAndUpdateOptions options,
                                      Decoder<T> decoder, NativeOperationContext context, @Nullable NativeAsyncClientSession session, SingleResultCallback<T> callback) {
        throw new UnsupportedOperationException("FFI: findOneAndUpdate not yet implemented");
    }

    @Override
    public <T> void findOneAndReplace(MongoNamespace namespace, Bson filter, BsonDocument replacement, FindOneAndReplaceOptions options,
                                       Decoder<T> decoder, NativeOperationContext context, @Nullable NativeAsyncClientSession session, SingleResultCallback<T> callback) {
        throw new UnsupportedOperationException("FFI: findOneAndReplace not yet implemented");
    }

    @Override
    public <T> void findOneAndDelete(MongoNamespace namespace, Bson filter, FindOneAndDeleteOptions options,
                                      Decoder<T> decoder, NativeOperationContext context, @Nullable NativeAsyncClientSession session, SingleResultCallback<T> callback) {
        throw new UnsupportedOperationException("FFI: findOneAndDelete not yet implemented");
    }

    // ==================== Aggregate Operations ====================

    @Override
    public <T> void aggregate(MongoNamespace namespace, List<BsonDocument> pipeline, AggregateOptions options,
                              @Nullable Boolean bypassDocumentValidation, Decoder<T> decoder,
                              NativeOperationContext context, @Nullable NativeAsyncClientSession session, SingleResultCallback<NativeAsyncCursor<T>> callback) {
        throw new UnsupportedOperationException("FFI: aggregate not yet implemented");
    }

    @Override
    public <T> void aggregateDatabase(String databaseName, List<BsonDocument> pipeline, AggregateOptions options,
                                       @Nullable Boolean bypassDocumentValidation, Decoder<T> decoder,
                                       NativeOperationContext context, @Nullable NativeAsyncClientSession session, SingleResultCallback<NativeAsyncCursor<T>> callback) {
        throw new UnsupportedOperationException("FFI: aggregateDatabase not yet implemented");
    }

    // ==================== Count Operations ====================

    @Override
    public void countDocuments(MongoNamespace namespace, Bson filter, CountOptions options,
                                NativeOperationContext context, @Nullable NativeAsyncClientSession session, SingleResultCallback<Long> callback) {
        throw new UnsupportedOperationException("FFI: countDocuments not yet implemented");
    }

    @Override
    public void estimatedDocumentCount(MongoNamespace namespace, EstimatedDocumentCountOptions options,
                                        NativeOperationContext context, SingleResultCallback<Long> callback) {
        throw new UnsupportedOperationException("FFI: estimatedDocumentCount not yet implemented");
    }

    @Override
    public <T> void distinct(MongoNamespace namespace, String fieldName, Bson filter, DistinctOptions options,
                              Decoder<T> decoder, NativeOperationContext context, @Nullable NativeAsyncClientSession session,
                              SingleResultCallback<NativeAsyncCursor<T>> callback) {
        throw new UnsupportedOperationException("FFI: distinct not yet implemented");
    }

    // ==================== Index Operations ====================

    @Override
    public void createIndex(MongoNamespace namespace, Bson keys, CreateIndexOptions options,
                             NativeOperationContext context, @Nullable NativeAsyncClientSession session, SingleResultCallback<String> callback) {
        throw new UnsupportedOperationException("FFI: createIndex not yet implemented");
    }

    @Override
    public void createIndexes(MongoNamespace namespace, List<IndexModel> indexes, CreateIndexOptions options,
                               NativeOperationContext context, @Nullable NativeAsyncClientSession session, SingleResultCallback<List<String>> callback) {
        throw new UnsupportedOperationException("FFI: createIndexes not yet implemented");
    }

    @Override
    public void dropIndex(MongoNamespace namespace, String indexName, DropIndexOptions options,
                           NativeOperationContext context, @Nullable NativeAsyncClientSession session, SingleResultCallback<Void> callback) {
        throw new UnsupportedOperationException("FFI: dropIndex not yet implemented");
    }

    @Override
    public void dropIndex(MongoNamespace namespace, Bson keys, DropIndexOptions options,
                           NativeOperationContext context, @Nullable NativeAsyncClientSession session, SingleResultCallback<Void> callback) {
        throw new UnsupportedOperationException("FFI: dropIndex not yet implemented");
    }

    @Override
    public <T> void listIndexes(MongoNamespace namespace, ListIndexesOptions options, Decoder<T> decoder,
                                 NativeOperationContext context, @Nullable NativeAsyncClientSession session, SingleResultCallback<NativeAsyncCursor<T>> callback) {
        throw new UnsupportedOperationException("FFI: listIndexes not yet implemented");
    }

    // ==================== Collection Admin Operations ====================

    @Override
    public void createCollection(String databaseName, String collectionName, CreateCollectionOptions options,
                                  NativeOperationContext context, @Nullable NativeAsyncClientSession session, SingleResultCallback<Void> callback) {
        throw new UnsupportedOperationException("FFI: createCollection not yet implemented");
    }

    @Override
    public void dropCollection(MongoNamespace namespace, DropCollectionOptions options,
                                NativeOperationContext context, @Nullable NativeAsyncClientSession session, SingleResultCallback<Void> callback) {
        Arena arena = Arena.ofAuto();
        try {
            MemorySegment dbName = arena.allocateFrom(namespace.getDatabaseName());
            MemorySegment collName = arena.allocateFrom(namespace.getCollectionName());
            MemorySegment operationContext = buildOperationContext(arena, context, session);

            long opId = CallbackRegistry.register(new PendingOperation<>(
                    callback,
                    (result) -> null,  // Void result
                    arena
            ));

            MongoDbFfi.mongo_drop_collection(
                    clientPtr,
                    operationContext,
                    dbName,
                    collName,
                    dropCallbackStub,
                    CallbackRegistry.toUserdata(opId));
        } catch (Exception e) {
            callback.onResult(null, e);
        }
    }

    @Override
    public void renameCollection(MongoNamespace namespace, MongoNamespace newNamespace, RenameCollectionOptions options,
                                  NativeOperationContext context, @Nullable NativeAsyncClientSession session, SingleResultCallback<Void> callback) {
        throw new UnsupportedOperationException("FFI: renameCollection not yet implemented");
    }

    @Override
    public <T> void listCollections(String databaseName, ListCollectionsOptions options, Decoder<T> decoder,
                                     NativeOperationContext context, @Nullable NativeAsyncClientSession session, SingleResultCallback<NativeAsyncCursor<T>> callback) {
        throw new UnsupportedOperationException("FFI: listCollections not yet implemented");
    }

    @Override
    public void listCollectionNames(String databaseName, ListCollectionsOptions options,
                                     NativeOperationContext context, @Nullable NativeAsyncClientSession session, SingleResultCallback<NativeAsyncCursor<String>> callback) {
        throw new UnsupportedOperationException("FFI: listCollectionNames not yet implemented");
    }

    // ==================== Database Admin Operations ====================

    @Override
    public void dropDatabase(String databaseName, NativeOperationContext context, @Nullable NativeAsyncClientSession session, SingleResultCallback<Void> callback) {
        Arena arena = Arena.ofAuto();
        try {
            MemorySegment dbName = arena.allocateFrom(databaseName);
            MemorySegment operationContext = buildOperationContext(arena, context, session);

            long opId = CallbackRegistry.register(new PendingOperation<>(
                    callback,
                    (result) -> null,  // Void result
                    arena
            ));

            MongoDbFfi.mongo_drop_database(
                    clientPtr,
                    operationContext,
                    dbName,
                    dropCallbackStub,
                    CallbackRegistry.toUserdata(opId));
        } catch (Exception e) {
            callback.onResult(null, e);
        }
    }

    @Override
    public <T> void listDatabases(ListDatabasesOptions options, Decoder<T> decoder,
                                   NativeOperationContext context, @Nullable NativeAsyncClientSession session, SingleResultCallback<NativeAsyncCursor<T>> callback) {
        throw new UnsupportedOperationException("FFI: listDatabases not yet implemented");
    }

    @Override
    public void listDatabaseNames(ListDatabasesOptions options, NativeOperationContext context, @Nullable NativeAsyncClientSession session,
                                   SingleResultCallback<NativeAsyncCursor<String>> callback) {
        throw new UnsupportedOperationException("FFI: listDatabaseNames not yet implemented");
    }

    // ==================== Change Stream Operations ====================

    @Override
    public <T> void watchCollection(MongoNamespace namespace, List<BsonDocument> pipeline, ChangeStreamOptions options,
                                     Decoder<T> decoder, NativeOperationContext context, @Nullable NativeAsyncClientSession session,
                                     SingleResultCallback<NativeAsyncChangeStream<T>> callback) {
        throw new UnsupportedOperationException("FFI: watchCollection not yet implemented");
    }

    @Override
    public <T> void watchDatabase(String databaseName, List<BsonDocument> pipeline, ChangeStreamOptions options,
                                   Decoder<T> decoder, NativeOperationContext context, @Nullable NativeAsyncClientSession session,
                                   SingleResultCallback<NativeAsyncChangeStream<T>> callback) {
        throw new UnsupportedOperationException("FFI: watchDatabase not yet implemented");
    }

    @Override
    public <T> void watchClient(List<BsonDocument> pipeline, ChangeStreamOptions options, Decoder<T> decoder,
                                 NativeOperationContext context, @Nullable NativeAsyncClientSession session, SingleResultCallback<NativeAsyncChangeStream<T>> callback) {
        throw new UnsupportedOperationException("FFI: watchClient not yet implemented");
    }

    // ==================== Bulk Write Operations ====================

    @Override
    public void bulkWrite(MongoNamespace namespace, List<? extends WriteModel<BsonDocument>> requests,
                           BulkWriteOptions options, NativeOperationContext context, @Nullable NativeAsyncClientSession session,
                           SingleResultCallback<BulkWriteResult> callback) {
        throw new UnsupportedOperationException("FFI: bulkWrite not yet implemented");
    }

    // ==================== Lifecycle ====================

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            // Print timing instrumentation (only if enabled)
            if (TIMING_ENABLED) {
                printRunCommandTimings();
                printTimings();
                PendingOperation.printTimings();
                CallbackRegistry.printTimings();
            }

            // Destroy cached FFI handles
            readPreferenceCache.values().forEach(FfmReadPreference::destroy);
            readPreferenceCache.clear();
            writeConcernCache.values().forEach(FfmWriteConcern::destroy);
            writeConcernCache.clear();
            readConcernCache.values().forEach(FfmReadConcern::destroy);
            readConcernCache.clear();

            MongoDbFfi.mongo_client_destroy(clientPtr);
            clientArena.close();
        }
    }

    // ==================== Helpers ====================

    /**
     * Builds an FFI OperationContext from the Java NativeOperationContext and session.
     *
     * @param arena the arena to allocate in
     * @param context the operation context containing read/write concerns, read preference, and timeout
     * @param session the client session, or null
     * @return the allocated OperationContext memory segment
     */
    private MemorySegment buildOperationContext(Arena arena, NativeOperationContext context,
                                                 @Nullable NativeAsyncClientSession session) {
        MemorySegment operationContext = OperationContext.allocate(arena);
        MemorySegment sessionPtr = session != null
                ? ((FfmAsyncClientSession) session).getSessionPtr()
                : MemorySegment.NULL;
        OperationContext.session(operationContext, sessionPtr);
        OperationContext.read_preference(operationContext, getOrCreateReadPreference(context.getReadPreference()));
        OperationContext.write_concern(operationContext, getOrCreateWriteConcern(context.getWriteConcern()));
        OperationContext.read_concern(operationContext, getOrCreateReadConcern(context.getReadConcern()));
        OperationContext.timeout_ms(operationContext, context.getTimeoutMs() != null ? context.getTimeoutMs() : -1L);
        return operationContext;
    }

    private MemorySegment getOrCreateReadPreference(@Nullable ReadPreference readPreference) {
        if (readPreference == null) {
            return MemorySegment.NULL;
        }
        return readPreferenceCache.computeIfAbsent(readPreference,
                rp -> FfmReadPreference.create(clientArena, rp));
    }

    private MemorySegment getOrCreateWriteConcern(@Nullable WriteConcern writeConcern) {
        if (writeConcern == null) {
            return MemorySegment.NULL;
        }
        return writeConcernCache.computeIfAbsent(writeConcern,
                wc -> FfmWriteConcern.create(clientArena, wc));
    }

    private MemorySegment getOrCreateReadConcern(@Nullable ReadConcern readConcern) {
        if (readConcern == null) {
            return MemorySegment.NULL;
        }
        return readConcernCache.computeIfAbsent(readConcern,
                rc -> FfmReadConcern.create(clientArena, rc));
    }
}
