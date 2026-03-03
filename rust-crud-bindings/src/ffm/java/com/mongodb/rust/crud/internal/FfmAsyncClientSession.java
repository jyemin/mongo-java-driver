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
import com.mongodb.MongoException;
import com.mongodb.TransactionOptions;
import com.mongodb.internal.rust.crud.ffi.MongoDbFfi;
import com.mongodb.internal.rust.crud.ffi.TransactionCallback;
import com.mongodb.internal.rust.crud.ffi.TransactionOptionsFFI;
import com.mongodb.lang.Nullable;
import com.mongodb.rust.crud.NativeAsyncClientSession;
import com.mongodb.rust.crud.SingleResultCallback;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * FFM implementation of NativeAsyncClientSession.
 *
 * <p>TODO: Track operation time and cluster time for causal consistency
 */
public final class FfmAsyncClientSession implements NativeAsyncClientSession {

    private final MemorySegment clientPtr;
    private final MemorySegment sessionPtr;
    private final ClientSessionOptions options;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private volatile boolean hasActiveTransaction = false;
    @Nullable
    private volatile TransactionOptions transactionOptions;

    FfmAsyncClientSession(MemorySegment clientPtr, MemorySegment sessionPtr, ClientSessionOptions options) {
        this.clientPtr = clientPtr;
        this.sessionPtr = sessionPtr;
        this.options = options;
    }

    MemorySegment getSessionPtr() {
        return sessionPtr;
    }

    @Override
    public ClientSessionOptions getOptions() {
        return options;
    }

    @Override
    public boolean isCausallyConsistent() {
        Boolean causal = options.isCausallyConsistent();
        return causal != null && causal;
    }

    @Override
    public boolean hasActiveTransaction() {
        return hasActiveTransaction;
    }

    @Override
    @Nullable
    public TransactionOptions getTransactionOptions() {
        return transactionOptions;
    }

    @Override
    public void startTransaction(TransactionOptions options, SingleResultCallback<Void> callback) {
        Arena arena = Arena.ofShared();
        try {
            MemorySegment txnOptions = toTransactionOptionsFFI(arena, options);
            MemorySegment callbackPtr = TransactionCallback.allocate(
                    (userdata, error) -> {
                        try {
                            if (error.address() != 0) {
                                callback.onResult(null, FfmErrorMapper.mapError(error));
                            } else {
                                hasActiveTransaction = true;
                                transactionOptions = options;
                                callback.onResult(null, null);
                            }
                        } finally {
                            arena.close();
                        }
                    },
                    arena);

            MongoDbFfi.mongo_session_start_transaction(clientPtr, sessionPtr, txnOptions, callbackPtr, MemorySegment.NULL);
        } catch (Exception e) {
            arena.close();
            callback.onResult(null, e);
        }
    }

    @Override
    public void commitTransaction(SingleResultCallback<Void> callback) {
        Arena arena = Arena.ofShared();
        try {
            MemorySegment callbackPtr = TransactionCallback.allocate(
                    (userdata, error) -> {
                        try {
                            if (error.address() != 0) {
                                callback.onResult(null, FfmErrorMapper.mapError(error));
                            } else {
                                hasActiveTransaction = false;
                                transactionOptions = null;
                                callback.onResult(null, null);
                            }
                        } finally {
                            arena.close();
                        }
                    },
                    arena);

            MongoDbFfi.mongo_session_commit_transaction(clientPtr, sessionPtr, callbackPtr, MemorySegment.NULL);
        } catch (Exception e) {
            arena.close();
            callback.onResult(null, e);
        }
    }

    @Override
    public void abortTransaction(SingleResultCallback<Void> callback) {
        Arena arena = Arena.ofShared();
        try {
            MemorySegment callbackPtr = TransactionCallback.allocate(
                    (userdata, error) -> {
                        try {
                            if (error.address() != 0) {
                                callback.onResult(null, FfmErrorMapper.mapError(error));
                            } else {
                                hasActiveTransaction = false;
                                transactionOptions = null;
                                callback.onResult(null, null);
                            }
                        } finally {
                            arena.close();
                        }
                    },
                    arena);

            MongoDbFfi.mongo_session_abort_transaction(clientPtr, sessionPtr, callbackPtr, MemorySegment.NULL);
        } catch (Exception e) {
            arena.close();
            callback.onResult(null, e);
        }
    }

    @Override
    public void closeAsync(SingleResultCallback<Void> callback) {
        if (closed.compareAndSet(false, true)) {
            MongoDbFfi.mongo_session_end(sessionPtr);
        }
        callback.onResult(null, null);
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            MongoDbFfi.mongo_session_end(sessionPtr);
        }
    }

    @Nullable
    static MemorySegment toTransactionOptionsFFI(Arena arena, @Nullable TransactionOptions options) {
        if (options == null) {
            return MemorySegment.NULL;
        }
        MemorySegment struct = TransactionOptionsFFI.allocate(arena);

        // read_concern_level
        if (options.getReadConcern() != null && options.getReadConcern().getLevel() != null) {
            TransactionOptionsFFI.read_concern_level(struct, arena.allocateFrom(options.getReadConcern().getLevel().getValue()));
        } else {
            TransactionOptionsFFI.read_concern_level(struct, MemorySegment.NULL);
        }

        // write_concern - simplified, just w value
        if (options.getWriteConcern() != null && options.getWriteConcern().getWObject() instanceof Integer) {
            TransactionOptionsFFI.write_concern_w(struct, (Integer) options.getWriteConcern().getWObject());
        } else {
            TransactionOptionsFFI.write_concern_w(struct, -1); // not set
        }

        // write_concern_w_tag
        if (options.getWriteConcern() != null && options.getWriteConcern().getWString() != null) {
            TransactionOptionsFFI.write_concern_w_tag(struct, arena.allocateFrom(options.getWriteConcern().getWString()));
        } else {
            TransactionOptionsFFI.write_concern_w_tag(struct, MemorySegment.NULL);
        }

        // write_concern_j
        if (options.getWriteConcern() != null && options.getWriteConcern().getJournal() != null) {
            TransactionOptionsFFI.write_concern_j(struct, (byte) (options.getWriteConcern().getJournal() ? 1 : 0));
        } else {
            TransactionOptionsFFI.write_concern_j(struct, (byte) -1); // not set
        }

        // write_concern_w_timeout_ms
        if (options.getWriteConcern() != null && options.getWriteConcern().getWTimeout(java.util.concurrent.TimeUnit.MILLISECONDS) != null) {
            TransactionOptionsFFI.write_concern_w_timeout_ms(struct, options.getWriteConcern().getWTimeout(java.util.concurrent.TimeUnit.MILLISECONDS));
        } else {
            TransactionOptionsFFI.write_concern_w_timeout_ms(struct, -1L); // not set
        }

        // read_preference_mode
        if (options.getReadPreference() != null) {
            TransactionOptionsFFI.read_preference_mode(struct, FfmAsyncClient.toReadPreferenceMode(options.getReadPreference()));
        } else {
            TransactionOptionsFFI.read_preference_mode(struct, (byte) 0); // Primary
        }

        // max_commit_time_ms
        if (options.getMaxCommitTime(java.util.concurrent.TimeUnit.MILLISECONDS) != null) {
            TransactionOptionsFFI.max_commit_time_ms(struct, options.getMaxCommitTime(java.util.concurrent.TimeUnit.MILLISECONDS));
        } else {
            TransactionOptionsFFI.max_commit_time_ms(struct, -1L); // not set
        }

        return struct;
    }
}

