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
import com.mongodb.TransactionOptions;
import com.mongodb.internal.rust.crud.ffi.MongoDbFfi;
import com.mongodb.lang.Nullable;
import com.mongodb.rust.crud.NativeAsyncClientSession;
import com.mongodb.rust.crud.SingleResultCallback;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * FFM implementation of NativeAsyncClientSession.
 */
public final class FfmAsyncClientSession implements NativeAsyncClientSession {
    private final MemorySegment clientPtr;
    private final MemorySegment sessionPtr;
    private final ClientSessionOptions options;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    
    private volatile boolean hasActiveTransaction = false;
    private volatile TransactionOptions transactionOptions;

    public FfmAsyncClientSession(MemorySegment clientPtr, MemorySegment sessionPtr, ClientSessionOptions options) {
        this.clientPtr = clientPtr;
        this.sessionPtr = sessionPtr;
        this.options = options;
    }

    /**
     * Returns the FFI session pointer. Package-private for use by FfmAsyncClient.
     */
    MemorySegment getSessionPtr() {
        if (closed.get()) {
            throw new IllegalStateException("Session is closed");
        }
        return sessionPtr;
    }

    @Override
    public ClientSessionOptions getOptions() {
        return options;
    }

    @Override
    public boolean isCausallyConsistent() {
        Boolean cc = options.isCausallyConsistent();
        return cc != null && cc;
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
        if (hasActiveTransaction) {
            callback.completeExceptionally(new IllegalStateException("Transaction already in progress"));
            return;
        }
        if (closed.get()) {
            callback.completeExceptionally(new IllegalStateException("Session is closed"));
            return;
        }

        this.transactionOptions = options;
        
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment opts = OptionsMarshaller.toTransactionOptions(arena, options);
            MemorySegment cb = CallbackBridge.createVoidCallback(arena, (result, error) -> {
                if (error != null) {
                    this.transactionOptions = null;
                    callback.completeExceptionally(error);
                } else {
                    hasActiveTransaction = true;
                    callback.complete(null);
                }
            });
            
            MongoDbFfi.mongo_session_start_transaction(clientPtr, sessionPtr, opts, MemorySegment.NULL, cb);
        }
    }

    @Override
    public void commitTransaction(SingleResultCallback<Void> callback) {
        if (!hasActiveTransaction) {
            callback.completeExceptionally(new IllegalStateException("No transaction in progress"));
            return;
        }

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment cb = CallbackBridge.createVoidCallback(arena, (result, error) -> {
                if (error != null) {
                    callback.completeExceptionally(error);
                } else {
                    hasActiveTransaction = false;
                    transactionOptions = null;
                    callback.complete(null);
                }
            });
            
            MongoDbFfi.mongo_session_commit_transaction(clientPtr, sessionPtr, MemorySegment.NULL, cb);
        }
    }

    @Override
    public void abortTransaction(SingleResultCallback<Void> callback) {
        if (!hasActiveTransaction) {
            callback.completeExceptionally(new IllegalStateException("No transaction in progress"));
            return;
        }

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment cb = CallbackBridge.createVoidCallback(arena, (result, error) -> {
                if (error != null) {
                    callback.completeExceptionally(error);
                } else {
                    hasActiveTransaction = false;
                    transactionOptions = null;
                    callback.complete(null);
                }
            });
            
            MongoDbFfi.mongo_session_abort_transaction(clientPtr, sessionPtr, MemorySegment.NULL, cb);
        }
    }

    @Override
    public void closeAsync(SingleResultCallback<Void> callback) {
        if (!closed.compareAndSet(false, true)) {
            callback.complete(null);
            return;
        }

        // Abort any active transaction first
        if (hasActiveTransaction) {
            abortTransaction((result, abortError) -> {
                // Continue with session end regardless of abort result
                endSession(callback);
            });
        } else {
            endSession(callback);
        }
    }

    private void endSession(SingleResultCallback<Void> callback) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment cb = CallbackBridge.createVoidCallback(arena, (result, error) -> {
                // Ignore errors on close
                callback.complete(null);
            });

            MongoDbFfi.mongo_session_end(clientPtr, sessionPtr, MemorySegment.NULL, cb);
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        // Abort any active transaction first (blocking)
        if (hasActiveTransaction) {
            CountDownLatch abortLatch = new CountDownLatch(1);
            abortTransaction((result, error) -> abortLatch.countDown());
            try {
                abortLatch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        // End session (blocking)
        CountDownLatch latch = new CountDownLatch(1);
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment cb = CallbackBridge.createVoidCallback(arena, (result, error) -> {
                latch.countDown();
            });

            MongoDbFfi.mongo_session_end(clientPtr, sessionPtr, MemorySegment.NULL, cb);
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}

