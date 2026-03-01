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
import com.mongodb.lang.Nullable;
import com.mongodb.rust.crud.NativeAsyncClientSession;
import com.mongodb.rust.crud.NativeSyncClientSession;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Default implementation of NativeSyncClientSession.
 * Wraps any NativeAsyncClientSession and blocks on async operations.
 */
public final class DefaultNativeSyncClientSession implements NativeSyncClientSession {
    private final NativeAsyncClientSession asyncSession;

    public DefaultNativeSyncClientSession(NativeAsyncClientSession asyncSession) {
        this.asyncSession = asyncSession;
    }

    /**
     * Returns the underlying async session. Package-private for use by DefaultNativeSyncClient.
     */
    NativeAsyncClientSession getAsyncSession() {
        return asyncSession;
    }

    @Override
    public ClientSessionOptions getOptions() {
        return asyncSession.getOptions();
    }

    @Override
    public boolean isCausallyConsistent() {
        return asyncSession.isCausallyConsistent();
    }

    @Override
    public boolean hasActiveTransaction() {
        return asyncSession.hasActiveTransaction();
    }

    @Override
    @Nullable
    public TransactionOptions getTransactionOptions() {
        return asyncSession.getTransactionOptions();
    }

    @Override
    public void startTransaction() {
        startTransaction(TransactionOptions.builder().build());
    }

    @Override
    public void startTransaction(TransactionOptions options) {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Throwable> error = new AtomicReference<>();

        asyncSession.startTransaction(options, (result, err) -> {
            if (err != null) {
                error.set(err);
            }
            latch.countDown();
        });

        awaitLatch(latch, "starting transaction");
        throwIfError(error.get(), "Failed to start transaction");
    }

    @Override
    public void commitTransaction() {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Throwable> error = new AtomicReference<>();

        asyncSession.commitTransaction((result, err) -> {
            if (err != null) {
                error.set(err);
            }
            latch.countDown();
        });

        awaitLatch(latch, "committing transaction");
        throwIfError(error.get(), "Failed to commit transaction");
    }

    @Override
    public void abortTransaction() {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Throwable> error = new AtomicReference<>();

        asyncSession.abortTransaction((result, err) -> {
            if (err != null) {
                error.set(err);
            }
            latch.countDown();
        });

        awaitLatch(latch, "aborting transaction");
        throwIfError(error.get(), "Failed to abort transaction");
    }

    @Override
    public void close() {
        asyncSession.close();
    }

    private void awaitLatch(CountDownLatch latch, String operation) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while " + operation, e);
        }
    }

    private void throwIfError(@Nullable Throwable error, String message) {
        if (error != null) {
            if (error instanceof RuntimeException) {
                throw (RuntimeException) error;
            }
            throw new RuntimeException(message, error);
        }
    }
}

