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
package com.mongodb.rust.crud;

import com.mongodb.ClientSessionOptions;
import com.mongodb.TransactionOptions;
import com.mongodb.lang.Nullable;

import java.io.Closeable;

/**
 * Native async client session interface.
 * 
 * <p>This interface abstracts session operations for the native (Rust) driver.
 * Implementations may use FFM or JNI to communicate with the Rust driver.</p>
 */
public interface NativeAsyncClientSession extends Closeable {

    /**
     * Gets the session options.
     */
    ClientSessionOptions getOptions();

    /**
     * Returns whether this session is causally consistent.
     */
    boolean isCausallyConsistent();

    /**
     * Returns whether there is an active transaction.
     */
    boolean hasActiveTransaction();

    /**
     * Gets the current transaction options, or null if no transaction is active.
     */
    @Nullable
    TransactionOptions getTransactionOptions();

    /**
     * Starts a new transaction asynchronously.
     */
    void startTransaction(TransactionOptions options, SingleResultCallback<Void> callback);

    /**
     * Commits the active transaction asynchronously.
     */
    void commitTransaction(SingleResultCallback<Void> callback);

    /**
     * Aborts the active transaction asynchronously.
     */
    void abortTransaction(SingleResultCallback<Void> callback);

    /**
     * Closes this session asynchronously.
     */
    void closeAsync(SingleResultCallback<Void> callback);

    /**
     * Closes this session synchronously (convenience method).
     */
    @Override
    void close();
}

