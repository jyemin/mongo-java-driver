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
import com.mongodb.ServerAddress;
import com.mongodb.TransactionOptions;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.observability.micrometer.TransactionSpan;
import com.mongodb.lang.Nullable;
import com.mongodb.reactivestreams.client.ClientSession;
import com.mongodb.rust.crud.NativeAsyncClientSession;
import com.mongodb.session.ServerSession;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.reactivestreams.Publisher;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * Native implementation of ClientSession using Rust FFI.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class NativeClientSession implements ClientSession {

    private final NativeAsyncClientSession nativeSession;
    private final NativeMongoClient client;
    private final ClientSessionOptions options;
    private boolean closed = false;

    // Session state managed on Java side
    @Nullable private ServerAddress pinnedServerAddress;
    @Nullable private Object transactionContext;
    @Nullable private BsonDocument recoveryToken;
    @Nullable private BsonTimestamp operationTime;
    @Nullable private BsonDocument clusterTime;
    @Nullable private BsonTimestamp snapshotTimestamp;
    private boolean hasActiveTransaction = false;
    private TransactionOptions transactionOptions = TransactionOptions.builder().build();

    NativeClientSession(NativeAsyncClientSession nativeSession, NativeMongoClient client, ClientSessionOptions options) {
        this.nativeSession = notNull("nativeSession", nativeSession);
        this.client = notNull("client", client);
        this.options = notNull("options", options);
    }

    NativeAsyncClientSession getNativeSession() {
        return nativeSession;
    }

    @Override
    public ClientSessionOptions getOptions() {
        return options;
    }

    @Override
    public boolean isCausallyConsistent() {
        return options.isCausallyConsistent() != null && options.isCausallyConsistent();
    }

    @Override
    public Object getOriginator() {
        return client;
    }

    @Override
    @Nullable
    public ServerAddress getPinnedServerAddress() {
        return pinnedServerAddress;
    }

    @Override
    @Nullable
    public Object getTransactionContext() {
        return transactionContext;
    }

    @Override
    public void setTransactionContext(ServerAddress address, Object transactionContext) {
        this.pinnedServerAddress = address;
        this.transactionContext = transactionContext;
    }

    @Override
    public void clearTransactionContext() {
        this.pinnedServerAddress = null;
        this.transactionContext = null;
    }

    @Override
    @Nullable
    public BsonDocument getRecoveryToken() {
        return recoveryToken;
    }

    @Override
    public void setRecoveryToken(BsonDocument recoveryToken) {
        this.recoveryToken = recoveryToken;
    }

    @Override
    public ServerSession getServerSession() {
        // TODO: Implement server session
        throw new UnsupportedOperationException("getServerSession not yet implemented");
    }

    @Override
    @Nullable
    public BsonTimestamp getOperationTime() {
        return operationTime;
    }

    @Override
    public void advanceOperationTime(@Nullable BsonTimestamp operationTime) {
        if (operationTime != null) {
            if (this.operationTime == null || operationTime.compareTo(this.operationTime) > 0) {
                this.operationTime = operationTime;
            }
        }
    }

    @Override
    public void advanceClusterTime(@Nullable BsonDocument clusterTime) {
        this.clusterTime = clusterTime;
    }

    @Override
    public void setSnapshotTimestamp(@Nullable BsonTimestamp snapshotTimestamp) {
        this.snapshotTimestamp = snapshotTimestamp;
    }

    @Override
    @Nullable
    public BsonTimestamp getSnapshotTimestamp() {
        return snapshotTimestamp;
    }

    @Override
    @Nullable
    public BsonDocument getClusterTime() {
        return clusterTime;
    }

    // ==================== Transaction Operations ====================

    @Override
    public boolean hasActiveTransaction() {
        return hasActiveTransaction;
    }

    @Override
    public boolean notifyMessageSent() {
        return true;
    }

    @Override
    public void notifyOperationInitiated(Object operation) {
        // No-op for native implementation
    }

    @Override
    public TransactionOptions getTransactionOptions() {
        return transactionOptions;
    }

    @Override
    public void startTransaction() {
        startTransaction(TransactionOptions.builder().build());
    }

    @Override
    public void startTransaction(TransactionOptions transactionOptions) {
        this.transactionOptions = transactionOptions;
        this.hasActiveTransaction = true;
        // Note: The async startTransaction is fire-and-forget for the sync wrapper
        nativeSession.startTransaction(transactionOptions, (result, error) -> {
            if (error != null) {
                hasActiveTransaction = false;
                throw new RuntimeException("Failed to start transaction", error);
            }
        });
    }

    @Override
    public Publisher<Void> commitTransaction() {
        return Publishers.toMonoVoid(callback ->
            nativeSession.commitTransaction((result, error) -> {
                if (error == null) {
                    hasActiveTransaction = false;
                }
                callback.onResult(null, error);
            }));
    }

    @Override
    public Publisher<Void> abortTransaction() {
        return Publishers.toMonoVoid(callback ->
            nativeSession.abortTransaction((result, error) -> {
                hasActiveTransaction = false;
                callback.onResult(null, error);
            }));
    }

    @Override
    @Nullable
    public TransactionSpan getTransactionSpan() {
        return null;
    }

    @Override
    @Nullable
    public TimeoutContext getTimeoutContext() {
        return null;
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            nativeSession.close();
        }
    }
}

