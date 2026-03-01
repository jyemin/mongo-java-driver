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
import com.mongodb.ServerAddress;
import com.mongodb.TransactionOptions;
import com.mongodb.client.ClientSession;
import com.mongodb.client.TransactionBody;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.observability.micrometer.TransactionSpan;
import com.mongodb.lang.Nullable;
import com.mongodb.rust.crud.NativeSyncClientSession;
import com.mongodb.session.ServerSession;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * Native implementation of ClientSession that wraps NativeSyncClientSession.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class NativeClientSession implements ClientSession {

    private final NativeSyncClientSession nativeSession;
    private final Object originator;

    // State managed on the Java side
    @Nullable
    private ServerAddress pinnedServerAddress;
    @Nullable
    private Object transactionContext;
    @Nullable
    private BsonDocument recoveryToken;
    @Nullable
    private BsonTimestamp operationTime;
    @Nullable
    private BsonDocument clusterTime;
    @Nullable
    private BsonTimestamp snapshotTimestamp;

    public NativeClientSession(NativeSyncClientSession nativeSession, Object originator) {
        this.nativeSession = notNull("nativeSession", nativeSession);
        this.originator = notNull("originator", originator);
    }

    @Override
    public ClientSessionOptions getOptions() {
        return nativeSession.getOptions();
    }

    @Override
    public boolean isCausallyConsistent() {
        return nativeSession.isCausallyConsistent();
    }

    @Override
    public Object getOriginator() {
        return originator;
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
    public boolean hasActiveTransaction() {
        return nativeSession.hasActiveTransaction();
    }

    @Override
    public boolean notifyMessageSent() {
        // For internal use - track if message was sent in transaction
        return hasActiveTransaction();
    }

    @Override
    public void notifyOperationInitiated(Object operation) {
        // For internal use - notification before server selection
    }

    @Override
    @Nullable
    public TransactionOptions getTransactionOptions() {
        return nativeSession.getTransactionOptions();
    }

    @Override
    public void startTransaction() {
        nativeSession.startTransaction();
    }

    @Override
    public void startTransaction(TransactionOptions transactionOptions) {
        nativeSession.startTransaction(transactionOptions);
    }

    @Override
    public void commitTransaction() {
        nativeSession.commitTransaction();
    }

    @Override
    public void abortTransaction() {
        nativeSession.abortTransaction();
    }

    @Override
    public <T> T withTransaction(TransactionBody<T> transactionBody) {
        return withTransaction(transactionBody, TransactionOptions.builder().build());
    }

    @Override
    public <T> T withTransaction(TransactionBody<T> transactionBody, TransactionOptions options) {
        startTransaction(options);
        try {
            T result = transactionBody.execute();
            commitTransaction();
            return result;
        } catch (RuntimeException e) {
            abortTransaction();
            throw e;
        }
    }

    @Override
    @Nullable
    public TransactionSpan getTransactionSpan() {
        // TODO: Implement transaction span for tracing
        return null;
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

    @Override
    @Nullable
    public TimeoutContext getTimeoutContext() {
        // TODO: Implement timeout context
        return null;
    }

    @Override
    public void close() {
        nativeSession.close();
    }

    /**
     * Returns the underlying native session for internal use.
     */
    NativeSyncClientSession getNativeSession() {
        return nativeSession;
    }
}

