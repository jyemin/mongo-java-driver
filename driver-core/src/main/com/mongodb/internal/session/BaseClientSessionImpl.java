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

package com.mongodb.internal.session;

import com.mongodb.ClientSessionOptions;
import com.mongodb.MongoClientException;
import com.mongodb.ServerAddress;
import com.mongodb.TransactionOptions;
import com.mongodb.internal.binding.ReferenceCounted;
import com.mongodb.internal.operation.CommitTransactionOperation;
import com.mongodb.internal.operation.ReadOperation;
import com.mongodb.internal.operation.WriteOperation;
import com.mongodb.lang.Nullable;
import com.mongodb.session.ClientSession;
import com.mongodb.session.ServerSession;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;

import static com.mongodb.assertions.Assertions.assertTrue;
import static com.mongodb.assertions.Assertions.isTrue;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public class BaseClientSessionImpl implements ClientSession {
    private static final String CLUSTER_TIME_KEY = "clusterTime";

    private final ServerSessionPool serverSessionPool;
    private ServerSession serverSession;
    private final Object originator;
    private final ClientSessionOptions options;
    private BsonDocument clusterTime;
    private BsonTimestamp operationTime;
    private BsonTimestamp snapshotTimestamp;
    private ServerAddress pinnedServerAddress;
    private BsonDocument recoveryToken;
    private ReferenceCounted transactionContext;
    private TransactionState transactionState = TransactionState.NONE;
    private boolean messageSentInCurrentTransaction;
    private boolean commitInProgress;
    private TransactionOptions transactionOptions;
    private volatile boolean closed;

    public BaseClientSessionImpl(final ServerSessionPool serverSessionPool, final Object originator, final ClientSessionOptions options) {
        this.serverSessionPool = serverSessionPool;
        this.originator = originator;
        this.options = options;
        this.pinnedServerAddress = null;
        closed = false;
    }

    @Override
    @Nullable
    public ServerAddress getPinnedServerAddress() {
        return pinnedServerAddress;
    }

    @Override
    public Object getTransactionContext() {
        return transactionContext;
    }

    @Override
    public void setTransactionContext(final ServerAddress address, final Object transactionContext) {
        assertTrue(transactionContext instanceof ReferenceCounted);
        pinnedServerAddress = address;
        this.transactionContext = (ReferenceCounted) transactionContext;
        this.transactionContext.retain();
    }

    @Override
    public void clearTransactionContext() {
        pinnedServerAddress = null;
        if (transactionContext != null) {
            transactionContext.release();
            transactionContext = null;
        }
    }

    @Override
    public BsonDocument getRecoveryToken() {
        return recoveryToken;
    }

    @Override
    public void setRecoveryToken(final BsonDocument recoveryToken) {
        this.recoveryToken = recoveryToken;
    }

    @Override
    public ClientSessionOptions getOptions() {
        return options;
    }

    @Override
    public boolean isCausallyConsistent() {
        Boolean causallyConsistent = options.isCausallyConsistent();
        return causallyConsistent == null || causallyConsistent;
    }

    @Override
    public Object getOriginator() {
        return originator;
    }

    @Override
    public BsonDocument getClusterTime() {
        return clusterTime;
    }

    @Override
    public BsonTimestamp getOperationTime() {
        return operationTime;
    }

    @Override
    public ServerSession getServerSession() {
        isTrue("open", !closed);
        if (serverSession == null) {
            serverSession = serverSessionPool.get();
        }
        return serverSession;
    }

    @Override
    public void advanceOperationTime(@Nullable final BsonTimestamp newOperationTime) {
        isTrue("open", !closed);
        this.operationTime = greaterOf(newOperationTime);
    }

    @Override
    public void advanceClusterTime(@Nullable final BsonDocument newClusterTime) {
        isTrue("open", !closed);
        this.clusterTime = greaterOf(newClusterTime);
    }

    @Override
    public void setSnapshotTimestamp(@Nullable final BsonTimestamp snapshotTimestamp) {
        isTrue("open", !closed);
        if (snapshotTimestamp != null) {
            if (this.snapshotTimestamp != null && !snapshotTimestamp.equals(this.snapshotTimestamp)) {
                throw new MongoClientException("Snapshot timestamps should not change during the lifetime of the session.  Current "
                        + "timestamp is " + this.snapshotTimestamp + ", and attempting to set it to " + snapshotTimestamp);
            }
            this.snapshotTimestamp = snapshotTimestamp;
        }
    }

    @Override
    @Nullable
    public BsonTimestamp getSnapshotTimestamp() {
        isTrue("open", !closed);
        return snapshotTimestamp;
    }

    private BsonDocument greaterOf(@Nullable final BsonDocument newClusterTime) {
        if (newClusterTime == null) {
            return clusterTime;
        } else if (clusterTime == null) {
            return newClusterTime;
        } else {
            return newClusterTime.getTimestamp(CLUSTER_TIME_KEY).compareTo(clusterTime.getTimestamp(CLUSTER_TIME_KEY)) > 0
                    ? newClusterTime : clusterTime;
        }
    }

    private BsonTimestamp greaterOf(@Nullable final BsonTimestamp newOperationTime) {
        if (newOperationTime == null) {
            return operationTime;
        } else if (operationTime == null) {
            return newOperationTime;
        } else {
            return newOperationTime.compareTo(operationTime) > 0 ? newOperationTime : operationTime;
        }
    }

    @Override
    public TransactionOptions getTransactionOptions() {
        isTrue("in transaction", transactionState == TransactionState.IN || transactionState == TransactionState.COMMITTED);
        return transactionOptions;
    }


    @Override
    public boolean hasActiveTransaction() {
        return transactionState == TransactionState.IN || (transactionState == TransactionState.COMMITTED && commitInProgress);
    }

    @Override
    public boolean notifyMessageSent() {
        if (hasActiveTransaction()) {
            boolean firstMessageInCurrentTransaction = !messageSentInCurrentTransaction;
            messageSentInCurrentTransaction = true;
            return firstMessageInCurrentTransaction;
        } else {
            if (transactionState == TransactionState.COMMITTED || transactionState == TransactionState.ABORTED) {
                cleanupTransaction(TransactionState.NONE);
            }
            return false;
        }
    }

    @Override
    public void notifyOperationInitiated(final Object operation) {
        assertTrue(operation instanceof ReadOperation || operation instanceof WriteOperation);
        if (!(hasActiveTransaction() || operation instanceof CommitTransactionOperation)) {
            assertTrue(getPinnedServerAddress() == null
                    || (transactionState != TransactionState.ABORTED && transactionState != TransactionState.NONE));
            clearTransactionContext();
        }
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            if (serverSession != null) {
                serverSessionPool.release(serverSession);
            }
            clearTransactionContext();
        }
    }

    protected void cleanupTransaction(final TransactionState nextState) {
        messageSentInCurrentTransaction = false;
        transactionOptions = null;
        transactionState = nextState;
    }

    protected boolean isMessageSentInCurrentTransaction() {
        return messageSentInCurrentTransaction;
    }

    protected boolean isCommitInProgress() {
        return commitInProgress;
    }

    protected void setCommitInProgress(final boolean commitInProgress) {
        this.commitInProgress = commitInProgress;
    }

    protected TransactionState getTransactionState() {
        return transactionState;
    }

    protected void setTransactionState(final TransactionState transactionState) {
        this.transactionState = transactionState;
    }

    @Nullable
    protected TransactionOptions getTransactionOptionsNullable() {
        return transactionOptions;
    }

    protected void setTransactionOptions(final TransactionOptions transactionOptions) {
        this.transactionOptions = transactionOptions;
    }

    protected enum TransactionState {
        NONE, IN, COMMITTED, ABORTED
    }
}
