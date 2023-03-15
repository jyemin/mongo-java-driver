/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.client.internal;

import com.mongodb.ClientSessionOptions;
import com.mongodb.MongoClientException;
import com.mongodb.MongoException;
import com.mongodb.MongoExecutionTimeoutException;
import com.mongodb.MongoInternalException;
import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.TransactionBody;
import com.mongodb.internal.operation.AbortTransactionOperation;
import com.mongodb.internal.operation.CommitTransactionOperation;
import com.mongodb.internal.session.BaseClientSessionImpl;
import com.mongodb.internal.session.ServerSessionPool;

import static com.mongodb.MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL;
import static com.mongodb.MongoException.UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL;
import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.notNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

final class ClientSessionImpl extends BaseClientSessionImpl implements ClientSession {

    private static final int MAX_RETRY_TIME_LIMIT_MS = 120000;

    private final MongoClientDelegate delegate;

    ClientSessionImpl(final ServerSessionPool serverSessionPool, final Object originator, final ClientSessionOptions options,
                      final MongoClientDelegate delegate) {
        super(serverSessionPool, originator, options);
        this.delegate = delegate;
    }

    @Override
    public void startTransaction() {
        startTransaction(TransactionOptions.builder().build());
    }

    @Override
    public void startTransaction(final TransactionOptions transactionOptions) {
        Boolean snapshot = getOptions().isSnapshot();
        if (snapshot != null && snapshot) {
            throw new IllegalArgumentException("Transactions are not supported in snapshot sessions");
        }
        notNull("transactionOptions", transactionOptions);
        if (getTransactionState() == TransactionState.IN) {
            throw new IllegalStateException("Transaction already in progress");
        }
        if (getTransactionState() == TransactionState.COMMITTED) {
            cleanupTransaction(TransactionState.IN);
        } else {
            setTransactionState(TransactionState.IN);
        }
        getServerSession().advanceTransactionNumber();
        this.setTransactionOptions(TransactionOptions.merge(transactionOptions, getOptions().getDefaultTransactionOptions()));
        WriteConcern writeConcern = getTransactionOptions().getWriteConcern();
        if (writeConcern == null) {
            throw new MongoInternalException("Invariant violated.  Transaction options write concern can not be null");
        }
        if (!writeConcern.isAcknowledged()) {
            throw new MongoClientException("Transactions do not support unacknowledged write concern");
        }
        clearTransactionContext();
    }

    @Override
    public void commitTransaction() {
        if (getTransactionState() == TransactionState.ABORTED) {
            throw new IllegalStateException("Cannot call commitTransaction after calling abortTransaction");
        }
        if (getTransactionState() == TransactionState.NONE) {
            throw new IllegalStateException("There is no transaction started");
        }

        try {
            if (isMessageSentInCurrentTransaction()) {
                TransactionOptions transactionOptions = getTransactionOptions();
                ReadConcern readConcern = transactionOptions.getReadConcern();
                if (readConcern == null) {
                    throw new MongoInternalException("Invariant violated.  Transaction options read concern can not be null");
                }
                setCommitInProgress(true);
                delegate.getOperationExecutor().execute(new CommitTransactionOperation(assertNotNull(transactionOptions.getWriteConcern()),
                        getTransactionState() == TransactionState.COMMITTED)
                                .recoveryToken(getRecoveryToken())
                                .maxCommitTime(transactionOptions.getMaxCommitTime(MILLISECONDS), MILLISECONDS),
                        readConcern, this);
            }
        } catch (MongoException e) {
            clearTransactionContextOnError(e);
            throw e;
        } finally {
            setTransactionState(TransactionState.COMMITTED);
            setCommitInProgress(false);
        }
    }

    @Override
    public void abortTransaction() {
        if (getTransactionState() == TransactionState.ABORTED) {
            throw new IllegalStateException("Cannot call abortTransaction twice");
        }
        if (getTransactionState() == TransactionState.COMMITTED) {
            throw new IllegalStateException("Cannot call abortTransaction after calling commitTransaction");
        }
        if (getTransactionState() == TransactionState.NONE) {
            throw new IllegalStateException("There is no transaction started");
        }
        try {
            if (isMessageSentInCurrentTransaction()) {
                TransactionOptions transactionOptions = getTransactionOptions();
                ReadConcern readConcern = transactionOptions.getReadConcern();
                if (readConcern == null) {
                    throw new MongoInternalException("Invariant violated.  Transaction options read concern can not be null");
                }
                delegate.getOperationExecutor().execute(new AbortTransactionOperation(assertNotNull(transactionOptions.getWriteConcern()))
                                .recoveryToken(getRecoveryToken()),
                        readConcern, this);
            }
        } catch (RuntimeException e) {
            // ignore exceptions from abort
        } finally {
            clearTransactionContext();
            cleanupTransaction(TransactionState.ABORTED);
        }
    }

    private void clearTransactionContextOnError(final MongoException e) {
        if (e.hasErrorLabel(TRANSIENT_TRANSACTION_ERROR_LABEL) || e.hasErrorLabel(UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL)) {
            clearTransactionContext();
        }
    }

    @Override
    public <T> T withTransaction(final TransactionBody<T> transactionBody) {
        return withTransaction(transactionBody, TransactionOptions.builder().build());
    }

    @Override
    public <T> T withTransaction(final TransactionBody<T> transactionBody, final TransactionOptions options) {
        notNull("transactionBody", transactionBody);
        long startTime = ClientSessionClock.INSTANCE.now();
        outer:
        while (true) {
            T retVal;
            try {
                startTransaction(options);
                retVal = transactionBody.execute();
            } catch (Throwable e) {
                if (getTransactionState() == TransactionState.IN) {
                    abortTransaction();
                }
                if (e instanceof MongoException) {
                    if (((MongoException) e).hasErrorLabel(TRANSIENT_TRANSACTION_ERROR_LABEL)
                            && ClientSessionClock.INSTANCE.now() - startTime < MAX_RETRY_TIME_LIMIT_MS) {
                        continue;
                    }
                }
                throw e;
            }
            if (getTransactionState() == TransactionState.IN) {
                while (true) {
                    try {
                        commitTransaction();
                        break;
                    } catch (MongoException e) {
                        clearTransactionContextOnError(e);
                        if (ClientSessionClock.INSTANCE.now() - startTime < MAX_RETRY_TIME_LIMIT_MS) {
                            applyMajorityWriteConcernToTransactionOptions();

                            if (!(e instanceof MongoExecutionTimeoutException)
                                    && e.hasErrorLabel(UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL)) {
                                continue;
                            } else if (e.hasErrorLabel(TRANSIENT_TRANSACTION_ERROR_LABEL)) {
                                continue outer;
                            }
                        }
                        throw e;
                    }
                }
            }
            return retVal;
        }
    }

    // Apply majority write concern if the commit is to be retried.
    private void applyMajorityWriteConcernToTransactionOptions() {
        TransactionOptions transactionOptions = getTransactionOptionsNullable();
        if (transactionOptions != null) {
            WriteConcern writeConcern = transactionOptions.getWriteConcern();
            if (writeConcern != null) {
                setTransactionOptions(TransactionOptions.merge(TransactionOptions.builder()
                        .writeConcern(writeConcern.withW("majority")).build(), transactionOptions));
            } else {
                setTransactionOptions(TransactionOptions.merge(TransactionOptions.builder()
                        .writeConcern(WriteConcern.MAJORITY).build(), transactionOptions));
            }
        } else {
            setTransactionOptions(TransactionOptions.builder().writeConcern(WriteConcern.MAJORITY).build());
        }
    }

    @Override
    public void close() {
        try {
            if (getTransactionState() == TransactionState.IN) {
                abortTransaction();
            }
        } finally {
            clearTransactionContext();
            super.close();
        }
    }

}
