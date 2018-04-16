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
import com.mongodb.MongoInternalException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.TransactionOptions;
import com.mongodb.client.ClientSession;
import com.mongodb.internal.session.BaseClientSessionImpl;
import com.mongodb.internal.session.ServerSessionPool;
import com.mongodb.operation.AbortTransactionOperation;
import com.mongodb.operation.CommitTransactionOperation;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;

final class ClientSessionImpl extends BaseClientSessionImpl implements ClientSession {

    private final MongoClientDelegate delegate;
    private boolean inTransaction;
    private TransactionOptions transactionOptions;

    ClientSessionImpl(final ServerSessionPool serverSessionPool, final Object originator, final ClientSessionOptions options,
                      final MongoClientDelegate delegate) {
        super(serverSessionPool, originator, options);
        this.delegate = delegate;
        if (options.getAutoStartTransaction()) {
            startTransaction(options.getDefaultTransactionOptions());
        }
    }

    @Override
    public boolean hasActiveTransaction() {
        return inTransaction;
    }

    @Override
    public TransactionOptions getTransactionOptions() {
        isTrue("in transaction", inTransaction);
        return transactionOptions;
    }

    @Override
    public void startTransaction() {
        startTransaction(TransactionOptions.builder().build());
    }

    @Override
    public void startTransaction(final TransactionOptions transactionOptions) {
        notNull("transactionOptions", transactionOptions);
        if (inTransaction) {
            throw new IllegalStateException("Transaction already in progress");
        }
        inTransaction = true;
        TransactionOptions.Builder newOptionsBuilder = TransactionOptions.builder();
        newOptionsBuilder.writeConcern(transactionOptions.getWriteConcern() == null
                ? getOptions().getDefaultTransactionOptions().getWriteConcern() : transactionOptions.getWriteConcern());
        newOptionsBuilder.readConcern(transactionOptions.getReadConcern() == null
                ? getOptions().getDefaultTransactionOptions().getReadConcern() : transactionOptions.getReadConcern());
        this.transactionOptions = newOptionsBuilder.build();
    }

    @Override
    public void commitTransaction() {
        if (!inTransaction) {
            throw new IllegalStateException("There is no transaction started");
        }
        try {
            if (getServerSession().getStatementId() > 0) {
                ReadConcern readConcern = transactionOptions.getReadConcern();
                if (readConcern == null) {
                    throw new MongoInternalException("Invariant violated.  Transaction options read concern can not be null");
                }
                delegate.getOperationExecutor().execute(new CommitTransactionOperation(transactionOptions.getWriteConcern()),
                        getTransactionReadPreferenceOrPrimary(), readConcern, this);
            }
        } finally {
            cleanupTransaction();
        }
    }

    @Override
    public void abortTransaction() {
        if (!inTransaction) {
            throw new IllegalStateException("There is no transaction started");
        }
        try {
            if (getServerSession().getStatementId() > 0) {
                ReadConcern readConcern = transactionOptions.getReadConcern();
                if (readConcern == null) {
                    throw new MongoInternalException("Invariant violated.  Transaction options read concern can not be null");
                }
                delegate.getOperationExecutor().execute(new AbortTransactionOperation(transactionOptions.getWriteConcern()),
                        getTransactionReadPreferenceOrPrimary(), readConcern, this);
            }
        } catch (Exception e) {
            // ignore errors
        }
        finally {
            cleanupTransaction();
        }
    }

    @Override
    public void close() {
        try {
            if (inTransaction) {
                abortTransaction();
            }
        } finally {
            super.close();
        }
    }



    private void cleanupTransaction() {
        inTransaction = false;
        transactionOptions = null;
        if (getOptions().getAutoStartTransaction()) {
            startTransaction(getOptions().getDefaultTransactionOptions());
        }
        getServerSession().advanceTransactionNumber();
    }

    private ReadPreference getTransactionReadPreferenceOrPrimary() {
        return ReadPreference.primary();
    }
}
