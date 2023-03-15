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

package com.mongodb.client;

import com.mongodb.TransactionOptions;

/**
 * A client session that supports transactions.
 *
 * @since 3.8
 */
public interface ClientSession extends com.mongodb.session.ClientSession {
    /**
     * Start a transaction in the context of this session with default transaction options. A transaction can not be started if there is
     * already an active transaction on this session.
     *
     * @mongodb.server.release 4.0
     */
    void startTransaction();

    /**
     * Start a transaction in the context of this session with the given transaction options. A transaction can not be started if there is
     * already an active transaction on this session.
     *
     * @param transactionOptions the options to apply to the transaction
     *
     * @mongodb.server.release 4.0
     */
    void startTransaction(TransactionOptions transactionOptions);

    /**
     * Commit a transaction in the context of this session.  A transaction can only be commmited if one has first been started.
     *
     * @mongodb.server.release 4.0
     */
    void commitTransaction();

    /**
     * Abort a transaction in the context of this session.  A transaction can only be aborted if one has first been started.
     *
     * @mongodb.server.release 4.0
     */
    void abortTransaction();

    /**
     * Execute the given function within a transaction.
     *
     * @param <T> the return type of the transaction body
     * @param transactionBody the body of the transaction
     * @return the return value of the transaction body
     * @mongodb.server.release 4.0
     * @since 3.11
     */
    <T> T withTransaction(TransactionBody<T> transactionBody);

    /**
     * Execute the given function within a transaction.
     *
     * @param <T> the return type of the transaction body
     * @param transactionBody the body of the transaction
     * @param options         the transaction options
     * @return the return value of the transaction body
     * @mongodb.server.release 4.0
     * @since 3.11
     */
    <T> T withTransaction(TransactionBody<T> transactionBody, TransactionOptions options);
}
