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

package com.mongodb.internal.operation;

import com.mongodb.MongoCommandException;
import com.mongodb.MongoNamespace;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ServerDescription;
import com.mongodb.internal.async.AsyncBatchCursor;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.async.function.AsyncCallbackSupplier;
import com.mongodb.internal.async.function.RetryState;
import com.mongodb.internal.binding.AsyncConnectionSource;
import com.mongodb.internal.binding.AsyncReadBinding;
import com.mongodb.internal.binding.ConnectionSource;
import com.mongodb.internal.binding.ReadBinding;
import com.mongodb.internal.connection.AsyncConnection;
import com.mongodb.internal.connection.Connection;
import com.mongodb.internal.operation.CommandOperationHelper.CommandReadTransformer;
import com.mongodb.internal.operation.CommandOperationHelper.CommandReadTransformerAsync;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.codecs.Codec;
import org.bson.codecs.Decoder;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.internal.operation.CommandOperationHelper.CommandCreator;
import static com.mongodb.internal.operation.CommandOperationHelper.createReadCommandAndExecute;
import static com.mongodb.internal.operation.CommandOperationHelper.createReadCommandAndExecuteAsync;
import static com.mongodb.internal.operation.CommandOperationHelper.decorateReadWithRetries;
import static com.mongodb.internal.operation.CommandOperationHelper.initialRetryState;
import static com.mongodb.internal.operation.CommandOperationHelper.isNamespaceError;
import static com.mongodb.internal.operation.CommandOperationHelper.logRetryExecute;
import static com.mongodb.internal.operation.CommandOperationHelper.rethrowIfNotNamespaceError;
import static com.mongodb.internal.operation.CursorHelper.getCursorDocumentFromBatchSize;
import static com.mongodb.internal.operation.DocumentHelper.putIfNotNull;
import static com.mongodb.internal.operation.OperationHelper.LOGGER;
import static com.mongodb.internal.operation.OperationHelper.canRetryRead;
import static com.mongodb.internal.operation.OperationHelper.createEmptyAsyncBatchCursor;
import static com.mongodb.internal.operation.OperationHelper.createEmptyBatchCursor;
import static com.mongodb.internal.operation.OperationHelper.cursorDocumentToAsyncBatchCursor;
import static com.mongodb.internal.operation.OperationHelper.cursorDocumentToBatchCursor;
import static com.mongodb.internal.operation.OperationHelper.withAsyncSourceAndConnection;
import static com.mongodb.internal.operation.OperationHelper.withSourceAndConnection;

/**
 * An operation that lists the indexes that have been created on a collection.  For flexibility, the type of each document returned is
 * generic.
 *
 * @param <T> the operations result type.
 * @since 3.0
 * @mongodb.driver.manual reference/command/listIndexes/ List indexes
 */
public class ListIndexesOperation<T> implements AsyncReadOperation<AsyncBatchCursor<T>>, ReadOperation<BatchCursor<T>> {
    private final MongoNamespace namespace;
    private final Decoder<T> decoder;
    private boolean retryReads;
    private int batchSize;
    private long maxTimeMS;
    private BsonValue comment;

    /**
     * Construct a new instance.
     *
     * @param namespace the database and collection namespace for the operation.
     * @param decoder   the decoder for the result documents.
     */
    public ListIndexesOperation(final MongoNamespace namespace, final Decoder<T> decoder) {
        this.namespace = notNull("namespace", namespace);
        this.decoder = notNull("decoder", decoder);
    }

    /**
     * Gets the number of documents to return per batch.
     *
     * @return the batch size
     * @mongodb.server.release 3.0
     * @mongodb.driver.manual reference/method/cursor.batchSize/#cursor.batchSize Batch Size
     */
    public Integer getBatchSize() {
        return batchSize;
    }

    /**
     * Sets the number of documents to return per batch.
     *
     * @param batchSize the batch size
     * @return this
     * @mongodb.server.release 3.0
     * @mongodb.driver.manual reference/method/cursor.batchSize/#cursor.batchSize Batch Size
     */
    public ListIndexesOperation<T> batchSize(final int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    /**
     * Gets the maximum execution time on the server for this operation.  The default is 0, which places no limit on the execution time.
     *
     * @param timeUnit the time unit to return the result in
     * @return the maximum execution time in the given time unit
     * @mongodb.driver.manual reference/operator/meta/maxTimeMS/ Max Time
     */
    public long getMaxTime(final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        return timeUnit.convert(maxTimeMS, TimeUnit.MILLISECONDS);
    }

    /**
     * Sets the maximum execution time on the server for this operation.
     *
     * @param maxTime  the max time
     * @param timeUnit the time unit, which may not be null
     * @return this
     * @mongodb.driver.manual reference/operator/meta/maxTimeMS/ Max Time
     */
    public ListIndexesOperation<T> maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxTimeMS = TimeUnit.MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    /**
     * Enables retryable reads if a read fails due to a network error.
     *
     * @param retryReads true if reads should be retried
     * @return this
     * @since 3.11
     */
    public ListIndexesOperation<T> retryReads(final boolean retryReads) {
        this.retryReads = retryReads;
        return this;
    }

    /**
     * Gets the value for retryable reads. The default is true.
     *
     * @return the retryable reads value
     * @since 3.11
     */
    public boolean getRetryReads() {
        return retryReads;
    }

    /**
     * @return the comment for this operation. A null value means no comment is set.
     * @since 4.6
     * @mongodb.server.release 4.4
     */
    @Nullable
    public BsonValue getComment() {
        return comment;
    }

    /**
     * Sets the comment for this operation. A null value means no comment is set.
     *
     * @param comment the comment
     * @return this
     * @since 4.6
     * @mongodb.server.release 4.4
     */
    public ListIndexesOperation<T> comment(@Nullable final BsonValue comment) {
        this.comment = comment;
        return this;
    }

    @Override
    public BatchCursor<T> execute(final ReadBinding binding) {
        RetryState retryState = initialRetryState(retryReads);
        Supplier<BatchCursor<T>> read = decorateReadWithRetries(retryState, () -> {
            logRetryExecute(retryState);
            return withSourceAndConnection(binding::getReadConnectionSource, false, (source, connection) -> {
                retryState.breakAndThrowIfRetryAnd(() -> !canRetryRead(source.getServerDescription(), connection.getDescription(),
                        binding.getSessionContext()));
                try {
                    return createReadCommandAndExecute(retryState, binding, source, namespace.getDatabaseName(), getCommandCreator(),
                            createCommandDecoder(), transformer(), connection);
                } catch (MongoCommandException e) {
                    return rethrowIfNotNamespaceError(e, createEmptyBatchCursor(namespace, decoder,
                            source.getServerDescription().getAddress(), batchSize));
                }
            });
        });
        return read.get();
    }

    @Override
    public void executeAsync(final AsyncReadBinding binding, final SingleResultCallback<AsyncBatchCursor<T>> callback) {
        RetryState retryState = initialRetryState(retryReads);
        binding.retain();
        AsyncCallbackSupplier<AsyncBatchCursor<T>> asyncRead = CommandOperationHelper.<AsyncBatchCursor<T>>decorateReadWithRetries(
                retryState, funcCallback -> {
                    logRetryExecute(retryState);
                    withAsyncSourceAndConnection(binding::getReadConnectionSource, false, funcCallback,
                            (source, connection, releasingCallback) -> {
                                if (retryState.breakAndCompleteIfRetryAnd(() -> !canRetryRead(source.getServerDescription(), connection.getDescription(),
                                        binding.getSessionContext()), releasingCallback)) {
                                    return;
                                }
                                createReadCommandAndExecuteAsync(retryState, binding, source, namespace.getDatabaseName(), getCommandCreator(),
                                        createCommandDecoder(), asyncTransformer(), connection, (result, t) -> {
                                            if (t != null && !isNamespaceError(t)) {
                                                releasingCallback.onResult(null, t);
                                            } else {
                                                releasingCallback.onResult(result != null ? result : emptyAsyncCursor(source), null);
                                            }
                                        });
                            });
                }).whenComplete(binding::release);
        asyncRead.get(errorHandlingCallback(callback, LOGGER));
    }

    private AsyncBatchCursor<T> emptyAsyncCursor(final AsyncConnectionSource source) {
        return createEmptyAsyncBatchCursor(namespace, source.getServerDescription().getAddress());
    }

    private CommandCreator getCommandCreator() {
        return new CommandCreator() {
            @Override
            public BsonDocument create(final ServerDescription serverDescription, final ConnectionDescription connectionDescription) {
                return getCommand();
            }
        };
    }

    private BsonDocument getCommand() {
        BsonDocument command = new BsonDocument("listIndexes", new BsonString(namespace.getCollectionName()))
                .append("cursor", getCursorDocumentFromBatchSize(batchSize == 0 ? null : batchSize));
        if (maxTimeMS > 0) {
            command.put("maxTimeMS", new BsonInt64(maxTimeMS));
        }
        putIfNotNull(command, "comment", comment);
        return command;
    }

    private CommandReadTransformer<BsonDocument, BatchCursor<T>> transformer() {
        return new CommandReadTransformer<BsonDocument, BatchCursor<T>>() {
            @Override
            public BatchCursor<T> apply(final BsonDocument result, final ConnectionSource source, final Connection connection) {
                return cursorDocumentToBatchCursor(result.getDocument("cursor"), decoder, comment, source, connection, batchSize);
            }
        };
    }

    private CommandReadTransformerAsync<BsonDocument, AsyncBatchCursor<T>> asyncTransformer() {
        return new CommandReadTransformerAsync<BsonDocument, AsyncBatchCursor<T>>() {
            @Override
            public AsyncBatchCursor<T> apply(final BsonDocument result, final AsyncConnectionSource source,
                                             final AsyncConnection connection) {
                return cursorDocumentToAsyncBatchCursor(result.getDocument("cursor"), decoder, comment, source, connection, batchSize);
            }
        };
    }

    private Codec<BsonDocument> createCommandDecoder() {
        return CommandResultDocumentCodec.create(decoder, "firstBatch");
    }
}
