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

import com.mongodb.MongoNamespace;
import com.mongodb.client.cursor.TimeoutMode;
import com.mongodb.internal.async.AsyncBatchCursor;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.binding.AsyncReadBinding;
import com.mongodb.internal.binding.ReadBinding;
import com.mongodb.internal.connection.OperationContext;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.codecs.Decoder;

import java.util.Arrays;
import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.internal.connection.CommandHelper.applyMaxTimeMS;
import static com.mongodb.internal.operation.AsyncOperationHelper.CommandReadTransformerAsync;
import static com.mongodb.internal.operation.AsyncOperationHelper.executeRetryableReadAsync;
import static com.mongodb.internal.operation.CommandOperationHelper.CommandCreator;
import static com.mongodb.internal.operation.OperationHelper.LOGGER;
import static com.mongodb.internal.operation.OperationHelper.applyTimeoutModeToOperationContext;
import static com.mongodb.internal.operation.SyncOperationHelper.CommandReadTransformer;
import static com.mongodb.internal.operation.SyncOperationHelper.executeRetryableRead;

class Mqlv2OperationImpl<T> implements ReadOperationCursor<T> {
    private static final String COMMAND_NAME = "mqlv2";
    private static final String CURSOR = "cursor";
    private static final String FIRST_BATCH = "firstBatch";
    private static final List<String> FIELD_NAMES_WITH_RESULT = Arrays.asList(FIRST_BATCH);

    private final String databaseName;
    private final String mqlv2Source;
    private final Decoder<T> decoder;

    private boolean retryReads;
    @Nullable private Integer batchSize;
    @Nullable private Long maxTimeMS;
    @Nullable private TimeoutMode timeoutMode;

    Mqlv2OperationImpl(final String databaseName, final String mqlv2Source, final Decoder<T> decoder) {
        this.databaseName = notNull("databaseName", databaseName);
        this.mqlv2Source = notNull("mqlv2Source", mqlv2Source);
        this.decoder = notNull("decoder", decoder);
    }

    Integer getBatchSize() {
        return batchSize;
    }

    Mqlv2OperationImpl<T> batchSize(@Nullable final Integer batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    boolean getRetryReads() {
        return retryReads;
    }

    Mqlv2OperationImpl<T> retryReads(final boolean retryReads) {
        this.retryReads = retryReads;
        return this;
    }

    @Nullable
    Long getMaxTimeMS() {
        return maxTimeMS;
    }

    Mqlv2OperationImpl<T> maxTimeMS(@Nullable final Long maxTimeMS) {
        this.maxTimeMS = maxTimeMS;
        return this;
    }

    Mqlv2OperationImpl<T> timeoutMode(@Nullable final TimeoutMode timeoutMode) {
        if (timeoutMode != null) {
            this.timeoutMode = timeoutMode;
        }
        return this;
    }

    @Override
    public String getCommandName() {
        return COMMAND_NAME;
    }

    @Override
    public MongoNamespace getNamespace() {
        return new MongoNamespace(databaseName, "$cmd");
    }

    @Override
    public BatchCursor<T> execute(final ReadBinding binding, final OperationContext operationContext) {
        return executeRetryableRead(binding, applyTimeoutModeToOperationContext(timeoutMode, operationContext), databaseName,
                getCommandCreator(), CommandResultDocumentCodec.create(decoder, FIELD_NAMES_WITH_RESULT),
                transformer(), retryReads);
    }

    @Override
    public void executeAsync(final AsyncReadBinding binding, final OperationContext operationContext,
            final SingleResultCallback<AsyncBatchCursor<T>> callback) {
        SingleResultCallback<AsyncBatchCursor<T>> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
        executeRetryableReadAsync(binding, applyTimeoutModeToOperationContext(timeoutMode, operationContext), databaseName,
                getCommandCreator(), CommandResultDocumentCodec.create(decoder, FIELD_NAMES_WITH_RESULT),
                asyncTransformer(), retryReads,
                errHandlingCallback);
    }

    private CommandCreator getCommandCreator() {
        return (operationContext, serverDescription, connectionDescription) ->
                getCommand(operationContext, connectionDescription.getMaxWireVersion());
    }

    BsonDocument getCommand(final OperationContext operationContext, final int maxWireVersion) {
        BsonDocument commandDocument = new BsonDocument(COMMAND_NAME, new BsonString(mqlv2Source));
        // The mqlv2 server command has strict IDL and does not accept a `cursor` sub-document.
        // Batch size is currently a client-side no-op; the server returns all results in firstBatch
        // with cursorId 0 (no continuation). If batchSize support is added to mqlv2 server-side,
        // re-enable cursor emission here.
        applyMaxTimeMS(operationContext.getTimeoutContext(), commandDocument);
        return commandDocument;
    }

    private CommandReadTransformer<BsonDocument, CommandBatchCursor<T>> transformer() {
        return (result, source, connection, operationContext) ->
                new CommandBatchCursor<>(getEffectiveTimeoutMode(), 0L, operationContext, new CommandCursor<>(
                        result, batchSize != null ? batchSize : 0,
                        decoder, null, source, connection
                ));
    }

    private CommandReadTransformerAsync<BsonDocument, AsyncBatchCursor<T>> asyncTransformer() {
        return (result, source, connection, operationContext) ->
                new AsyncCommandBatchCursor<>(getEffectiveTimeoutMode(), 0L,
                        operationContext, new AsyncCommandCursor<>(
                        result, batchSize != null ? batchSize : 0, decoder, null, source, connection
                ));
    }

    private TimeoutMode getEffectiveTimeoutMode() {
        return timeoutMode != null ? timeoutMode : TimeoutMode.CURSOR_LIFETIME;
    }
}
