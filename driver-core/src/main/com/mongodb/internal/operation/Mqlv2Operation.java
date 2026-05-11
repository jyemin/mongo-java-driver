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

import com.mongodb.ExplainVerbosity;
import com.mongodb.MongoNamespace;
import com.mongodb.client.cursor.TimeoutMode;
import com.mongodb.internal.async.AsyncBatchCursor;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.binding.AsyncReadBinding;
import com.mongodb.internal.binding.ReadBinding;
import com.mongodb.internal.connection.OperationContext;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.codecs.Decoder;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.connection.CommandHelper.applyMaxTimeMS;
import static com.mongodb.internal.operation.ExplainHelper.asExplainCommand;
import static com.mongodb.internal.operation.ServerVersionHelper.UNKNOWN_WIRE_VERSION;

/**
 * An operation that executes an MQLv2 query.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public class Mqlv2Operation<T> implements ReadOperationExplainable<T> {
    private final Mqlv2OperationImpl<T> wrapped;

    public Mqlv2Operation(final String databaseName, final String mqlv2Source, final Decoder<T> decoder) {
        this.wrapped = new Mqlv2OperationImpl<>(
                notNull("databaseName", databaseName),
                notNull("mqlv2Source", mqlv2Source),
                notNull("decoder", decoder));
    }

    public Integer getBatchSize() {
        return wrapped.getBatchSize();
    }

    public Mqlv2Operation<T> batchSize(@Nullable final Integer batchSize) {
        wrapped.batchSize(batchSize);
        return this;
    }

    public Mqlv2Operation<T> retryReads(final boolean retryReads) {
        wrapped.retryReads(retryReads);
        return this;
    }

    public boolean getRetryReads() {
        return wrapped.getRetryReads();
    }

    public Mqlv2Operation<T> maxTime(@Nullable final Long maxTimeMS) {
        wrapped.maxTimeMS(maxTimeMS);
        return this;
    }

    @Nullable
    public Long getMaxTime() {
        return wrapped.getMaxTimeMS();
    }

    public Mqlv2Operation<T> timeoutMode(@Nullable final TimeoutMode timeoutMode) {
        wrapped.timeoutMode(timeoutMode);
        return this;
    }

    @Override
    public String getCommandName() {
        return wrapped.getCommandName();
    }

    @Override
    public MongoNamespace getNamespace() {
        return wrapped.getNamespace();
    }

    @Override
    public BatchCursor<T> execute(final ReadBinding binding, final OperationContext operationContext) {
        return wrapped.execute(binding, operationContext);
    }

    @Override
    public void executeAsync(final AsyncReadBinding binding, final OperationContext operationContext,
            final SingleResultCallback<AsyncBatchCursor<T>> callback) {
        wrapped.executeAsync(binding, operationContext, callback);
    }

    @Override
    public <R> ReadOperationSimple<R> asExplainableOperation(@Nullable final ExplainVerbosity verbosity, final Decoder<R> resultDecoder) {
        return new ExplainCommandOperation<>(getNamespace().getDatabaseName(), getCommandName(),
                (operationContext, serverDescription, connectionDescription) -> {
                    BsonDocument command = wrapped.getCommand(operationContext, UNKNOWN_WIRE_VERSION);
                    applyMaxTimeMS(operationContext.getTimeoutContext(), command);
                    return asExplainCommand(command, verbosity);
                }, resultDecoder);
    }
}
