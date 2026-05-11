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

package com.mongodb.client.internal;

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.client.ClientSession;
import com.mongodb.client.Mqlv2Iterable;
import com.mongodb.internal.TimeoutSettings;
import com.mongodb.internal.operation.Mqlv2Operation;
import com.mongodb.internal.operation.ReadOperationCursor;
import com.mongodb.lang.Nullable;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
final class Mqlv2IterableImpl<TResult> extends MongoIterableImpl<TResult> implements Mqlv2Iterable<TResult> {
    private final String databaseName;
    private final String mqlv2Source;
    private final Class<TResult> resultClass;
    private final CodecRegistry codecRegistry;
    private final boolean retryReads;

    @Nullable private Long maxTimeMS;

    @SuppressWarnings("checkstyle:ParameterNumber")
    Mqlv2IterableImpl(@Nullable final ClientSession clientSession, final String databaseName, final String mqlv2Source,
            final Class<TResult> resultClass, final CodecRegistry codecRegistry, final ReadPreference readPreference,
            final ReadConcern readConcern, final OperationExecutor executor, final boolean retryReads,
            final TimeoutSettings timeoutSettings) {
        super(clientSession, executor, readConcern, readPreference, retryReads, timeoutSettings);
        this.databaseName = notNull("databaseName", databaseName);
        this.mqlv2Source = notNull("mqlv2Source", mqlv2Source);
        this.resultClass = notNull("resultClass", resultClass);
        this.codecRegistry = notNull("codecRegistry", codecRegistry);
        this.retryReads = retryReads;
    }

    @Override
    public Mqlv2Iterable<TResult> batchSize(final int batchSize) {
        super.batchSize(batchSize);
        return this;
    }

    @Override
    public Mqlv2Iterable<TResult> maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxTimeMS = TimeUnit.MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    @Override
    public ReadOperationCursor<TResult> asReadOperation() {
        Mqlv2Operation<TResult> operation = new Mqlv2Operation<>(databaseName, mqlv2Source, codecRegistry.get(resultClass));
        operation.retryReads(retryReads);
        Integer batchSize = getBatchSize();
        if (batchSize != null) {
            operation.batchSize(batchSize);
        }
        if (maxTimeMS != null) {
            operation.maxTime(maxTimeMS);
        }
        operation.timeoutMode(getTimeoutMode());
        return operation;
    }

    @Override
    protected OperationExecutor getExecutor() {
        TimeoutSettings ts = maxTimeMS != null
                ? getTimeoutSettings().withMaxTimeMS(maxTimeMS)
                : getTimeoutSettings();
        return getExecutor(ts);
    }
}
