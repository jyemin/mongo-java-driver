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

import com.mongodb.ExplainVerbosity;
import com.mongodb.MongoNamespace;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.cursor.TimeoutMode;
import com.mongodb.client.model.AggregateOptions;
import com.mongodb.client.model.Collation;
import com.mongodb.lang.Nullable;
import com.mongodb.rust.crud.NativeOperationContext;
import com.mongodb.rust.crud.NativeSyncClient;
import com.mongodb.rust.crud.NativeSyncClientSession;
import com.mongodb.rust.crud.NativeSyncCursor;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * Native implementation of AggregateIterable using Rust FFI.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class NativeAggregateIterable<TResult>
        extends NativeMongoIterableBase<TResult>
        implements AggregateIterable<TResult> {

    // If namespace is null, this is a database-level aggregation
    @Nullable
    private final MongoNamespace namespace;
    private final String databaseName;
    private final List<BsonDocument> pipeline;
    private final AggregateOptions options = new AggregateOptions();
    @Nullable
    private Boolean bypassDocumentValidation;

    /**
     * Creates a collection-level aggregate iterable.
     */
    public NativeAggregateIterable(NativeSyncClient nativeClient,
                                   @Nullable NativeSyncClientSession nativeSession,
                                   NativeOperationContext operationContext,
                                   MongoNamespace namespace,
                                   List<? extends Bson> pipeline,
                                   Class<TResult> resultClass,
                                   CodecRegistry codecRegistry) {
        super(nativeClient, nativeSession, operationContext, resultClass, codecRegistry);
        this.namespace = notNull("namespace", namespace);
        this.databaseName = namespace.getDatabaseName();
        this.pipeline = toBsonDocumentList(pipeline, codecRegistry);
    }

    /**
     * Creates a database-level aggregate iterable.
     */
    public NativeAggregateIterable(NativeSyncClient nativeClient,
                                   @Nullable NativeSyncClientSession nativeSession,
                                   NativeOperationContext operationContext,
                                   String databaseName,
                                   List<? extends Bson> pipeline,
                                   Class<TResult> resultClass,
                                   CodecRegistry codecRegistry) {
        super(nativeClient, nativeSession, operationContext, resultClass, codecRegistry);
        this.namespace = null;  // database-level aggregation
        this.databaseName = notNull("databaseName", databaseName);
        this.pipeline = toBsonDocumentList(pipeline, codecRegistry);
    }

    private List<BsonDocument> toBsonDocumentList(List<? extends Bson> pipeline, CodecRegistry codecRegistry) {
        List<BsonDocument> result = new ArrayList<>(pipeline.size());
        for (Bson stage : pipeline) {
            result.add(stage.toBsonDocument(BsonDocument.class, codecRegistry));
        }
        return result;
    }

    @Override
    public void toCollection() {
        // Execute the aggregation with $out or $merge - cursor must be exhausted
        try (MongoCursor<TResult> cursor = cursor()) {
            while (cursor.hasNext()) {
                cursor.next();
            }
        }
    }

    @Override
    public AggregateIterable<TResult> allowDiskUse(@Nullable Boolean allowDiskUse) {
        options.allowDiskUse(allowDiskUse);
        return this;
    }

    @Override
    public AggregateIterable<TResult> batchSize(int batchSize) {
        options.batchSize(batchSize);
        return this;
    }

    @Override
    public AggregateIterable<TResult> timeoutMode(TimeoutMode timeoutMode) {
        // TODO: Implement timeout mode
        return this;
    }

    @Override
    public AggregateIterable<TResult> maxTime(long maxTime, TimeUnit timeUnit) {
        options.maxTime(maxTime, timeUnit);
        return this;
    }

    @Override
    public AggregateIterable<TResult> maxAwaitTime(long maxAwaitTime, TimeUnit timeUnit) {
        // TODO: Support maxAwaitTime when options support it
        return this;
    }

    @Override
    public AggregateIterable<TResult> bypassDocumentValidation(@Nullable Boolean bypassDocumentValidation) {
        this.bypassDocumentValidation = bypassDocumentValidation;
        return this;
    }

    @Override
    public AggregateIterable<TResult> collation(@Nullable Collation collation) {
        options.collation(collation);
        return this;
    }

    @Override
    public AggregateIterable<TResult> comment(@Nullable String comment) {
        options.comment(comment != null ? new BsonString(comment) : null);
        return this;
    }

    @Override
    public AggregateIterable<TResult> comment(@Nullable BsonValue comment) {
        options.comment(comment);
        return this;
    }

    @Override
    public AggregateIterable<TResult> hint(@Nullable Bson hint) {
        options.hint(hint);
        return this;
    }

    @Override
    public AggregateIterable<TResult> hintString(@Nullable String hint) {
        options.hintString(hint);
        return this;
    }

    @Override
    public AggregateIterable<TResult> let(@Nullable Bson variables) {
        options.let(variables);
        return this;
    }

    @Override
    public Document explain() {
        return explain(Document.class);
    }

    @Override
    public Document explain(ExplainVerbosity verbosity) {
        return explain(Document.class, verbosity);
    }

    @Override
    public <E> E explain(Class<E> explainResultClass) {
        // TODO: Implement explain
        throw new UnsupportedOperationException("explain not yet implemented");
    }

    @Override
    public <E> E explain(Class<E> explainResultClass, ExplainVerbosity verbosity) {
        // TODO: Implement explain with verbosity
        throw new UnsupportedOperationException("explain not yet implemented");
    }

    @Override
    public MongoCursor<TResult> cursor() {
        NativeSyncCursor<TResult> nativeCursor;
        if (namespace != null) {
            // Collection-level aggregation
            nativeCursor = getNativeClient().aggregate(namespace, pipeline, options,
                    bypassDocumentValidation, getCodec(), getOperationContext(), getNativeSession());
        } else {
            // Database-level aggregation
            nativeCursor = getNativeClient().aggregateDatabase(databaseName, pipeline, options,
                    bypassDocumentValidation, getCodec(), getOperationContext(), getNativeSession());
        }
        return new NativeMongoCursor<>(nativeCursor);
    }
}
