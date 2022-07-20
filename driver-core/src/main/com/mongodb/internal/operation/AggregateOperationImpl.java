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

import com.mongodb.MongoInternalException;
import com.mongodb.MongoNamespace;
import com.mongodb.client.model.Collation;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ServerDescription;
import com.mongodb.internal.async.AsyncBatchCursor;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.binding.AsyncConnectionSource;
import com.mongodb.internal.binding.AsyncReadBinding;
import com.mongodb.internal.binding.ReadBinding;
import com.mongodb.internal.client.model.AggregationLevel;
import com.mongodb.internal.connection.AsyncConnection;
import com.mongodb.internal.connection.QueryResult;
import com.mongodb.internal.operation.CommandOperationHelper.CommandCreator;
import com.mongodb.internal.operation.CommandOperationHelper.CommandReadTransformer;
import com.mongodb.internal.operation.CommandOperationHelper.CommandReadTransformerAsync;
import com.mongodb.internal.session.SessionContext;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.codecs.Decoder;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.internal.operation.CommandOperationHelper.executeRetryableRead;
import static com.mongodb.internal.operation.CommandOperationHelper.executeRetryableReadAsync;
import static com.mongodb.internal.operation.OperationHelper.LOGGER;
import static com.mongodb.internal.operation.OperationHelper.cursorDocumentToQueryResult;
import static com.mongodb.internal.operation.OperationHelper.validateReadConcernAndCollation;
import static com.mongodb.internal.operation.OperationReadConcernHelper.appendReadConcernToCommand;

class AggregateOperationImpl<T> implements AsyncReadOperation<AsyncBatchCursor<T>>, ReadOperation<BatchCursor<T>> {
    private static final String RESULT = "result";
    private static final String CURSOR = "cursor";
    private static final String CURSORS = "cursors";
    private static final String FIRST_BATCH = "firstBatch";
    private static final List<String> FIELD_NAMES_WITH_RESULT = Arrays.asList(RESULT, FIRST_BATCH);

    private final MongoNamespace namespace;
    private final List<BsonDocument> pipeline;
    private List<List<BsonDocument>> facetPipelines;
    private final Decoder<T> decoder;
    private final AggregateTarget aggregateTarget;
    private final PipelineCreator pipelineCreator;

    private boolean retryReads;
    private Boolean allowDiskUse;
    private Integer batchSize;
    private Collation collation;
    private BsonValue comment;
    private BsonValue hint;
    private long maxAwaitTimeMS;
    private long maxTimeMS;
    private BsonDocument variables;

    AggregateOperationImpl(final MongoNamespace namespace, final List<BsonDocument> pipeline, final Decoder<T> decoder,
                           final AggregationLevel aggregationLevel) {
        this(namespace, pipeline, decoder, defaultAggregateTarget(notNull("aggregationLevel", aggregationLevel),
                notNull("namespace", namespace).getCollectionName()), defaultPipelineCreator(pipeline));
    }

    AggregateOperationImpl(final MongoNamespace namespace, final List<BsonDocument> pipeline,
            final List<List<BsonDocument>> facetPipelines, final Decoder<T> decoder, final AggregationLevel aggregationLevel) {
        this(namespace, pipeline, facetPipelines, decoder, defaultAggregateTarget(notNull("aggregationLevel", aggregationLevel),
                notNull("namespace", namespace).getCollectionName()), defaultPipelineCreator(pipeline));
    }

    AggregateOperationImpl(final MongoNamespace namespace, final List<BsonDocument> pipeline, final Decoder<T> decoder,
                           final AggregateTarget aggregateTarget, final PipelineCreator pipelineCreator) {
        this.namespace = notNull("namespace", namespace);
        this.pipeline = notNull("pipeline", pipeline);
        this.decoder = notNull("decoder", decoder);
        this.aggregateTarget = notNull("aggregateTarget", aggregateTarget);
        this.pipelineCreator = notNull("pipelineCreator", pipelineCreator);
    }

    AggregateOperationImpl(final MongoNamespace namespace, final List<BsonDocument> pipeline,
            final List<List<BsonDocument>> facetPipelines, final Decoder<T> decoder, final AggregateTarget aggregateTarget,
            final PipelineCreator pipelineCreator) {
        this.namespace = notNull("namespace", namespace);
        this.pipeline = notNull("pipeline", pipeline);
        this.facetPipelines = facetPipelines;
        this.decoder = notNull("decoder", decoder);
        this.aggregateTarget = notNull("aggregateTarget", aggregateTarget);
        this.pipelineCreator = notNull("pipelineCreator", pipelineCreator);
    }

    MongoNamespace getNamespace() {
        return namespace;
    }

    List<BsonDocument> getPipeline() {
        return pipeline;
    }

    Decoder<T> getDecoder() {
        return decoder;
    }

    Boolean getAllowDiskUse() {
        return allowDiskUse;
    }

    AggregateOperationImpl<T> allowDiskUse(final Boolean allowDiskUse) {
        this.allowDiskUse = allowDiskUse;
        return this;
    }

    Integer getBatchSize() {
        return batchSize;
    }

    AggregateOperationImpl<T> batchSize(final Integer batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    long getMaxAwaitTime(final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        return timeUnit.convert(maxAwaitTimeMS, TimeUnit.MILLISECONDS);
    }

    AggregateOperationImpl<T> maxAwaitTime(final long maxAwaitTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        isTrueArgument("maxAwaitTime >= 0", maxAwaitTime >= 0);
        this.maxAwaitTimeMS = TimeUnit.MILLISECONDS.convert(maxAwaitTime, timeUnit);
        return this;
    }

    long getMaxTime(final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        return timeUnit.convert(maxTimeMS, TimeUnit.MILLISECONDS);
    }

    AggregateOperationImpl<T> maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        isTrueArgument("maxTime >= 0", maxTime >= 0);
        this.maxTimeMS = TimeUnit.MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    Collation getCollation() {
        return collation;
    }

    AggregateOperationImpl<T> collation(final Collation collation) {
        this.collation = collation;
        return this;
    }

    BsonValue getComment() {
        return comment;
    }

    AggregateOperationImpl<T> comment(final BsonValue comment) {
        this.comment = comment;
        return this;
    }

    AggregateOperationImpl<T> let(final BsonDocument variables) {
        this.variables = variables;
        return this;
    }

    AggregateOperationImpl<T> retryReads(final boolean retryReads) {
        this.retryReads = retryReads;
        return this;
    }

    boolean getRetryReads() {
        return retryReads;
    }

    BsonValue getHint() {
        return hint;
    }

    AggregateOperationImpl<T> hint(final BsonValue hint) {
        isTrueArgument("BsonString or BsonDocument", hint == null || hint.isDocument() || hint.isString());
        this.hint = hint;
        return this;
    }

    @Override
    public BatchCursor<T> execute(final ReadBinding binding) {
        return executeRetryableRead(binding, namespace.getDatabaseName(), getCommandCreator(binding.getSessionContext(), false),
                CommandResultDocumentCodec.create(decoder, FIELD_NAMES_WITH_RESULT), transformer(), retryReads);
    }

    public List<BatchCursor<T>> executeMultipleCursors(final ReadBinding binding) {
        List<AggregateResponseBatchCursor<T>> aggregateResponseBatchCursors = executeRetryableRead(binding, namespace.getDatabaseName(),
                getCommandCreator(binding.getSessionContext(), true),
                CommandResultDocumentCodec.create(decoder, FIELD_NAMES_WITH_RESULT), transformerMultipleCursors(), retryReads);
        return aggregateResponseBatchCursors.stream()
                .map((Function<AggregateResponseBatchCursor<T>, BatchCursor<T>>) aggregateResponseBatchCursor -> aggregateResponseBatchCursor)
                .collect(Collectors.toList());
    }

    @Override
    public void executeAsync(final AsyncReadBinding binding, final SingleResultCallback<AsyncBatchCursor<T>> callback) {
        SingleResultCallback<AsyncBatchCursor<T>> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
        executeRetryableReadAsync(binding, namespace.getDatabaseName(), getCommandCreator(binding.getSessionContext(), false),
                CommandResultDocumentCodec.create(decoder, FIELD_NAMES_WITH_RESULT), asyncTransformer(), retryReads, errHandlingCallback);
    }

    private CommandCreator getCommandCreator(final SessionContext sessionContext, final boolean requestMultipleCursors) {
        return new CommandCreator() {
            @Override
            public BsonDocument create(final ServerDescription serverDescription, final ConnectionDescription connectionDescription) {
                validateReadConcernAndCollation(connectionDescription, sessionContext.getReadConcern(), collation);
                return getCommand(sessionContext, connectionDescription.getMaxWireVersion(), requestMultipleCursors);
            }
        };
    }

    BsonDocument getCommand(final SessionContext sessionContext, final int maxWireVersion, final boolean requestMultipleCursors) {
        BsonDocument commandDocument = new BsonDocument("aggregate", aggregateTarget.create());

        appendReadConcernToCommand(sessionContext, maxWireVersion, commandDocument);
        commandDocument.put("pipeline", pipelineCreator.create());

        if (facetPipelines != null) {
            commandDocument.put("facetCursors", new BsonArray(facetPipelines.stream()
                    .map(BsonArray::new).collect(Collectors.toList())));
        }

        if (maxTimeMS > 0) {
            commandDocument.put("maxTimeMS", maxTimeMS > Integer.MAX_VALUE
                    ? new BsonInt64(maxTimeMS) : new BsonInt32((int) maxTimeMS));
        }
        BsonDocument cursor = new BsonDocument();
        if (batchSize != null) {
            cursor.put("batchSize", new BsonInt32(batchSize));
        } else {
            cursor.put("batchSize", new BsonInt32(0));
        }
        // TODO: this is temporary
        if (requestMultipleCursors) {
            cursor.put("multiple", BsonBoolean.TRUE);
        }

        commandDocument.put(CURSOR, cursor);
        if (allowDiskUse != null) {
            commandDocument.put("allowDiskUse", BsonBoolean.valueOf(allowDiskUse));
        }
        if (collation != null) {
            commandDocument.put("collation", collation.asDocument());
        }
        if (comment != null) {
            commandDocument.put("comment", comment);
        }
        if (hint != null) {
            commandDocument.put("hint", hint);
        }
        if (variables != null) {
            commandDocument.put("let", variables);
        }

        return commandDocument;
    }

    private QueryResult<T> createQueryResult(final BsonDocument result, final ConnectionDescription description) {
        return cursorDocumentToQueryResult(result.getDocument(CURSOR), description.getServerAddress());
    }

    private List<QueryResult<T>> createQueryResults(final BsonDocument result, final ConnectionDescription description) {
        if (result.containsKey(CURSOR)) {
           return Collections.singletonList(cursorDocumentToQueryResult(result.getDocument(CURSOR), description.getServerAddress()));
        } else if (result.containsKey(CURSORS)) {
            return result.getArray(CURSORS).stream().map(bsonValue -> bsonValue.asDocument().getDocument(CURSOR))
                    .map((Function<BsonDocument, QueryResult<T>>) cursorDocument ->
                            cursorDocumentToQueryResult(cursorDocument, description.getServerAddress()))
                    .collect(Collectors.toList());
        } else {
            throw new MongoInternalException("Expected either cursor or cursors field in aggregate reply document");
        }
    }

    private CommandReadTransformer<BsonDocument, AggregateResponseBatchCursor<T>> transformer() {
        return (bsonDocument, source, connection) -> transformerMultipleCursors().apply(bsonDocument, source, connection).get(0);
    }

    private CommandReadTransformer<BsonDocument, List<AggregateResponseBatchCursor<T>>> transformerMultipleCursors() {
        return (result, source, connection) -> {
            List<QueryResult<T>> queryResults = createQueryResults(result, connection.getDescription());
            return queryResults.stream()
                    .map((Function<QueryResult<T>, AggregateResponseBatchCursor<T>>) queryResult ->
                            new QueryBatchCursor<T>(queryResult, 0, batchSize != null ? batchSize : 0, maxAwaitTimeMS, decoder,
                            comment, source, connection, result)).collect(Collectors.toList());
        };
    }

    private CommandReadTransformerAsync<BsonDocument, AsyncBatchCursor<T>> asyncTransformer() {
        return new CommandOperationHelper.CommandReadTransformerAsync<BsonDocument, AsyncBatchCursor<T>>() {
            @Override
            public AsyncBatchCursor<T> apply(final BsonDocument result, final AsyncConnectionSource source,
                                             final AsyncConnection connection) {
                QueryResult<T> queryResult = createQueryResult(result, connection.getDescription());
                return new AsyncQueryBatchCursor<T>(queryResult, 0, batchSize != null ? batchSize : 0, maxAwaitTimeMS, decoder,
                        comment, source, connection, result);
            }
        };
    }

    interface AggregateTarget {
        BsonValue create();
    }

    interface PipelineCreator {
        BsonArray create();
    }

    private static AggregateTarget defaultAggregateTarget(final AggregationLevel aggregationLevel, final String collectionName) {
        return new AggregateTarget() {
            @Override
            public BsonValue create() {
                if (aggregationLevel == AggregationLevel.DATABASE) {
                    return new BsonInt32(1);
                } else {
                    return new BsonString(collectionName);
                }
            }
        };
    }

    private static PipelineCreator defaultPipelineCreator(final List<BsonDocument> pipeline) {
        return new PipelineCreator() {
            @Override
            public BsonArray create() {
                return new BsonArray(pipeline);
            }
        };
    }
}
