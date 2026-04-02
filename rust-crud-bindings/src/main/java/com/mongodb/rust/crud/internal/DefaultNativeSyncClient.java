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
package com.mongodb.rust.crud.internal;

import com.mongodb.ClientSessionOptions;
import com.mongodb.MongoNamespace;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.AggregateOptions;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.ChangeStreamOptions;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.CreateIndexOptions;
import com.mongodb.client.model.DeleteOptions;
import com.mongodb.client.model.DropCollectionOptions;
import com.mongodb.client.model.DistinctOptions;
import com.mongodb.client.model.DropIndexOptions;
import com.mongodb.client.model.EstimatedDocumentCountOptions;
import com.mongodb.client.model.FindOneAndDeleteOptions;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.FindOptions;
import com.mongodb.client.model.IndexModel;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.InsertOneOptions;
import com.mongodb.client.model.RenameCollectionOptions;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.lang.Nullable;
import com.mongodb.rust.crud.NativeAsyncChangeStream;
import com.mongodb.rust.crud.NativeAsyncClient;
import com.mongodb.rust.crud.NativeAsyncClientSession;
import com.mongodb.rust.crud.NativeAsyncCursor;
import com.mongodb.rust.crud.NativeOperationContext;
import com.mongodb.rust.crud.NativeSyncChangeStream;
import com.mongodb.rust.crud.NativeSyncClient;
import com.mongodb.rust.crud.NativeSyncClientSession;
import com.mongodb.rust.crud.NativeSyncCursor;
import com.mongodb.client.model.ListCollectionsOptions;
import com.mongodb.client.model.ListDatabasesOptions;
import com.mongodb.client.model.ListIndexesOptions;
import com.mongodb.rust.crud.SingleResultCallback;
import org.bson.BsonDocument;
import org.bson.codecs.Decoder;
import org.bson.conversions.Bson;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Default implementation of NativeSyncClient.
 *
 * <p>This class wraps any NativeAsyncClient and blocks on async operations.</p>
 */
public final class DefaultNativeSyncClient implements NativeSyncClient {

    private final NativeAsyncClient asyncClient;

    public DefaultNativeSyncClient(NativeAsyncClient asyncClient) {
        this.asyncClient = asyncClient;
    }

    // ==================== Session ====================

    @Override
    public NativeSyncClientSession startSession(ClientSessionOptions options) {
        return blockForResult(callback -> asyncClient.startSession(options, (result, error) -> {
            if (error != null) {
                callback.completeExceptionally(error);
            } else {
                callback.complete(new DefaultNativeSyncClientSession(result));
            }
        }), "starting session");
    }

    // ==================== Insert Operations ====================

    @Override
    public InsertOneResult insertOne(MongoNamespace namespace, BsonDocument document, InsertOneOptions options,
                                     NativeOperationContext context, @Nullable NativeSyncClientSession session) {
        return blockForResult(callback -> asyncClient.insertOne(namespace, document, options, context, toAsync(session), callback),
                "insertOne");
    }

    @Override
    public InsertManyResult insertMany(MongoNamespace namespace, List<BsonDocument> documents, InsertManyOptions options,
                                       NativeOperationContext context, @Nullable NativeSyncClientSession session) {
        return blockForResult(callback -> asyncClient.insertMany(namespace, documents, options, context, toAsync(session), callback),
                "insertMany");
    }

    // ==================== Update Operations ====================

    @Override
    public UpdateResult updateOne(MongoNamespace namespace, Bson filter, Bson update, UpdateOptions options,
                                  NativeOperationContext context, @Nullable NativeSyncClientSession session) {
        return blockForResult(callback -> asyncClient.updateOne(namespace, filter, update, options, context, toAsync(session), callback),
                "updateOne");
    }

    @Override
    public UpdateResult updateMany(MongoNamespace namespace, Bson filter, Bson update, UpdateOptions options,
                                   NativeOperationContext context, @Nullable NativeSyncClientSession session) {
        return blockForResult(callback -> asyncClient.updateMany(namespace, filter, update, options, context, toAsync(session), callback),
                "updateMany");
    }

    @Override
    public UpdateResult replaceOne(MongoNamespace namespace, Bson filter, BsonDocument replacement, ReplaceOptions options,
                                   NativeOperationContext context, @Nullable NativeSyncClientSession session) {
        return blockForResult(callback -> asyncClient.replaceOne(namespace, filter, replacement, options, context, toAsync(session), callback),
                "replaceOne");
    }

    // ==================== Delete Operations ====================

    @Override
    public DeleteResult deleteOne(MongoNamespace namespace, Bson filter, DeleteOptions options,
                                  NativeOperationContext context, @Nullable NativeSyncClientSession session) {
        return blockForResult(callback -> asyncClient.deleteOne(namespace, filter, options, context, toAsync(session), callback),
                "deleteOne");
    }

    @Override
    public DeleteResult deleteMany(MongoNamespace namespace, Bson filter, DeleteOptions options,
                                   NativeOperationContext context, @Nullable NativeSyncClientSession session) {
        return blockForResult(callback -> asyncClient.deleteMany(namespace, filter, options, context, toAsync(session), callback),
                "deleteMany");
    }

    // ==================== Find Operations ====================

    @Override
    public <T> NativeSyncCursor<T> find(MongoNamespace namespace, Bson filter, FindOptions options, Decoder<T> decoder,
                                        NativeOperationContext context, @Nullable NativeSyncClientSession session) {
        NativeAsyncCursor<T> cursor = blockForResult(
                callback -> asyncClient.find(namespace, filter, options, decoder, context, toAsync(session), callback), "find");
        return new DefaultNativeSyncCursor<>(cursor);
    }

    // ==================== Find and Modify Operations ====================

    @Override
    @Nullable
    public <T> T findOneAndUpdate(MongoNamespace namespace, Bson filter, Bson update, FindOneAndUpdateOptions options,
                                  Decoder<T> decoder, NativeOperationContext context, @Nullable NativeSyncClientSession session) {
        return blockForResult(callback -> asyncClient.findOneAndUpdate(namespace, filter, update, options, decoder, context, toAsync(session), callback),
                "findOneAndUpdate");
    }

    @Override
    @Nullable
    public <T> T findOneAndReplace(MongoNamespace namespace, Bson filter, BsonDocument replacement,
                                   FindOneAndReplaceOptions options, Decoder<T> decoder, NativeOperationContext context, @Nullable NativeSyncClientSession session) {
        return blockForResult(callback -> asyncClient.findOneAndReplace(namespace, filter, replacement, options, decoder, context, toAsync(session), callback),
                "findOneAndReplace");
    }

    @Override
    @Nullable
    public <T> T findOneAndDelete(MongoNamespace namespace, Bson filter, FindOneAndDeleteOptions options,
                                  Decoder<T> decoder, NativeOperationContext context, @Nullable NativeSyncClientSession session) {
        return blockForResult(callback -> asyncClient.findOneAndDelete(namespace, filter, options, decoder, context, toAsync(session), callback),
                "findOneAndDelete");
    }

    // ==================== Aggregate Operations ====================

    @Override
    public <T> NativeSyncCursor<T> aggregate(MongoNamespace namespace, List<BsonDocument> pipeline, AggregateOptions options,
                                             @Nullable Boolean bypassDocumentValidation, Decoder<T> decoder,
                                             NativeOperationContext context, @Nullable NativeSyncClientSession session) {
        NativeAsyncCursor<T> cursor = blockForResult(
                callback -> asyncClient.aggregate(namespace, pipeline, options, bypassDocumentValidation, decoder, context, toAsync(session), callback),
                "aggregate");
        return new DefaultNativeSyncCursor<>(cursor);
    }

    @Override
    public <T> NativeSyncCursor<T> aggregateDatabase(String databaseName, List<BsonDocument> pipeline, AggregateOptions options,
                                                     @Nullable Boolean bypassDocumentValidation, Decoder<T> decoder,
                                                     NativeOperationContext context, @Nullable NativeSyncClientSession session) {
        NativeAsyncCursor<T> cursor = blockForResult(
                callback -> asyncClient.aggregateDatabase(databaseName, pipeline, options, bypassDocumentValidation, decoder, context, toAsync(session), callback),
                "aggregateDatabase");
        return new DefaultNativeSyncCursor<>(cursor);
    }

    // ==================== Count Operations ====================

    @Override
    public long countDocuments(MongoNamespace namespace, Bson filter, CountOptions options,
                               NativeOperationContext context, @Nullable NativeSyncClientSession session) {
        return blockForResult(callback -> asyncClient.countDocuments(namespace, filter, options, context, toAsync(session), callback),
                "countDocuments");
    }

    @Override
    public long estimatedDocumentCount(MongoNamespace namespace, EstimatedDocumentCountOptions options, NativeOperationContext context) {
        return blockForResult(callback -> asyncClient.estimatedDocumentCount(namespace, options, context, callback),
                "estimatedDocumentCount");
    }

    @Override
    public <T> NativeSyncCursor<T> distinct(MongoNamespace namespace, String fieldName, Bson filter, DistinctOptions options,
                                            Decoder<T> decoder, NativeOperationContext context, @Nullable NativeSyncClientSession session) {
        NativeAsyncCursor<T> cursor = blockForResult(
                callback -> asyncClient.distinct(namespace, fieldName, filter, options, decoder, context, toAsync(session), callback), "distinct");
        return new DefaultNativeSyncCursor<>(cursor);
    }

    // ==================== Index Operations ====================

    @Override
    public String createIndex(MongoNamespace namespace, Bson keys, CreateIndexOptions options,
                              NativeOperationContext context, @Nullable NativeSyncClientSession session) {
        return blockForResult(callback -> asyncClient.createIndex(namespace, keys, options, context, toAsync(session), callback),
                "createIndex");
    }

    @Override
    public List<String> createIndexes(MongoNamespace namespace, List<IndexModel> indexes, CreateIndexOptions options,
                                      NativeOperationContext context, @Nullable NativeSyncClientSession session) {
        return blockForResult(callback -> asyncClient.createIndexes(namespace, indexes, options, context, toAsync(session), callback),
                "createIndexes");
    }

    @Override
    public void dropIndex(MongoNamespace namespace, String indexName, DropIndexOptions options,
                          NativeOperationContext context, @Nullable NativeSyncClientSession session) {
        blockForVoid(callback -> asyncClient.dropIndex(namespace, indexName, options, context, toAsync(session), callback),
                "dropIndex");
    }

    @Override
    public void dropIndex(MongoNamespace namespace, Bson keys, DropIndexOptions options,
                          NativeOperationContext context, @Nullable NativeSyncClientSession session) {
        blockForVoid(callback -> asyncClient.dropIndex(namespace, keys, options, context, toAsync(session), callback),
                "dropIndex");
    }

    @Override
    public <T> NativeSyncCursor<T> listIndexes(MongoNamespace namespace, ListIndexesOptions options, Decoder<T> decoder,
                                               NativeOperationContext context, @Nullable NativeSyncClientSession session) {
        NativeAsyncCursor<T> cursor = blockForResult(
                callback -> asyncClient.listIndexes(namespace, options, decoder, context, toAsync(session), callback), "listIndexes");
        return new DefaultNativeSyncCursor<>(cursor);
    }

    // ==================== Collection Admin Operations ====================

    @Override
    public void createCollection(String databaseName, String collectionName, CreateCollectionOptions options,
                                 NativeOperationContext context, @Nullable NativeSyncClientSession session) {
        blockForVoid(callback -> asyncClient.createCollection(databaseName, collectionName, options, context, toAsync(session), callback),
                "createCollection");
    }

    @Override
    public void dropCollection(MongoNamespace namespace, DropCollectionOptions options,
                               NativeOperationContext context, @Nullable NativeSyncClientSession session) {
        blockForVoid(callback -> asyncClient.dropCollection(namespace, options, context, toAsync(session), callback),
                "dropCollection");
    }

    @Override
    public void renameCollection(MongoNamespace namespace, MongoNamespace newNamespace, RenameCollectionOptions options,
                                 NativeOperationContext context, @Nullable NativeSyncClientSession session) {
        blockForVoid(callback -> asyncClient.renameCollection(namespace, newNamespace, options, context, toAsync(session), callback),
                "renameCollection");
    }

    @Override
    public <T> NativeSyncCursor<T> listCollections(String databaseName, ListCollectionsOptions options, Decoder<T> decoder,
                                                   NativeOperationContext context, @Nullable NativeSyncClientSession session) {
        NativeAsyncCursor<T> cursor = blockForResult(
                callback -> asyncClient.listCollections(databaseName, options, decoder, context, toAsync(session), callback), "listCollections");
        return new DefaultNativeSyncCursor<>(cursor);
    }

    @Override
    public NativeSyncCursor<String> listCollectionNames(String databaseName, ListCollectionsOptions options,
                                                  NativeOperationContext context, @Nullable NativeSyncClientSession session) {
        NativeAsyncCursor<String> cursor = blockForResult(
                callback -> asyncClient.listCollectionNames(databaseName, options, context, toAsync(session), callback), "listCollectionNames");
        return new DefaultNativeSyncCursor<>(cursor);
    }

    // ==================== Database Admin Operations ====================

    @Override
    public void dropDatabase(String databaseName, NativeOperationContext context, @Nullable NativeSyncClientSession session) {
        blockForVoid(callback -> asyncClient.dropDatabase(databaseName, context, toAsync(session), callback), "dropDatabase");
    }

    @Override
    public <T> NativeSyncCursor<T> listDatabases(ListDatabasesOptions options, Decoder<T> decoder,
                                                  NativeOperationContext context, @Nullable NativeSyncClientSession session) {
        NativeAsyncCursor<T> cursor = blockForResult(
                callback -> asyncClient.listDatabases(options, decoder, context, toAsync(session), callback), "listDatabases");
        return new DefaultNativeSyncCursor<>(cursor);
    }

    @Override
    public NativeSyncCursor<String> listDatabaseNames(ListDatabasesOptions options, NativeOperationContext context, @Nullable NativeSyncClientSession session) {
        NativeAsyncCursor<String> cursor = blockForResult(
                callback -> asyncClient.listDatabaseNames(options, context, toAsync(session), callback), "listDatabaseNames");
        return new DefaultNativeSyncCursor<>(cursor);
    }

    // ==================== Command Operations ====================

    @Override
    public <T> T runCommand(String databaseName, BsonDocument command, Decoder<T> decoder,
                            NativeOperationContext context, @Nullable NativeSyncClientSession session) {
        return blockForResult(callback -> asyncClient.runCommand(databaseName, command, decoder, context, toAsync(session), callback), "runCommand");
    }

    @Override
    public <T> NativeSyncCursor<T> runCursorCommand(String databaseName, BsonDocument command, Decoder<T> decoder,
                                                     NativeOperationContext context, @Nullable NativeSyncClientSession session) {
        NativeAsyncCursor<T> cursor = blockForResult(
                callback -> asyncClient.runCursorCommand(databaseName, command, decoder, context, toAsync(session), callback), "runCursorCommand");
        return new DefaultNativeSyncCursor<>(cursor);
    }

    // ==================== Change Stream Operations ====================

    @Override
    public <T> NativeSyncChangeStream<T> watchCollection(MongoNamespace namespace, List<BsonDocument> pipeline,
                                                          ChangeStreamOptions options, Decoder<T> decoder,
                                                          NativeOperationContext context, @Nullable NativeSyncClientSession session) {
        NativeAsyncChangeStream<T> stream = blockForResult(
                callback -> asyncClient.watchCollection(namespace, pipeline, options, decoder, context, toAsync(session), callback), "watchCollection");
        return new DefaultNativeSyncChangeStream<>(stream);
    }

    @Override
    public <T> NativeSyncChangeStream<T> watchDatabase(String databaseName, List<BsonDocument> pipeline,
                                                        ChangeStreamOptions options, Decoder<T> decoder,
                                                        NativeOperationContext context, @Nullable NativeSyncClientSession session) {
        NativeAsyncChangeStream<T> stream = blockForResult(
                callback -> asyncClient.watchDatabase(databaseName, pipeline, options, decoder, context, toAsync(session), callback), "watchDatabase");
        return new DefaultNativeSyncChangeStream<>(stream);
    }

    @Override
    public <T> NativeSyncChangeStream<T> watchClient(List<BsonDocument> pipeline, ChangeStreamOptions options,
                                                      Decoder<T> decoder, NativeOperationContext context, @Nullable NativeSyncClientSession session) {
        NativeAsyncChangeStream<T> stream = blockForResult(
                callback -> asyncClient.watchClient(pipeline, options, decoder, context, toAsync(session), callback), "watchClient");
        return new DefaultNativeSyncChangeStream<>(stream);
    }

    // ==================== Bulk Write Operations ====================

    @Override
    public BulkWriteResult bulkWrite(MongoNamespace namespace, List<? extends WriteModel<BsonDocument>> requests,
                                     BulkWriteOptions options, NativeOperationContext context, @Nullable NativeSyncClientSession session) {
        return blockForResult(callback -> asyncClient.bulkWrite(namespace, requests, options, context, toAsync(session), callback),
                "bulkWrite");
    }

    // ==================== Lifecycle ====================

    @Override
    public void close() {
        asyncClient.close();
    }

    // ==================== Helpers ====================

    @Nullable
    private NativeAsyncClientSession toAsync(@Nullable NativeSyncClientSession session) {
        if (session == null) {
            return null;
        }
        return ((DefaultNativeSyncClientSession) session).getAsyncSession();
    }

    private <T> T blockForResult(java.util.function.Consumer<SingleResultCallback<T>> operation, String operationName) {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<T> result = new AtomicReference<>();
        AtomicReference<Throwable> error = new AtomicReference<>();

        operation.accept((r, e) -> {
            if (e != null) {
                error.set(e);
            } else {
                result.set(r);
            }
            latch.countDown();
        });

        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while " + operationName, e);
        }

        Throwable t = error.get();
        if (t != null) {
            if (t instanceof RuntimeException) {
                throw (RuntimeException) t;
            }
            throw new RuntimeException("Error during " + operationName, t);
        }

        return result.get();
    }

    private void blockForVoid(java.util.function.Consumer<SingleResultCallback<Void>> operation, String operationName) {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Throwable> error = new AtomicReference<>();

        operation.accept((r, e) -> {
            if (e != null) {
                error.set(e);
            }
            latch.countDown();
        });

        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while " + operationName, e);
        }

        Throwable t = error.get();
        if (t != null) {
            if (t instanceof RuntimeException) {
                throw (RuntimeException) t;
            }
            throw new RuntimeException("Error during " + operationName, t);
        }
    }
}

