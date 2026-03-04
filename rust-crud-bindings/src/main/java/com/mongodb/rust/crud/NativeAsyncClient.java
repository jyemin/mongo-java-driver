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
package com.mongodb.rust.crud;

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
import com.mongodb.client.model.DistinctOptions;
import com.mongodb.client.model.DropCollectionOptions;
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
import com.mongodb.client.model.ListCollectionsOptions;
import com.mongodb.client.model.ListDatabasesOptions;
import com.mongodb.client.model.ListIndexesOptions;
import org.bson.BsonDocument;
import org.bson.codecs.Decoder;
import org.bson.conversions.Bson;

import java.io.Closeable;
import java.util.List;

/**
 * Native async client interface.
 *
 * <p>This interface abstracts all MongoDB operations for the native (Rust) driver.
 * Implementations may use FFM or JNI to communicate with the Rust driver.</p>
 *
 * <p>All operations accept a callback that will be invoked with either a result or an error,
 * never both. The callback may be invoked on any thread.</p>
 */
public interface NativeAsyncClient extends Closeable {

    // ==================== Session ====================

    void startSession(ClientSessionOptions options, SingleResultCallback<NativeAsyncClientSession> callback);

    // ==================== Insert Operations ====================

    void insertOne(MongoNamespace namespace,
                   BsonDocument document,
                   InsertOneOptions options,
                   NativeOperationContext context,
                   @Nullable NativeAsyncClientSession session,
                   SingleResultCallback<InsertOneResult> callback);

    void insertMany(MongoNamespace namespace,
                    List<BsonDocument> documents,
                    InsertManyOptions options,
                    NativeOperationContext context,
                    @Nullable NativeAsyncClientSession session,
                    SingleResultCallback<InsertManyResult> callback);

    // ==================== Update Operations ====================

    void updateOne(MongoNamespace namespace,
                   Bson filter,
                   Bson update,
                   UpdateOptions options,
                   NativeOperationContext context,
                   @Nullable NativeAsyncClientSession session,
                   SingleResultCallback<UpdateResult> callback);

    void updateMany(MongoNamespace namespace,
                    Bson filter,
                    Bson update,
                    UpdateOptions options,
                    NativeOperationContext context,
                    @Nullable NativeAsyncClientSession session,
                    SingleResultCallback<UpdateResult> callback);

    void replaceOne(MongoNamespace namespace,
                    Bson filter,
                    BsonDocument replacement,
                    ReplaceOptions options,
                    NativeOperationContext context,
                    @Nullable NativeAsyncClientSession session,
                    SingleResultCallback<UpdateResult> callback);

    // ==================== Delete Operations ====================

    void deleteOne(MongoNamespace namespace,
                   Bson filter,
                   DeleteOptions options,
                   NativeOperationContext context,
                   @Nullable NativeAsyncClientSession session,
                   SingleResultCallback<DeleteResult> callback);

    void deleteMany(MongoNamespace namespace,
                    Bson filter,
                    DeleteOptions options,
                    NativeOperationContext context,
                    @Nullable NativeAsyncClientSession session,
                    SingleResultCallback<DeleteResult> callback);

    // ==================== Find Operations ====================

    <T> void findOne(MongoNamespace namespace,
                     Bson filter,
                     FindOptions options,
                     Decoder<T> decoder,
                     NativeOperationContext context,
                     @Nullable NativeAsyncClientSession session,
                     SingleResultCallback<T> callback);

    <T> void find(MongoNamespace namespace,
                  Bson filter,
                  FindOptions options,
                  Decoder<T> decoder,
                  NativeOperationContext context,
                  @Nullable NativeAsyncClientSession session,
                  SingleResultCallback<NativeAsyncCursor<T>> callback);

    // ==================== Find and Modify Operations ====================

    <T> void findOneAndUpdate(MongoNamespace namespace,
                              Bson filter,
                              Bson update,
                              FindOneAndUpdateOptions options,
                              Decoder<T> decoder,
                              NativeOperationContext context,
                              @Nullable NativeAsyncClientSession session,
                              SingleResultCallback<T> callback);

    <T> void findOneAndReplace(MongoNamespace namespace,
                               Bson filter,
                               BsonDocument replacement,
                               FindOneAndReplaceOptions options,
                               Decoder<T> decoder,
                               NativeOperationContext context,
                               @Nullable NativeAsyncClientSession session,
                               SingleResultCallback<T> callback);

    <T> void findOneAndDelete(MongoNamespace namespace,
                              Bson filter,
                              FindOneAndDeleteOptions options,
                              Decoder<T> decoder,
                              NativeOperationContext context,
                              @Nullable NativeAsyncClientSession session,
                              SingleResultCallback<T> callback);

    // ==================== Aggregate Operations ====================

    <T> void aggregate(MongoNamespace namespace,
                       List<BsonDocument> pipeline,
                       AggregateOptions options,
                       @Nullable Boolean bypassDocumentValidation,
                       Decoder<T> decoder,
                       NativeOperationContext context,
                       @Nullable NativeAsyncClientSession session,
                       SingleResultCallback<NativeAsyncCursor<T>> callback);

    <T> void aggregateDatabase(String databaseName,
                               List<BsonDocument> pipeline,
                               AggregateOptions options,
                               @Nullable Boolean bypassDocumentValidation,
                               Decoder<T> decoder,
                               NativeOperationContext context,
                               @Nullable NativeAsyncClientSession session,
                               SingleResultCallback<NativeAsyncCursor<T>> callback);

    // ==================== Count Operations ====================

    void countDocuments(MongoNamespace namespace,
                        Bson filter,
                        CountOptions options,
                        NativeOperationContext context,
                        @Nullable NativeAsyncClientSession session,
                        SingleResultCallback<Long> callback);

    void estimatedDocumentCount(MongoNamespace namespace,
                                EstimatedDocumentCountOptions options,
                                NativeOperationContext context,
                                SingleResultCallback<Long> callback);

    <T> void distinct(MongoNamespace namespace,
                      String fieldName,
                      Bson filter,
                      DistinctOptions options,
                      Decoder<T> decoder,
                      NativeOperationContext context,
                      @Nullable NativeAsyncClientSession session,
                      SingleResultCallback<NativeAsyncCursor<T>> callback);

    // ==================== Index Operations ====================

    void createIndex(MongoNamespace namespace,
                     Bson keys,
                     CreateIndexOptions options,
                     NativeOperationContext context,
                     @Nullable NativeAsyncClientSession session,
                     SingleResultCallback<String> callback);

    void createIndexes(MongoNamespace namespace,
                       List<IndexModel> indexes,
                       CreateIndexOptions options,
                       NativeOperationContext context,
                       @Nullable NativeAsyncClientSession session,
                       SingleResultCallback<List<String>> callback);

    void dropIndex(MongoNamespace namespace,
                   String indexName,
                   DropIndexOptions options,
                   NativeOperationContext context,
                   @Nullable NativeAsyncClientSession session,
                   SingleResultCallback<Void> callback);

    void dropIndex(MongoNamespace namespace,
                   Bson keys,
                   DropIndexOptions options,
                   NativeOperationContext context,
                   @Nullable NativeAsyncClientSession session,
                   SingleResultCallback<Void> callback);

    <T> void listIndexes(MongoNamespace namespace,
                         ListIndexesOptions options,
                         Decoder<T> decoder,
                         NativeOperationContext context,
                         @Nullable NativeAsyncClientSession session,
                         SingleResultCallback<NativeAsyncCursor<T>> callback);

    // ==================== Collection Admin Operations ====================

    void createCollection(String databaseName,
                          String collectionName,
                          CreateCollectionOptions options,
                          NativeOperationContext context,
                          @Nullable NativeAsyncClientSession session,
                          SingleResultCallback<Void> callback);

    void dropCollection(MongoNamespace namespace,
                        DropCollectionOptions options,
                        NativeOperationContext context,
                        @Nullable NativeAsyncClientSession session,
                        SingleResultCallback<Void> callback);

    void renameCollection(MongoNamespace namespace,
                          MongoNamespace newNamespace,
                          RenameCollectionOptions options,
                          NativeOperationContext context,
                          @Nullable NativeAsyncClientSession session,
                          SingleResultCallback<Void> callback);

    <T> void listCollections(String databaseName,
                             ListCollectionsOptions options,
                             Decoder<T> decoder,
                             NativeOperationContext context,
                             @Nullable NativeAsyncClientSession session,
                             SingleResultCallback<NativeAsyncCursor<T>> callback);

    void listCollectionNames(String databaseName,
                             ListCollectionsOptions options,
                             NativeOperationContext context,
                             @Nullable NativeAsyncClientSession session,
                             SingleResultCallback<NativeAsyncCursor<String>> callback);

    // ==================== Database Admin Operations ====================

    void dropDatabase(String databaseName,
                      NativeOperationContext context,
                      @Nullable NativeAsyncClientSession session,
                      SingleResultCallback<Void> callback);

    <T> void listDatabases(ListDatabasesOptions options,
                           Decoder<T> decoder,
                           NativeOperationContext context,
                           @Nullable NativeAsyncClientSession session,
                           SingleResultCallback<NativeAsyncCursor<T>> callback);

    void listDatabaseNames(ListDatabasesOptions options,
                           NativeOperationContext context,
                           @Nullable NativeAsyncClientSession session,
                           SingleResultCallback<NativeAsyncCursor<String>> callback);

    // ==================== Command Operations ====================

    <T> void runCommand(String databaseName,
                        BsonDocument command,
                        Decoder<T> decoder,
                        NativeOperationContext context,
                        @Nullable NativeAsyncClientSession session,
                        SingleResultCallback<T> callback);

    <T> void runCursorCommand(String databaseName,
                              BsonDocument command,
                              Decoder<T> decoder,
                              NativeOperationContext context,
                              @Nullable NativeAsyncClientSession session,
                              SingleResultCallback<NativeAsyncCursor<T>> callback);

    // ==================== Change Stream Operations ====================

    <T> void watchCollection(MongoNamespace namespace,
                              List<BsonDocument> pipeline,
                              ChangeStreamOptions options,
                              Decoder<T> decoder,
                              NativeOperationContext context,
                              @Nullable NativeAsyncClientSession session,
                              SingleResultCallback<NativeAsyncChangeStream<T>> callback);

    <T> void watchDatabase(String databaseName,
                            List<BsonDocument> pipeline,
                            ChangeStreamOptions options,
                            Decoder<T> decoder,
                            NativeOperationContext context,
                            @Nullable NativeAsyncClientSession session,
                            SingleResultCallback<NativeAsyncChangeStream<T>> callback);

    <T> void watchClient(List<BsonDocument> pipeline,
                          ChangeStreamOptions options,
                          Decoder<T> decoder,
                          NativeOperationContext context,
                          @Nullable NativeAsyncClientSession session,
                          SingleResultCallback<NativeAsyncChangeStream<T>> callback);

    // ==================== Bulk Write Operations ====================

    void bulkWrite(MongoNamespace namespace,
                   List<? extends WriteModel<BsonDocument>> requests,
                   BulkWriteOptions options,
                   NativeOperationContext context,
                   @Nullable NativeAsyncClientSession session,
                   SingleResultCallback<BulkWriteResult> callback);

    // ==================== Lifecycle ====================

    @Override
    void close();
}

