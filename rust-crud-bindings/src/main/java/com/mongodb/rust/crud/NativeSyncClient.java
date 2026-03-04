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
import com.mongodb.client.model.ListCollectionsOptions;
import com.mongodb.client.model.ListDatabasesOptions;
import com.mongodb.client.model.ListIndexesOptions;
import org.bson.BsonDocument;
import org.bson.codecs.Decoder;
import org.bson.conversions.Bson;

import java.io.Closeable;
import java.util.List;

/**
 * Native sync client interface.
 *
 * <p>This interface abstracts all MongoDB operations for the native (Rust) driver.
 * Implementations may use FFM or JNI to communicate with the Rust driver.</p>
 */
public interface NativeSyncClient extends Closeable {

    // ==================== Session ====================

    NativeSyncClientSession startSession(ClientSessionOptions options);

    // ==================== Insert Operations ====================

    InsertOneResult insertOne(MongoNamespace namespace,
                              BsonDocument document,
                              InsertOneOptions options,
                              NativeOperationContext context,
                              @Nullable NativeSyncClientSession session);

    InsertManyResult insertMany(MongoNamespace namespace,
                                List<BsonDocument> documents,
                                InsertManyOptions options,
                                NativeOperationContext context,
                                @Nullable NativeSyncClientSession session);

    // ==================== Update Operations ====================

    UpdateResult updateOne(MongoNamespace namespace,
                           Bson filter,
                           Bson update,
                           UpdateOptions options,
                           NativeOperationContext context,
                           @Nullable NativeSyncClientSession session);

    UpdateResult updateMany(MongoNamespace namespace,
                            Bson filter,
                            Bson update,
                            UpdateOptions options,
                            NativeOperationContext context,
                            @Nullable NativeSyncClientSession session);

    UpdateResult replaceOne(MongoNamespace namespace,
                            Bson filter,
                            BsonDocument replacement,
                            ReplaceOptions options,
                            NativeOperationContext context,
                            @Nullable NativeSyncClientSession session);

    // ==================== Delete Operations ====================

    DeleteResult deleteOne(MongoNamespace namespace,
                           Bson filter,
                           DeleteOptions options,
                           NativeOperationContext context,
                           @Nullable NativeSyncClientSession session);

    DeleteResult deleteMany(MongoNamespace namespace,
                            Bson filter,
                            DeleteOptions options,
                            NativeOperationContext context,
                            @Nullable NativeSyncClientSession session);

    // ==================== Find Operations ====================

    @Nullable
    <T> T findOne(MongoNamespace namespace,
                  Bson filter,
                  FindOptions options,
                  Decoder<T> decoder,
                  NativeOperationContext context,
                  @Nullable NativeSyncClientSession session);

    <T> NativeSyncCursor<T> find(MongoNamespace namespace,
                                 Bson filter,
                                 FindOptions options,
                                 Decoder<T> decoder,
                                 NativeOperationContext context,
                                 @Nullable NativeSyncClientSession session);

    // ==================== Find and Modify Operations ====================

    @Nullable
    <T> T findOneAndUpdate(MongoNamespace namespace,
                           Bson filter,
                           Bson update,
                           FindOneAndUpdateOptions options,
                           Decoder<T> decoder,
                           NativeOperationContext context,
                           @Nullable NativeSyncClientSession session);

    @Nullable
    <T> T findOneAndReplace(MongoNamespace namespace,
                            Bson filter,
                            BsonDocument replacement,
                            FindOneAndReplaceOptions options,
                            Decoder<T> decoder,
                            NativeOperationContext context,
                            @Nullable NativeSyncClientSession session);

    @Nullable
    <T> T findOneAndDelete(MongoNamespace namespace,
                           Bson filter,
                           FindOneAndDeleteOptions options,
                           Decoder<T> decoder,
                           NativeOperationContext context,
                           @Nullable NativeSyncClientSession session);

    // ==================== Aggregate Operations ====================

    <T> NativeSyncCursor<T> aggregate(MongoNamespace namespace,
                                      List<BsonDocument> pipeline,
                                      AggregateOptions options,
                                      @Nullable Boolean bypassDocumentValidation,
                                      Decoder<T> decoder,
                                      NativeOperationContext context,
                                      @Nullable NativeSyncClientSession session);

    <T> NativeSyncCursor<T> aggregateDatabase(String databaseName,
                                              List<BsonDocument> pipeline,
                                              AggregateOptions options,
                                              @Nullable Boolean bypassDocumentValidation,
                                              Decoder<T> decoder,
                                              NativeOperationContext context,
                                              @Nullable NativeSyncClientSession session);

    // ==================== Count Operations ====================

    long countDocuments(MongoNamespace namespace,
                        Bson filter,
                        CountOptions options,
                        NativeOperationContext context,
                        @Nullable NativeSyncClientSession session);

    long estimatedDocumentCount(MongoNamespace namespace,
                                EstimatedDocumentCountOptions options,
                                NativeOperationContext context);

    <T> NativeSyncCursor<T> distinct(MongoNamespace namespace,
                                     String fieldName,
                                     Bson filter,
                                     DistinctOptions options,
                                     Decoder<T> decoder,
                                     NativeOperationContext context,
                                     @Nullable NativeSyncClientSession session);

    // ==================== Index Operations ====================

    String createIndex(MongoNamespace namespace,
                       Bson keys,
                       CreateIndexOptions options,
                       NativeOperationContext context,
                       @Nullable NativeSyncClientSession session);

    List<String> createIndexes(MongoNamespace namespace,
                               List<IndexModel> indexes,
                               CreateIndexOptions options,
                               NativeOperationContext context,
                               @Nullable NativeSyncClientSession session);

    void dropIndex(MongoNamespace namespace,
                   String indexName,
                   DropIndexOptions options,
                   NativeOperationContext context,
                   @Nullable NativeSyncClientSession session);

    void dropIndex(MongoNamespace namespace,
                   Bson keys,
                   DropIndexOptions options,
                   NativeOperationContext context,
                   @Nullable NativeSyncClientSession session);

    <T> NativeSyncCursor<T> listIndexes(MongoNamespace namespace,
                                        ListIndexesOptions options,
                                        Decoder<T> decoder,
                                        NativeOperationContext context,
                                        @Nullable NativeSyncClientSession session);

    // ==================== Collection Admin Operations ====================

    void createCollection(String databaseName,
                          String collectionName,
                          CreateCollectionOptions options,
                          NativeOperationContext context,
                          @Nullable NativeSyncClientSession session);

    void dropCollection(MongoNamespace namespace,
                        DropCollectionOptions options,
                        NativeOperationContext context,
                        @Nullable NativeSyncClientSession session);

    void renameCollection(MongoNamespace namespace,
                          MongoNamespace newNamespace,
                          RenameCollectionOptions options,
                          NativeOperationContext context,
                          @Nullable NativeSyncClientSession session);

    <T> NativeSyncCursor<T> listCollections(String databaseName,
                                            ListCollectionsOptions options,
                                            Decoder<T> decoder,
                                            NativeOperationContext context,
                                            @Nullable NativeSyncClientSession session);

    NativeSyncCursor<String> listCollectionNames(String databaseName,
                                                 ListCollectionsOptions options,
                                                 NativeOperationContext context,
                                                 @Nullable NativeSyncClientSession session);

    // ==================== Database Admin Operations ====================

    void dropDatabase(String databaseName,
                      NativeOperationContext context,
                      @Nullable NativeSyncClientSession session);

    <T> NativeSyncCursor<T> listDatabases(ListDatabasesOptions options,
                                          Decoder<T> decoder,
                                          NativeOperationContext context,
                                          @Nullable NativeSyncClientSession session);

    NativeSyncCursor<String> listDatabaseNames(ListDatabasesOptions options,
                                               NativeOperationContext context,
                                               @Nullable NativeSyncClientSession session);

    // ==================== Command Operations ====================

    <T> T runCommand(String databaseName,
                     BsonDocument command,
                     Decoder<T> decoder,
                     NativeOperationContext context,
                     @Nullable NativeSyncClientSession session);

    <T> NativeSyncCursor<T> runCursorCommand(String databaseName,
                                             BsonDocument command,
                                             Decoder<T> decoder,
                                             NativeOperationContext context,
                                             @Nullable NativeSyncClientSession session);

    // ==================== Change Stream Operations ====================

    <T> NativeSyncChangeStream<T> watchCollection(MongoNamespace namespace,
                                                   List<BsonDocument> pipeline,
                                                   ChangeStreamOptions options,
                                                   Decoder<T> decoder,
                                                   NativeOperationContext context,
                                                   @Nullable NativeSyncClientSession session);

    <T> NativeSyncChangeStream<T> watchDatabase(String databaseName,
                                                 List<BsonDocument> pipeline,
                                                 ChangeStreamOptions options,
                                                 Decoder<T> decoder,
                                                 NativeOperationContext context,
                                                 @Nullable NativeSyncClientSession session);

    <T> NativeSyncChangeStream<T> watchClient(List<BsonDocument> pipeline,
                                               ChangeStreamOptions options,
                                               Decoder<T> decoder,
                                               NativeOperationContext context,
                                               @Nullable NativeSyncClientSession session);

    // ==================== Bulk Write Operations ====================

    BulkWriteResult bulkWrite(MongoNamespace namespace,
                              List<? extends WriteModel<BsonDocument>> requests,
                              BulkWriteOptions options,
                              NativeOperationContext context,
                              @Nullable NativeSyncClientSession session);

    // ==================== Lifecycle ====================

    @Override
    void close();
}

