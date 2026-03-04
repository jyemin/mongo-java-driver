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

import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.ClientSession;
import com.mongodb.client.DistinctIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.ListSearchIndexesIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.*;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.lang.Nullable;
import com.mongodb.rust.crud.NativeOperationContext;
import com.mongodb.rust.crud.NativeSyncClient;
import com.mongodb.rust.crud.NativeSyncClientSession;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWrapper;
import org.bson.Document;
import org.bson.codecs.Codec;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * Native implementation of MongoCollection using Rust FFI.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class NativeMongoCollection<TDocument> implements MongoCollection<TDocument> {

    private final NativeSyncClient nativeClient;
    private final MongoNamespace namespace;
    private final Class<TDocument> documentClass;
    private final CodecRegistry codecRegistry;
    private final NativeOperationContext operationContext;

    public NativeMongoCollection(NativeSyncClient nativeClient,
                                 String databaseName,
                                 String collectionName,
                                 Class<TDocument> documentClass,
                                 CodecRegistry codecRegistry,
                                 ReadPreference readPreference,
                                 WriteConcern writeConcern,
                                 ReadConcern readConcern) {
        this.nativeClient = notNull("nativeClient", nativeClient);
        this.namespace = new MongoNamespace(notNull("databaseName", databaseName), notNull("collectionName", collectionName));
        this.documentClass = notNull("documentClass", documentClass);
        this.codecRegistry = notNull("codecRegistry", codecRegistry);
        this.operationContext = NativeOperationContext.builder()
                .readPreference(notNull("readPreference", readPreference))
                .writeConcern(notNull("writeConcern", writeConcern))
                .readConcern(notNull("readConcern", readConcern))
                .build();
    }

    // ==================== Basic Getters ====================

    @Override
    public MongoNamespace getNamespace() {
        return namespace;
    }

    @Override
    public Class<TDocument> getDocumentClass() {
        return documentClass;
    }

    @Override
    public CodecRegistry getCodecRegistry() {
        return codecRegistry;
    }

    @Override
    public ReadPreference getReadPreference() {
        return operationContext.getReadPreference();
    }

    @Override
    public WriteConcern getWriteConcern() {
        return operationContext.getWriteConcern();
    }

    @Override
    public ReadConcern getReadConcern() {
        return operationContext.getReadConcern();
    }

    // ==================== With* Methods ====================

    @Override
    public <NewTDocument> MongoCollection<NewTDocument> withDocumentClass(Class<NewTDocument> clazz) {
        return new NativeMongoCollection<>(nativeClient, namespace.getDatabaseName(), namespace.getCollectionName(),
                clazz, codecRegistry, getReadPreference(), getWriteConcern(), getReadConcern());
    }

    @Override
    public MongoCollection<TDocument> withCodecRegistry(CodecRegistry codecRegistry) {
        return new NativeMongoCollection<>(nativeClient, namespace.getDatabaseName(), namespace.getCollectionName(),
                documentClass, codecRegistry, getReadPreference(), getWriteConcern(), getReadConcern());
    }

    @Override
    public MongoCollection<TDocument> withReadPreference(ReadPreference readPreference) {
        return new NativeMongoCollection<>(nativeClient, namespace.getDatabaseName(), namespace.getCollectionName(),
                documentClass, codecRegistry, readPreference, getWriteConcern(), getReadConcern());
    }

    @Override
    public MongoCollection<TDocument> withWriteConcern(WriteConcern writeConcern) {
        return new NativeMongoCollection<>(nativeClient, namespace.getDatabaseName(), namespace.getCollectionName(),
                documentClass, codecRegistry, getReadPreference(), writeConcern, getReadConcern());
    }

    @Override
    public MongoCollection<TDocument> withReadConcern(ReadConcern readConcern) {
        return new NativeMongoCollection<>(nativeClient, namespace.getDatabaseName(), namespace.getCollectionName(),
                documentClass, codecRegistry, getReadPreference(), getWriteConcern(), readConcern);
    }

    @Override
    public MongoCollection<TDocument> withTimeout(long timeout, TimeUnit timeUnit) {
        // TODO: Implement timeout support
        return this;
    }

    @Override
    @Nullable
    public Long getTimeout(TimeUnit timeUnit) {
        // TODO: Implement timeout support
        return null;
    }

    // ==================== Helper Methods ====================

    private BsonDocument toBsonDocument(Bson bson) {
        return BsonDocumentWrapper.asBsonDocument(bson, codecRegistry);
    }

    private BsonDocument documentToBson(TDocument document) {
        return BsonDocumentWrapper.asBsonDocument(document, codecRegistry);
    }

    // ==================== Count Operations ====================

    @Override
    public long countDocuments() {
        return countDocuments(new BsonDocument());
    }

    @Override
    public long countDocuments(Bson filter) {
        return countDocuments(filter, new CountOptions());
    }

    @Override
    public long countDocuments(Bson filter, CountOptions options) {
        return countDocumentsInternal(null, filter, options);
    }

    @Override
    public long countDocuments(ClientSession clientSession) {
        return countDocuments(clientSession, new BsonDocument());
    }

    @Override
    public long countDocuments(ClientSession clientSession, Bson filter) {
        return countDocuments(clientSession, filter, new CountOptions());
    }

    @Override
    public long countDocuments(ClientSession clientSession, Bson filter, CountOptions options) {
        return countDocumentsInternal(clientSession, filter, options);
    }

    private long countDocumentsInternal(@Nullable ClientSession clientSession, Bson filter, CountOptions options) {
        return nativeClient.countDocuments(namespace, toBsonDocument(filter), options, operationContext, getNativeSession(clientSession));
    }

    @Override
    public long estimatedDocumentCount() {
        return estimatedDocumentCount(new EstimatedDocumentCountOptions());
    }

    @Override
    public long estimatedDocumentCount(EstimatedDocumentCountOptions options) {
        return nativeClient.estimatedDocumentCount(namespace, options, operationContext);
    }

    // ==================== Find Operations ====================

    @Override
    public FindIterable<TDocument> find() {
        return find(new BsonDocument(), documentClass);
    }

    @Override
    public <TResult> FindIterable<TResult> find(Class<TResult> resultClass) {
        return find(new BsonDocument(), resultClass);
    }

    @Override
    public FindIterable<TDocument> find(Bson filter) {
        return find(filter, documentClass);
    }

    @Override
    public <TResult> FindIterable<TResult> find(Bson filter, Class<TResult> resultClass) {
        return findInternal(null, filter, resultClass);
    }

    @Override
    public FindIterable<TDocument> find(ClientSession clientSession) {
        return find(clientSession, new BsonDocument(), documentClass);
    }

    @Override
    public <TResult> FindIterable<TResult> find(ClientSession clientSession, Class<TResult> resultClass) {
        return find(clientSession, new BsonDocument(), resultClass);
    }

    @Override
    public FindIterable<TDocument> find(ClientSession clientSession, Bson filter) {
        return find(clientSession, filter, documentClass);
    }

    @Override
    public <TResult> FindIterable<TResult> find(ClientSession clientSession, Bson filter, Class<TResult> resultClass) {
        return findInternal(clientSession, filter, resultClass);
    }

    private <TResult> FindIterable<TResult> findInternal(@Nullable ClientSession clientSession, Bson filter, Class<TResult> resultClass) {
        NativeFindIterable<TResult> iterable = new NativeFindIterable<>(nativeClient, getNativeSession(clientSession),
                operationContext, namespace, resultClass, codecRegistry);
        iterable.filter(filter);
        return iterable;
    }

    // ==================== Aggregate Operations ====================

    @Override
    public AggregateIterable<TDocument> aggregate(List<? extends Bson> pipeline) {
        return aggregate(pipeline, documentClass);
    }

    @Override
    public <TResult> AggregateIterable<TResult> aggregate(List<? extends Bson> pipeline, Class<TResult> resultClass) {
        return aggregateInternal(null, pipeline, resultClass);
    }

    @Override
    public AggregateIterable<TDocument> aggregate(ClientSession clientSession, List<? extends Bson> pipeline) {
        return aggregate(clientSession, pipeline, documentClass);
    }

    @Override
    public <TResult> AggregateIterable<TResult> aggregate(ClientSession clientSession, List<? extends Bson> pipeline, Class<TResult> resultClass) {
        return aggregateInternal(clientSession, pipeline, resultClass);
    }

    private <TResult> AggregateIterable<TResult> aggregateInternal(@Nullable ClientSession clientSession,
                                                                    List<? extends Bson> pipeline,
                                                                    Class<TResult> resultClass) {
        return new NativeAggregateIterable<>(nativeClient, getNativeSession(clientSession), operationContext, namespace, pipeline, resultClass, codecRegistry);
    }

    // ==================== Watch Operations ====================

    @Override
    public ChangeStreamIterable<TDocument> watch() {
        return watch(Collections.emptyList(), documentClass);
    }

    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(Class<TResult> resultClass) {
        return watch(Collections.emptyList(), resultClass);
    }

    @Override
    public ChangeStreamIterable<TDocument> watch(List<? extends Bson> pipeline) {
        return watch(pipeline, documentClass);
    }

    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(List<? extends Bson> pipeline, Class<TResult> resultClass) {
        return new NativeChangeStreamIterable<>(nativeClient, null, operationContext, pipeline, resultClass, codecRegistry,
                NativeChangeStreamIterable.WatchLevel.COLLECTION, namespace.getDatabaseName(), namespace.getCollectionName());
    }

    @Override
    public ChangeStreamIterable<TDocument> watch(ClientSession clientSession) {
        return watch(clientSession, Collections.emptyList(), documentClass);
    }

    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(ClientSession clientSession, Class<TResult> resultClass) {
        return watch(clientSession, Collections.emptyList(), resultClass);
    }

    @Override
    public ChangeStreamIterable<TDocument> watch(ClientSession clientSession, List<? extends Bson> pipeline) {
        return watch(clientSession, pipeline, documentClass);
    }

    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(ClientSession clientSession, List<? extends Bson> pipeline, Class<TResult> resultClass) {
        return new NativeChangeStreamIterable<>(nativeClient, getNativeSession(clientSession), operationContext, pipeline, resultClass, codecRegistry,
                NativeChangeStreamIterable.WatchLevel.COLLECTION, namespace.getDatabaseName(), namespace.getCollectionName());
    }

    // ==================== Distinct Operations ====================

    @Override
    public <TResult> DistinctIterable<TResult> distinct(String fieldName, Class<TResult> resultClass) {
        return distinct(fieldName, new BsonDocument(), resultClass);
    }

    @Override
    public <TResult> DistinctIterable<TResult> distinct(String fieldName, Bson filter, Class<TResult> resultClass) {
        return distinctInternal(null, fieldName, filter, resultClass);
    }

    @Override
    public <TResult> DistinctIterable<TResult> distinct(ClientSession clientSession, String fieldName, Class<TResult> resultClass) {
        return distinct(clientSession, fieldName, new BsonDocument(), resultClass);
    }

    @Override
    public <TResult> DistinctIterable<TResult> distinct(ClientSession clientSession, String fieldName, Bson filter, Class<TResult> resultClass) {
        return distinctInternal(clientSession, fieldName, filter, resultClass);
    }

    private <TResult> DistinctIterable<TResult> distinctInternal(@Nullable ClientSession clientSession, String fieldName,
                                                                  Bson filter, Class<TResult> resultClass) {
        NativeDistinctIterable<TResult> iterable = new NativeDistinctIterable<>(nativeClient, getNativeSession(clientSession),
                operationContext, namespace, fieldName, resultClass, codecRegistry);
        iterable.filter(filter);
        return iterable;
    }

    // ==================== Insert Operations ====================

    @Override
    public InsertOneResult insertOne(TDocument document) {
        return insertOne(document, new InsertOneOptions());
    }

    @Override
    public InsertOneResult insertOne(TDocument document, InsertOneOptions options) {
        return insertOneInternal(null, document, options);
    }

    @Override
    public InsertOneResult insertOne(ClientSession clientSession, TDocument document) {
        return insertOne(clientSession, document, new InsertOneOptions());
    }

    @Override
    public InsertOneResult insertOne(ClientSession clientSession, TDocument document, InsertOneOptions options) {
        return insertOneInternal(clientSession, document, options);
    }

    private InsertOneResult insertOneInternal(@Nullable ClientSession clientSession, TDocument document, InsertOneOptions options) {
        return nativeClient.insertOne(namespace, documentToBson(document), options, operationContext, getNativeSession(clientSession));
    }

    @Override
    public InsertManyResult insertMany(List<? extends TDocument> documents) {
        return insertMany(documents, new InsertManyOptions());
    }

    @Override
    public InsertManyResult insertMany(List<? extends TDocument> documents, InsertManyOptions options) {
        return insertManyInternal(null, documents, options);
    }

    @Override
    public InsertManyResult insertMany(ClientSession clientSession, List<? extends TDocument> documents) {
        return insertMany(clientSession, documents, new InsertManyOptions());
    }

    @Override
    public InsertManyResult insertMany(ClientSession clientSession, List<? extends TDocument> documents, InsertManyOptions options) {
        return insertManyInternal(clientSession, documents, options);
    }

    private InsertManyResult insertManyInternal(@Nullable ClientSession clientSession, List<? extends TDocument> documents, InsertManyOptions options) {
        List<BsonDocument> bsonDocs = new ArrayList<>(documents.size());
        for (TDocument doc : documents) {
            bsonDocs.add(documentToBson(doc));
        }
        return nativeClient.insertMany(namespace, bsonDocs, options, operationContext, getNativeSession(clientSession));
    }

    // ==================== Delete Operations ====================

    @Override
    public DeleteResult deleteOne(Bson filter) {
        return deleteOne(filter, new DeleteOptions());
    }

    @Override
    public DeleteResult deleteOne(Bson filter, DeleteOptions options) {
        return deleteOneInternal(null, filter, options);
    }

    @Override
    public DeleteResult deleteOne(ClientSession clientSession, Bson filter) {
        return deleteOne(clientSession, filter, new DeleteOptions());
    }

    @Override
    public DeleteResult deleteOne(ClientSession clientSession, Bson filter, DeleteOptions options) {
        return deleteOneInternal(clientSession, filter, options);
    }

    private DeleteResult deleteOneInternal(@Nullable ClientSession clientSession, Bson filter, DeleteOptions options) {
        return nativeClient.deleteOne(namespace, toBsonDocument(filter), options, operationContext, getNativeSession(clientSession));
    }

    @Override
    public DeleteResult deleteMany(Bson filter) {
        return deleteMany(filter, new DeleteOptions());
    }

    @Override
    public DeleteResult deleteMany(Bson filter, DeleteOptions options) {
        return deleteManyInternal(null, filter, options);
    }

    @Override
    public DeleteResult deleteMany(ClientSession clientSession, Bson filter) {
        return deleteMany(clientSession, filter, new DeleteOptions());
    }

    @Override
    public DeleteResult deleteMany(ClientSession clientSession, Bson filter, DeleteOptions options) {
        return deleteManyInternal(clientSession, filter, options);
    }

    private DeleteResult deleteManyInternal(@Nullable ClientSession clientSession, Bson filter, DeleteOptions options) {
        return nativeClient.deleteMany(namespace, toBsonDocument(filter), options, operationContext, getNativeSession(clientSession));
    }

    // ==================== Update Operations ====================

    @Override
    public UpdateResult updateOne(Bson filter, Bson update) {
        return updateOne(filter, update, new UpdateOptions());
    }

    @Override
    public UpdateResult updateOne(Bson filter, Bson update, UpdateOptions options) {
        return updateOneInternal(null, filter, update, options);
    }

    @Override
    public UpdateResult updateOne(ClientSession clientSession, Bson filter, Bson update) {
        return updateOne(clientSession, filter, update, new UpdateOptions());
    }

    @Override
    public UpdateResult updateOne(ClientSession clientSession, Bson filter, Bson update, UpdateOptions options) {
        return updateOneInternal(clientSession, filter, update, options);
    }

    @Override
    public UpdateResult updateOne(Bson filter, List<? extends Bson> update) {
        return updateOne(filter, update, new UpdateOptions());
    }

    @Override
    public UpdateResult updateOne(Bson filter, List<? extends Bson> update, UpdateOptions options) {
        return updateOneInternal(null, filter, pipelineToBson(update), options);
    }

    @Override
    public UpdateResult updateOne(ClientSession clientSession, Bson filter, List<? extends Bson> update) {
        return updateOne(clientSession, filter, update, new UpdateOptions());
    }

    @Override
    public UpdateResult updateOne(ClientSession clientSession, Bson filter, List<? extends Bson> update, UpdateOptions options) {
        return updateOneInternal(clientSession, filter, pipelineToBson(update), options);
    }

    private UpdateResult updateOneInternal(@Nullable ClientSession clientSession, Bson filter, Bson update, UpdateOptions options) {
        return nativeClient.updateOne(namespace, toBsonDocument(filter), toBsonDocument(update), options, operationContext, getNativeSession(clientSession));
    }

    @Override
    public UpdateResult updateMany(Bson filter, Bson update) {
        return updateMany(filter, update, new UpdateOptions());
    }

    @Override
    public UpdateResult updateMany(Bson filter, Bson update, UpdateOptions options) {
        return updateManyInternal(null, filter, update, options);
    }

    @Override
    public UpdateResult updateMany(ClientSession clientSession, Bson filter, Bson update) {
        return updateMany(clientSession, filter, update, new UpdateOptions());
    }

    @Override
    public UpdateResult updateMany(ClientSession clientSession, Bson filter, Bson update, UpdateOptions options) {
        return updateManyInternal(clientSession, filter, update, options);
    }

    @Override
    public UpdateResult updateMany(Bson filter, List<? extends Bson> update) {
        return updateMany(filter, update, new UpdateOptions());
    }

    @Override
    public UpdateResult updateMany(Bson filter, List<? extends Bson> update, UpdateOptions options) {
        return updateManyInternal(null, filter, pipelineToBson(update), options);
    }

    @Override
    public UpdateResult updateMany(ClientSession clientSession, Bson filter, List<? extends Bson> update) {
        return updateMany(clientSession, filter, update, new UpdateOptions());
    }

    @Override
    public UpdateResult updateMany(ClientSession clientSession, Bson filter, List<? extends Bson> update, UpdateOptions options) {
        return updateManyInternal(clientSession, filter, pipelineToBson(update), options);
    }

    private UpdateResult updateManyInternal(@Nullable ClientSession clientSession, Bson filter, Bson update, UpdateOptions options) {
        return nativeClient.updateMany(namespace, toBsonDocument(filter), toBsonDocument(update), options, operationContext, getNativeSession(clientSession));
    }

    private BsonDocument pipelineToBson(List<? extends Bson> pipeline) {
        List<BsonDocument> stages = new ArrayList<>(pipeline.size());
        for (Bson stage : pipeline) {
            stages.add(toBsonDocument(stage));
        }
        return new BsonDocument("$pipeline", new org.bson.BsonArray(stages));
    }

    // ==================== Replace Operations ====================

    @Override
    public UpdateResult replaceOne(Bson filter, TDocument replacement) {
        return replaceOne(filter, replacement, new ReplaceOptions());
    }

    @Override
    public UpdateResult replaceOne(Bson filter, TDocument replacement, ReplaceOptions options) {
        return replaceOneInternal(null, filter, replacement, options);
    }

    @Override
    public UpdateResult replaceOne(ClientSession clientSession, Bson filter, TDocument replacement) {
        return replaceOne(clientSession, filter, replacement, new ReplaceOptions());
    }

    @Override
    public UpdateResult replaceOne(ClientSession clientSession, Bson filter, TDocument replacement, ReplaceOptions options) {
        return replaceOneInternal(clientSession, filter, replacement, options);
    }

    private UpdateResult replaceOneInternal(@Nullable ClientSession clientSession, Bson filter, TDocument replacement, ReplaceOptions options) {
        return nativeClient.replaceOne(namespace, toBsonDocument(filter), documentToBson(replacement), options, operationContext, getNativeSession(clientSession));
    }

    // ==================== FindOneAnd* Operations ====================

    @Override
    @Nullable
    public TDocument findOneAndDelete(Bson filter) {
        return findOneAndDelete(filter, new FindOneAndDeleteOptions());
    }

    @Override
    @Nullable
    public TDocument findOneAndDelete(Bson filter, FindOneAndDeleteOptions options) {
        return findOneAndDeleteInternal(null, filter, options);
    }

    @Override
    @Nullable
    public TDocument findOneAndDelete(ClientSession clientSession, Bson filter) {
        return findOneAndDelete(clientSession, filter, new FindOneAndDeleteOptions());
    }

    @Override
    @Nullable
    public TDocument findOneAndDelete(ClientSession clientSession, Bson filter, FindOneAndDeleteOptions options) {
        return findOneAndDeleteInternal(clientSession, filter, options);
    }

    @Nullable
    private TDocument findOneAndDeleteInternal(@Nullable ClientSession clientSession, Bson filter, FindOneAndDeleteOptions options) {
        return nativeClient.findOneAndDelete(namespace, toBsonDocument(filter), options, getCodec(), operationContext, getNativeSession(clientSession));
    }

    @Override
    @Nullable
    public TDocument findOneAndReplace(Bson filter, TDocument replacement) {
        return findOneAndReplace(filter, replacement, new FindOneAndReplaceOptions());
    }

    @Override
    @Nullable
    public TDocument findOneAndReplace(Bson filter, TDocument replacement, FindOneAndReplaceOptions options) {
        return findOneAndReplaceInternal(null, filter, replacement, options);
    }

    @Override
    @Nullable
    public TDocument findOneAndReplace(ClientSession clientSession, Bson filter, TDocument replacement) {
        return findOneAndReplace(clientSession, filter, replacement, new FindOneAndReplaceOptions());
    }

    @Override
    @Nullable
    public TDocument findOneAndReplace(ClientSession clientSession, Bson filter, TDocument replacement, FindOneAndReplaceOptions options) {
        return findOneAndReplaceInternal(clientSession, filter, replacement, options);
    }

    @Nullable
    private TDocument findOneAndReplaceInternal(@Nullable ClientSession clientSession, Bson filter, TDocument replacement, FindOneAndReplaceOptions options) {
        return nativeClient.findOneAndReplace(namespace, toBsonDocument(filter), documentToBson(replacement), options, getCodec(), operationContext, getNativeSession(clientSession));
    }

    @Override
    @Nullable
    public TDocument findOneAndUpdate(Bson filter, Bson update) {
        return findOneAndUpdate(filter, update, new FindOneAndUpdateOptions());
    }

    @Override
    @Nullable
    public TDocument findOneAndUpdate(Bson filter, Bson update, FindOneAndUpdateOptions options) {
        return findOneAndUpdateInternal(null, filter, update, options);
    }

    @Override
    @Nullable
    public TDocument findOneAndUpdate(ClientSession clientSession, Bson filter, Bson update) {
        return findOneAndUpdate(clientSession, filter, update, new FindOneAndUpdateOptions());
    }

    @Override
    @Nullable
    public TDocument findOneAndUpdate(ClientSession clientSession, Bson filter, Bson update, FindOneAndUpdateOptions options) {
        return findOneAndUpdateInternal(clientSession, filter, update, options);
    }

    @Override
    @Nullable
    public TDocument findOneAndUpdate(Bson filter, List<? extends Bson> update) {
        return findOneAndUpdate(filter, update, new FindOneAndUpdateOptions());
    }

    @Override
    @Nullable
    public TDocument findOneAndUpdate(Bson filter, List<? extends Bson> update, FindOneAndUpdateOptions options) {
        return findOneAndUpdateInternal(null, filter, pipelineToBson(update), options);
    }

    @Override
    @Nullable
    public TDocument findOneAndUpdate(ClientSession clientSession, Bson filter, List<? extends Bson> update) {
        return findOneAndUpdate(clientSession, filter, update, new FindOneAndUpdateOptions());
    }

    @Override
    @Nullable
    public TDocument findOneAndUpdate(ClientSession clientSession, Bson filter, List<? extends Bson> update, FindOneAndUpdateOptions options) {
        return findOneAndUpdateInternal(clientSession, filter, pipelineToBson(update), options);
    }

    @Nullable
    private TDocument findOneAndUpdateInternal(@Nullable ClientSession clientSession, Bson filter, Bson update, FindOneAndUpdateOptions options) {
        return nativeClient.findOneAndUpdate(namespace, toBsonDocument(filter), toBsonDocument(update), options, getCodec(), operationContext, getNativeSession(clientSession));
    }

    // ==================== BulkWrite Operations ====================

    @Override
    public BulkWriteResult bulkWrite(List<? extends WriteModel<? extends TDocument>> requests) {
        return bulkWrite(requests, new BulkWriteOptions());
    }

    @Override
    public BulkWriteResult bulkWrite(List<? extends WriteModel<? extends TDocument>> requests, BulkWriteOptions options) {
        return bulkWriteInternal(null, requests, options);
    }

    @Override
    public BulkWriteResult bulkWrite(ClientSession clientSession, List<? extends WriteModel<? extends TDocument>> requests) {
        return bulkWrite(clientSession, requests, new BulkWriteOptions());
    }

    @Override
    public BulkWriteResult bulkWrite(ClientSession clientSession, List<? extends WriteModel<? extends TDocument>> requests, BulkWriteOptions options) {
        return bulkWriteInternal(clientSession, requests, options);
    }

    @SuppressWarnings("unchecked")
    private BulkWriteResult bulkWriteInternal(@Nullable ClientSession clientSession,
                                               List<? extends WriteModel<? extends TDocument>> requests,
                                               BulkWriteOptions options) {
        // Convert WriteModel<TDocument> to WriteModel<BsonDocument>
        List<WriteModel<BsonDocument>> bsonRequests = new ArrayList<>(requests.size());
        for (WriteModel<? extends TDocument> request : requests) {
            bsonRequests.add(convertWriteModel(request));
        }
        return nativeClient.bulkWrite(namespace, bsonRequests, options, operationContext, getNativeSession(clientSession));
    }

    @SuppressWarnings("unchecked")
    private WriteModel<BsonDocument> convertWriteModel(WriteModel<? extends TDocument> model) {
        if (model instanceof InsertOneModel) {
            InsertOneModel<? extends TDocument> insertModel = (InsertOneModel<? extends TDocument>) model;
            return new InsertOneModel<>(documentToBson((TDocument) insertModel.getDocument()));
        } else if (model instanceof UpdateOneModel) {
            UpdateOneModel<? extends TDocument> updateModel = (UpdateOneModel<? extends TDocument>) model;
            return new UpdateOneModel<>(toBsonDocument(updateModel.getFilter()),
                    toBsonDocument(updateModel.getUpdate()), updateModel.getOptions());
        } else if (model instanceof UpdateManyModel) {
            UpdateManyModel<? extends TDocument> updateModel = (UpdateManyModel<? extends TDocument>) model;
            return new UpdateManyModel<>(toBsonDocument(updateModel.getFilter()),
                    toBsonDocument(updateModel.getUpdate()), updateModel.getOptions());
        } else if (model instanceof ReplaceOneModel) {
            ReplaceOneModel<? extends TDocument> replaceModel = (ReplaceOneModel<? extends TDocument>) model;
            return new ReplaceOneModel<>(toBsonDocument(replaceModel.getFilter()),
                    documentToBson((TDocument) replaceModel.getReplacement()), replaceModel.getReplaceOptions());
        } else if (model instanceof DeleteOneModel) {
            DeleteOneModel<? extends TDocument> deleteModel = (DeleteOneModel<? extends TDocument>) model;
            return new DeleteOneModel<>(toBsonDocument(deleteModel.getFilter()), deleteModel.getOptions());
        } else if (model instanceof DeleteManyModel) {
            DeleteManyModel<? extends TDocument> deleteModel = (DeleteManyModel<? extends TDocument>) model;
            return new DeleteManyModel<>(toBsonDocument(deleteModel.getFilter()), deleteModel.getOptions());
        }
        throw new IllegalArgumentException("Unknown WriteModel type: " + model.getClass());
    }

    // ==================== Drop Operations ====================

    @Override
    public void drop() {
        drop(new DropCollectionOptions());
    }

    @Override
    public void drop(DropCollectionOptions options) {
        dropInternal(null, options);
    }

    @Override
    public void drop(ClientSession clientSession) {
        drop(clientSession, new DropCollectionOptions());
    }

    @Override
    public void drop(ClientSession clientSession, DropCollectionOptions options) {
        dropInternal(clientSession, options);
    }

    private void dropInternal(@Nullable ClientSession clientSession, DropCollectionOptions options) {
        nativeClient.dropCollection(namespace, options, operationContext, getNativeSession(clientSession));
    }



    // ==================== Index Operations ====================

    @Override
    public String createIndex(Bson keys) {
        return createIndex(keys, new IndexOptions());
    }

    @Override
    public String createIndex(Bson keys, IndexOptions indexOptions) {
        return createIndexInternal(null, keys, indexOptions);
    }

    @Override
    public String createIndex(ClientSession clientSession, Bson keys) {
        return createIndex(clientSession, keys, new IndexOptions());
    }

    @Override
    public String createIndex(ClientSession clientSession, Bson keys, IndexOptions indexOptions) {
        return createIndexInternal(clientSession, keys, indexOptions);
    }

    private String createIndexInternal(@Nullable ClientSession clientSession, Bson keys, IndexOptions indexOptions) {
        CreateIndexOptions options = new CreateIndexOptions();
        // TODO: Copy timeout settings from IndexOptions to CreateIndexOptions if needed
        return nativeClient.createIndex(namespace, toBsonDocument(keys), options, operationContext, getNativeSession(clientSession));
    }

    @Override
    public List<String> createIndexes(List<IndexModel> indexes) {
        return createIndexes(indexes, new CreateIndexOptions());
    }

    @Override
    public List<String> createIndexes(List<IndexModel> indexes, CreateIndexOptions createIndexOptions) {
        return createIndexesInternal(null, indexes, createIndexOptions);
    }

    @Override
    public List<String> createIndexes(ClientSession clientSession, List<IndexModel> indexes) {
        return createIndexes(clientSession, indexes, new CreateIndexOptions());
    }

    @Override
    public List<String> createIndexes(ClientSession clientSession, List<IndexModel> indexes, CreateIndexOptions createIndexOptions) {
        return createIndexesInternal(clientSession, indexes, createIndexOptions);
    }

    private List<String> createIndexesInternal(@Nullable ClientSession clientSession, List<IndexModel> indexes, CreateIndexOptions createIndexOptions) {
        return nativeClient.createIndexes(namespace, indexes, createIndexOptions, operationContext, getNativeSession(clientSession));
    }

    @Override
    public ListIndexesIterable<Document> listIndexes() {
        return listIndexes(Document.class);
    }

    @Override
    public <TResult> ListIndexesIterable<TResult> listIndexes(Class<TResult> resultClass) {
        return listIndexesInternal(null, resultClass);
    }

    @Override
    public ListIndexesIterable<Document> listIndexes(ClientSession clientSession) {
        return listIndexes(clientSession, Document.class);
    }

    @Override
    public <TResult> ListIndexesIterable<TResult> listIndexes(ClientSession clientSession, Class<TResult> resultClass) {
        return listIndexesInternal(clientSession, resultClass);
    }

    private <TResult> ListIndexesIterable<TResult> listIndexesInternal(@Nullable ClientSession clientSession, Class<TResult> resultClass) {
        return new NativeListIndexesIterable<>(nativeClient, getNativeSession(clientSession), operationContext, namespace, resultClass, codecRegistry);
    }

    @Override
    public void dropIndex(String indexName) {
        dropIndex(indexName, new DropIndexOptions());
    }

    @Override
    public void dropIndex(String indexName, DropIndexOptions dropIndexOptions) {
        dropIndexInternal(null, indexName, dropIndexOptions);
    }

    @Override
    public void dropIndex(Bson keys) {
        dropIndex(keys, new DropIndexOptions());
    }

    @Override
    public void dropIndex(Bson keys, DropIndexOptions dropIndexOptions) {
        nativeClient.dropIndex(namespace, toBsonDocument(keys), dropIndexOptions, operationContext, getNativeSession(null));
    }

    @Override
    public void dropIndex(ClientSession clientSession, String indexName) {
        dropIndex(clientSession, indexName, new DropIndexOptions());
    }

    @Override
    public void dropIndex(ClientSession clientSession, String indexName, DropIndexOptions dropIndexOptions) {
        dropIndexInternal(clientSession, indexName, dropIndexOptions);
    }

    @Override
    public void dropIndex(ClientSession clientSession, Bson keys) {
        dropIndex(clientSession, keys, new DropIndexOptions());
    }

    @Override
    public void dropIndex(ClientSession clientSession, Bson keys, DropIndexOptions dropIndexOptions) {
        nativeClient.dropIndex(namespace, toBsonDocument(keys), dropIndexOptions, operationContext, getNativeSession(clientSession));
    }

    private void dropIndexInternal(@Nullable ClientSession clientSession, String indexName, DropIndexOptions dropIndexOptions) {
        nativeClient.dropIndex(namespace, indexName, dropIndexOptions, operationContext, getNativeSession(clientSession));
    }

    @Override
    public void dropIndexes() {
        dropIndexes(new DropIndexOptions());
    }

    @Override
    public void dropIndexes(DropIndexOptions dropIndexOptions) {
        dropIndexesInternal(null, dropIndexOptions);
    }

    @Override
    public void dropIndexes(ClientSession clientSession) {
        dropIndexes(clientSession, new DropIndexOptions());
    }

    @Override
    public void dropIndexes(ClientSession clientSession, DropIndexOptions dropIndexOptions) {
        dropIndexesInternal(clientSession, dropIndexOptions);
    }

    private void dropIndexesInternal(@Nullable ClientSession clientSession, DropIndexOptions dropIndexOptions) {
        nativeClient.dropIndex(namespace, "*", dropIndexOptions, operationContext, getNativeSession(clientSession));
    }

    private Codec<TDocument> getCodec() {
        return codecRegistry.get(documentClass);
    }

    @Nullable
    private NativeSyncClientSession getNativeSession(@Nullable ClientSession clientSession) {
        if (clientSession == null) {
            return null;
        }
        if (!(clientSession instanceof NativeClientSession)) {
            throw new IllegalArgumentException("ClientSession must be a NativeClientSession");
        }
        return ((NativeClientSession) clientSession).getNativeSession();
    }

    // ==================== Rename Operations ====================

    @Override
    public void renameCollection(MongoNamespace newCollectionNamespace) {
        renameCollection(newCollectionNamespace, new RenameCollectionOptions());
    }

    @Override
    public void renameCollection(MongoNamespace newCollectionNamespace, RenameCollectionOptions options) {
        renameCollectionInternal(null, newCollectionNamespace, options);
    }

    @Override
    public void renameCollection(ClientSession clientSession, MongoNamespace newCollectionNamespace) {
        renameCollection(clientSession, newCollectionNamespace, new RenameCollectionOptions());
    }

    @Override
    public void renameCollection(ClientSession clientSession, MongoNamespace newCollectionNamespace, RenameCollectionOptions options) {
        renameCollectionInternal(clientSession, newCollectionNamespace, options);
    }

    private void renameCollectionInternal(@Nullable ClientSession clientSession, MongoNamespace newCollectionNamespace, RenameCollectionOptions options) {
        nativeClient.renameCollection(namespace, newCollectionNamespace, options, operationContext, getNativeSession(clientSession));
    }

    // ==================== Search Index Operations (Unsupported) ====================

    @Override
    public String createSearchIndex(String indexName, Bson definition) {
        throw new UnsupportedOperationException("Search indexes not yet supported in native client");
    }

    @Override
    public String createSearchIndex(Bson definition) {
        throw new UnsupportedOperationException("Search indexes not yet supported in native client");
    }

    @Override
    public List<String> createSearchIndexes(List<SearchIndexModel> searchIndexModels) {
        throw new UnsupportedOperationException("Search indexes not yet supported in native client");
    }

    @Override
    public void updateSearchIndex(String indexName, Bson definition) {
        throw new UnsupportedOperationException("Search indexes not yet supported in native client");
    }

    @Override
    public void dropSearchIndex(String indexName) {
        throw new UnsupportedOperationException("Search indexes not yet supported in native client");
    }

    @Override
    public ListSearchIndexesIterable<Document> listSearchIndexes() {
        return listSearchIndexes(Document.class);
    }

    @Override
    public <TResult> ListSearchIndexesIterable<TResult> listSearchIndexes(Class<TResult> resultClass) {
        return new NativeListSearchIndexesIterable<>(nativeClient, null, operationContext, namespace, resultClass, codecRegistry);
    }

    // ==================== MapReduce Operations (Deprecated) ====================

    @Override
    @SuppressWarnings("deprecation")
    public com.mongodb.client.MapReduceIterable<TDocument> mapReduce(String mapFunction, String reduceFunction) {
        throw new UnsupportedOperationException("mapReduce is deprecated and not supported in native client");
    }

    @Override
    @SuppressWarnings("deprecation")
    public <TResult> com.mongodb.client.MapReduceIterable<TResult> mapReduce(String mapFunction, String reduceFunction, Class<TResult> resultClass) {
        throw new UnsupportedOperationException("mapReduce is deprecated and not supported in native client");
    }

    @Override
    @SuppressWarnings("deprecation")
    public com.mongodb.client.MapReduceIterable<TDocument> mapReduce(ClientSession clientSession, String mapFunction, String reduceFunction) {
        throw new UnsupportedOperationException("mapReduce is deprecated and not supported in native client");
    }

    @Override
    @SuppressWarnings("deprecation")
    public <TResult> com.mongodb.client.MapReduceIterable<TResult> mapReduce(ClientSession clientSession, String mapFunction, String reduceFunction, Class<TResult> resultClass) {
        throw new UnsupportedOperationException("mapReduce is deprecated and not supported in native client");
    }
}
