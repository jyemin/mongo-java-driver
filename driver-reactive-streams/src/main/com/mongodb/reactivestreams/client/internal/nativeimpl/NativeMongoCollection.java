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
package com.mongodb.reactivestreams.client.internal.nativeimpl;

import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.*;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.lang.Nullable;
import com.mongodb.reactivestreams.client.*;
import com.mongodb.rust.crud.NativeAsyncClient;
import com.mongodb.rust.crud.NativeAsyncClientSession;
import com.mongodb.rust.crud.NativeOperationContext;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWrapper;
import org.bson.Document;
import org.bson.codecs.Codec;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.reactivestreams.Publisher;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * Native implementation of MongoCollection using Rust FFI.
 */
public final class NativeMongoCollection<TDocument> implements MongoCollection<TDocument> {

    private final NativeAsyncClient nativeClient;
    private final MongoNamespace namespace;
    private final Class<TDocument> documentClass;
    private final CodecRegistry codecRegistry;
    private final NativeOperationContext operationContext;

    NativeMongoCollection(NativeAsyncClient nativeClient, String databaseName, String collectionName,
                          Class<TDocument> documentClass, CodecRegistry codecRegistry,
                          ReadPreference readPreference, WriteConcern writeConcern, ReadConcern readConcern) {
        this.nativeClient = notNull("nativeClient", nativeClient);
        this.namespace = new MongoNamespace(notNull("databaseName", databaseName), notNull("collectionName", collectionName));
        this.documentClass = notNull("documentClass", documentClass);
        this.codecRegistry = notNull("codecRegistry", codecRegistry);
        this.operationContext = NativeOperationContext.builder()
                .readPreference(readPreference)
                .writeConcern(writeConcern)
                .readConcern(readConcern)
                .build();
    }

    @Override public MongoNamespace getNamespace() { return namespace; }
    @Override public Class<TDocument> getDocumentClass() { return documentClass; }
    @Override public CodecRegistry getCodecRegistry() { return codecRegistry; }
    @Override public ReadPreference getReadPreference() { return operationContext.getReadPreference(); }
    @Override public WriteConcern getWriteConcern() { return operationContext.getWriteConcern(); }
    @Override public ReadConcern getReadConcern() { return operationContext.getReadConcern(); }
    @Override @Nullable public Long getTimeout(TimeUnit timeUnit) { return null; }

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

    @Override public MongoCollection<TDocument> withTimeout(long timeout, TimeUnit timeUnit) { return this; }

    private BsonDocument toBsonDocument(Bson bson) {
        return BsonDocumentWrapper.asBsonDocument(bson, codecRegistry);
    }

    private BsonDocument documentToBson(TDocument document) {
        return BsonDocumentWrapper.asBsonDocument(document, codecRegistry);
    }

    @Nullable
    private NativeAsyncClientSession getNativeSession(@Nullable ClientSession clientSession) {
        if (clientSession == null) return null;
        if (!(clientSession instanceof NativeClientSession)) {
            throw new IllegalArgumentException("ClientSession must be a NativeClientSession");
        }
        return ((NativeClientSession) clientSession).getNativeSession();
    }

    // ==================== Count Operations ====================
    @Override public Publisher<Long> countDocuments() { return countDocuments(new BsonDocument()); }
    @Override public Publisher<Long> countDocuments(Bson filter) { return countDocuments(filter, new CountOptions()); }
    @Override public Publisher<Long> countDocuments(Bson filter, CountOptions options) {
        return Publishers.toMono(cb -> nativeClient.countDocuments(namespace, toBsonDocument(filter), options, operationContext, null, cb));
    }
    @Override public Publisher<Long> countDocuments(ClientSession cs) { return countDocuments(cs, new BsonDocument()); }
    @Override public Publisher<Long> countDocuments(ClientSession cs, Bson filter) { return countDocuments(cs, filter, new CountOptions()); }
    @Override public Publisher<Long> countDocuments(ClientSession cs, Bson filter, CountOptions options) {
        return Publishers.toMono(cb -> nativeClient.countDocuments(namespace, toBsonDocument(filter), options, operationContext, getNativeSession(cs), cb));
    }
    @Override public Publisher<Long> estimatedDocumentCount() { return estimatedDocumentCount(new EstimatedDocumentCountOptions()); }
    @Override public Publisher<Long> estimatedDocumentCount(EstimatedDocumentCountOptions options) {
        return Publishers.toMono(cb -> nativeClient.estimatedDocumentCount(namespace, options, operationContext, cb));
    }

    // ==================== Find Operations ====================
    @Override public FindPublisher<TDocument> find() { return find(new BsonDocument(), documentClass); }
    @Override public <T> FindPublisher<T> find(Class<T> resultClass) { return find(new BsonDocument(), resultClass); }
    @Override public FindPublisher<TDocument> find(Bson filter) { return find(filter, documentClass); }
    @Override public <T> FindPublisher<T> find(Bson filter, Class<T> resultClass) {
        return new NativeFindPublisher<>(nativeClient, null, operationContext, namespace, resultClass, codecRegistry).filter(filter);
    }
    @Override public FindPublisher<TDocument> find(ClientSession cs) { return find(cs, new BsonDocument(), documentClass); }
    @Override public <T> FindPublisher<T> find(ClientSession cs, Class<T> resultClass) { return find(cs, new BsonDocument(), resultClass); }
    @Override public FindPublisher<TDocument> find(ClientSession cs, Bson filter) { return find(cs, filter, documentClass); }
    @Override public <T> FindPublisher<T> find(ClientSession cs, Bson filter, Class<T> resultClass) {
        return new NativeFindPublisher<>(nativeClient, getNativeSession(cs), operationContext, namespace, resultClass, codecRegistry).filter(filter);
    }

    // ==================== Aggregate Operations ====================
    @Override public AggregatePublisher<TDocument> aggregate(List<? extends Bson> p) { return aggregate(p, documentClass); }
    @Override public <T> AggregatePublisher<T> aggregate(List<? extends Bson> p, Class<T> resultClass) {
        return new NativeAggregatePublisher<>(nativeClient, null, operationContext, namespace.getDatabaseName(), namespace.getCollectionName(), p, resultClass, codecRegistry);
    }
    @Override public AggregatePublisher<TDocument> aggregate(ClientSession cs, List<? extends Bson> p) { return aggregate(cs, p, documentClass); }
    @Override public <T> AggregatePublisher<T> aggregate(ClientSession cs, List<? extends Bson> p, Class<T> resultClass) {
        return new NativeAggregatePublisher<>(nativeClient, getNativeSession(cs), operationContext, namespace.getDatabaseName(), namespace.getCollectionName(), p, resultClass, codecRegistry);
    }

    // ==================== Distinct Operations ====================
    @Override public <T> DistinctPublisher<T> distinct(String fieldName, Class<T> resultClass) { return distinct(fieldName, new BsonDocument(), resultClass); }
    @Override public <T> DistinctPublisher<T> distinct(String fieldName, Bson filter, Class<T> resultClass) {
        return new NativeDistinctPublisher<>(nativeClient, null, operationContext, namespace, fieldName, resultClass, codecRegistry).filter(filter);
    }
    @Override public <T> DistinctPublisher<T> distinct(ClientSession cs, String fieldName, Class<T> resultClass) { return distinct(cs, fieldName, new BsonDocument(), resultClass); }
    @Override public <T> DistinctPublisher<T> distinct(ClientSession cs, String fieldName, Bson filter, Class<T> resultClass) {
        return new NativeDistinctPublisher<>(nativeClient, getNativeSession(cs), operationContext, namespace, fieldName, resultClass, codecRegistry).filter(filter);
    }

    // ==================== Insert Operations ====================
    @Override public Publisher<InsertOneResult> insertOne(TDocument document) { return insertOne(document, new InsertOneOptions()); }
    @Override public Publisher<InsertOneResult> insertOne(TDocument document, InsertOneOptions options) {
        return Publishers.toMono(cb -> nativeClient.insertOne(namespace, documentToBson(document), options, operationContext, null, cb));
    }
    @Override public Publisher<InsertOneResult> insertOne(ClientSession cs, TDocument document) { return insertOne(cs, document, new InsertOneOptions()); }
    @Override public Publisher<InsertOneResult> insertOne(ClientSession cs, TDocument document, InsertOneOptions options) {
        return Publishers.toMono(cb -> nativeClient.insertOne(namespace, documentToBson(document), options, operationContext, getNativeSession(cs), cb));
    }
    @Override public Publisher<InsertManyResult> insertMany(List<? extends TDocument> documents) { return insertMany(documents, new InsertManyOptions()); }
    @Override public Publisher<InsertManyResult> insertMany(List<? extends TDocument> documents, InsertManyOptions options) {
        List<BsonDocument> bsonDocs = documents.stream().map(this::documentToBson).collect(java.util.stream.Collectors.toList());
        return Publishers.toMono(cb -> nativeClient.insertMany(namespace, bsonDocs, options, operationContext, null, cb));
    }
    @Override public Publisher<InsertManyResult> insertMany(ClientSession cs, List<? extends TDocument> documents) { return insertMany(cs, documents, new InsertManyOptions()); }
    @Override public Publisher<InsertManyResult> insertMany(ClientSession cs, List<? extends TDocument> documents, InsertManyOptions options) {
        List<BsonDocument> bsonDocs = documents.stream().map(this::documentToBson).collect(java.util.stream.Collectors.toList());
        return Publishers.toMono(cb -> nativeClient.insertMany(namespace, bsonDocs, options, operationContext, getNativeSession(cs), cb));
    }

    // ==================== Delete Operations ====================
    @Override public Publisher<DeleteResult> deleteOne(Bson filter) { return deleteOne(filter, new DeleteOptions()); }
    @Override public Publisher<DeleteResult> deleteOne(Bson filter, DeleteOptions options) {
        return Publishers.toMono(cb -> nativeClient.deleteOne(namespace, filter, options, operationContext, null, cb));
    }
    @Override public Publisher<DeleteResult> deleteOne(ClientSession cs, Bson filter) { return deleteOne(cs, filter, new DeleteOptions()); }
    @Override public Publisher<DeleteResult> deleteOne(ClientSession cs, Bson filter, DeleteOptions options) {
        return Publishers.toMono(cb -> nativeClient.deleteOne(namespace, filter, options, operationContext, getNativeSession(cs), cb));
    }
    @Override public Publisher<DeleteResult> deleteMany(Bson filter) { return deleteMany(filter, new DeleteOptions()); }
    @Override public Publisher<DeleteResult> deleteMany(Bson filter, DeleteOptions options) {
        return Publishers.toMono(cb -> nativeClient.deleteMany(namespace, filter, options, operationContext, null, cb));
    }
    @Override public Publisher<DeleteResult> deleteMany(ClientSession cs, Bson filter) { return deleteMany(cs, filter, new DeleteOptions()); }
    @Override public Publisher<DeleteResult> deleteMany(ClientSession cs, Bson filter, DeleteOptions options) {
        return Publishers.toMono(cb -> nativeClient.deleteMany(namespace, filter, options, operationContext, getNativeSession(cs), cb));
    }

    // ==================== Replace Operations ====================
    @Override public Publisher<UpdateResult> replaceOne(Bson filter, TDocument replacement) { return replaceOne(filter, replacement, new ReplaceOptions()); }
    @Override public Publisher<UpdateResult> replaceOne(Bson filter, TDocument replacement, ReplaceOptions options) {
        return Publishers.toMono(cb -> nativeClient.replaceOne(namespace, filter, documentToBson(replacement), options, operationContext, null, cb));
    }
    @Override public Publisher<UpdateResult> replaceOne(ClientSession cs, Bson filter, TDocument replacement) { return replaceOne(cs, filter, replacement, new ReplaceOptions()); }
    @Override public Publisher<UpdateResult> replaceOne(ClientSession cs, Bson filter, TDocument replacement, ReplaceOptions options) {
        return Publishers.toMono(cb -> nativeClient.replaceOne(namespace, filter, documentToBson(replacement), options, operationContext, getNativeSession(cs), cb));
    }

    // ==================== Update Operations ====================
    @Override public Publisher<UpdateResult> updateOne(Bson filter, Bson update) { return updateOne(filter, update, new UpdateOptions()); }
    @Override public Publisher<UpdateResult> updateOne(Bson filter, Bson update, UpdateOptions options) {
        return Publishers.toMono(cb -> nativeClient.updateOne(namespace, filter, update, options, operationContext, null, cb));
    }
    @Override public Publisher<UpdateResult> updateOne(ClientSession cs, Bson filter, Bson update) { return updateOne(cs, filter, update, new UpdateOptions()); }
    @Override public Publisher<UpdateResult> updateOne(ClientSession cs, Bson filter, Bson update, UpdateOptions options) {
        return Publishers.toMono(cb -> nativeClient.updateOne(namespace, filter, update, options, operationContext, getNativeSession(cs), cb));
    }
    @Override public Publisher<UpdateResult> updateOne(Bson filter, List<? extends Bson> update) { return updateOne(filter, update, new UpdateOptions()); }
    @Override public Publisher<UpdateResult> updateOne(Bson filter, List<? extends Bson> update, UpdateOptions options) {
        throw new UnsupportedOperationException("Pipeline update not yet implemented");
    }
    @Override public Publisher<UpdateResult> updateOne(ClientSession cs, Bson filter, List<? extends Bson> update) { return updateOne(cs, filter, update, new UpdateOptions()); }
    @Override public Publisher<UpdateResult> updateOne(ClientSession cs, Bson filter, List<? extends Bson> update, UpdateOptions options) {
        throw new UnsupportedOperationException("Pipeline update not yet implemented");
    }
    @Override public Publisher<UpdateResult> updateMany(Bson filter, Bson update) { return updateMany(filter, update, new UpdateOptions()); }
    @Override public Publisher<UpdateResult> updateMany(Bson filter, Bson update, UpdateOptions options) {
        return Publishers.toMono(cb -> nativeClient.updateMany(namespace, filter, update, options, operationContext, null, cb));
    }
    @Override public Publisher<UpdateResult> updateMany(ClientSession cs, Bson filter, Bson update) { return updateMany(cs, filter, update, new UpdateOptions()); }
    @Override public Publisher<UpdateResult> updateMany(ClientSession cs, Bson filter, Bson update, UpdateOptions options) {
        return Publishers.toMono(cb -> nativeClient.updateMany(namespace, filter, update, options, operationContext, getNativeSession(cs), cb));
    }
    @Override public Publisher<UpdateResult> updateMany(Bson filter, List<? extends Bson> update) { return updateMany(filter, update, new UpdateOptions()); }
    @Override public Publisher<UpdateResult> updateMany(Bson filter, List<? extends Bson> update, UpdateOptions options) {
        throw new UnsupportedOperationException("Pipeline update not yet implemented");
    }
    @Override public Publisher<UpdateResult> updateMany(ClientSession cs, Bson filter, List<? extends Bson> update) { return updateMany(cs, filter, update, new UpdateOptions()); }
    @Override public Publisher<UpdateResult> updateMany(ClientSession cs, Bson filter, List<? extends Bson> update, UpdateOptions options) {
        throw new UnsupportedOperationException("Pipeline update not yet implemented");
    }

    // ==================== FindOneAnd Operations ====================
    @Override public Publisher<TDocument> findOneAndDelete(Bson filter) { return findOneAndDelete(filter, new FindOneAndDeleteOptions()); }
    @Override public Publisher<TDocument> findOneAndDelete(Bson filter, FindOneAndDeleteOptions options) {
        return Publishers.toMono(cb -> nativeClient.findOneAndDelete(namespace, filter, options, codecRegistry.get(documentClass), operationContext, null, cb));
    }
    @Override public Publisher<TDocument> findOneAndDelete(ClientSession cs, Bson filter) { return findOneAndDelete(cs, filter, new FindOneAndDeleteOptions()); }
    @Override public Publisher<TDocument> findOneAndDelete(ClientSession cs, Bson filter, FindOneAndDeleteOptions options) {
        return Publishers.toMono(cb -> nativeClient.findOneAndDelete(namespace, filter, options, codecRegistry.get(documentClass), operationContext, getNativeSession(cs), cb));
    }

    @Override public Publisher<TDocument> findOneAndReplace(Bson filter, TDocument r) { return findOneAndReplace(filter, r, new FindOneAndReplaceOptions()); }
    @Override public Publisher<TDocument> findOneAndReplace(Bson filter, TDocument r, FindOneAndReplaceOptions options) {
        return Publishers.toMono(cb -> nativeClient.findOneAndReplace(namespace, filter, documentToBson(r), options, codecRegistry.get(documentClass), operationContext, null, cb));
    }
    @Override public Publisher<TDocument> findOneAndReplace(ClientSession cs, Bson filter, TDocument r) { return findOneAndReplace(cs, filter, r, new FindOneAndReplaceOptions()); }
    @Override public Publisher<TDocument> findOneAndReplace(ClientSession cs, Bson filter, TDocument r, FindOneAndReplaceOptions options) {
        return Publishers.toMono(cb -> nativeClient.findOneAndReplace(namespace, filter, documentToBson(r), options, codecRegistry.get(documentClass), operationContext, getNativeSession(cs), cb));
    }

    @Override public Publisher<TDocument> findOneAndUpdate(Bson filter, Bson update) { return findOneAndUpdate(filter, update, new FindOneAndUpdateOptions()); }
    @Override public Publisher<TDocument> findOneAndUpdate(Bson filter, Bson update, FindOneAndUpdateOptions options) {
        return Publishers.toMono(cb -> nativeClient.findOneAndUpdate(namespace, filter, update, options, codecRegistry.get(documentClass), operationContext, null, cb));
    }
    @Override public Publisher<TDocument> findOneAndUpdate(ClientSession cs, Bson filter, Bson update) { return findOneAndUpdate(cs, filter, update, new FindOneAndUpdateOptions()); }
    @Override public Publisher<TDocument> findOneAndUpdate(ClientSession cs, Bson filter, Bson update, FindOneAndUpdateOptions options) {
        return Publishers.toMono(cb -> nativeClient.findOneAndUpdate(namespace, filter, update, options, codecRegistry.get(documentClass), operationContext, getNativeSession(cs), cb));
    }
    @Override public Publisher<TDocument> findOneAndUpdate(Bson filter, List<? extends Bson> update) { throw new UnsupportedOperationException("Pipeline update not yet implemented"); }
    @Override public Publisher<TDocument> findOneAndUpdate(Bson filter, List<? extends Bson> update, FindOneAndUpdateOptions options) { throw new UnsupportedOperationException("Pipeline update not yet implemented"); }
    @Override public Publisher<TDocument> findOneAndUpdate(ClientSession cs, Bson filter, List<? extends Bson> update) { throw new UnsupportedOperationException("Pipeline update not yet implemented"); }
    @Override public Publisher<TDocument> findOneAndUpdate(ClientSession cs, Bson filter, List<? extends Bson> update, FindOneAndUpdateOptions options) { throw new UnsupportedOperationException("Pipeline update not yet implemented"); }

    // ==================== Bulk Write Operations ====================
    @Override public Publisher<BulkWriteResult> bulkWrite(List<? extends WriteModel<? extends TDocument>> requests) { return bulkWrite(requests, new BulkWriteOptions()); }
    @Override public Publisher<BulkWriteResult> bulkWrite(List<? extends WriteModel<? extends TDocument>> requests, BulkWriteOptions options) {
        throw new UnsupportedOperationException("bulkWrite not yet implemented");
    }
    @Override public Publisher<BulkWriteResult> bulkWrite(ClientSession cs, List<? extends WriteModel<? extends TDocument>> requests) { return bulkWrite(cs, requests, new BulkWriteOptions()); }
    @Override public Publisher<BulkWriteResult> bulkWrite(ClientSession cs, List<? extends WriteModel<? extends TDocument>> requests, BulkWriteOptions options) {
        throw new UnsupportedOperationException("bulkWrite not yet implemented");
    }

    // ==================== Index Operations ====================
    @Override public Publisher<String> createIndex(Bson keys) { return createIndex(keys, new IndexOptions()); }
    @Override public Publisher<String> createIndex(Bson keys, IndexOptions options) {
        // TODO: Copy timeout settings from IndexOptions to CreateIndexOptions if needed
        return Publishers.toMono(cb -> nativeClient.createIndex(namespace, keys, new CreateIndexOptions(), operationContext, null, cb));
    }
    @Override public Publisher<String> createIndex(ClientSession cs, Bson keys) { return createIndex(cs, keys, new IndexOptions()); }
    @Override public Publisher<String> createIndex(ClientSession cs, Bson keys, IndexOptions options) {
        // TODO: Copy timeout settings from IndexOptions to CreateIndexOptions if needed
        return Publishers.toMono(cb -> nativeClient.createIndex(namespace, keys, new CreateIndexOptions(), operationContext, getNativeSession(cs), cb));
    }
    @Override public Publisher<String> createIndexes(List<IndexModel> indexes) { return createIndexes(indexes, new CreateIndexOptions()); }
    @Override public Publisher<String> createIndexes(List<IndexModel> indexes, CreateIndexOptions options) {
        return Publishers.<List<String>>toMono(cb -> nativeClient.createIndexes(namespace, indexes, options, operationContext, null, cb)).flatMapMany(reactor.core.publisher.Flux::fromIterable);
    }
    @Override public Publisher<String> createIndexes(ClientSession cs, List<IndexModel> indexes) { return createIndexes(cs, indexes, new CreateIndexOptions()); }
    @Override public Publisher<String> createIndexes(ClientSession cs, List<IndexModel> indexes, CreateIndexOptions options) {
        return Publishers.<List<String>>toMono(cb -> nativeClient.createIndexes(namespace, indexes, options, operationContext, getNativeSession(cs), cb)).flatMapMany(reactor.core.publisher.Flux::fromIterable);
    }
    @Override public ListIndexesPublisher<Document> listIndexes() { return listIndexes(Document.class); }
    @Override public <T> ListIndexesPublisher<T> listIndexes(Class<T> resultClass) { return new NativeListIndexesPublisher<>(nativeClient, null, operationContext, namespace, resultClass, codecRegistry); }
    @Override public ListIndexesPublisher<Document> listIndexes(ClientSession cs) { return listIndexes(cs, Document.class); }
    @Override public <T> ListIndexesPublisher<T> listIndexes(ClientSession cs, Class<T> resultClass) { return new NativeListIndexesPublisher<>(nativeClient, getNativeSession(cs), operationContext, namespace, resultClass, codecRegistry); }
    @Override public Publisher<Void> dropIndex(String indexName) { return dropIndex(indexName, new DropIndexOptions()); }
    @Override public Publisher<Void> dropIndex(String indexName, DropIndexOptions options) {
        return Publishers.toMonoVoid(cb -> nativeClient.dropIndex(namespace, indexName, options, operationContext, null, cb));
    }
    @Override public Publisher<Void> dropIndex(Bson keys) { return dropIndex(keys, new DropIndexOptions()); }
    @Override public Publisher<Void> dropIndex(Bson keys, DropIndexOptions options) {
        return Publishers.toMonoVoid(cb -> nativeClient.dropIndex(namespace, keys, options, operationContext, null, cb));
    }
    @Override public Publisher<Void> dropIndex(ClientSession cs, String indexName) { return dropIndex(cs, indexName, new DropIndexOptions()); }
    @Override public Publisher<Void> dropIndex(ClientSession cs, String indexName, DropIndexOptions options) {
        return Publishers.toMonoVoid(cb -> nativeClient.dropIndex(namespace, indexName, options, operationContext, getNativeSession(cs), cb));
    }
    @Override public Publisher<Void> dropIndex(ClientSession cs, Bson keys) { return dropIndex(cs, keys, new DropIndexOptions()); }
    @Override public Publisher<Void> dropIndex(ClientSession cs, Bson keys, DropIndexOptions options) {
        return Publishers.toMonoVoid(cb -> nativeClient.dropIndex(namespace, keys, options, operationContext, getNativeSession(cs), cb));
    }
    @Override public Publisher<Void> dropIndexes() { return dropIndex("*"); }
    @Override public Publisher<Void> dropIndexes(DropIndexOptions options) { return dropIndex("*", options); }
    @Override public Publisher<Void> dropIndexes(ClientSession cs) { return dropIndex(cs, "*"); }
    @Override public Publisher<Void> dropIndexes(ClientSession cs, DropIndexOptions options) { return dropIndex(cs, "*", options); }

    // ==================== Drop/Rename Operations ====================
    @Override public Publisher<Void> drop() { return drop(new DropCollectionOptions()); }
    @Override public Publisher<Void> drop(DropCollectionOptions options) {
        return Publishers.toMonoVoid(cb -> nativeClient.dropCollection(namespace, options, operationContext, null, cb));
    }
    @Override public Publisher<Void> drop(ClientSession cs) { return drop(cs, new DropCollectionOptions()); }
    @Override public Publisher<Void> drop(ClientSession cs, DropCollectionOptions options) {
        return Publishers.toMonoVoid(cb -> nativeClient.dropCollection(namespace, options, operationContext, getNativeSession(cs), cb));
    }
    @Override public Publisher<Void> renameCollection(MongoNamespace newNamespace) { return renameCollection(newNamespace, new RenameCollectionOptions()); }
    @Override public Publisher<Void> renameCollection(MongoNamespace newNamespace, RenameCollectionOptions options) {
        return Publishers.toMonoVoid(cb -> nativeClient.renameCollection(namespace, newNamespace, options, operationContext, null, cb));
    }
    @Override public Publisher<Void> renameCollection(ClientSession cs, MongoNamespace newNamespace) { return renameCollection(cs, newNamespace, new RenameCollectionOptions()); }
    @Override public Publisher<Void> renameCollection(ClientSession cs, MongoNamespace newNamespace, RenameCollectionOptions options) {
        return Publishers.toMonoVoid(cb -> nativeClient.renameCollection(namespace, newNamespace, options, operationContext, getNativeSession(cs), cb));
    }

    // ==================== Watch Operations ====================
    @Override public ChangeStreamPublisher<Document> watch() { return watch(java.util.Collections.emptyList(), Document.class); }
    @Override public <T> ChangeStreamPublisher<T> watch(Class<T> resultClass) { return watch(java.util.Collections.emptyList(), resultClass); }
    @Override public ChangeStreamPublisher<Document> watch(List<? extends Bson> p) { return watch(p, Document.class); }
    @Override public <T> ChangeStreamPublisher<T> watch(List<? extends Bson> p, Class<T> resultClass) {
        return new NativeChangeStreamPublisher<>(nativeClient, null, operationContext, p, resultClass, codecRegistry,
                NativeChangeStreamPublisher.WatchLevel.COLLECTION, namespace.getDatabaseName(), namespace.getCollectionName());
    }
    @Override public ChangeStreamPublisher<Document> watch(ClientSession cs) { return watch(cs, java.util.Collections.emptyList(), Document.class); }
    @Override public <T> ChangeStreamPublisher<T> watch(ClientSession cs, Class<T> resultClass) { return watch(cs, java.util.Collections.emptyList(), resultClass); }
    @Override public ChangeStreamPublisher<Document> watch(ClientSession cs, List<? extends Bson> p) { return watch(cs, p, Document.class); }
    @Override public <T> ChangeStreamPublisher<T> watch(ClientSession cs, List<? extends Bson> p, Class<T> resultClass) {
        return new NativeChangeStreamPublisher<>(nativeClient, getNativeSession(cs), operationContext, p, resultClass, codecRegistry,
                NativeChangeStreamPublisher.WatchLevel.COLLECTION, namespace.getDatabaseName(), namespace.getCollectionName());
    }

    // ==================== MapReduce (Deprecated) ====================
    @Override @Deprecated public MapReducePublisher<TDocument> mapReduce(String map, String reduce) { throw new UnsupportedOperationException("mapReduce not supported"); }
    @Override @Deprecated public <T> MapReducePublisher<T> mapReduce(String map, String reduce, Class<T> c) { throw new UnsupportedOperationException("mapReduce not supported"); }
    @Override @Deprecated public MapReducePublisher<TDocument> mapReduce(ClientSession cs, String map, String reduce) { throw new UnsupportedOperationException("mapReduce not supported"); }
    @Override @Deprecated public <T> MapReducePublisher<T> mapReduce(ClientSession cs, String map, String reduce, Class<T> c) { throw new UnsupportedOperationException("mapReduce not supported"); }

    // ==================== Search Index Operations ====================
    @Override public Publisher<String> createSearchIndex(String name, Bson def) { throw new UnsupportedOperationException("Search indexes not yet implemented"); }
    @Override public Publisher<String> createSearchIndex(Bson def) { throw new UnsupportedOperationException("Search indexes not yet implemented"); }
    @Override public Publisher<String> createSearchIndexes(List<SearchIndexModel> idx) { throw new UnsupportedOperationException("Search indexes not yet implemented"); }
    @Override public Publisher<Void> updateSearchIndex(String name, Bson def) { throw new UnsupportedOperationException("Search indexes not yet implemented"); }
    @Override public Publisher<Void> dropSearchIndex(String name) { throw new UnsupportedOperationException("Search indexes not yet implemented"); }
    @Override public ListSearchIndexesPublisher<Document> listSearchIndexes() { throw new UnsupportedOperationException("Search indexes not yet implemented"); }
    @Override public <T> ListSearchIndexesPublisher<T> listSearchIndexes(Class<T> c) { throw new UnsupportedOperationException("Search indexes not yet implemented"); }
}
