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

package com.mongodb.client.test;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoNamespace;
import com.mongodb.MongoWriteConcernException;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.FindIterable;
import com.mongodb.client.Fixture;
import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.SearchIndexModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.ValidationOptions;
import com.mongodb.internal.diagnostics.logging.Logger;
import com.mongodb.internal.diagnostics.logging.Loggers;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWrapper;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.codecs.Codec;
import org.bson.codecs.Decoder;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.bson.codecs.configuration.CodecRegistries.fromCodecs;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

public final class CollectionHelper<T> {
    private static final Logger LOGGER = Loggers.getLogger("test");

    private final Codec<T> codec;
    private final CodecRegistry registry = MongoClientSettings.getDefaultCodecRegistry();
    private final MongoNamespace namespace;

    public CollectionHelper(final Codec<T> codec, final MongoNamespace namespace) {
        this.codec = codec;
        this.namespace = namespace;
    }

    private static MongoClient getClient() {
        return Fixture.getMongoClient();
    }

    private MongoDatabase getDatabase() {
        return getClient().getDatabase(namespace.getDatabaseName());
    }

    private MongoCollection<BsonDocument> getCollection() {
        return getDatabase().getCollection(namespace.getCollectionName(), BsonDocument.class);
    }

    private <D> MongoCollection<D> getCollection(final Codec<D> codec) {
        CodecRegistry codecRegistry = fromRegistries(registry, fromCodecs(codec));
        return getDatabase().getCollection(namespace.getCollectionName(), (Class<D>) codec.getEncoderClass())
                .withCodecRegistry(codecRegistry);
    }

    public T hello() {
        CodecRegistry codecRegistry = fromRegistries(registry, fromCodecs(codec));
        return getClient().getDatabase("admin")
                .withCodecRegistry(codecRegistry)
                .runCommand(BsonDocument.parse("{isMaster: 1}"), codec.getEncoderClass());
    }

    public static void drop(final MongoNamespace namespace) {
        drop(namespace, WriteConcern.ACKNOWLEDGED);
    }

    public static void drop(final MongoNamespace namespace, final WriteConcern writeConcern) {
        boolean success = false;
        while (!success) {
            try {
                getClient().getDatabase(namespace.getDatabaseName())
                        .getCollection(namespace.getCollectionName())
                        .withWriteConcern(writeConcern)
                        .drop();
                success = true;
            } catch (MongoWriteConcernException e) {
                LOGGER.info("Retrying drop collection after a write concern error: " + e);
            } catch (MongoCommandException e) {
                if ("Interrupted".equals(e.getErrorCodeName())) {
                    LOGGER.info("Retrying drop collection after an Interrupted error: " + e);
                } else {
                    throw e;
                }
            }
        }
    }

    public static void dropDatabase(final String name) {
        dropDatabase(name, WriteConcern.ACKNOWLEDGED);
    }

    public static void dropDatabase(final String name, final WriteConcern writeConcern) {
        if (name == null) {
            return;
        }
        try {
            getClient().getDatabase(name).withWriteConcern(writeConcern).drop();
        } catch (MongoCommandException e) {
            if (!e.getErrorMessage().contains("ns not found")) {
                throw e;
            }
        }
    }

    public static BsonDocument getCurrentClusterTime() {
        return getClient().getDatabase("admin")
                .runCommand(new BsonDocument("ping", new org.bson.BsonInt32(1)), BsonDocument.class)
                .getDocument("$clusterTime", null);
    }

    public MongoNamespace getNamespace() {
        return namespace;
    }

    public void drop() {
        drop(WriteConcern.ACKNOWLEDGED);
    }

    public void drop(final WriteConcern writeConcern) {
        drop(namespace, writeConcern);
    }

    public void dropAndCreate(final BsonDocument createOptions) {
        drop(namespace, WriteConcern.MAJORITY);
        drop(new MongoNamespace(namespace.getDatabaseName(), format("enxcol_.%s.esc", namespace.getCollectionName())),
                WriteConcern.MAJORITY);
        drop(new MongoNamespace(namespace.getDatabaseName(), format("enxcol_.%s.ecoc", namespace.getCollectionName())),
                WriteConcern.MAJORITY);
        create(WriteConcern.MAJORITY, createOptions);
    }

    public void create() {
        create(namespace.getCollectionName(), new CreateCollectionOptions(), WriteConcern.ACKNOWLEDGED);
    }

    public void create(final WriteConcern writeConcern) {
        create(namespace.getCollectionName(), new CreateCollectionOptions(), writeConcern);
    }

    public void create(final String collectionName, final CreateCollectionOptions options) {
        create(collectionName, options, WriteConcern.ACKNOWLEDGED);
    }

    public void create(final WriteConcern writeConcern, final BsonDocument createOptions) {
        CreateCollectionOptions createCollectionOptions = new CreateCollectionOptions();
        for (String option : createOptions.keySet()) {
            switch (option) {
                case "capped":
                    createCollectionOptions.capped(createOptions.getBoolean("capped").getValue());
                    break;
                case "size":
                    createCollectionOptions.sizeInBytes(createOptions.getNumber("size").longValue());
                    break;
                case "encryptedFields":
                    createCollectionOptions.encryptedFields(createOptions.getDocument("encryptedFields"));
                    break;
                case "validator":
                    ValidationOptions validationOptions = new ValidationOptions();
                    validationOptions.validator(createOptions.getDocument("validator"));
                    createCollectionOptions.validationOptions(validationOptions);
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported create collection option: " + option);
            }
        }
        create(namespace.getCollectionName(), createCollectionOptions, writeConcern);
    }

    public void create(final String collectionName, final CreateCollectionOptions options, final WriteConcern writeConcern) {
        drop(namespace, writeConcern);
        boolean success = false;
        while (!success) {
            try {
                getDatabase().withWriteConcern(writeConcern).createCollection(collectionName, options);
                success = true;
            } catch (MongoCommandException e) {
                if ("Interrupted".equals(e.getErrorCodeName())) {
                    LOGGER.info("Retrying create collection after an Interrupted error: " + e);
                } else {
                    throw e;
                }
            }
        }
    }

    public void insertDocuments(final BsonDocument... documents) {
        insertDocuments(asList(documents));
    }

    public void insertDocuments(final WriteConcern writeConcern, final BsonDocument... documents) {
        insertDocuments(asList(documents), writeConcern);
    }

    public void insertDocuments(final List<BsonDocument> documents) {
        insertDocuments(documents, WriteConcern.ACKNOWLEDGED);
    }

    public void insertDocuments(final List<BsonDocument> documents, final WriteConcern writeConcern) {
        if (!documents.isEmpty()) {
            getCollection().withWriteConcern(writeConcern).insertMany(documents);
        }
    }

    public void insertDocuments(final Document... documents) {
        insertDocuments(new DocumentCodec(registry), asList(documents));
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    public final <I> void insertDocuments(final Codec<I> iCodec, final I... documents) {
        insertDocuments(iCodec, asList(documents));
    }

    public <I> void insertDocuments(final Codec<I> iCodec, final List<I> documents) {
        List<BsonDocument> bsonDocuments = new ArrayList<>(documents.size());
        for (I document : documents) {
            bsonDocuments.add(new BsonDocumentWrapper<>(document, iCodec));
        }
        insertDocuments(bsonDocuments);
    }

    public void insertDocuments(final String insertAll) {
        List<BsonDocument> documents = BsonArray.parse(insertAll).stream().map(BsonValue::asDocument).collect(Collectors.toList());
        insertDocuments(documents);
    }

    public List<T> find() {
        return find(codec);
    }

    public <D> List<D> find(final Codec<D> codec) {
        List<D> results = new ArrayList<>();
        try (MongoCursor<D> cursor = getCollection(codec).find().sort(new BsonDocument("_id", new org.bson.BsonInt32(1))).iterator()) {
            while (cursor.hasNext()) {
                results.add(cursor.next());
            }
        }
        return results;
    }

    public void updateOne(final Bson filter, final Bson update) {
        updateOne(filter, update, false);
    }

    public void updateOne(final Bson filter, final Bson update, final boolean isUpsert) {
        getCollection().updateOne(filter, update, new UpdateOptions().upsert(isUpsert));
    }

    public void replaceOne(final Bson filter, final Bson replacement, final boolean isUpsert) {
        getCollection().replaceOne(filter, replacement.toBsonDocument(), new ReplaceOptions().upsert(isUpsert));
    }

    public void deleteOne(final Bson filter) {
        getCollection().deleteOne(filter);
    }

    public void deleteMany(final Bson filter) {
        getCollection().deleteMany(filter);
    }

    public List<T> find(final Bson filter) {
        return find(filter, null);
    }

    public List<T> aggregate(final List<Bson> pipeline) {
        return aggregate(pipeline, codec);
    }

    public <D> List<D> aggregate(final List<Bson> pipeline, final Decoder<D> decoder) {
        List<D> results = new ArrayList<>();
        try (MongoCursor<D> cursor = getCollection((Codec<D>) decoder).aggregate(pipeline).iterator()) {
            while (cursor.hasNext()) {
                results.add(cursor.next());
            }
        }
        return results;
    }

    public List<T> aggregateDb(final List<Bson> pipeline) {
        List<T> results = new ArrayList<>();
        CodecRegistry codecRegistry = fromRegistries(registry, fromCodecs(codec));
        try (MongoCursor<T> cursor = getDatabase().withCodecRegistry(codecRegistry)
                .aggregate(pipeline, codec.getEncoderClass()).iterator()) {
            while (cursor.hasNext()) {
                results.add(cursor.next());
            }
        }
        return results;
    }

    @SuppressWarnings("overloads")
    public List<T> find(final Bson filter, final Bson sort) {
        return find(filter != null ? filter.toBsonDocument(Document.class, registry) : null,
                    sort != null ? sort.toBsonDocument(Document.class, registry) : null,
                    codec);
    }

    @SuppressWarnings("overloads")
    public List<T> find(final Bson filter, final Bson sort, final Bson projection) {
        return find(filter != null ? filter.toBsonDocument(Document.class, registry) : null,
                    sort != null ? sort.toBsonDocument(Document.class, registry) : null,
                    projection != null ? projection.toBsonDocument(Document.class, registry) : null,
                    codec);
    }

    @SuppressWarnings("overloads")
    public <D> List<D> find(final BsonDocument filter, final Decoder<D> decoder) {
        return find(filter, null, decoder);
    }

    @SuppressWarnings("overloads")
    public <D> List<D> find(final BsonDocument filter, final BsonDocument sort, final Decoder<D> decoder) {
        return find(filter, sort, null, decoder);
    }

    public <D> List<D> find(final BsonDocument filter, final BsonDocument sort, final BsonDocument projection, final Decoder<D> decoder) {
        List<D> results = new ArrayList<>();
        MongoCollection<D> coll = getCollection((Codec<D>) decoder);
        FindIterable<D> findIterable = coll.find();
        if (filter != null) {
            findIterable = findIterable.filter(filter);
        }
        if (sort != null) {
            findIterable = findIterable.sort(sort);
        }
        if (projection != null) {
            findIterable = findIterable.projection(projection);
        }
        try (MongoCursor<D> cursor = findIterable.iterator()) {
            while (cursor.hasNext()) {
                results.add(cursor.next());
            }
        }
        return results;
    }

    public long count() {
        return getCollection().countDocuments();
    }

    public long count(final Bson filter) {
        return getCollection().countDocuments(filter);
    }

    public BsonDocument wrap(final Document document) {
        return new BsonDocumentWrapper<>(document, new DocumentCodec());
    }

    public BsonDocument toBsonDocument(final Bson document) {
        return document.toBsonDocument(BsonDocument.class, registry);
    }

    public void createIndex(final BsonDocument key) {
        getCollection().createIndex(key);
    }

    public void createIndex(final Document key) {
        getCollection().createIndex(wrap(key));
    }

    public void createUniqueIndex(final Document key) {
        getCollection().createIndex(wrap(key), new IndexOptions().unique(true));
    }

    public void createIndex(final Document key, final String defaultLanguage) {
        getCollection().createIndex(wrap(key), new IndexOptions().defaultLanguage(defaultLanguage));
    }

    public void createIndex(final Bson key) {
        getCollection().createIndex(key);
    }

    public List<BsonDocument> listIndexes() {
        List<BsonDocument> indexes = new ArrayList<>();
        try (MongoCursor<BsonDocument> cursor = getCollection().listIndexes(BsonDocument.class).iterator()) {
            while (cursor.hasNext()) {
                indexes.add(cursor.next());
            }
        }
        return indexes;
    }

    public static void killAllSessions() {
        try {
            getClient().getDatabase("admin")
                    .runCommand(new BsonDocument("killAllSessions", new BsonArray()));
        } catch (MongoCommandException e) {
            // ignore exception caused by killing the implicit session
        }
    }

    public void renameCollection(final MongoNamespace newNamespace) {
        try {
            getClient().getDatabase("admin")
                    .runCommand(new BsonDocument("renameCollection", new org.bson.BsonString(getNamespace().getFullName()))
                            .append("to", new org.bson.BsonString(newNamespace.getFullName())));
        } catch (MongoCommandException e) {
            // do nothing
        }
    }

    public void runAdminCommand(final String command) {
        runAdminCommand(BsonDocument.parse(command));
    }

    public void runAdminCommand(final BsonDocument command) {
        getClient().getDatabase("admin").runCommand(command);
    }

    public void runAdminCommand(final BsonDocument command, final ReadPreference readPreference) {
        getClient().getDatabase("admin").withReadPreference(readPreference).runCommand(command);
    }
}
