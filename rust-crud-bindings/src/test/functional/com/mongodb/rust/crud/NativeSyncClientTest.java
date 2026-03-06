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
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadPreference;
import com.mongodb.Tag;
import com.mongodb.TagSet;
import com.mongodb.TransactionOptions;
import com.mongodb.client.model.FindOptions;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.InsertOneOptions;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.DocumentCodec;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.mongodb.rust.crud.Fixture.*;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Integration tests for NativeSyncClient.
 * These tests require a running MongoDB instance.
 */
class NativeSyncClientTest {

    @Test
    void testCreateAndCloseClient() {
        MongoClientSettings settings = getMongoClientSettings();
        NativeSyncClient client = assertDoesNotThrow(() -> NativeSyncClients.create(settings));
        assertNotNull(client);
        assertDoesNotThrow(client::close);
    }

    @Test
    void testCreateClientWithAppName() {
        MongoClientSettings settings = getMongoClientSettingsBuilder()
                .applicationName("rust-crud-test")
                .build();

        try (NativeSyncClient client = NativeSyncClients.create(settings)) {
            assertNotNull(client);
        }
    }

    @Test
    void testCreateClientMultipleTimes() {
        MongoClientSettings settings = getMongoClientSettings();

        for (int i = 0; i < 3; i++) {
            try (NativeSyncClient client = NativeSyncClients.create(settings)) {
                assertNotNull(client);
            }
        }
    }

    @Test
    void testRunCommandPing() {
        MongoClientSettings settings = getMongoClientSettings();

        try (NativeSyncClient client = NativeSyncClients.create(settings)) {
            BsonDocument result = client.runCommand(
                    "admin",
                    new BsonDocument("ping", new BsonInt32(1)),
                    new BsonDocumentCodec(),
                    NativeOperationContext.builder().build(),
                    null);

            assertNotNull(result);
            assertTrue(result.containsKey("ok"));
            assertEquals(1.0, result.getDouble("ok").getValue(), 0.001);
        }
    }

    @Test
    void testRunCommandBuildInfo() {
        MongoClientSettings settings = getMongoClientSettings();

        try (NativeSyncClient client = NativeSyncClients.create(settings)) {
            BsonDocument result = client.runCommand(
                    "admin",
                    new BsonDocument("buildInfo", new BsonInt32(1)),
                    new BsonDocumentCodec(),
                    NativeOperationContext.builder().build(),
                    null);

            assertNotNull(result);
            assertTrue(result.containsKey("version"));
            assertTrue(result.containsKey("ok"));
        }
    }

    @Test
    void testRunCommandWithDocumentCodec() {
        MongoClientSettings settings = getMongoClientSettings();

        try (NativeSyncClient client = NativeSyncClients.create(settings)) {
            Document result = client.runCommand(
                    "admin",
                    new BsonDocument("ping", new BsonInt32(1)),
                    new DocumentCodec(),
                    NativeOperationContext.builder().build(),
                    null);

            assertNotNull(result);
            assertTrue(result.containsKey("ok"));
            assertEquals(1.0, result.getDouble("ok"), 0.001);
        }
    }

    @Test
    void testStartSession() {
        MongoClientSettings settings = getMongoClientSettings();

        try (NativeSyncClient client = NativeSyncClients.create(settings)) {
            NativeSyncClientSession session = client.startSession(ClientSessionOptions.builder().build());
            assertNotNull(session);
            assertNotNull(session.getOptions());
            session.close();
        }
    }

    @Test
    void testSessionWithCausalConsistency() {
        MongoClientSettings settings = getMongoClientSettings();

        try (NativeSyncClient client = NativeSyncClients.create(settings)) {
            NativeSyncClientSession session = client.startSession(
                    ClientSessionOptions.builder().causallyConsistent(true).build());
            assertNotNull(session);
            assertTrue(session.isCausallyConsistent());
            session.close();
        }
    }

    @Test
    void testRunCommandWithSession() {
        MongoClientSettings settings = getMongoClientSettings();

        try (NativeSyncClient client = NativeSyncClients.create(settings)) {
            NativeSyncClientSession session = client.startSession(ClientSessionOptions.builder().build());

            BsonDocument result = client.runCommand(
                    "admin",
                    new BsonDocument("ping", new BsonInt32(1)),
                    new BsonDocumentCodec(),
                    NativeOperationContext.builder().build(),
                    session);

            assertNotNull(result);
            assertEquals(1.0, result.getDouble("ok").getValue(), 0.001);
            session.close();
        }
    }

    @Test
    void testTransaction() {
        assumeTrue(!isStandalone());
        MongoClientSettings settings = getMongoClientSettings();

        try (NativeSyncClient client = NativeSyncClients.create(settings)) {
            MongoNamespace namespace = new MongoNamespace(getDefaultDatabaseName(), "test_txn_collection");
            NativeSyncClientSession session = client.startSession(ClientSessionOptions.builder().build());

            try {
                session.startTransaction(TransactionOptions.builder().build());
                assertTrue(session.hasActiveTransaction());

                BsonDocument doc = new BsonDocument("_id", new BsonInt32(1)).append("value", new BsonString("test"));
                InsertOneResult insertResult = client.insertOne(
                        namespace,
                        doc,
                        new InsertOneOptions(),
                        NativeOperationContext.builder().build(),
                        session);

                assertNotNull(insertResult);
                assertTrue(insertResult.wasAcknowledged());
                assertEquals(new BsonInt32(1), insertResult.getInsertedId());

                session.abortTransaction();
                assertFalse(session.hasActiveTransaction());
            } finally {
                session.close();
            }
        }
    }

    @Test
    void testRunCommandWithReadPreferenceTags() {
        MongoClientSettings settings = getMongoClientSettings();

        try (NativeSyncClient client = NativeSyncClients.create(settings)) {
            TagSet tagSet = new TagSet(Arrays.asList(
                    new Tag("dc", "east"),
                    new Tag("rack", "r1")
            ));
            TagSet emptyTagSet = new TagSet();
            ReadPreference readPref = ReadPreference.nearest(Arrays.asList(tagSet, emptyTagSet));

            NativeOperationContext opCtx = NativeOperationContext.builder()
                    .readPreference(readPref)
                    .build();

            BsonDocument result = client.runCommand(
                    "admin",
                    new BsonDocument("ping", new BsonInt32(1)),
                    new BsonDocumentCodec(),
                    opCtx,
                    null);

            assertNotNull(result);
            assertTrue(result.containsKey("ok"));
            assertEquals(1.0, result.getDouble("ok").getValue(), 0.001);
        }
    }

    @Test
    void testInsertOneWithGeneratedId() {
        MongoClientSettings settings = getMongoClientSettings();

        try (NativeSyncClient client = NativeSyncClients.create(settings)) {
            MongoNamespace namespace = new MongoNamespace(getDefaultDatabaseName(), "test_insert_one");

            // Drop collection first via runCommand
            client.runCommand(
                    namespace.getDatabaseName(),
                    new BsonDocument("drop", new BsonString(namespace.getCollectionName())),
                    new BsonDocumentCodec(),
                    NativeOperationContext.builder().build(),
                    null);

            // Insert document without _id - server will generate one
            BsonDocument doc = new BsonDocument("name", new BsonString("test"));
            InsertOneResult result = client.insertOne(
                    namespace,
                    doc,
                    new InsertOneOptions(),
                    NativeOperationContext.builder().build(),
                    null);

            assertNotNull(result);
            assertTrue(result.wasAcknowledged());
            assertNotNull(result.getInsertedId());
            // Server generates ObjectId for _id
            assertInstanceOf(BsonObjectId.class, result.getInsertedId());
        }
    }

    @Test
    void testInsertOneWithProvidedId() {
        MongoClientSettings settings = getMongoClientSettings();

        try (NativeSyncClient client = NativeSyncClients.create(settings)) {
            MongoNamespace namespace = new MongoNamespace(getDefaultDatabaseName(), "test_insert_one");

            // Drop collection first
            client.runCommand(
                    namespace.getDatabaseName(),
                    new BsonDocument("drop", new BsonString(namespace.getCollectionName())),
                    new BsonDocumentCodec(),
                    NativeOperationContext.builder().build(),
                    null);

            // Insert document with explicit _id
            ObjectId providedId = new ObjectId();
            BsonDocument doc = new BsonDocument()
                    .append("_id", new BsonObjectId(providedId))
                    .append("name", new BsonString("test with id"));

            InsertOneResult result = client.insertOne(
                    namespace,
                    doc,
                    new InsertOneOptions(),
                    NativeOperationContext.builder().build(),
                    null);

            assertNotNull(result);
            assertTrue(result.wasAcknowledged());
            assertEquals(new BsonObjectId(providedId), result.getInsertedId());
        }
    }

    // ==================== InsertMany Tests ====================

    @Test
    void testInsertManyBasic() {
        MongoClientSettings settings = getMongoClientSettings();

        try (NativeSyncClient client = NativeSyncClients.create(settings)) {
            MongoNamespace namespace = new MongoNamespace(getDefaultDatabaseName(), "test_insert_many");

            // Drop collection first
            dropCollection(client, namespace);

            // Insert multiple documents
            List<BsonDocument> docs = Arrays.asList(
                    new BsonDocument("_id", new BsonInt32(1)).append("name", new BsonString("Alice")),
                    new BsonDocument("_id", new BsonInt32(2)).append("name", new BsonString("Bob")),
                    new BsonDocument("_id", new BsonInt32(3)).append("name", new BsonString("Charlie"))
            );

            InsertManyResult result = client.insertMany(
                    namespace,
                    docs,
                    new InsertManyOptions(),
                    NativeOperationContext.builder().build(),
                    null);

            assertNotNull(result);
            assertTrue(result.wasAcknowledged());
            assertEquals(3, result.getInsertedIds().size());
            assertEquals(new BsonInt32(1), result.getInsertedIds().get(0));
            assertEquals(new BsonInt32(2), result.getInsertedIds().get(1));
            assertEquals(new BsonInt32(3), result.getInsertedIds().get(2));
        }
    }

    @Test
    void testInsertManyWithGeneratedIds() {
        MongoClientSettings settings = getMongoClientSettings();

        try (NativeSyncClient client = NativeSyncClients.create(settings)) {
            MongoNamespace namespace = new MongoNamespace(getDefaultDatabaseName(), "test_insert_many_gen_ids");

            // Drop collection first
            dropCollection(client, namespace);

            // Insert documents without _id - server will generate ObjectIds
            List<BsonDocument> docs = Arrays.asList(
                    new BsonDocument("name", new BsonString("Doc1")),
                    new BsonDocument("name", new BsonString("Doc2"))
            );

            InsertManyResult result = client.insertMany(
                    namespace,
                    docs,
                    new InsertManyOptions(),
                    NativeOperationContext.builder().build(),
                    null);

            assertNotNull(result);
            assertTrue(result.wasAcknowledged());
            assertEquals(2, result.getInsertedIds().size());
            // Server generates ObjectIds
            assertInstanceOf(BsonObjectId.class, result.getInsertedIds().get(0));
            assertInstanceOf(BsonObjectId.class, result.getInsertedIds().get(1));
        }
    }

    // Empty insertMany not supported - MongoDB requires at least one document

    // ==================== Find Tests ====================

    @Test
    void testFindAll() {
        MongoClientSettings settings = getMongoClientSettings();

        try (NativeSyncClient client = NativeSyncClients.create(settings)) {
            MongoNamespace namespace = new MongoNamespace(getDefaultDatabaseName(), "test_find");

            // Drop and insert test data
            dropCollection(client, namespace);
            for (int i = 0; i < 5; i++) {
                client.insertOne(namespace,
                        new BsonDocument("_id", new BsonInt32(i)).append("value", new BsonString("doc" + i)),
                        new InsertOneOptions(),
                        NativeOperationContext.builder().build(),
                        null);
            }

            // Find all documents
            try (NativeSyncCursor<BsonDocument> cursor = client.find(
                    namespace,
                    new BsonDocument(),
                    new FindOptions(),
                    new BsonDocumentCodec(),
                    NativeOperationContext.builder().build(),
                    null)) {

                List<BsonDocument> results = new ArrayList<>();
                while (cursor.hasNext()) {
                    results.add(cursor.next());
                }

                assertEquals(5, results.size());
                for (int i = 0; i < 5; i++) {
                    assertEquals("doc" + i, results.get(i).getString("value").getValue());
                }
            }
        }
    }

    @Test
    void testFindWithFilter() {
        MongoClientSettings settings = getMongoClientSettings();

        try (NativeSyncClient client = NativeSyncClients.create(settings)) {
            MongoNamespace namespace = new MongoNamespace(getDefaultDatabaseName(), "test_find_filter");

            // Drop and insert test data
            dropCollection(client, namespace);
            for (int i = 0; i < 10; i++) {
                client.insertOne(namespace,
                        new BsonDocument("_id", new BsonInt32(i)).append("even", new org.bson.BsonBoolean(i % 2 == 0)),
                        new InsertOneOptions(),
                        NativeOperationContext.builder().build(),
                        null);
            }

            // Find only even documents
            try (NativeSyncCursor<BsonDocument> cursor = client.find(
                    namespace,
                    new BsonDocument("even", new org.bson.BsonBoolean(true)),
                    new FindOptions(),
                    new BsonDocumentCodec(),
                    NativeOperationContext.builder().build(),
                    null)) {

                List<BsonDocument> results = new ArrayList<>();
                while (cursor.hasNext()) {
                    results.add(cursor.next());
                }

                assertEquals(5, results.size());
                for (BsonDocument doc : results) {
                    assertTrue(doc.getBoolean("even").getValue());
                }
            }
        }
    }

    @Test
    void testFindWithSmallBatchSize() {
        MongoClientSettings settings = getMongoClientSettings();

        try (NativeSyncClient client = NativeSyncClients.create(settings)) {
            MongoNamespace namespace = new MongoNamespace(getDefaultDatabaseName(), "test_find_batch");

            // Drop and insert test data - more than one batch
            dropCollection(client, namespace);
            int totalDocs = 25;
            for (int i = 0; i < totalDocs; i++) {
                client.insertOne(namespace,
                        new BsonDocument("_id", new BsonInt32(i)).append("index", new BsonInt32(i)),
                        new InsertOneOptions(),
                        NativeOperationContext.builder().build(),
                        null);
            }

            // Find with small batch size to force multiple getMore calls
            FindOptions options = new FindOptions().batchSize(3);
            try (NativeSyncCursor<BsonDocument> cursor = client.find(
                    namespace,
                    new BsonDocument(),
                    options,
                    new BsonDocumentCodec(),
                    NativeOperationContext.builder().build(),
                    null)) {

                List<BsonDocument> results = new ArrayList<>();
                while (cursor.hasNext()) {
                    results.add(cursor.next());
                }

                assertEquals(totalDocs, results.size());
                // Verify all documents retrieved
                for (int i = 0; i < totalDocs; i++) {
                    int index = i;
                    boolean found = results.stream()
                            .anyMatch(doc -> doc.getInt32("index").getValue() == index);
                    assertTrue(found, "Missing document with index " + i);
                }
            }
        }
    }

    @Test
    void testFindWithLimitAndSkip() {
        MongoClientSettings settings = getMongoClientSettings();

        try (NativeSyncClient client = NativeSyncClients.create(settings)) {
            MongoNamespace namespace = new MongoNamespace(getDefaultDatabaseName(), "test_find_limit_skip");

            // Drop and insert test data
            dropCollection(client, namespace);
            for (int i = 0; i < 20; i++) {
                client.insertOne(namespace,
                        new BsonDocument("_id", new BsonInt32(i)).append("index", new BsonInt32(i)),
                        new InsertOneOptions(),
                        NativeOperationContext.builder().build(),
                        null);
            }

            // Find with skip and limit
            FindOptions options = new FindOptions()
                    .skip(5)
                    .limit(10)
                    .sort(new BsonDocument("_id", new BsonInt32(1)));

            try (NativeSyncCursor<BsonDocument> cursor = client.find(
                    namespace,
                    new BsonDocument(),
                    options,
                    new BsonDocumentCodec(),
                    NativeOperationContext.builder().build(),
                    null)) {

                List<BsonDocument> results = new ArrayList<>();
                while (cursor.hasNext()) {
                    results.add(cursor.next());
                }

                assertEquals(10, results.size());
                // Should have docs 5-14 (skip 5, limit 10)
                for (int i = 0; i < 10; i++) {
                    assertEquals(5 + i, results.get(i).getInt32("index").getValue());
                }
            }
        }
    }

    @Test
    void testFindWithProjection() {
        MongoClientSettings settings = getMongoClientSettings();

        try (NativeSyncClient client = NativeSyncClients.create(settings)) {
            MongoNamespace namespace = new MongoNamespace(getDefaultDatabaseName(), "test_find_projection");

            // Drop and insert test data
            dropCollection(client, namespace);
            client.insertOne(namespace,
                    new BsonDocument("_id", new BsonInt32(1))
                            .append("name", new BsonString("Alice"))
                            .append("age", new BsonInt32(30))
                            .append("city", new BsonString("NYC")),
                    new InsertOneOptions(),
                    NativeOperationContext.builder().build(),
                    null);

            // Find with projection - only name field
            FindOptions options = new FindOptions()
                    .projection(new BsonDocument("name", new BsonInt32(1)).append("_id", new BsonInt32(0)));

            try (NativeSyncCursor<BsonDocument> cursor = client.find(
                    namespace,
                    new BsonDocument(),
                    options,
                    new BsonDocumentCodec(),
                    NativeOperationContext.builder().build(),
                    null)) {

                assertTrue(cursor.hasNext());
                BsonDocument result = cursor.next();

                assertTrue(result.containsKey("name"));
                assertFalse(result.containsKey("_id"));
                assertFalse(result.containsKey("age"));
                assertFalse(result.containsKey("city"));
                assertEquals("Alice", result.getString("name").getValue());
            }
        }
    }

    @Test
    void testFindEmptyCollection() {
        MongoClientSettings settings = getMongoClientSettings();

        try (NativeSyncClient client = NativeSyncClients.create(settings)) {
            MongoNamespace namespace = new MongoNamespace(getDefaultDatabaseName(), "test_find_empty");

            // Drop collection to ensure empty
            dropCollection(client, namespace);

            // Find on empty collection
            try (NativeSyncCursor<BsonDocument> cursor = client.find(
                    namespace,
                    new BsonDocument(),
                    new FindOptions(),
                    new BsonDocumentCodec(),
                    NativeOperationContext.builder().build(),
                    null)) {

                assertFalse(cursor.hasNext());
            }
        }
    }

    private void dropCollection(NativeSyncClient client, MongoNamespace namespace) {
        try {
            client.runCommand(
                    namespace.getDatabaseName(),
                    new BsonDocument("drop", new BsonString(namespace.getCollectionName())),
                    new BsonDocumentCodec(),
                    NativeOperationContext.builder().build(),
                    null);
        } catch (Exception e) {
            // Collection might not exist, ignore
        }
    }
}

