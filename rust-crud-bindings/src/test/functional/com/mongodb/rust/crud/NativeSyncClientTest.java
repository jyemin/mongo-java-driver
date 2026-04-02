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
import com.mongodb.client.model.AggregateOptions;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.DeleteOptions;
import com.mongodb.client.model.EstimatedDocumentCountOptions;
import com.mongodb.client.model.FindOneAndDeleteOptions;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.FindOptions;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.InsertOneOptions;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
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
import static org.junit.jupiter.api.Assertions.assertNull;
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

    // ==================== Drop Collection Tests ====================

    @Test
    void testDropCollection() {
        MongoClientSettings settings = getMongoClientSettings();

        try (NativeSyncClient client = NativeSyncClients.create(settings)) {
            MongoNamespace namespace = new MongoNamespace(getDefaultDatabaseName(), "test_drop_collection");

            // Insert a document to ensure collection exists
            client.insertOne(namespace,
                    new BsonDocument("_id", new BsonInt32(1)).append("name", new BsonString("test")),
                    new InsertOneOptions(),
                    NativeOperationContext.builder().build(),
                    null);

            // Verify collection has data
            try (NativeSyncCursor<BsonDocument> cursor = client.find(
                    namespace,
                    new BsonDocument(),
                    new FindOptions(),
                    new BsonDocumentCodec(),
                    NativeOperationContext.builder().build(),
                    null)) {
                assertTrue(cursor.hasNext());
            }

            // Drop the collection using the FFI method
            assertDoesNotThrow(() -> client.dropCollection(
                    namespace,
                    new com.mongodb.client.model.DropCollectionOptions(),
                    NativeOperationContext.builder().build(),
                    null));

            // Verify collection is empty (dropped)
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

    @Test
    void testDropCollectionThatDoesNotExist() {
        MongoClientSettings settings = getMongoClientSettings();

        try (NativeSyncClient client = NativeSyncClients.create(settings)) {
            MongoNamespace namespace = new MongoNamespace(getDefaultDatabaseName(), "test_drop_nonexistent_" + System.currentTimeMillis());

            // Drop a collection that doesn't exist - should not throw
            assertDoesNotThrow(() -> client.dropCollection(
                    namespace,
                    new com.mongodb.client.model.DropCollectionOptions(),
                    NativeOperationContext.builder().build(),
                    null));
        }
    }

    @Test
    void testDropCollectionWithSession() {
        MongoClientSettings settings = getMongoClientSettings();

        try (NativeSyncClient client = NativeSyncClients.create(settings)) {
            MongoNamespace namespace = new MongoNamespace(getDefaultDatabaseName(), "test_drop_with_session");

            // Insert a document to ensure collection exists
            client.insertOne(namespace,
                    new BsonDocument("_id", new BsonInt32(1)),
                    new InsertOneOptions(),
                    NativeOperationContext.builder().build(),
                    null);

            // Drop with session
            NativeSyncClientSession session = client.startSession(ClientSessionOptions.builder().build());
            assertDoesNotThrow(() -> client.dropCollection(
                    namespace,
                    new com.mongodb.client.model.DropCollectionOptions(),
                    NativeOperationContext.builder().build(),
                    session));
            session.close();

            // Verify collection is dropped
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

    // ==================== Drop Database Tests ====================

    @Test
    void testDropDatabase() {
        MongoClientSettings settings = getMongoClientSettings();

        try (NativeSyncClient client = NativeSyncClients.create(settings)) {
            String databaseName = "test_drop_database_" + System.currentTimeMillis();
            MongoNamespace namespace = new MongoNamespace(databaseName, "test_collection");

            // Insert a document to ensure database exists
            client.insertOne(namespace,
                    new BsonDocument("_id", new BsonInt32(1)),
                    new InsertOneOptions(),
                    NativeOperationContext.builder().build(),
                    null);

            // Verify database has data
            try (NativeSyncCursor<BsonDocument> cursor = client.find(
                    namespace,
                    new BsonDocument(),
                    new FindOptions(),
                    new BsonDocumentCodec(),
                    NativeOperationContext.builder().build(),
                    null)) {
                assertTrue(cursor.hasNext());
            }

            // Drop the database using the FFI method
            assertDoesNotThrow(() -> client.dropDatabase(
                    databaseName,
                    NativeOperationContext.builder().build(),
                    null));

            // Verify database is dropped (collection is empty/gone)
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

    @Test
    void testDropDatabaseThatDoesNotExist() {
        MongoClientSettings settings = getMongoClientSettings();

        try (NativeSyncClient client = NativeSyncClients.create(settings)) {
            String databaseName = "test_drop_nonexistent_db_" + System.currentTimeMillis();

            // Drop a database that doesn't exist - should not throw
            assertDoesNotThrow(() -> client.dropDatabase(
                    databaseName,
                    NativeOperationContext.builder().build(),
                    null));
        }
    }

    @Test
    void testDropDatabaseWithSession() {
        MongoClientSettings settings = getMongoClientSettings();

        try (NativeSyncClient client = NativeSyncClients.create(settings)) {
            String databaseName = "test_drop_db_session_" + System.currentTimeMillis();
            MongoNamespace namespace = new MongoNamespace(databaseName, "test_collection");

            // Insert a document to ensure database exists
            client.insertOne(namespace,
                    new BsonDocument("_id", new BsonInt32(1)),
                    new InsertOneOptions(),
                    NativeOperationContext.builder().build(),
                    null);

            // Drop with session
            NativeSyncClientSession session = client.startSession(ClientSessionOptions.builder().build());
            assertDoesNotThrow(() -> client.dropDatabase(
                    databaseName,
                    NativeOperationContext.builder().build(),
                    session));
            session.close();

            // Verify database is dropped
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

    // ==================== Delete Tests ====================

    @Test
    void testDeleteOne() {
        try (NativeSyncClient client = NativeSyncClients.create(getMongoClientSettings())) {
            MongoNamespace ns = new MongoNamespace(getDefaultDatabaseName(), "test_delete_one");
            dropCollection(client, ns);
            insertDocs(client, ns,
                    new BsonDocument("_id", new BsonInt32(1)).append("x", new BsonInt32(1)),
                    new BsonDocument("_id", new BsonInt32(2)).append("x", new BsonInt32(1)),
                    new BsonDocument("_id", new BsonInt32(3)).append("x", new BsonInt32(2)));

            DeleteResult result = client.deleteOne(ns,
                    new BsonDocument("x", new BsonInt32(1)),
                    new DeleteOptions(),
                    NativeOperationContext.builder().build(), null);

            assertTrue(result.wasAcknowledged());
            assertEquals(1, result.getDeletedCount());
            assertEquals(2, countAll(client, ns));
        }
    }

    @Test
    void testDeleteOneNoMatch() {
        try (NativeSyncClient client = NativeSyncClients.create(getMongoClientSettings())) {
            MongoNamespace ns = new MongoNamespace(getDefaultDatabaseName(), "test_delete_one_nomatch");
            dropCollection(client, ns);
            insertDocs(client, ns, new BsonDocument("_id", new BsonInt32(1)));

            DeleteResult result = client.deleteOne(ns,
                    new BsonDocument("_id", new BsonInt32(999)),
                    new DeleteOptions(),
                    NativeOperationContext.builder().build(), null);

            assertTrue(result.wasAcknowledged());
            assertEquals(0, result.getDeletedCount());
        }
    }

    @Test
    void testDeleteMany() {
        try (NativeSyncClient client = NativeSyncClients.create(getMongoClientSettings())) {
            MongoNamespace ns = new MongoNamespace(getDefaultDatabaseName(), "test_delete_many");
            dropCollection(client, ns);
            insertDocs(client, ns,
                    new BsonDocument("_id", new BsonInt32(1)).append("x", new BsonInt32(1)),
                    new BsonDocument("_id", new BsonInt32(2)).append("x", new BsonInt32(1)),
                    new BsonDocument("_id", new BsonInt32(3)).append("x", new BsonInt32(2)));

            DeleteResult result = client.deleteMany(ns,
                    new BsonDocument("x", new BsonInt32(1)),
                    new DeleteOptions(),
                    NativeOperationContext.builder().build(), null);

            assertTrue(result.wasAcknowledged());
            assertEquals(2, result.getDeletedCount());
            assertEquals(1, countAll(client, ns));
        }
    }

    // ==================== Update Tests ====================

    @Test
    void testUpdateOne() {
        try (NativeSyncClient client = NativeSyncClients.create(getMongoClientSettings())) {
            MongoNamespace ns = new MongoNamespace(getDefaultDatabaseName(), "test_update_one");
            dropCollection(client, ns);
            insertDocs(client, ns,
                    new BsonDocument("_id", new BsonInt32(1)).append("x", new BsonInt32(1)),
                    new BsonDocument("_id", new BsonInt32(2)).append("x", new BsonInt32(1)));

            UpdateResult result = client.updateOne(ns,
                    new BsonDocument("x", new BsonInt32(1)),
                    new BsonDocument("$set", new BsonDocument("x", new BsonInt32(99))),
                    new UpdateOptions(),
                    NativeOperationContext.builder().build(), null);

            assertTrue(result.wasAcknowledged());
            assertEquals(1, result.getMatchedCount());
            assertEquals(1, result.getModifiedCount());
            assertNull(result.getUpsertedId());
        }
    }

    @Test
    void testUpdateOneUpsert() {
        try (NativeSyncClient client = NativeSyncClients.create(getMongoClientSettings())) {
            MongoNamespace ns = new MongoNamespace(getDefaultDatabaseName(), "test_update_one_upsert");
            dropCollection(client, ns);

            UpdateResult result = client.updateOne(ns,
                    new BsonDocument("_id", new BsonInt32(42)),
                    new BsonDocument("$set", new BsonDocument("x", new BsonInt32(1))),
                    new UpdateOptions().upsert(true),
                    NativeOperationContext.builder().build(), null);

            assertTrue(result.wasAcknowledged());
            assertEquals(0, result.getMatchedCount());
            assertNotNull(result.getUpsertedId());
            assertEquals(1, countAll(client, ns));
        }
    }

    @Test
    void testUpdateMany() {
        try (NativeSyncClient client = NativeSyncClients.create(getMongoClientSettings())) {
            MongoNamespace ns = new MongoNamespace(getDefaultDatabaseName(), "test_update_many");
            dropCollection(client, ns);
            insertDocs(client, ns,
                    new BsonDocument("_id", new BsonInt32(1)).append("x", new BsonInt32(1)),
                    new BsonDocument("_id", new BsonInt32(2)).append("x", new BsonInt32(1)),
                    new BsonDocument("_id", new BsonInt32(3)).append("x", new BsonInt32(2)));

            UpdateResult result = client.updateMany(ns,
                    new BsonDocument("x", new BsonInt32(1)),
                    new BsonDocument("$set", new BsonDocument("updated", new BsonBoolean(true))),
                    new UpdateOptions(),
                    NativeOperationContext.builder().build(), null);

            assertTrue(result.wasAcknowledged());
            assertEquals(2, result.getMatchedCount());
            assertEquals(2, result.getModifiedCount());
        }
    }

    @Test
    void testUpdateManyWithPipeline() {
        try (NativeSyncClient client = NativeSyncClients.create(getMongoClientSettings())) {
            MongoNamespace ns = new MongoNamespace(getDefaultDatabaseName(), "test_update_pipeline");
            dropCollection(client, ns);
            insertDocs(client, ns,
                    new BsonDocument("_id", new BsonInt32(1)).append("x", new BsonInt32(10)),
                    new BsonDocument("_id", new BsonInt32(2)).append("x", new BsonInt32(20)));

            // Pipeline update: wrap stages as {"$pipeline": [...]}
            BsonArray stages = new BsonArray(Collections.singletonList(
                    new BsonDocument("$set", new BsonDocument("doubled", new BsonDocument("$multiply",
                            new BsonArray(Arrays.asList(new BsonString("$x"), new BsonInt32(2))))))));
            BsonDocument pipelineUpdate = new BsonDocument("$pipeline", stages);

            UpdateResult result = client.updateMany(ns,
                    new BsonDocument(),
                    pipelineUpdate,
                    new UpdateOptions(),
                    NativeOperationContext.builder().build(), null);

            assertTrue(result.wasAcknowledged());
            assertEquals(2, result.getMatchedCount());
            assertEquals(2, result.getModifiedCount());

            // Verify field was set
            BsonDocument doc1 = findById(client, ns, 1);
            assertNotNull(doc1);
            assertEquals(20, doc1.getInt32("doubled").getValue());
        }
    }

    @Test
    void testReplaceOne() {
        try (NativeSyncClient client = NativeSyncClients.create(getMongoClientSettings())) {
            MongoNamespace ns = new MongoNamespace(getDefaultDatabaseName(), "test_replace_one");
            dropCollection(client, ns);
            insertDocs(client, ns,
                    new BsonDocument("_id", new BsonInt32(1)).append("x", new BsonInt32(1)).append("y", new BsonInt32(2)));

            UpdateResult result = client.replaceOne(ns,
                    new BsonDocument("_id", new BsonInt32(1)),
                    new BsonDocument("_id", new BsonInt32(1)).append("z", new BsonInt32(99)),
                    new ReplaceOptions(),
                    NativeOperationContext.builder().build(), null);

            assertTrue(result.wasAcknowledged());
            assertEquals(1, result.getMatchedCount());
            assertEquals(1, result.getModifiedCount());

            BsonDocument replaced = findById(client, ns, 1);
            assertNotNull(replaced);
            assertFalse(replaced.containsKey("x"));
            assertFalse(replaced.containsKey("y"));
            assertEquals(99, replaced.getInt32("z").getValue());
        }
    }

    // ==================== FindOneAnd* Tests ====================

    @Test
    void testFindOneAndDelete() {
        try (NativeSyncClient client = NativeSyncClients.create(getMongoClientSettings())) {
            MongoNamespace ns = new MongoNamespace(getDefaultDatabaseName(), "test_find_one_and_delete");
            dropCollection(client, ns);
            insertDocs(client, ns,
                    new BsonDocument("_id", new BsonInt32(1)).append("x", new BsonInt32(1)),
                    new BsonDocument("_id", new BsonInt32(2)).append("x", new BsonInt32(2)));

            BsonDocument deleted = client.findOneAndDelete(ns,
                    new BsonDocument("_id", new BsonInt32(1)),
                    new FindOneAndDeleteOptions(),
                    new BsonDocumentCodec(),
                    NativeOperationContext.builder().build(), null);

            assertNotNull(deleted);
            assertEquals(1, deleted.getInt32("_id").getValue());
            assertEquals(1, countAll(client, ns));
        }
    }

    @Test
    void testFindOneAndDeleteNoMatch() {
        try (NativeSyncClient client = NativeSyncClients.create(getMongoClientSettings())) {
            MongoNamespace ns = new MongoNamespace(getDefaultDatabaseName(), "test_find_one_and_delete_nomatch");
            dropCollection(client, ns);
            insertDocs(client, ns, new BsonDocument("_id", new BsonInt32(1)));

            BsonDocument result = client.findOneAndDelete(ns,
                    new BsonDocument("_id", new BsonInt32(999)),
                    new FindOneAndDeleteOptions(),
                    new BsonDocumentCodec(),
                    NativeOperationContext.builder().build(), null);

            assertNull(result);
            assertEquals(1, countAll(client, ns));
        }
    }

    @Test
    void testFindOneAndReplaceBefore() {
        try (NativeSyncClient client = NativeSyncClients.create(getMongoClientSettings())) {
            MongoNamespace ns = new MongoNamespace(getDefaultDatabaseName(), "test_find_one_and_replace");
            dropCollection(client, ns);
            insertDocs(client, ns,
                    new BsonDocument("_id", new BsonInt32(1)).append("x", new BsonInt32(1)));

            BsonDocument before = client.findOneAndReplace(ns,
                    new BsonDocument("_id", new BsonInt32(1)),
                    new BsonDocument("_id", new BsonInt32(1)).append("x", new BsonInt32(99)),
                    new FindOneAndReplaceOptions().returnDocument(ReturnDocument.BEFORE),
                    new BsonDocumentCodec(),
                    NativeOperationContext.builder().build(), null);

            assertNotNull(before);
            assertEquals(1, before.getInt32("x").getValue());

            BsonDocument after = findById(client, ns, 1);
            assertNotNull(after);
            assertEquals(99, after.getInt32("x").getValue());
        }
    }

    @Test
    void testFindOneAndReplaceAfter() {
        try (NativeSyncClient client = NativeSyncClients.create(getMongoClientSettings())) {
            MongoNamespace ns = new MongoNamespace(getDefaultDatabaseName(), "test_find_one_and_replace_after");
            dropCollection(client, ns);
            insertDocs(client, ns,
                    new BsonDocument("_id", new BsonInt32(1)).append("x", new BsonInt32(1)));

            BsonDocument after = client.findOneAndReplace(ns,
                    new BsonDocument("_id", new BsonInt32(1)),
                    new BsonDocument("_id", new BsonInt32(1)).append("x", new BsonInt32(99)),
                    new FindOneAndReplaceOptions().returnDocument(ReturnDocument.AFTER),
                    new BsonDocumentCodec(),
                    NativeOperationContext.builder().build(), null);

            assertNotNull(after);
            assertEquals(99, after.getInt32("x").getValue());
        }
    }

    @Test
    void testFindOneAndUpdate() {
        try (NativeSyncClient client = NativeSyncClients.create(getMongoClientSettings())) {
            MongoNamespace ns = new MongoNamespace(getDefaultDatabaseName(), "test_find_one_and_update");
            dropCollection(client, ns);
            insertDocs(client, ns,
                    new BsonDocument("_id", new BsonInt32(1)).append("x", new BsonInt32(1)));

            BsonDocument after = client.findOneAndUpdate(ns,
                    new BsonDocument("_id", new BsonInt32(1)),
                    new BsonDocument("$set", new BsonDocument("x", new BsonInt32(99))),
                    new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER),
                    new BsonDocumentCodec(),
                    NativeOperationContext.builder().build(), null);

            assertNotNull(after);
            assertEquals(99, after.getInt32("x").getValue());
        }
    }

    @Test
    void testFindOneAndUpdateUpsert() {
        try (NativeSyncClient client = NativeSyncClients.create(getMongoClientSettings())) {
            MongoNamespace ns = new MongoNamespace(getDefaultDatabaseName(), "test_find_one_and_update_upsert");
            dropCollection(client, ns);

            BsonDocument after = client.findOneAndUpdate(ns,
                    new BsonDocument("_id", new BsonInt32(42)),
                    new BsonDocument("$set", new BsonDocument("x", new BsonInt32(1))),
                    new FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER),
                    new BsonDocumentCodec(),
                    NativeOperationContext.builder().build(), null);

            assertNotNull(after);
            assertEquals(42, after.getInt32("_id").getValue());
            assertEquals(1, countAll(client, ns));
        }
    }

    // ==================== Aggregate Tests ====================

    @Test
    void testAggregateCollection() {
        try (NativeSyncClient client = NativeSyncClients.create(getMongoClientSettings())) {
            MongoNamespace ns = new MongoNamespace(getDefaultDatabaseName(), "test_aggregate");
            dropCollection(client, ns);
            insertDocs(client, ns,
                    new BsonDocument("_id", new BsonInt32(1)).append("x", new BsonInt32(10)),
                    new BsonDocument("_id", new BsonInt32(2)).append("x", new BsonInt32(20)),
                    new BsonDocument("_id", new BsonInt32(3)).append("x", new BsonInt32(30)));

            List<BsonDocument> pipeline = Arrays.asList(
                    BsonDocument.parse("{$match: {x: {$gte: 15}}}"),
                    BsonDocument.parse("{$sort: {x: 1}}"));

            try (NativeSyncCursor<BsonDocument> cursor = client.aggregate(ns, pipeline,
                    new AggregateOptions(), null, new BsonDocumentCodec(),
                    NativeOperationContext.builder().build(), null)) {

                List<BsonDocument> results = drainCursor(cursor);
                assertEquals(2, results.size());
                assertEquals(20, results.get(0).getInt32("x").getValue());
                assertEquals(30, results.get(1).getInt32("x").getValue());
            }
        }
    }

    @Test
    void testAggregateWithGroupStage() {
        try (NativeSyncClient client = NativeSyncClients.create(getMongoClientSettings())) {
            MongoNamespace ns = new MongoNamespace(getDefaultDatabaseName(), "test_aggregate_group");
            dropCollection(client, ns);
            insertDocs(client, ns,
                    new BsonDocument("_id", new BsonInt32(1)).append("cat", new BsonString("a")).append("v", new BsonInt32(1)),
                    new BsonDocument("_id", new BsonInt32(2)).append("cat", new BsonString("a")).append("v", new BsonInt32(2)),
                    new BsonDocument("_id", new BsonInt32(3)).append("cat", new BsonString("b")).append("v", new BsonInt32(10)));

            List<BsonDocument> pipeline = Collections.singletonList(
                    BsonDocument.parse("{$group: {_id: '$cat', total: {$sum: '$v'}}}"));

            try (NativeSyncCursor<BsonDocument> cursor = client.aggregate(ns, pipeline,
                    new AggregateOptions(), null, new BsonDocumentCodec(),
                    NativeOperationContext.builder().build(), null)) {

                List<BsonDocument> results = drainCursor(cursor);
                assertEquals(2, results.size());
                // Find group "a" and verify sum
                BsonDocument groupA = results.stream()
                        .filter(d -> "a".equals(d.getString("_id").getValue()))
                        .findFirst().orElse(null);
                assertNotNull(groupA);
                assertEquals(3, groupA.getInt32("total").getValue());
            }
        }
    }

    @Test
    void testAggregateDatabase() {
        try (NativeSyncClient client = NativeSyncClients.create(getMongoClientSettings())) {
            String dbName = getDefaultDatabaseName();
            MongoNamespace ns = new MongoNamespace(dbName, "test_aggregate_db");
            dropCollection(client, ns);
            insertDocs(client, ns, new BsonDocument("_id", new BsonInt32(1)));

            List<BsonDocument> pipeline = Arrays.asList(
                    BsonDocument.parse("{$listLocalSessions: {}}"),
                    BsonDocument.parse("{$limit: 1}"));

            // Just verify it doesn't throw and returns a cursor
            try (NativeSyncCursor<BsonDocument> cursor = client.aggregateDatabase(dbName, pipeline,
                    new AggregateOptions(), null, new BsonDocumentCodec(),
                    NativeOperationContext.builder().build(), null)) {
                assertNotNull(cursor);
            }
        }
    }

    // ==================== Count Tests ====================

    @Test
    void testCountDocuments() {
        try (NativeSyncClient client = NativeSyncClients.create(getMongoClientSettings())) {
            MongoNamespace ns = new MongoNamespace(getDefaultDatabaseName(), "test_count_docs");
            dropCollection(client, ns);
            insertDocs(client, ns,
                    new BsonDocument("_id", new BsonInt32(1)).append("x", new BsonInt32(1)),
                    new BsonDocument("_id", new BsonInt32(2)).append("x", new BsonInt32(1)),
                    new BsonDocument("_id", new BsonInt32(3)).append("x", new BsonInt32(2)));

            long total = client.countDocuments(ns, new BsonDocument(), new CountOptions(),
                    NativeOperationContext.builder().build(), null);
            assertEquals(3, total);

            long filtered = client.countDocuments(ns, new BsonDocument("x", new BsonInt32(1)),
                    new CountOptions(), NativeOperationContext.builder().build(), null);
            assertEquals(2, filtered);
        }
    }

    @Test
    void testCountDocumentsEmpty() {
        try (NativeSyncClient client = NativeSyncClients.create(getMongoClientSettings())) {
            MongoNamespace ns = new MongoNamespace(getDefaultDatabaseName(), "test_count_empty");
            dropCollection(client, ns);

            long count = client.countDocuments(ns, new BsonDocument(), new CountOptions(),
                    NativeOperationContext.builder().build(), null);
            assertEquals(0, count);
        }
    }

    @Test
    void testEstimatedDocumentCount() {
        try (NativeSyncClient client = NativeSyncClients.create(getMongoClientSettings())) {
            MongoNamespace ns = new MongoNamespace(getDefaultDatabaseName(), "test_estimated_count");
            dropCollection(client, ns);
            insertDocs(client, ns,
                    new BsonDocument("_id", new BsonInt32(1)),
                    new BsonDocument("_id", new BsonInt32(2)),
                    new BsonDocument("_id", new BsonInt32(3)));

            long count = client.estimatedDocumentCount(ns, new EstimatedDocumentCountOptions(),
                    NativeOperationContext.builder().build());
            assertEquals(3, count);
        }
    }

    // ==================== Test Helpers ====================

    private void insertDocs(NativeSyncClient client, MongoNamespace ns, BsonDocument... docs) {
        for (BsonDocument doc : docs) {
            client.insertOne(ns, doc, new InsertOneOptions(), NativeOperationContext.builder().build(), null);
        }
    }

    private long countAll(NativeSyncClient client, MongoNamespace ns) {
        return client.countDocuments(ns, new BsonDocument(), new CountOptions(),
                NativeOperationContext.builder().build(), null);
    }

    private BsonDocument findById(NativeSyncClient client, MongoNamespace ns, int id) {
        NativeSyncCursor<BsonDocument> cursor = client.find(ns, new BsonDocument("_id", new BsonInt32(id)),
                new FindOptions().limit(1), new BsonDocumentCodec(),
                NativeOperationContext.builder().build(), null);
        return cursor.hasNext() ? cursor.next() : null;
    }

    private List<BsonDocument> drainCursor(NativeSyncCursor<BsonDocument> cursor) {
        List<BsonDocument> results = new ArrayList<>();
        while (cursor.hasNext()) {
            results.add(cursor.next());
        }
        return results;
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

