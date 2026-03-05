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
import com.mongodb.client.model.InsertOneOptions;
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

import java.util.Arrays;
import java.util.Collections;

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
}

