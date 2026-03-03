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
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.TransactionOptions;
import com.mongodb.rust.crud.NativeAsyncClientSession;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.DocumentCodec;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for FfmAsyncClient.
 *
 * These tests require a running MongoDB instance. By default, connects to localhost:27017.
 * Set the MONGODB_URI environment variable to override.
 */
class FfmAsyncClientTest {

    private static final String MONGODB_URI = System.getenv("MONGODB_URI") != null
            ? System.getenv("MONGODB_URI")
            : "mongodb://localhost:27017";

    @Test
    void testCreateAndCloseClient() {
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(MONGODB_URI))
                .build();

        FfmAsyncClient client = assertDoesNotThrow(() -> new FfmAsyncClient(settings));
        assertNotNull(client);
        assertDoesNotThrow(client::close);
    }

    @Test
    void testCreateClientWithAppName() {
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(MONGODB_URI))
                .applicationName("rust-crud-test")
                .build();

        try (FfmAsyncClient client = new FfmAsyncClient(settings)) {
            assertNotNull(client);
        }
    }

    @Test
    void testCreateClientMultipleTimes() {
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(MONGODB_URI))
                .build();

        // Create and close multiple clients to ensure no memory leaks or issues
        for (int i = 0; i < 3; i++) {
            try (FfmAsyncClient client = new FfmAsyncClient(settings)) {
                assertNotNull(client);
            }
        }
    }

    @Test
    void testRunCommandPing() throws Exception {
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(MONGODB_URI))
                .build();

        try (FfmAsyncClient client = new FfmAsyncClient(settings)) {
            CompletableFuture<BsonDocument> future = new CompletableFuture<>();

            client.runCommand(
                    "admin",
                    new BsonDocument("ping", new BsonInt32(1)),
                    new BsonDocumentCodec(),
                    null,
                    (result, error) -> {
                        if (error != null) {
                            future.completeExceptionally(error);
                        } else {
                            future.complete(result);
                        }
                    });

            BsonDocument result = future.get(10, TimeUnit.SECONDS);
            assertNotNull(result);
            assertTrue(result.containsKey("ok"));
            assertEquals(1.0, result.getDouble("ok").getValue(), 0.001);
        }
    }

    @Test
    void testRunCommandBuildInfo() throws Exception {
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(MONGODB_URI))
                .build();

        try (FfmAsyncClient client = new FfmAsyncClient(settings)) {
            CompletableFuture<BsonDocument> future = new CompletableFuture<>();

            client.runCommand(
                    "admin",
                    new BsonDocument("buildInfo", new BsonInt32(1)),
                    new BsonDocumentCodec(),
                    null,
                    (result, error) -> {
                        if (error != null) {
                            future.completeExceptionally(error);
                        } else {
                            future.complete(result);
                        }
                    });

            BsonDocument result = future.get(10, TimeUnit.SECONDS);
            assertNotNull(result);
            assertTrue(result.containsKey("version"));
            assertTrue(result.containsKey("ok"));
        }
    }

    @Test
    void testRunCommandWithDocumentCodec() throws Exception {
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(MONGODB_URI))
                .build();

        try (FfmAsyncClient client = new FfmAsyncClient(settings)) {
            CompletableFuture<Document> future = new CompletableFuture<>();

            client.runCommand(
                    "admin",
                    new BsonDocument("ping", new BsonInt32(1)),
                    new DocumentCodec(),
                    null,
                    (result, error) -> {
                        if (error != null) {
                            future.completeExceptionally(error);
                        } else {
                            future.complete(result);
                        }
                    });

            Document result = future.get(10, TimeUnit.SECONDS);
            assertNotNull(result);
            assertTrue(result.containsKey("ok"));
            assertEquals(1.0, result.getDouble("ok"), 0.001);
        }
    }

    @Test
    void testStartSession() throws Exception {
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(MONGODB_URI))
                .build();

        try (FfmAsyncClient client = new FfmAsyncClient(settings)) {
            CompletableFuture<NativeAsyncClientSession> future = new CompletableFuture<>();

            client.startSession(
                    ClientSessionOptions.builder().build(),
                    (result, error) -> {
                        if (error != null) {
                            future.completeExceptionally(error);
                        } else {
                            future.complete(result);
                        }
                    });

            NativeAsyncClientSession session = future.get(10, TimeUnit.SECONDS);
            assertNotNull(session);
            assertNotNull(session.getOptions());
            session.close();
        }
    }

    @Test
    void testSessionWithCausalConsistency() throws Exception {
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(MONGODB_URI))
                .build();

        try (FfmAsyncClient client = new FfmAsyncClient(settings)) {
            CompletableFuture<NativeAsyncClientSession> future = new CompletableFuture<>();

            client.startSession(
                    ClientSessionOptions.builder().causallyConsistent(true).build(),
                    (result, error) -> {
                        if (error != null) {
                            future.completeExceptionally(error);
                        } else {
                            future.complete(result);
                        }
                    });

            NativeAsyncClientSession session = future.get(10, TimeUnit.SECONDS);
            assertNotNull(session);
            assertTrue(session.isCausallyConsistent());
            session.close();
        }
    }

    @Test
    void testRunCommandWithSession() throws Exception {
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(MONGODB_URI))
                .build();

        try (FfmAsyncClient client = new FfmAsyncClient(settings)) {
            // Start session
            CompletableFuture<NativeAsyncClientSession> sessionFuture = new CompletableFuture<>();
            client.startSession(ClientSessionOptions.builder().build(), (result, error) -> {
                if (error != null) sessionFuture.completeExceptionally(error);
                else sessionFuture.complete(result);
            });
            NativeAsyncClientSession session = sessionFuture.get(10, TimeUnit.SECONDS);

            // Run command with session
            CompletableFuture<BsonDocument> cmdFuture = new CompletableFuture<>();
            client.runCommand("admin", new BsonDocument("ping", new BsonInt32(1)),
                    new BsonDocumentCodec(), session, (result, error) -> {
                        if (error != null) cmdFuture.completeExceptionally(error);
                        else cmdFuture.complete(result);
                    });

            BsonDocument result = cmdFuture.get(10, TimeUnit.SECONDS);
            assertNotNull(result);
            assertEquals(1.0, result.getDouble("ok").getValue(), 0.001);

            session.close();
        }
    }

    @Test
    void testTransaction() throws Exception {
        // Note: Transactions require a replica set
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(MONGODB_URI))
                .build();

        try (FfmAsyncClient client = new FfmAsyncClient(settings)) {
            // Start session
            CompletableFuture<NativeAsyncClientSession> sessionFuture = new CompletableFuture<>();
            client.startSession(ClientSessionOptions.builder().build(), (result, error) -> {
                if (error != null) sessionFuture.completeExceptionally(error);
                else sessionFuture.complete(result);
            });
            NativeAsyncClientSession session = sessionFuture.get(10, TimeUnit.SECONDS);

            // Start transaction
            CompletableFuture<Void> startTxnFuture = new CompletableFuture<>();
            session.startTransaction(TransactionOptions.builder().build(), (result, error) -> {
                if (error != null) startTxnFuture.completeExceptionally(error);
                else startTxnFuture.complete(result);
            });
            startTxnFuture.get(10, TimeUnit.SECONDS);
            assertTrue(session.hasActiveTransaction());

            // Insert a document using runCommand with session
            BsonDocument insertCmd = new BsonDocument()
                    .append("insert", new BsonString("test_txn_collection"))
                    .append("documents", new BsonArray(java.util.List.of(
                            new BsonDocument("_id", new BsonInt32(1)).append("value", new BsonString("test"))
                    )));

            CompletableFuture<BsonDocument> insertFuture = new CompletableFuture<>();
            client.runCommand("test", insertCmd, new BsonDocumentCodec(), session, (result, error) -> {
                if (error != null) insertFuture.completeExceptionally(error);
                else insertFuture.complete(result);
            });
            BsonDocument insertResult = insertFuture.get(10, TimeUnit.SECONDS);
            assertNotNull(insertResult);
            assertEquals(1, insertResult.getInt32("n").getValue());

            // Abort transaction (to not leave test data)
            CompletableFuture<Void> abortFuture = new CompletableFuture<>();
            session.abortTransaction((result, error) -> {
                if (error != null) abortFuture.completeExceptionally(error);
                else abortFuture.complete(result);
            });
            abortFuture.get(10, TimeUnit.SECONDS);
            assertTrue(!session.hasActiveTransaction());

            session.close();
        }
    }
}

