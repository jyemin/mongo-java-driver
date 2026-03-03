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

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
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
}

