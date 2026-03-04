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

package com.mongodb.reactivestreams.client;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoNamespace;
import org.bson.Document;
import reactor.core.publisher.Mono;

import static com.mongodb.client.Fixture.TIMEOUT_DURATION;
import static com.mongodb.client.Fixture.getDefaultDatabaseName;
import static com.mongodb.client.Fixture.getServerApi;

/**
 * Helper class for reactive streams tests.
 * <p>
 * For shared test utilities (connection string, timeout, encryption checks, etc.),
 * use {@link com.mongodb.client.Fixture} directly.
 */
public final class Fixture {
    private static MongoClient mongoClient;

    private Fixture() {
    }

    public static synchronized MongoClient getMongoClient() {
        if (mongoClient == null) {
            mongoClient = MongoClients.create(getMongoClientSettings());
            Runtime.getRuntime().addShutdownHook(new ShutdownHook());
        }
        return mongoClient;
    }

    public static MongoClientSettings getMongoClientSettings() {
        return getMongoClientSettingsBuilder().build();
    }

    public static MongoClientSettings.Builder getMongoClientSettingsBuilder() {
        return getMongoClientSettingsBuilder(com.mongodb.client.Fixture.getConnectionString());
    }

    public static MongoClientSettings.Builder getMongoClientSettingsBuilder(final ConnectionString connectionString) {
        MongoClientSettings.Builder builder = MongoClientSettings.builder();
        if (getServerApi() != null) {
            builder.serverApi(getServerApi());
        }
        return builder.applyConnectionString(connectionString);
    }

    public static MongoClientSettings.Builder getMongoClientBuilderFromConnectionString() {
        MongoClientSettings.Builder builder = MongoClientSettings.builder()
                .applyConnectionString(com.mongodb.client.Fixture.getConnectionString());
        if (getServerApi() != null) {
            builder.serverApi(getServerApi());
        }
        return builder;
    }

    public static MongoDatabase getDefaultDatabase() {
        return getMongoClient().getDatabase(getDefaultDatabaseName());
    }

    public static MongoCollection<Document> initializeCollection(final MongoNamespace namespace) {
        MongoDatabase database = getMongoClient().getDatabase(namespace.getDatabaseName());
        try {
            Mono.from(database.runCommand(new Document("drop", namespace.getCollectionName()))).block(TIMEOUT_DURATION);
        } catch (MongoCommandException e) {
            if (!e.getErrorMessage().contains("ns not found")) {
                throw e;
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
        return database.getCollection(namespace.getCollectionName());
    }

    public static void dropDatabase(final String name) {
        if (name == null) {
            return;
        }
        try {
            Mono.from(getMongoClient().getDatabase(name).runCommand(new Document("dropDatabase", 1))).block(TIMEOUT_DURATION);
        } catch (MongoCommandException e) {
            if (!e.getErrorMessage().contains("ns not found")) {
                throw e;
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    public static void drop(final MongoNamespace namespace) {
        try {
            Mono.from(getMongoClient().getDatabase(namespace.getDatabaseName())
                    .runCommand(new Document("drop", namespace.getCollectionName()))).block(TIMEOUT_DURATION);
        } catch (MongoCommandException e) {
            if (!e.getErrorMessage().contains("ns not found")) {
                throw e;
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    public static synchronized void waitForLastServerSessionPoolRelease() {
        // Session pool checking not available - internal API removed
    }

    static class ShutdownHook extends Thread {
        @Override
        public void run() {
            try {
                dropDatabase(getDefaultDatabaseName());
            } catch (Exception e) {
                // ignore
            }
            mongoClient.close();
            mongoClient = null;
        }
    }
}
