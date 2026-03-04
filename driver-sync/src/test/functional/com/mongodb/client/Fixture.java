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

package com.mongodb.client;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.connection.ClusterType;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerVersion;
import com.mongodb.connection.SslSettings;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;

import java.util.List;

import static com.mongodb.connection.ClusterConnectionMode.LOAD_BALANCED;
import static com.mongodb.connection.ClusterConnectionMode.MULTIPLE;
import static com.mongodb.connection.ClusterType.REPLICA_SET;
import static com.mongodb.connection.ClusterType.SHARDED;
import static com.mongodb.connection.ClusterType.STANDALONE;
import static com.mongodb.internal.connection.ClusterDescriptionHelper.getPrimaries;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

/**
 * Helper class for the acceptance tests.
 * Delegates to {@link com.mongodb.rust.crud.Fixture} for connection string discovery and settings.
 */
public final class Fixture {
    private static MongoClient mongoClient;
    private static MongoDatabase defaultDatabase;

    private Fixture() {
    }

    public static synchronized MongoClient getMongoClient() {
        if (mongoClient != null) {
            return mongoClient;
        }
        MongoClientSettings mongoClientSettings = getMongoClientSettings();
        mongoClient = MongoClients.create(mongoClientSettings);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            synchronized (Fixture.class) {
                if (mongoClient == null) {
                    return;
                }
                if (defaultDatabase != null) {
                    defaultDatabase.drop();
                }
                mongoClient.close();
                mongoClient = null;
            }
        }));
        return mongoClient;
    }

    public static synchronized MongoDatabase getDefaultDatabase() {
        if (defaultDatabase == null) {
            defaultDatabase = getMongoClient().getDatabase(getDefaultDatabaseName());
        }
        return defaultDatabase;
    }

    public static String getDefaultDatabaseName() {
        return com.mongodb.rust.crud.Fixture.getDefaultDatabaseName();
    }

    public static MongoClientSettings getMongoClientSettings() {
        return com.mongodb.rust.crud.Fixture.getMongoClientSettings();
    }

    public static MongoClientSettings.Builder getMongoClientSettingsBuilder() {
        return com.mongodb.rust.crud.Fixture.getMongoClientSettingsBuilder();
    }

    public static MongoClientSettings.Builder getMultiMongosMongoClientSettingsBuilder() {
        return com.mongodb.rust.crud.Fixture.getMongoClientSettings(requireNonNull(getMultiMongosConnectionString()));
    }

    public static MongoClientSettings.Builder getMongoClientSettings(final ConnectionString connectionString) {
        return com.mongodb.rust.crud.Fixture.getMongoClientSettings(connectionString);
    }

    /**
     * Beware of a potential race condition hiding here: the primary you discover may differ from the one used by the {@code client}
     * when performing some operations, as the primary may change.
     */
    public static ServerAddress getPrimary() throws InterruptedException {
        MongoClient client = getMongoClient();
        List<ServerDescription> serverDescriptions = getPrimaries(client.getClusterDescription());
        while (serverDescriptions.isEmpty()) {
            Thread.sleep(100);
            serverDescriptions = getPrimaries(client.getClusterDescription());
        }
        return serverDescriptions.get(0).getAddress();
    }

    @Nullable
    public static ConnectionString getMultiMongosConnectionString() {
        return com.mongodb.rust.crud.Fixture.getMultiMongosConnectionString();
    }

    // Cluster type helpers

    public static ClusterDescription getClusterDescription() {
        return getMongoClient().getClusterDescription();
    }

    public static boolean clusterIsType(final ClusterType clusterType) {
        return getClusterDescription().getType() == clusterType;
    }

    public static ClusterConnectionMode getClusterConnectionMode() {
        return getClusterDescription().getConnectionMode();
    }

    public static boolean isDiscoverableReplicaSet() {
        return clusterIsType(REPLICA_SET) && getClusterConnectionMode() == MULTIPLE;
    }

    public static boolean isSharded() {
        return clusterIsType(SHARDED);
    }

    public static boolean isStandalone() {
        return clusterIsType(STANDALONE);
    }

    public static boolean isLoadBalanced() {
        return getClusterConnectionMode() == LOAD_BALANCED;
    }

    // Server version helpers

    public static ServerVersion getServerVersion() {
        return com.mongodb.rust.crud.Fixture.getServerVersion();
    }

    public static boolean serverVersionAtLeast(final int majorVersion, final int minorVersion) {
        return com.mongodb.rust.crud.Fixture.serverVersionAtLeast(majorVersion, minorVersion);
    }

    public static boolean serverVersionLessThan(final int majorVersion, final int minorVersion) {
        return com.mongodb.rust.crud.Fixture.serverVersionLessThan(majorVersion, minorVersion);
    }

    // Failpoint helpers

    public static void configureFailPoint(final BsonDocument failPointDocument) {
        getMongoClient().getDatabase("admin").runCommand(failPointDocument);
    }

    public static void disableFailPoint(final String failPoint) {
        BsonDocument failPointDocument = new BsonDocument("configureFailPoint", new org.bson.BsonString(failPoint))
                .append("mode", new org.bson.BsonString("off"));
        try {
            getMongoClient().getDatabase("admin").runCommand(failPointDocument);
        } catch (com.mongodb.MongoCommandException e) {
            // ignore
        }
    }

    public static void enableMaxTimeFailPoint() {
        configureFailPoint(BsonDocument.parse("{configureFailPoint: 'maxTimeAlwaysTimeOut', mode: 'alwaysOn'}"));
    }

    public static void disableMaxTimeFailPoint() {
        disableFailPoint("maxTimeAlwaysTimeOut");
    }

    public static ClusterSettings.Builder setDirectConnection(final ClusterSettings.Builder builder) {
        try {
            return builder.mode(ClusterConnectionMode.SINGLE).hosts(singletonList(getPrimary()));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private static BsonDocument serverParameters;

    public static BsonDocument getServerParameters() {
        if (serverParameters == null) {
            serverParameters = getMongoClient().getDatabase("admin")
                    .runCommand(new BsonDocument("getParameter", new org.bson.BsonString("*")), BsonDocument.class);
        }
        return serverParameters;
    }

    public static ConnectionString getConnectionString() {
        return com.mongodb.rust.crud.Fixture.getConnectionString();
    }

    public static SslSettings getSslSettings() {
        return com.mongodb.rust.crud.Fixture.getSslSettings();
    }

    public static SslSettings getSslSettings(final ConnectionString connectionString) {
        return SslSettings.builder().applyConnectionString(connectionString).build();
    }

    @Nullable
    public static MongoCredential getCredential() {
        return getConnectionString().getCredential();
    }

    public static boolean isUnixSocket() {
        return getConnectionString().getConnectionString().contains(".sock");
    }

    public static boolean isAuthenticated() {
        return getConnectionString().getCredential() != null;
    }

    // Environment and encryption test helpers - delegated

    @Nullable
    public static String getEnv(final String name) {
        return com.mongodb.rust.crud.Fixture.getEnv(name);
    }

    public static String getEnv(final String name, final String defaultValue) {
        return com.mongodb.rust.crud.Fixture.getEnv(name, defaultValue);
    }

    public static boolean hasEncryptionTestsEnabled() {
        return com.mongodb.rust.crud.Fixture.hasEncryptionTestsEnabled();
    }

    public static boolean isClientSideEncryptionTest() {
        return com.mongodb.rust.crud.Fixture.isClientSideEncryptionTest();
    }

    public static java.util.Optional<String> cryptSharedLibPathSysPropValue() {
        return com.mongodb.rust.crud.Fixture.cryptSharedLibPathSysPropValue();
    }

    public static boolean isAtlasSearchTest() {
        return com.mongodb.rust.crud.Fixture.isAtlasSearchTest();
    }

    public static boolean getOcspShouldSucceed() {
        return com.mongodb.rust.crud.Fixture.getOcspShouldSucceed();
    }

    public static ServerVersion getMongoCryptVersion() {
        return com.mongodb.rust.crud.Fixture.getMongoCryptVersion();
    }

    public static java.util.List<Integer> getVersionList(final String versionString) {
        return com.mongodb.rust.crud.Fixture.getVersionList(versionString);
    }

    public static final long TIMEOUT = com.mongodb.rust.crud.Fixture.TIMEOUT;
    public static final java.time.Duration TIMEOUT_DURATION = com.mongodb.rust.crud.Fixture.TIMEOUT_DURATION;

    public static String getConnectionStringSystemPropertyOrDefault() {
        return com.mongodb.rust.crud.Fixture.getConnectionStringSystemPropertyOrDefault();
    }

    @Nullable
    public static com.mongodb.ServerApi getServerApi() {
        return com.mongodb.rust.crud.Fixture.getServerApi();
    }

    public static void sleep(final long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
