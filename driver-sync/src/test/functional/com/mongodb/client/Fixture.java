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

import com.mongodb.ClusterFixture;
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
import org.bson.BsonInt32;
import org.bson.BsonValue;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.ClusterFixture.DEFAULT_URI;
import static com.mongodb.ClusterFixture.MONGODB_URI_SYSTEM_PROPERTY_NAME;
import static com.mongodb.ClusterFixture.getConnectionStringFromSystemProperty;
import static com.mongodb.ClusterFixture.getServerApi;
import static com.mongodb.connection.ClusterConnectionMode.LOAD_BALANCED;
import static com.mongodb.connection.ClusterConnectionMode.MULTIPLE;
import static com.mongodb.connection.ClusterType.REPLICA_SET;
import static com.mongodb.connection.ClusterType.SHARDED;
import static com.mongodb.connection.ClusterType.STANDALONE;
import static com.mongodb.internal.connection.ClusterDescriptionHelper.getPrimaries;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

/**
 * Helper class for the acceptance tests.
 */
public final class Fixture {
    private static final long MIN_HEARTBEAT_FREQUENCY_MS = 50L;

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
        return ClusterFixture.getDefaultDatabaseName();
    }

    public static MongoClientSettings getMongoClientSettings() {
        return getMongoClientSettingsBuilder().build();
    }

    public static MongoClientSettings.Builder getMongoClientSettingsBuilder() {
        return getMongoClientSettings(getConnectionString());
    }

    public static MongoClientSettings.Builder getMultiMongosMongoClientSettingsBuilder() {
        return getMongoClientSettings(requireNonNull(getMultiMongosConnectionString()));
    }

    public static MongoClientSettings.Builder getMongoClientSettings(final ConnectionString connectionString) {
        MongoClientSettings.Builder builder = MongoClientSettings.builder()
                .applyConnectionString(connectionString)
                .applyToSocketSettings(socketSettingsBuilder -> {
                    socketSettingsBuilder.readTimeout(5, TimeUnit.MINUTES);
                })
                .applyToServerSettings(serverSettingsBuilder -> {
                    serverSettingsBuilder.minHeartbeatFrequency(MIN_HEARTBEAT_FREQUENCY_MS, TimeUnit.MILLISECONDS);
                });
        if (getServerApi() != null) {
            builder.serverApi(getServerApi());
        }
        return builder;
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
        return ClusterFixture.getConnectionStringFromSystemProperty(
                ClusterFixture.MONGODB_MULTI_MONGOS_URI_SYSTEM_PROPERTY_NAME);
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

    private static ServerVersion serverVersion;

    public static ServerVersion getServerVersion() {
        if (serverVersion == null) {
            BsonDocument buildInfo = getMongoClient()
                    .getDatabase("admin")
                    .runCommand(new BsonDocument("buildInfo", new org.bson.BsonInt32(1)), BsonDocument.class);
            List<BsonValue> versionArray = buildInfo.getArray("versionArray").subList(0, 3);
            serverVersion = new ServerVersion(asList(
                    versionArray.get(0).asInt32().getValue(),
                    versionArray.get(1).asInt32().getValue(),
                    versionArray.get(2).asInt32().getValue()));
        }
        return serverVersion;
    }

    public static boolean serverVersionAtLeast(final int majorVersion, final int minorVersion) {
        return getServerVersion().compareTo(new ServerVersion(asList(majorVersion, minorVersion, 0))) >= 0;
    }

    public static boolean serverVersionLessThan(final int majorVersion, final int minorVersion) {
        return getServerVersion().compareTo(new ServerVersion(asList(majorVersion, minorVersion, 0))) < 0;
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

    private static ConnectionString connectionString;

    public static synchronized ConnectionString getConnectionString() {
        if (connectionString != null) {
            return connectionString;
        }

        ConnectionString mongoURIProperty = getConnectionStringFromSystemProperty(MONGODB_URI_SYSTEM_PROPERTY_NAME);
        if (mongoURIProperty != null) {
            connectionString = mongoURIProperty;
            return connectionString;
        }

        // Figure out what the connection string should be by running hello command
        try (MongoClient client = MongoClients.create(DEFAULT_URI)) {
            BsonDocument helloResult = client.getDatabase("admin")
                    .runCommand(new BsonDocument("isMaster", new BsonInt32(1)), BsonDocument.class);
            if (helloResult.containsKey("setName")) {
                connectionString = new ConnectionString(DEFAULT_URI + "/?replicaSet="
                        + helloResult.getString("setName").getValue());
            } else {
                connectionString = new ConnectionString(DEFAULT_URI);
            }
        }
        return connectionString;
    }

    public static SslSettings getSslSettings() {
        return getSslSettings(getConnectionString());
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
}
