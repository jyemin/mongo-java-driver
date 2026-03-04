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

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.ServerApi;
import com.mongodb.ServerApiVersion;
import com.mongodb.connection.ServerVersion;
import com.mongodb.connection.SslSettings;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonValue;
import org.bson.codecs.BsonDocumentCodec;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;

/**
 * Helper class for the rust-crud-bindings functional tests.
 * Uses the NativeSyncClient interface (no compile-time dependency on FFM/Java 23).
 */
public final class Fixture {
    public static final String DEFAULT_URI = "mongodb://localhost:27017";
    public static final String MONGODB_URI_SYSTEM_PROPERTY_NAME = "org.mongodb.test.uri";
    private static final String MONGODB_MULTI_MONGOS_URI_SYSTEM_PROPERTY_NAME = "org.mongodb.test.multi.mongos.uri";
    private static final String DEFAULT_DATABASE_NAME = "JavaDriverTest";
    private static final long MIN_HEARTBEAT_FREQUENCY_MS = 50L;

    public static final long TIMEOUT = 120L;
    public static final java.time.Duration TIMEOUT_DURATION = java.time.Duration.ofSeconds(TIMEOUT);

    private static NativeSyncClient nativeClient;
    private static ConnectionString connectionString;
    private static ServerVersion serverVersion;

    private Fixture() {
    }

    public static synchronized NativeSyncClient getNativeClient() {
        if (nativeClient != null) {
            return nativeClient;
        }
        MongoClientSettings mongoClientSettings = getMongoClientSettings();
        nativeClient = NativeSyncClients.create(mongoClientSettings);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            synchronized (Fixture.class) {
                if (nativeClient == null) {
                    return;
                }
                nativeClient.close();
                nativeClient = null;
            }
        }));
        return nativeClient;
    }

    public static String getDefaultDatabaseName() {
        return DEFAULT_DATABASE_NAME;
    }

    public static MongoClientSettings getMongoClientSettings() {
        return getMongoClientSettingsBuilder().build();
    }

    public static MongoClientSettings.Builder getMongoClientSettingsBuilder() {
        return getMongoClientSettings(getConnectionString());
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
        ServerApi serverApi = getServerApi();
        if (serverApi != null) {
            builder.serverApi(serverApi);
        }
        return builder;
    }

    @Nullable
    public static ServerApi getServerApi() {
        String serverApiVersion = System.getProperty("org.mongodb.test.serverApi");
        if (serverApiVersion != null) {
            return ServerApi.builder().version(ServerApiVersion.findByValue(serverApiVersion)).build();
        }
        return null;
    }

    @Nullable
    private static ConnectionString getConnectionStringFromSystemProperty(final String propertyName) {
        String uri = System.getProperty(propertyName);
        if (uri == null) {
            uri = System.getenv("MONGODB_URI");
        }
        return uri != null ? new ConnectionString(uri) : null;
    }

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
        // Use a temporary client with default URI to discover replica set name
        MongoClientSettings tempSettings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(DEFAULT_URI))
                .build();
        try (NativeSyncClient tempClient = NativeSyncClients.create(tempSettings)) {
            BsonDocument helloResult = runCommandSync(tempClient, "admin",
                    new BsonDocument("isMaster", new BsonInt32(1)));
            if (helloResult.containsKey("setName")) {
                connectionString = new ConnectionString(DEFAULT_URI + "/?replicaSet="
                        + helloResult.getString("setName").getValue());
            } else {
                connectionString = new ConnectionString(DEFAULT_URI);
            }
        }
        return connectionString;
    }

    @Nullable
    public static ConnectionString getMultiMongosConnectionString() {
        return getConnectionStringFromSystemProperty(MONGODB_MULTI_MONGOS_URI_SYSTEM_PROPERTY_NAME);
    }

    public static String getConnectionStringSystemPropertyOrDefault() {
        return System.getProperty(MONGODB_URI_SYSTEM_PROPERTY_NAME, DEFAULT_URI);
    }

    /**
     * Helper to run a command synchronously using the sync client.
     */
    public static BsonDocument runCommandSync(NativeSyncClient client, String dbName, BsonDocument command) {
        return client.runCommand(dbName, command, new BsonDocumentCodec(),
                NativeOperationContext.builder().build(), null);
    }

    /**
     * Helper to run a command synchronously on the shared client.
     */
    public static BsonDocument runCommandSync(String dbName, BsonDocument command) {
        return runCommandSync(getNativeClient(), dbName, command);
    }

    // Server version helpers

    public static ServerVersion getServerVersion() {
        if (serverVersion == null) {
            BsonDocument buildInfo = runCommandSync("admin",
                    new BsonDocument("buildInfo", new BsonInt32(1)));
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

    // Cluster type helpers

    public static boolean isReplicaSet() {
        return getConnectionString().getRequiredReplicaSetName() != null;
    }

    public static boolean isStandalone() {
        return !isReplicaSet() && !isSharded();
    }

    public static boolean isSharded() {
        // Check via hello command
        BsonDocument hello = runCommandSync("admin", new BsonDocument("isMaster", new BsonInt32(1)));
        return hello.containsKey("msg") && "isdbgrid".equals(hello.getString("msg").getValue());
    }

    public static SslSettings getSslSettings() {
        return SslSettings.builder().applyConnectionString(getConnectionString()).build();
    }

    // Environment and encryption test helpers

    @Nullable
    public static String getEnv(final String name) {
        return System.getenv(name);
    }

    public static String getEnv(final String name, final String defaultValue) {
        String value = getEnv(name);
        return value == null ? defaultValue : value;
    }

    public static boolean hasEncryptionTestsEnabled() {
        List<String> requiredSystemProperties = asList("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AZURE_TENANT_ID",
                "AZURE_CLIENT_ID", "AZURE_CLIENT_SECRET", "GCP_EMAIL", "GCP_PRIVATE_KEY", "AWS_TEMP_ACCESS_KEY_ID",
                "AWS_TEMP_SECRET_ACCESS_KEY", "AWS_TEMP_SESSION_TOKEN");
        return requiredSystemProperties.stream()
                .map(name -> getEnv(name, ""))
                .filter(s -> !s.isEmpty())
                .count() == requiredSystemProperties.size();
    }

    public static boolean isClientSideEncryptionTest() {
        return !getEnv("AWS_ACCESS_KEY_ID", "").isEmpty();
    }

    public static java.util.Optional<String> cryptSharedLibPathSysPropValue() {
        String value = getEnv("CRYPT_SHARED_LIB_PATH", "");
        return value.isEmpty() ? java.util.Optional.empty() : java.util.Optional.of(value);
    }

    public static boolean isAtlasSearchTest() {
        return System.getProperty("org.mongodb.test.atlas.search") != null;
    }

    public static boolean getOcspShouldSucceed() {
        return Integer.parseInt(System.getProperty("org.mongodb.test.ocsp.should.succeed", "0")) == 1;
    }

    public static ServerVersion getMongoCryptVersion() {
        // Use reflection to avoid compile-time dependency on mongodb-crypt
        try {
            Class<?> capiClass = Class.forName("com.mongodb.crypt.capi.CAPI");
            Object versionPtr = capiClass.getMethod("mongocrypt_version", (Class<?>) null).invoke(null, (Object) null);
            String version = versionPtr.toString();
            return new ServerVersion(getVersionList(version));
        } catch (Exception e) {
            throw new RuntimeException("Failed to get mongocrypt version", e);
        }
    }

    public static List<Integer> getVersionList(final String versionString) {
        List<Integer> versionList = new java.util.ArrayList<>();
        for (String s : versionString.split("\\.|-")) {
            try {
                versionList.add(Integer.parseInt(s));
            } catch (NumberFormatException e) {
                break;
            }
        }
        while (versionList.size() < 3) {
            versionList.add(0);
        }
        return versionList;
    }

    // Failpoint helpers

    public static void configureFailPoint(final BsonDocument failPointDocument) {
        runCommandSync("admin", failPointDocument);
    }

    public static void disableFailPoint(final String failPoint) {
        BsonDocument failPointDocument = new BsonDocument("configureFailPoint", new org.bson.BsonString(failPoint))
                .append("mode", new org.bson.BsonString("off"));
        try {
            runCommandSync("admin", failPointDocument);
        } catch (Exception e) {
            // ignore
        }
    }
}

