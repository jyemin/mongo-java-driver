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

package com.mongodb;

import com.mongodb.connection.ServerVersion;
import com.mongodb.internal.TimeoutSettings;
import com.mongodb.internal.crypt.capi.CAPI;
import com.mongodb.lang.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Helper class for the acceptance tests.  Used primarily by DatabaseTestCase and FunctionalSpecification.  This fixture allows Test
 * super-classes to share functionality whilst minimising duplication.
 */
public final class ClusterFixture {
    public static final String DEFAULT_URI = "mongodb://localhost:27017";
    public static final String MONGODB_URI_SYSTEM_PROPERTY_NAME = "org.mongodb.test.uri";
    public static final String MONGODB_API_VERSION = "org.mongodb.test.api.version";
    public static final String MONGODB_MULTI_MONGOS_URI_SYSTEM_PROPERTY_NAME = "org.mongodb.test.multi.mongos.uri";
    public static final String ATLAS_SEARCH_TEST_SYSTEM_PROPERTY_NAME = "org.mongodb.test.atlas.search";
    private static final String MONGODB_OCSP_SHOULD_SUCCEED = "org.mongodb.test.ocsp.tls.should.succeed";
    private static final String DEFAULT_DATABASE_NAME = "JavaDriverTest";
    public static final long TIMEOUT = 120L;
    public static final Duration TIMEOUT_DURATION = Duration.ofSeconds(TIMEOUT);

    public static final TimeoutSettings TIMEOUT_SETTINGS = new TimeoutSettings(30_000, 10_000, 0, null, SECONDS.toMillis(5));

    private static ConnectionString connectionString;

    private static ServerVersion mongoCryptVersion;

    private ClusterFixture() {
    }

    public static String getDefaultDatabaseName() {
        return DEFAULT_DATABASE_NAME;
    }

    public static ServerVersion getMongoCryptVersion() {
        if (mongoCryptVersion == null) {
            mongoCryptVersion = new ServerVersion(getVersionList(CAPI.mongocrypt_version(null).toString()));
        }
        return mongoCryptVersion;
    }

    public static List<Integer> getVersionList(final String versionString) {
        List<Integer> versionList = new ArrayList<>();
        for (String s : versionString.split("\\.")) {
            versionList.add(Integer.valueOf(s));
        }
        while (versionList.size() < 3) {
            versionList.add(0);
        }
        return versionList;
    }

    public static boolean hasEncryptionTestsEnabled() {
        List<String> requiredSystemProperties = asList("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AZURE_TENANT_ID", "AZURE_CLIENT_ID",
                "AZURE_CLIENT_SECRET", "GCP_EMAIL", "GCP_PRIVATE_KEY", "AWS_TEMP_ACCESS_KEY_ID", "AWS_TEMP_SECRET_ACCESS_KEY",
                "AWS_TEMP_SESSION_TOKEN");
        return requiredSystemProperties.stream()
                        .map(name -> getEnv(name, ""))
                        .filter(s -> !s.isEmpty())
                        .count() == requiredSystemProperties.size();
    }

    public static String getEnv(final String name, final String defaultValue) {
        String value = getEnv(name);
        return value == null ? defaultValue : value;
    }

    public static Optional<String> cryptSharedLibPathSysPropValue() {
        String value = getEnv("CRYPT_SHARED_LIB_PATH", "");
        return value.isEmpty() ? Optional.empty() : Optional.of(value);
    }

    @Nullable
    public static String getEnv(final String name) {
        return System.getenv(name);
    }

    public static boolean getOcspShouldSucceed() {
        return Integer.parseInt(System.getProperty(MONGODB_OCSP_SHOULD_SUCCEED)) == 1;
    }

    @Nullable
    public static ServerApi getServerApi() {
         if (System.getProperty(MONGODB_API_VERSION) == null) {
             return null;
         } else {
             return ServerApi.builder().version(ServerApiVersion.findByValue(System.getProperty(MONGODB_API_VERSION))).build();
         }
    }

    public static String getConnectionStringSystemPropertyOrDefault() {
        return System.getProperty(MONGODB_URI_SYSTEM_PROPERTY_NAME, DEFAULT_URI);
    }

    @Nullable
    public static ConnectionString getConnectionStringFromSystemProperty(final String property) {
        String mongoURIProperty = System.getProperty(property);
        if (mongoURIProperty != null && !mongoURIProperty.isEmpty()) {
            return new ConnectionString(mongoURIProperty);
        }
        return null;
    }

    public static void sleep(final int sleepMS) {
        try {
            Thread.sleep(sleepMS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Nullable
    public static MongoCredential getCredential() {
        return getConnectionString().getCredential();
    }

    public static boolean isClientSideEncryptionTest() {
        return !getEnv("AWS_ACCESS_KEY_ID", "").isEmpty();
    }

    public static boolean isAtlasSearchTest() {
        return System.getProperty(ATLAS_SEARCH_TEST_SYSTEM_PROPERTY_NAME) != null;
    }

    private static synchronized ConnectionString getConnectionString() {
        if (connectionString != null) {
            return connectionString;
        }

        ConnectionString mongoURIProperty = getConnectionStringFromSystemProperty(MONGODB_URI_SYSTEM_PROPERTY_NAME);
        if (mongoURIProperty != null) {
            connectionString = mongoURIProperty;
            return connectionString;
        }

        // Default - assume standalone for driver-core tests
        connectionString = new ConnectionString(DEFAULT_URI);
        return connectionString;
    }
}
