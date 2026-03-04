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

import com.mongodb.internal.TimeoutSettings;
import com.mongodb.lang.Nullable;

import java.time.Duration;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Helper class for the acceptance tests.  Used primarily by DatabaseTestCase and FunctionalSpecification.  This fixture allows Test
 * super-classes to share functionality whilst minimising duplication.
 */
public final class ClusterFixture {
    public static final String DEFAULT_URI = "mongodb://localhost:27017";
    public static final String MONGODB_URI_SYSTEM_PROPERTY_NAME = "org.mongodb.test.uri";
    private static final String DEFAULT_DATABASE_NAME = "JavaDriverTest";
    public static final long TIMEOUT = 120L;
    public static final Duration TIMEOUT_DURATION = Duration.ofSeconds(TIMEOUT);

    public static final TimeoutSettings TIMEOUT_SETTINGS = new TimeoutSettings(30_000, 10_000, 0, null, SECONDS.toMillis(5));

    private static ConnectionString connectionString;

    private ClusterFixture() {
    }

    public static String getDefaultDatabaseName() {
        return DEFAULT_DATABASE_NAME;
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

    @Nullable
    public static String getEnv(final String name) {
        return System.getenv(name);
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
