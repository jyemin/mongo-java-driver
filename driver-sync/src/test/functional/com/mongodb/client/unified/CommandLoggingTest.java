/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.client.unified;

import com.mongodb.lang.Nullable;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collection;

import static com.mongodb.ClusterFixture.isServerlessTest;
import static org.junit.Assume.assumeFalse;

public class CommandLoggingTest extends UnifiedSyncTest {


    public CommandLoggingTest(@SuppressWarnings("unused") final String fileDescription,
                              @SuppressWarnings("unused") final String testDescription,
                              final String schemaVersion,
                              @Nullable final BsonArray runOnRequirements, final BsonArray entities, final BsonArray initialData,
                              final BsonDocument definition) {
        super(schemaVersion, runOnRequirements, entities, initialData, definition);
        assumeFalse(isServerlessTest());
        // The driver has a hack where getLastError command is executed as part of the handshake in order to get a connectionId
        // even when the hello command response doesn't contain it.
        assumeFalse(fileDescription.equals("pre-42-server-connection-id"));
    }

    @Parameterized.Parameters(name = "{0}: {1}")
    public static Collection<Object[]> data() throws URISyntaxException, IOException {
        return getTestData("unified-test-format/command-logging");
    }
}
