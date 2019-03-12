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

package com.mongodb.client.internal;

import com.mongodb.MongoClientException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.client.MongoClient;
import com.mongodb.internal.connection.ElementExtendingBsonWriter;
import org.bson.BsonBinaryReader;
import org.bson.BsonBinaryWriter;
import org.bson.BsonDocument;
import org.bson.BsonElement;
import org.bson.RawBsonDocument;
import org.bson.io.BasicOutputBuffer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;

@SuppressWarnings("UseOfProcessBuilder")
class CommandMarker {
    private MongoClient client;
    private final ProcessBuilder processBuilder;
    private boolean active;

    CommandMarker(final MongoClient client, final Map<String, Object> options) {
        this.client = client;
        this.active = false;


        // TODO: this isn't in the spec yet
        if (!options.containsKey("spawn") || ((Boolean) options.get("spawn"))) {
            List<String> spawnArgs = createSpawnArgs(options);
            processBuilder = new ProcessBuilder(spawnArgs);

        } else {
            processBuilder = null;
        }
    }

    @SuppressWarnings("unchecked")
    private List<String> createSpawnArgs(final Map<String, Object> options) {
        List<String> spawnArgs = new ArrayList<String>();

        String path = options.containsKey("mongocryptdPath")
                ? (String) options.get("mongocryptdPath")
                : "mongocryptd";

        spawnArgs.add(path);
        if (options.containsKey("mongocryptdSpawnArgs")) {
            spawnArgs.addAll((List<String>) options.get("mongocryptdSpawnArgs"));
        }

        if (!spawnArgs.contains("--idleShutdownTimeoutSecs")) {
            spawnArgs.add("--idleShutdownTimeoutSecs");
            spawnArgs.add("60");
        }
        return spawnArgs;
    }

    public BsonDocument mark(final String databaseName, final BsonDocument schema, final RawBsonDocument command) {
        BasicOutputBuffer buffer = new BasicOutputBuffer(command.getByteBuffer().remaining());

        ElementExtendingBsonWriter elementExtendingBsonWriter = new ElementExtendingBsonWriter(
                new BsonBinaryWriter(buffer), singletonList(new BsonElement("jsonSchema", schema)));

        BsonBinaryReader bsonBinaryReader = new BsonBinaryReader(command.getByteBuffer().asNIO());

        elementExtendingBsonWriter.pipe(bsonBinaryReader);

        RawBsonDocument markableCommand = new RawBsonDocument(buffer.getInternalBuffer(), 0, buffer.getSize());

        if (processBuilder != null) {
            synchronized (this) {
                if (!active) {
                    spawn();
                    active = true;
                }
            }
        }

        try {
            return executeCommand(databaseName, markableCommand);
        } catch (MongoTimeoutException e) {
            spawn();
            return executeCommand(databaseName, markableCommand);
        }
    }

    private BsonDocument executeCommand(final String databaseName, final RawBsonDocument markableCommand) {
        return client.getDatabase(databaseName)
                .withReadConcern(ReadConcern.DEFAULT)
                .withReadPreference(ReadPreference.primary())
                .runCommand(markableCommand, BsonDocument.class);
    }

    private synchronized void spawn() {
        try {
            processBuilder.start();
        } catch (IOException e) {
            throw new MongoClientException("Exception starting mongocryptd process", e);
        }
    }
}
