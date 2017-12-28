/*
 * Copyright 2017 MongoDB, Inc.
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

package com.mongodb.connection;

import org.bson.BsonDocument;
import org.bson.BsonValue;

// Lazily determine the command document and command name, since they're only needed if either a command listener or compression
// is enabled
final class LazyCommandDocument {
    private final CommandMessage commandMessage;
    private final ByteBufferBsonOutput bsonOutput;
    private BsonDocument commandDocument;
    private String commandName;
    private BsonValue firstValue;

    LazyCommandDocument(final CommandMessage commandMessage, final ByteBufferBsonOutput bsonOutput) {
        this.commandMessage = commandMessage;
        this.bsonOutput = bsonOutput;
    }

    String getName() {
        if (commandName == null) {
            commandName = getDocument().getFirstKey();
        }
        return commandName;
    }

    BsonValue getFirstValue() {
        if (firstValue == null) {
            firstValue = getDocument().getFirstValue();
        }
        return firstValue;
    }

    BsonDocument getDocument() {
        if (commandDocument == null) {
            commandDocument = commandMessage.getCommandDocument(bsonOutput);
        }
        return commandDocument;
    }
}
