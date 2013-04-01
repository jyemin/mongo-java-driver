/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

package org.mongodb;

import org.mongodb.annotations.ThreadSafe;
import org.mongodb.command.MongoCommand;
import org.mongodb.result.CommandResult;
import org.mongodb.serialization.CollectibleSerializer;

/**
 * Additions to this interface will not be considered to break binary compatibility.
 */
@ThreadSafe
public interface MongoDatabase {
    String getName();

    CommandResult executeCommand(MongoCommand commandOperation);

//    MongoClient getClient();

    MongoDatabaseOptions getOptions();

    MongoCollection<Document> getCollection(String name);

    MongoCollection<Document> getCollection(String name, MongoCollectionOptions options);

    <T> MongoCollection<T> getCollection(String name, CollectibleSerializer<T> serializer);

    <T> MongoCollection<T> getCollection(String name, CollectibleSerializer<T> serializer, MongoCollectionOptions options);

    //TODO: still need to come up with a sensible name for this
    DatabaseAdmin tools();

    //    MongoDatabase withClient(MongoClient client);
    //
    //    MongoDatabase withWriteConcern(WriteConcern writeConcern);
}
