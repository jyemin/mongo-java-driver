/*
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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

import org.bson.types.Document;
import org.mongodb.operation.MongoCommandOperation;
import org.mongodb.result.CommandResult;
import org.mongodb.serialization.PrimitiveSerializers;
import org.mongodb.serialization.Serializer;

/**
 * Additions to this interface will not be considered to break binary compatibility.
 */
public interface MongoDatabase {
    String getName();

    CommandResult executeCommand(MongoCommandOperation commandOperation);

    MongoClient getClient();

    WriteConcern getWriteConcern();

    ReadPreference getReadPreference();

    PrimitiveSerializers getPrimitiveSerializers();

    MongoCollection<Document> getCollection(String name);

    <T> MongoCollection<T> getTypedCollection(String name, final PrimitiveSerializers basePrimitiveSerializers,
                                              final Serializer<T> serializer);

    MongoAsyncCollection<Document> getAsyncCollection(String name);

    <T> MongoAsyncCollection<T> getAsyncTypedCollection(String name,
                                                        final PrimitiveSerializers basePrimitiveSerializers,
                                                        final Serializer<T> serializer);

    DatabaseAdmin admin();

    //    MongoDatabase withClient(MongoClient client);
    //
    //    MongoDatabase withWriteConcern(WriteConcern writeConcern);
}
