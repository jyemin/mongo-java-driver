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

package com.mongodb.client.internal;

import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.ClientSession;
import com.mongodb.client.FindIterable;
import com.mongodb.client.model.changestream.ChangeStreamLevel;
import com.mongodb.lang.Nullable;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.List;

interface MongoIterableFactory {
    <TDocument, TResult>
    FindIterable<TResult> findOf(@Nullable ClientSession clientSession, MongoNamespace namespace, Class<TDocument> documentClass,
                                 Class<TResult> resultClass, CodecRegistry codecRegistry, ReadPreference readPreference,
                                 ReadConcern readConcern, OperationExecutor executor, Bson filter);

    <TResult, TDocument>
    AggregateIterable<TResult> aggregateOf(@Nullable ClientSession clientSession, MongoNamespace namespace, Class<TDocument> documentClass,
                                           Class<TResult> resultClass, CodecRegistry codecRegistry, ReadPreference readPreference,
                                           ReadConcern readConcern, WriteConcern writeConcern, OperationExecutor executor,
                                           List<? extends Bson> pipeline);

    <TResult>
    ChangeStreamIterable<TResult> changeStreamOf(@Nullable ClientSession clientSession, String databaseName, CodecRegistry codecRegistry,
                                                 ReadPreference readPreference, ReadConcern readConcern, OperationExecutor executor,
                                                 List<? extends Bson> pipeline, Class<TResult> resultClass,
                                                 ChangeStreamLevel changeStreamLevel);

    <TResult>
    ChangeStreamIterable<TResult> changeStreamOf(@Nullable ClientSession clientSession, MongoNamespace namespace,
                                                 CodecRegistry codecRegistry, ReadPreference readPreference, ReadConcern readConcern,
                                                 OperationExecutor executor, List<? extends Bson> pipeline, Class<TResult> resultClass,
                                                 ChangeStreamLevel changeStreamLevel);
}
