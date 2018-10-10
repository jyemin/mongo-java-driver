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
import com.mongodb.client.ClientSession;
import com.mongodb.client.FindIterable;
import com.mongodb.lang.Nullable;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.List;

class Java8MongoIterableFactory implements MongoIterableFactory {
    @Override
    public <TDocument, TResult> FindIterable<TResult> findOf(final @Nullable ClientSession clientSession, final MongoNamespace namespace,
                                                             final Class<TDocument> documentClass, final Class<TResult> resultClass,
                                                             final CodecRegistry codecRegistry, final ReadPreference readPreference,
                                                             final ReadConcern readConcern, final OperationExecutor executor,
                                                             final Bson filter) {
        return new Java8FindIterableImpl<TDocument, TResult>(clientSession, namespace, documentClass, resultClass, codecRegistry,
                readPreference, readConcern, executor, filter);
    }

    @Override
    public <TResult, TDocument>
    AggregateIterable<TResult> aggregateOf(final @Nullable ClientSession clientSession, final MongoNamespace namespace,
                                           final Class<TDocument> documentClass, final Class<TResult> resultClass,
                                           final CodecRegistry codecRegistry, final ReadPreference readPreference,
                                           final ReadConcern readConcern, final WriteConcern writeConcern,
                                           final OperationExecutor executor, final List<? extends Bson> pipeline) {
        return new Java8AggregateIterableImpl<TDocument, TResult>(clientSession, namespace, documentClass, resultClass, codecRegistry,
                readPreference, readConcern, writeConcern, executor, pipeline);
    }
}

