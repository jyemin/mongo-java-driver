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

// Helper class for instantiating the right implementation class for each MongoIterable.  In proper Java 8 environments, which includes
// the java.util.function.Consumer interface, it instantiates an implementation which overrides the default forEach(Consumer<TResult)
// method that in Java 8 is defined as a default method on java.lang.Iterable.  Otherwise it instantiates an implementation which does not
// override this method.
// It does this by delegating to an implementation of MongoIterableFactory that is determined based on a runtime check for the existence of
// the java.util.function.Consumer class.
public final class MongoIterables {
    private static MongoIterableFactory factory;

    static {
        try {
            // Protect against running in an environment where Consumer is not available.  Either
            // 1. Java version < 8
            // 2. Android (which doesn't currently include Consumer even with its Java 8 support
            Class.forName("java.util.function.Consumer");
            factory = new Java8MongoIterableFactory();
        } catch (ClassNotFoundException e) {
            factory = new FallbackMongoIterableFactory();
        }
    }

    public static <TDocument, TResult>
    FindIterable<TResult> findOf(final @Nullable ClientSession clientSession, final MongoNamespace namespace,
                                 final Class<TDocument> documentClass, final Class<TResult> resultClass,
                                 final CodecRegistry codecRegistry,
                                 final ReadPreference readPreference, final ReadConcern readConcern,
                                 final OperationExecutor executor, final Bson filter) {
        return factory.findOf(clientSession, namespace, documentClass, resultClass, codecRegistry, readPreference, readConcern, executor,
                filter);
    }

    public static <TDocument, TResult>
    AggregateIterable<TResult> aggregateOf(final @Nullable ClientSession clientSession, final MongoNamespace namespace,
                                           final Class<TDocument> documentClass, final Class<TResult> resultClass,
                                           final CodecRegistry codecRegistry, final ReadPreference readPreference,
                                           final ReadConcern readConcern, final WriteConcern writeConcern, final OperationExecutor executor,
                                           final List<? extends Bson> pipeline) {
        return factory.aggregateOf(clientSession, namespace, documentClass, resultClass, codecRegistry,
                readPreference, readConcern, writeConcern, executor, pipeline);
    }

    public static <TResult>
    ChangeStreamIterable<TResult> changeStreamOf(final @Nullable ClientSession clientSession, final String databaseName,
                                                 final CodecRegistry codecRegistry, final ReadPreference readPreference,
                                                 final ReadConcern readConcern, final OperationExecutor executor,
                                                 final List<? extends Bson> pipeline, final Class<TResult> resultClass,
                                                 final ChangeStreamLevel changeStreamLevel) {
        return factory.changeStreamOf(clientSession, databaseName, codecRegistry, readPreference, readConcern, executor, pipeline,
                resultClass, changeStreamLevel);
    }

    public static <TResult>
    ChangeStreamIterable<TResult> changeStreamOf(final @Nullable ClientSession clientSession, final MongoNamespace namespace,
                                                 final CodecRegistry codecRegistry, final ReadPreference readPreference,
                                                 final ReadConcern readConcern, final OperationExecutor executor,
                                                 final List<? extends Bson> pipeline, final Class<TResult> resultClass,
                                                 final ChangeStreamLevel changeStreamLevel) {
        return factory.changeStreamOf(clientSession, namespace, codecRegistry, readPreference, readConcern, executor, pipeline, resultClass,
                changeStreamLevel);
    }

    private MongoIterables() {
    }
}
