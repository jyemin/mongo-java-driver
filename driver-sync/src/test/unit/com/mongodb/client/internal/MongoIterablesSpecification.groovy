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

package com.mongodb.client.internal

import com.mongodb.MongoClientSettings
import com.mongodb.MongoNamespace
import com.mongodb.ReadConcern
import com.mongodb.WriteConcern
import com.mongodb.client.ClientSession
import org.bson.BsonBoolean
import org.bson.BsonDocument
import org.bson.Document
import spock.lang.IgnoreIf
import spock.lang.Specification

import static com.mongodb.CustomMatchers.isTheSameAs
import static com.mongodb.ReadPreference.secondary
import static spock.util.matcher.HamcrestSupport.expect

class MongoIterablesSpecification extends Specification {

    def executor = new TestOperationExecutor([])
    def clientSession = Stub(ClientSession)
    def namespace = new MongoNamespace('databaseName', 'collectionName')
    def codecRegistry = MongoClientSettings.getDefaultCodecRegistry()
    def readPreference = secondary()
    def readConcern = ReadConcern.MAJORITY
    def writeConcern = WriteConcern.MAJORITY
    def filter = new BsonDocument('x', BsonBoolean.TRUE)
    def pipeline = Collections.emptyList()

    @IgnoreIf({ !Java8MongoIterablesSpecification.IS_CONSUMER_CLASS_AVAILABLE })
    def 'should create Java 8 iterables when java.util.function.Consumer is available'() {
        when:
        def findIterable = MongoIterables.findOf(clientSession, namespace, Document, BsonDocument, codecRegistry, readPreference,
                readConcern, executor, filter)

        then:
        expect findIterable, isTheSameAs(new Java8FindIterableImpl<Document, BsonDocument>(clientSession, namespace, Document,
                BsonDocument, codecRegistry, readPreference, readConcern, executor, filter))

        when:
        def aggregateIterable = MongoIterables.aggregateOf(clientSession, namespace, Document, BsonDocument, codecRegistry, readPreference,
                readConcern, writeConcern, executor, pipeline)

        then:
        expect aggregateIterable, isTheSameAs(new Java8AggregateIterableImpl<Document, BsonDocument>(clientSession, namespace, Document,
                BsonDocument, codecRegistry, readPreference, readConcern, writeConcern, executor, pipeline))
    }

    @IgnoreIf({ Java8MongoIterablesSpecification.IS_CONSUMER_CLASS_AVAILABLE })
    def 'should create non-Java 8 iterables when java.util.function.Consumer is unavailable'() {
        when:
        def findIterable = MongoIterables.findOf(clientSession, namespace, Document, BsonDocument, codecRegistry, readPreference,
                readConcern, executor, filter)

        then:
        expect findIterable, isTheSameAs(new FindIterableImpl<Document, BsonDocument>(clientSession, namespace, Document,
                BsonDocument, codecRegistry, readPreference, readConcern, executor, filter))

        when:
        def aggregateIterable = MongoIterables.aggregateOf(clientSession, namespace, Document, BsonDocument, codecRegistry, readPreference,
                readConcern, writeConcern, executor, pipeline)

        then:
        expect aggregateIterable, isTheSameAs(new AggregateIterableImpl<Document, BsonDocument>(clientSession, namespace, Document,
                BsonDocument, codecRegistry, readPreference, readConcern, writeConcern, executor, pipeline))
    }
}
