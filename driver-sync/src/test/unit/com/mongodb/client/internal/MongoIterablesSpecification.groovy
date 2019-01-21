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
import com.mongodb.MongoInternalException
import com.mongodb.MongoNamespace
import com.mongodb.ReadConcern
import com.mongodb.WriteConcern
import com.mongodb.client.ClientSession
import com.mongodb.client.model.AggregationLevel
import com.mongodb.client.model.changestream.ChangeStreamLevel
import com.mongodb.operation.BatchCursor
import org.bson.BsonBoolean
import org.bson.BsonDocument
import org.bson.Document
import spock.lang.Specification

import java.util.function.Consumer

import static com.mongodb.CustomMatchers.isTheSameAs
import static com.mongodb.ReadPreference.secondary
import static spock.util.matcher.HamcrestSupport.expect

class MongoIterablesSpecification extends Specification {

    static namespace = new MongoNamespace('databaseName', 'collectionName')
    static codecRegistry = MongoClientSettings.getDefaultCodecRegistry()
    static readPreference = secondary()
    static readConcern = ReadConcern.MAJORITY
    static writeConcern = WriteConcern.MAJORITY
    static filter = new BsonDocument('x', BsonBoolean.TRUE)
    static pipeline = Collections.emptyList()

    def clientSession = Stub(ClientSession)
    def executor = new TestOperationExecutor([])

    def 'should create iterables'() {
        when:
        def findIterable = MongoIterables.findOf(clientSession, namespace, Document, BsonDocument, codecRegistry, readPreference,
                readConcern, executor, filter)

        then:
        expect findIterable, isTheSameAs(new FindIterableImpl<Document, BsonDocument>(clientSession, namespace, Document,
                BsonDocument, codecRegistry, readPreference, readConcern, executor, filter))

        when:
        def aggregateIterable = MongoIterables.aggregateOf(clientSession, namespace, Document, BsonDocument, codecRegistry,
                readPreference, readConcern, writeConcern, executor, pipeline, AggregationLevel.COLLECTION)

        then:
        expect aggregateIterable, isTheSameAs(new AggregateIterableImpl<Document, BsonDocument>(clientSession, namespace,
                Document, BsonDocument, codecRegistry, readPreference, readConcern, writeConcern, executor, pipeline,
                AggregationLevel.COLLECTION))

        when:
        aggregateIterable = MongoIterables.aggregateOf(clientSession, namespace.databaseName, Document, BsonDocument, codecRegistry,
                readPreference, readConcern, writeConcern, executor, pipeline, AggregationLevel.DATABASE)

        then:
        expect aggregateIterable, isTheSameAs(new AggregateIterableImpl<Document, BsonDocument>(clientSession, namespace.databaseName,
                Document, BsonDocument, codecRegistry, readPreference, readConcern, writeConcern, executor, pipeline,
                AggregationLevel.DATABASE))

        when:
        def changeStreamIterable = MongoIterables.changeStreamOf(clientSession, namespace, codecRegistry,
                readPreference, readConcern, executor, pipeline, Document, ChangeStreamLevel.COLLECTION)

        then:
        expect changeStreamIterable, isTheSameAs(new ChangeStreamIterableImpl(clientSession, namespace, codecRegistry,
                readPreference, readConcern, executor, pipeline, Document, ChangeStreamLevel.COLLECTION), ['codec'])

        when:
        changeStreamIterable = MongoIterables.changeStreamOf(clientSession, namespace.databaseName, codecRegistry,
                readPreference, readConcern, executor, pipeline, Document, ChangeStreamLevel.COLLECTION)

        then:
        expect changeStreamIterable, isTheSameAs(new ChangeStreamIterableImpl(clientSession, namespace.databaseName, codecRegistry,
                readPreference, readConcern, executor, pipeline, Document, ChangeStreamLevel.COLLECTION), ['codec'])

        when:
        def distinctIterable = MongoIterables.distinctOf(clientSession, namespace, Document, BsonDocument, codecRegistry, readPreference,
                readConcern, executor, 'f1', filter)

        then:
        expect distinctIterable, isTheSameAs(new DistinctIterableImpl(clientSession, namespace, Document, BsonDocument, codecRegistry,
                readPreference, readConcern, executor, 'f1', filter))

        when:
        def listDatabasesIterable = MongoIterables.listDatabasesOf(clientSession, Document, codecRegistry, readPreference, executor)

        then:
        expect listDatabasesIterable, isTheSameAs(new ListDatabasesIterableImpl(clientSession, Document, codecRegistry, readPreference,
                executor))

        when:
        def listCollectionsIterable = MongoIterables.listCollectionsOf(clientSession, 'test', true, Document,
                codecRegistry, readPreference, executor)

        then:
        expect listCollectionsIterable, isTheSameAs(new ListCollectionsIterableImpl(clientSession, 'test', true, Document,
                codecRegistry, readPreference, executor))

        when:
        def listIndexesIterable = MongoIterables.listIndexesOf(clientSession, namespace, Document, codecRegistry, readPreference, executor)

        then:
        expect listIndexesIterable, isTheSameAs(new ListIndexesIterableImpl(clientSession, namespace, Document, codecRegistry,
                readPreference, executor))

        when:
        def mapReduceIterable = MongoIterables.mapReduceOf(clientSession, namespace, Document, BsonDocument, codecRegistry, readPreference,
                readConcern, writeConcern, executor, 'map', 'reduce')

        then:
        expect mapReduceIterable, isTheSameAs(new MapReduceIterableImpl(clientSession, namespace, Document, BsonDocument,
                codecRegistry, readPreference, readConcern, writeConcern, executor, 'map', 'reduce'))
    }

    def 'should properly override Iterable forEach of Consumer'() {
        given:
        def cannedResults = [new Document('_id', 1), new Document('_id', 2), new Document('_id', 3)]
        def isClosed = false
        def count = 0
        def accepted = 0

        def cursor = {
            Stub(BatchCursor) {
                def results;
                def getResult = {
                    count++
                    results = count == 1 ? cannedResults : null
                    results
                }
                next() >> {
                    getResult()
                }
                hasNext() >> {
                    count == 0
                }
                close() >> {
                    isClosed = true
                }
            }
        }

        when:
        count = 0
        accepted = 0
        def mongoIterable = mongoIterableFactory(new TestOperationExecutor([cursor(), cursor(), cursor(), cursor()]))
        mongoIterable.forEach(new Consumer<Document>() {
            @Override
            void accept(Document document) {
                accepted++
            }
        })

        then:
        accepted == 3
        isClosed

        when:
        count = 0
        accepted = 0
        mongoIterable = mongoIterableFactory(new TestOperationExecutor([cursor(), cursor(), cursor(), cursor()]))
        mongoIterable.forEach(new Consumer<Document>() {
            @Override
            void accept(Document document) {
                accepted++
                throw new MongoInternalException("I don't accept this")
            }
        })

        then:
        thrown(MongoInternalException)
        accepted == 1
        isClosed

        where:
        mongoIterableFactory << [
                { executor ->
                    new FindIterableImpl(null, namespace, Document, Document, codecRegistry,
                            readPreference, readConcern, executor, filter)
                },
                { executor ->
                    new AggregateIterableImpl(null, namespace, Document, Document, codecRegistry,
                            readPreference, readConcern, writeConcern, executor, pipeline, AggregationLevel.COLLECTION)
                },
                { executor ->
                    new ChangeStreamIterableImpl(null, namespace, codecRegistry, readPreference, readConcern, executor, pipeline,
                            Document, ChangeStreamLevel.COLLECTION)
                },
                { executor ->
                    new DistinctIterableImpl(null, namespace, Document, Document, codecRegistry, readPreference, readConcern,
                            executor, 'f1', filter)
                },
                { executor ->
                    new ListDatabasesIterableImpl(null, Document, codecRegistry, readPreference,
                            executor)
                },
                { executor ->
                    new ListCollectionsIterableImpl(null, 'test', true, Document,
                            codecRegistry, readPreference, executor)
                },
                { executor ->
                    new ListIndexesIterableImpl(null, namespace, Document, codecRegistry,
                            readPreference, executor)
                },
                { executor ->
                    new MapReduceIterableImpl(null, namespace, Document, BsonDocument,
                            codecRegistry, readPreference, readConcern, writeConcern, executor, 'map', 'reduce')
                }
        ]
    }
}
