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

package com.mongodb.client

import com.mongodb.MongoClientSettings
import com.mongodb.MongoNamespace
import com.mongodb.ReadConcern
import com.mongodb.ServerAddress
import com.mongodb.WriteConcern
import com.mongodb.client.internal.MongoClientImpl
import com.mongodb.client.internal.MongoDatabaseImpl
import com.mongodb.client.internal.MongoIterables
import com.mongodb.client.internal.TestOperationExecutor
import com.mongodb.connection.Cluster
import com.mongodb.connection.ClusterConnectionMode
import com.mongodb.connection.ClusterDescription
import com.mongodb.connection.ClusterType
import com.mongodb.connection.ServerConnectionState
import com.mongodb.connection.ServerDescription
import com.mongodb.connection.ServerType
import com.mongodb.connection.ServerVersion
import com.mongodb.internal.client.model.changestream.ChangeStreamLevel
import org.bson.BsonDocument
import org.bson.Document
import spock.lang.Specification

import static com.mongodb.CustomMatchers.isTheSameAs
import static com.mongodb.MongoClientSettings.getDefaultCodecRegistry
import static com.mongodb.ReadPreference.primary
import static com.mongodb.ReadPreference.secondary
import static com.mongodb.client.internal.TestHelper.execute
import static spock.util.matcher.HamcrestSupport.expect

class MongoClientSpecification extends Specification {

    def 'should pass the correct settings to getDatabase'() {
        given:
        def settings = MongoClientSettings.builder()
                .readPreference(secondary())
                .writeConcern(WriteConcern.MAJORITY)
                .readConcern(ReadConcern.MAJORITY)
                .retryWrites(true)
                .build()
        def client = new MongoClientImpl(Stub(Cluster), settings, new TestOperationExecutor([]))

        when:
        def database = client.getDatabase('name')

        then:
        expect database, isTheSameAs(expectedDatabase)

        where:
        expectedDatabase << new MongoDatabaseImpl('name', getDefaultCodecRegistry(), secondary(),
                WriteConcern.MAJORITY, true, true, ReadConcern.MAJORITY, new TestOperationExecutor([]))
    }

    def 'should use ListDatabasesIterableImpl correctly'() {
        given:
        def executor = new TestOperationExecutor([null, null])
        def client = new MongoClientImpl(Stub(Cluster), MongoClientSettings.builder().build(), executor)
        def listDatabasesMethod = client.&listDatabases
        def listDatabasesNamesMethod = client.&listDatabaseNames

        when:
        def listDatabasesIterable = execute(listDatabasesMethod, session)

        then:
        expect listDatabasesIterable, isTheSameAs(MongoIterables.listDatabasesOf(session, Document, getDefaultCodecRegistry(),
                primary(), executor, true))

        when:
        listDatabasesIterable = execute(listDatabasesMethod, session, BsonDocument)

        then:
        expect listDatabasesIterable, isTheSameAs(MongoIterables.listDatabasesOf(session, BsonDocument,
                getDefaultCodecRegistry(), primary(), executor, true))

        when:
        def listDatabaseNamesIterable = execute(listDatabasesNamesMethod, session) as MongoIterable<String>

        then:
        // listDatabaseNamesIterable is an instance of a MappingIterable, so have to get the mapped iterable inside it
        expect listDatabaseNamesIterable.getMapped(), isTheSameAs(MongoIterables.listDatabasesOf(session, BsonDocument,
                getDefaultCodecRegistry(), primary(), executor, true).nameOnly(true))

        cleanup:
        client?.close()

        where:
        session << [null, Stub(ClientSession)]
    }

    def 'should create ChangeStreamIterable correctly'() {
        given:
        def executor = new TestOperationExecutor([])
        def namespace = new MongoNamespace('admin', 'ignored')
        def settings = MongoClientSettings.builder()
                .readPreference(secondary())
                .readConcern(ReadConcern.MAJORITY)
                .codecRegistry(getDefaultCodecRegistry())
                .build()
        def codecRegistry = settings.getCodecRegistry()
        def readPreference = settings.getReadPreference()
        def readConcern = settings.getReadConcern()
        def client = new MongoClientImpl(Stub(Cluster), settings, executor)
        def watchMethod = client.&watch

        when:
        def changeStreamIterable = execute(watchMethod, session)

        then:
        expect changeStreamIterable, isTheSameAs(MongoIterables.changeStreamOf(session, namespace, codecRegistry, readPreference,
                readConcern, executor, [], Document, ChangeStreamLevel.CLIENT, true), ['codec'])

        when:
        changeStreamIterable = execute(watchMethod, session, [new Document('$match', 1)])

        then:
        expect changeStreamIterable, isTheSameAs(MongoIterables.changeStreamOf(session, namespace, codecRegistry, readPreference,
                readConcern, executor, [new Document('$match', 1)], Document, ChangeStreamLevel.CLIENT, true), ['codec'])

        when:
        changeStreamIterable = execute(watchMethod, session, [new Document('$match', 1)], BsonDocument)

        then:
        expect changeStreamIterable, isTheSameAs(MongoIterables.changeStreamOf(session, namespace, codecRegistry, readPreference,
                readConcern, executor, [new Document('$match', 1)], BsonDocument, ChangeStreamLevel.CLIENT, true), ['codec'])

        where:
        session << [null, Stub(ClientSession)]
    }

    def 'should validate the ChangeStreamIterable pipeline data correctly'() {
        given:
        def executor = new TestOperationExecutor([])
        def client = new MongoClientImpl(Stub(Cluster), MongoClientSettings.builder().build(), executor)

        when:
        client.watch((Class) null)

        then:
        thrown(IllegalArgumentException)

        when:
        client.watch([null]).into([])

        then:
        thrown(IllegalArgumentException)
    }

    def 'should get the cluster description'() {
        given:
        def clusterDescription = new ClusterDescription(ClusterConnectionMode.SINGLE, ClusterType.STANDALONE,
                [ServerDescription.builder()
                         .address(new ServerAddress())
                         .type(ServerType.UNKNOWN)
                         .state(ServerConnectionState.CONNECTING)
                         .version(new ServerVersion())
                         .build()])
        def cluster = Mock(Cluster) {
            1 * getCurrentDescription() >> {
                clusterDescription
            }
        }
        def settings = MongoClientSettings.builder().build()
        def client = new MongoClientImpl(cluster, settings, new TestOperationExecutor([]))

        expect:
        client.getClusterDescription() == clusterDescription
    }
}
