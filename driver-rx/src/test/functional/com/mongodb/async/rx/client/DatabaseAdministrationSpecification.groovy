/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package com.mongodb.async.rx.client

import org.bson.types.Document
import spock.lang.IgnoreIf

import static Fixture.get
import static Fixture.getAsList
import static Fixture.getMongoClient
import static com.mongodb.async.client.Fixture.dropDatabase
import static com.mongodb.async.client.Fixture.isSharded

@IgnoreIf({ isSharded() })
class DatabaseAdministrationSpecification extends FunctionalSpecification {

    def 'drop should drop the database'() {
        given:
        def client = getMongoClient()
        def databaseToDrop = 'RxDatabaseAdministrationSpecificationDatabase'
        def database = client.getDatabase(databaseToDrop)
        get(database.getCollection('DatabaseAdministrationSpecificationCollection').insert(new Document()))

        when:
        get(database.tools().drop())

        then:
        !getAsList(client.tools().getDatabaseNames()).contains(databaseToDrop)

        cleanup:
        dropDatabase(databaseToDrop)
    }

}
