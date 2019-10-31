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

package com.mongodb.internal.async.client;

import org.bson.Document;
import org.junit.After;
import org.junit.Before;

import static com.mongodb.internal.async.client.Fixture.drop;
import static com.mongodb.internal.async.client.Fixture.getDefaultDatabaseName;
import static com.mongodb.internal.async.client.Fixture.getMongoClient;
import static com.mongodb.internal.async.client.Fixture.waitForLastServerSessionPoolRelease;

public class DatabaseTestCase {
    //For ease of use and readability, in this specific case we'll allow protected variables
    //CHECKSTYLE:OFF
    protected AsyncMongoClient client;
    protected AsyncMongoDatabase database;
    protected AsyncMongoCollection<Document> collection;
    //CHECKSTYLE:ON

    @Before
    public void setUp() {
        client =  getMongoClient();
        database = client.getDatabase(getDefaultDatabaseName());
        collection = database.getCollection(getClass().getName());
        drop(collection.getNamespace());
    }

    @After
    public void tearDown() {
        if (collection != null) {
            drop(collection.getNamespace());
        }

        waitForLastServerSessionPoolRelease();
    }

}
