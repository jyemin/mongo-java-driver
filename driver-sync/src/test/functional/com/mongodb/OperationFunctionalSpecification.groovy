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

package com.mongodb

import com.mongodb.async.FutureResultCallback
import com.mongodb.client.test.CollectionHelper
import org.bson.BsonDocument
import org.bson.Document
import org.bson.codecs.DocumentCodec
import spock.lang.Specification

import java.util.concurrent.TimeUnit

import static com.mongodb.client.Fixture.TIMEOUT

class OperationFunctionalSpecification extends Specification {

    def setup() {
        setupInternal()
    }

    protected void setupInternal() {
        ServerHelper.checkPool(getPrimary())
        CollectionHelper.drop(getNamespace())
    }

    void cleanup() {
        cleanupInternal()
    }

    protected void cleanupInternal() {
        CollectionHelper.drop(getNamespace())
    }

    String getDatabaseName() {
        Fixture.getDefaultDatabaseName()
    }

    String getCollectionName() {
        getClass().getName()
    }

    MongoNamespace getNamespace() {
        new MongoNamespace(getDatabaseName(), getCollectionName())
    }


    CollectionHelper<Document> getCollectionHelper() {
        getCollectionHelper(getNamespace())
    }

    CollectionHelper<Document> getCollectionHelper(MongoNamespace namespace) {
        new CollectionHelper<Document>(new DocumentCodec(), namespace)
    }

    def next(cursor, boolean async, int minimumCount) {
        next(cursor, async, false, minimumCount)
    }

    def next(cursor, boolean async, boolean callHasNextBeforeNext, int minimumCount) {
        List<BsonDocument> retVal = []

        while (retVal.size() < minimumCount) {
            retVal.addAll(doNext(cursor, async, callHasNextBeforeNext))
        }

        retVal
    }

    def next(cursor, boolean async) {
        doNext(cursor, async, false)
    }

    def doNext(cursor, boolean async, boolean callHasNextBeforeNext) {
        if (async) {
            def futureResultCallback = new FutureResultCallback<List<BsonDocument>>()
            cursor.next(futureResultCallback)
            futureResultCallback.get(TIMEOUT, TimeUnit.SECONDS)
        } else {
            if (callHasNextBeforeNext) {
                cursor.hasNext()
            }
            cursor.next()
        }
    }
}
