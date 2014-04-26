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

package org.mongodb.operation

import category.Async
import org.bson.types.Code
import org.junit.experimental.categories.Category
import org.mongodb.Block
import org.mongodb.Document
import org.mongodb.FunctionalSpecification
import org.mongodb.MapReduceCursor
import org.mongodb.codecs.DocumentCodec
import org.mongodb.test.CollectionHelper

import static org.mongodb.Fixture.getAsyncBinding
import static org.mongodb.Fixture.getBinding

class MapReduceWithInlineResultsOperationFunctionalSpecification extends FunctionalSpecification {
    private final documentCodec = new DocumentCodec()
    def mapReduce = new MapReduce(new Code('function(){ emit( this.name , 1 ); }'),
                                  new Code('function(key, values){ return values.length; }'))
    def expectedResults = [['_id': 'Pete', 'value': 2.0] as Document,
                           ['_id': 'Sam', 'value': 1.0] as Document]

    def setup() {
        CollectionHelper<Document> helper = new CollectionHelper<Document>(documentCodec, getNamespace())
        Document pete = new Document('name', 'Pete').append('job', 'handyman')
        Document sam = new Document('name', 'Sam').append('job', 'plumber')
        Document pete2 = new Document('name', 'Pete').append('job', 'electrician')
        helper.insertDocuments(pete, sam, pete2)
    }


    def 'should return the correct results'() {
        given:
        def operation = new MapReduceWithInlineResultsOperation(namespace, mapReduce, documentCodec)

        when:
        MapReduceCursor<Document> results = operation.execute(getBinding())

        then:
        results.iterator().toList() == expectedResults
    }

    @Category(Async)
    def 'should return the correct results asynchronously'() {
        given:
        def operation = new MapReduceWithInlineResultsOperation(namespace, mapReduce, documentCodec)

        when:
        List<Document> docList = []
        operation.executeAsync(getAsyncBinding()).get().forEach(new Block<Document>() {
            @Override
            void apply(final Document value) {
                if (value != null) {
                    docList += value
                }
            }
        }).get()

        then:
        docList.iterator().toList() == expectedResults
    }

}
