/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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
 *
 */

package org.mongodb.async;

import org.mongodb.Document;
import org.mongodb.MongoNamespace;
import org.mongodb.operation.MongoGetMore;
import org.mongodb.command.MongoCommand;
import org.mongodb.operation.MongoFind;
import org.mongodb.operation.MongoInsert;
import org.mongodb.operation.MongoRemove;
import org.mongodb.operation.MongoReplace;
import org.mongodb.operation.MongoUpdate;
import org.mongodb.result.CommandResult;
import org.mongodb.result.QueryResult;
import org.mongodb.result.WriteResult;
import org.mongodb.serialization.Serializer;

import java.util.concurrent.Future;

public interface MongoAsyncConnector {
    Future<CommandResult> asyncCommand(String database, MongoCommand commandOperation, Serializer<Document> serializer);

    void asyncCommand(String database, MongoCommand commandOperation, Serializer<Document> serializer,
                      SingleResultCallback<CommandResult> callback);

    <T> Future<QueryResult<T>> asyncQuery(final MongoNamespace namespace, MongoFind find, Serializer<Document> querySerializer,
                                          Serializer<T> resultSerializer);

    <T> void asyncQuery(final MongoNamespace namespace, MongoFind find, Serializer<Document> querySerializer,
                        Serializer<T> resultSerializer, SingleResultCallback<QueryResult<T>> callback);

    <T> Future<QueryResult<T>> asyncGetMore(final MongoNamespace namespace, MongoGetMore mongoGetMore, Serializer<T> resultSerializer);

    <T> void asyncGetMore(final MongoNamespace namespace, MongoGetMore mongoGetMore, Serializer<T> resultSerializer,
                          SingleResultCallback<QueryResult<T>> callback);

    <T> Future<WriteResult> asyncInsert(MongoNamespace namespace, MongoInsert<T> insert, Serializer<T> serializer);

    <T> void asyncInsert(MongoNamespace namespace, MongoInsert<T> insert, Serializer<T> serializer,
                         SingleResultCallback<WriteResult> callback);


    Future<WriteResult> asyncUpdate(final MongoNamespace namespace, MongoUpdate update, Serializer<Document> querySerializer);

    void asyncUpdate(final MongoNamespace namespace, MongoUpdate update, Serializer<Document> serializer,
                     SingleResultCallback<WriteResult> callback);


    <T> Future<WriteResult> asyncReplace(MongoNamespace namespace, MongoReplace<T> replace, Serializer<Document> querySerializer,
                                         Serializer<T> serializer);

    <T> void asyncReplace(MongoNamespace namespace, MongoReplace<T> replace, Serializer<Document> querySerializer,
                          Serializer<T> serializer, SingleResultCallback<WriteResult> callback);


    Future<WriteResult> asyncRemove(final MongoNamespace namespace, MongoRemove remove, Serializer<Document> querySerializer);

    void asyncRemove(final MongoNamespace namespace, MongoRemove remove, Serializer<Document> querySerializer,
                     SingleResultCallback<WriteResult> callback);

}
