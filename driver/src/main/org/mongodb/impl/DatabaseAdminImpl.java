/*
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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

package org.mongodb.impl;

import org.bson.types.Document;
import org.mongodb.CommandDocument;
import org.mongodb.DatabaseAdmin;
import org.mongodb.MongoNamespace;
import org.mongodb.MongoOperations;
import org.mongodb.QueryFilterDocument;
import org.mongodb.operation.MongoCommandOperation;
import org.mongodb.operation.MongoFind;
import org.mongodb.result.CommandResult;
import org.mongodb.result.QueryResult;
import org.mongodb.serialization.PrimitiveSerializers;
import org.mongodb.serialization.serializers.DocumentSerializer;

import java.util.HashSet;
import java.util.Set;

/**
 * Runs the admin commands for a selected database.  This should be accessed from MongoDatabase.  The methods here are
 * not implemented in MongoDatabase in order to keep the API very simple, these should be the methods that are
 * not commonly used by clients of the driver.
 */
public class DatabaseAdminImpl implements DatabaseAdmin {
    private static final DropDatabase DROP_DATABASE = new DropDatabase();
    private static final MongoFind FIND_ALL = new MongoFind(new QueryFilterDocument());

    private final MongoOperations operations;
    private final String databaseName;
    private final DocumentSerializer documentSerializer;

    public DatabaseAdminImpl(final String databaseName, final MongoOperations operations,
                             final PrimitiveSerializers primitiveSerializers) {
        this.operations = operations;
        this.databaseName = databaseName;
        documentSerializer = new DocumentSerializer(primitiveSerializers);
    }

    @Override
    public void drop() {
        new CommandResult(operations.executeCommand(databaseName, DROP_DATABASE, documentSerializer));
    }

    @Override
    public Set<String> getCollectionNames() {
        final MongoNamespace namespacesCollection = new MongoNamespace(databaseName, "system.namespaces");
        final QueryResult<Document> query = operations.query(namespacesCollection, FIND_ALL,
                                                             documentSerializer, documentSerializer);

        final HashSet<String> collections = new HashSet<String>();
        final int lengthOfDatabaseName = databaseName.length();
        for (final Document namespace : query.getResults()) {
            final String collectionName = (String) namespace.get("name");
            if (!collectionName.contains("$")) {
                final String collectionNameWithoutDatabasePrefix = collectionName.substring(lengthOfDatabaseName + 1);
                collections.add(collectionNameWithoutDatabasePrefix);
            }
        }
        return collections;
    }

    private static final class DropDatabase extends MongoCommandOperation {
        private DropDatabase() {
            super(new CommandDocument("dropDatabase", 1));
        }
    }
}
