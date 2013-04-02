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
 */

package org.mongodb.command;

import org.mongodb.Document;
import org.mongodb.operation.MongoFindAndUpdate;

import static org.mongodb.command.CommandDocumentTemplates.getFindAndModify;

public final class FindAndUpdate<T> extends MongoCommand {

    public FindAndUpdate(final MongoFindAndUpdate<T> findAndUpdate, final String collectionName) {
        super(asDocument(findAndUpdate, collectionName));
    }

    private static <T> Document asDocument(final MongoFindAndUpdate<T> findAndUpdate,
                                           final String collectionName) {
        final Document cmd = getFindAndModify(findAndUpdate, collectionName);
        cmd.put("update", findAndUpdate.getUpdateOperations());
        return cmd;
    }
}
