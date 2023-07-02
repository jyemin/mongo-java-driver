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

package com.mongodb;

import java.lang.ref.Cleaner;

import static com.mongodb.assertions.Assertions.assertNotNull;

/**
 * A cleaner for abandoned {@link DBCursor} instances.
 *
 * @see DBCursor
 */
class DBCursorCleaner {

    private static final Cleaner CLEANER = Cleaner.create();

    private final CleanerState cleanerState;
    private final Cleaner.Cleanable cleanable;

    DBCursorCleaner(final MongoClient mongoClient, final MongoNamespace namespace, final ServerCursor serverCursor) {
        cleanerState = new CleanerState(mongoClient, namespace, serverCursor);
        cleanable = CLEANER.register(this, cleanerState);
    }

    void clearCursor() {
        cleanerState.clear();
        cleanable.clean();
    }

    protected static class CleanerState implements Runnable {
        private final MongoClient mongoClient;
        private final MongoNamespace namespace;
        private volatile ServerCursor serverCursor;

        CleanerState(final MongoClient mongoClient, final MongoNamespace namespace, final ServerCursor serverCursor) {
            this.mongoClient = assertNotNull(mongoClient);
            this.namespace = assertNotNull(namespace);
            this.serverCursor = assertNotNull(serverCursor);
        }

        public void run() {
            if (serverCursor != null) {
                mongoClient.addOrphanedCursor(serverCursor, namespace);
            }
        }

        public void clear() {
            serverCursor = null;
        }
    }
}
