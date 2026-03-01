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

package com.mongodb.rust.crud;

import org.bson.BsonDocument;

/**
 * Async change stream interface for watching changes.
 *
 * @param <T> the type of change events (typically BsonDocument)
 */
public interface NativeAsyncChangeStream<T> extends AutoCloseable {

    /**
     * Gets the next change event.
     *
     * @param callback called with the next change event, or null if no more events
     */
    void next(SingleResultCallback<T> callback);

    /**
     * Gets the resume token for the current position.
     *
     * @return the resume token, or null if not available
     */
    BsonDocument getResumeToken();

    /**
     * Closes the change stream asynchronously.
     *
     * @param callback called when the change stream has been closed
     */
    void close(SingleResultCallback<Void> callback);

    /**
     * Closes the change stream synchronously.
     */
    @Override
    void close();
}

