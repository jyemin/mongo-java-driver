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

import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;

import java.util.Iterator;

/**
 * Native synchronous change stream interface.
 *
 * @param <T> the type of change events
 */
public interface NativeSyncChangeStream<T> extends Iterator<T>, AutoCloseable {

    /**
     * Returns true if there are more events.
     */
    @Override
    boolean hasNext();

    /**
     * Returns the next event.
     */
    @Override
    T next();

    /**
     * Tries to get the next change event, blocking until one is available.
     * Returns null if the stream is closed.
     */
    @Nullable
    T tryNext();

    /**
     * Returns the resume token for the change stream.
     * @return the resume token or null if not available
     */
    @Nullable
    BsonDocument getResumeToken();

    /**
     * Closes the change stream.
     */
    @Override
    void close();
}

