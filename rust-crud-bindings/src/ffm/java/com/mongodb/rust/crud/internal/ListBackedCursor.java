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

package com.mongodb.rust.crud.internal;

import com.mongodb.rust.crud.NativeAsyncCursor;
import com.mongodb.rust.crud.SingleResultCallback;

import java.util.Iterator;
import java.util.List;

/**
 * A simple AsyncCursor implementation backed by a pre-fetched list.
 * Used for operations that return all results at once rather than streaming.
 *
 * @param <T> the type of elements in the cursor
 */
class ListBackedCursor<T> implements NativeAsyncCursor<T> {

    private final Iterator<T> iterator;
    private boolean closed = false;

    public ListBackedCursor(List<T> items) {
        this.iterator = items.iterator();
    }

    @Override
    public void next(SingleResultCallback<List<T>> callback) {
        if (closed) {
            callback.completeExceptionally(new IllegalStateException("Cursor is closed"));
            return;
        }
        
        // Return remaining items or null if exhausted
        if (iterator.hasNext()) {
            // Return all remaining items in one batch
            java.util.List<T> remaining = new java.util.ArrayList<>();
            while (iterator.hasNext()) {
                remaining.add(iterator.next());
            }
            callback.complete(remaining);
        } else {
            callback.complete(null);
        }
    }

    @Override
    public boolean isExhausted() {
        return closed || !iterator.hasNext();
    }

    @Override
    public void close(SingleResultCallback<Void> callback) {
        closed = true;
        callback.complete(null);
    }

    @Override
    public void close() {
        closed = true;
    }
}

