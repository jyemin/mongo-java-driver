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

import com.mongodb.lang.Nullable;
import com.mongodb.rust.crud.NativeAsyncChangeStream;
import com.mongodb.rust.crud.NativeSyncChangeStream;

import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Default implementation of NativeSyncChangeStream.
 * Wraps a NativeAsyncChangeStream and blocks on async operations.
 *
 * @param <T> the type of change events
 */
public final class DefaultNativeSyncChangeStream<T> implements NativeSyncChangeStream<T> {

    private final NativeAsyncChangeStream<T> asyncStream;
    private T nextEvent;
    private boolean hasNextComputed = false;
    private boolean exhausted = false;

    public DefaultNativeSyncChangeStream(NativeAsyncChangeStream<T> asyncStream) {
        this.asyncStream = asyncStream;
    }

    @Override
    public boolean hasNext() {
        if (hasNextComputed) {
            return nextEvent != null;
        }
        
        if (exhausted) {
            return false;
        }

        nextEvent = fetchNext();
        hasNextComputed = true;
        
        if (nextEvent == null) {
            exhausted = true;
            return false;
        }
        
        return true;
    }

    @Override
    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        
        hasNextComputed = false;
        T result = nextEvent;
        nextEvent = null;
        return result;
    }

    /**
     * Tries to get the next change event, blocking until one is available.
     * Returns null if the stream is closed.
     */
    @Nullable
    public T tryNext() {
        if (hasNextComputed && nextEvent != null) {
            hasNextComputed = false;
            T result = nextEvent;
            nextEvent = null;
            return result;
        }
        
        return fetchNext();
    }

    @Nullable
    private T fetchNext() {
        CompletableFuture<T> future = new CompletableFuture<>();
        asyncStream.next((result, error) -> {
            if (error != null) {
                future.completeExceptionally(error);
            } else {
                future.complete(result);
            }
        });
        
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Operation interrupted", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw new RuntimeException(cause);
        }
    }

    @Override
    public org.bson.BsonDocument getResumeToken() {
        return asyncStream.getResumeToken();
    }

    @Override
    public void close() {
        asyncStream.close();
    }
}

