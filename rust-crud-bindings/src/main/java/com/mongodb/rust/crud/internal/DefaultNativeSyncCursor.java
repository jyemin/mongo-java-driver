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
import com.mongodb.rust.crud.NativeSyncCursor;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Default implementation of NativeSyncCursor.
 * Wraps a NativeAsyncCursor and blocks on async operations.
 *
 * @param <T> the document type
 */
public final class DefaultNativeSyncCursor<T> implements NativeSyncCursor<T> {

    private final NativeAsyncCursor<T> asyncCursor;
    private Iterator<T> currentBatchIterator;
    private boolean fetchedNext;
    private T nextDocument;

    public DefaultNativeSyncCursor(NativeAsyncCursor<T> asyncCursor) {
        this.asyncCursor = asyncCursor;
    }

    @Override
    public boolean hasNext() {
        if (!fetchedNext) {
            fetchNext();
        }
        return nextDocument != null;
    }

    @Override
    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        T doc = nextDocument;
        nextDocument = null;
        fetchedNext = false;
        return doc;
    }

    private void fetchNext() {
        fetchedNext = true;
        
        // Check current batch first
        if (currentBatchIterator != null && currentBatchIterator.hasNext()) {
            nextDocument = currentBatchIterator.next();
            return;
        }

        // If exhausted, we're done
        if (asyncCursor.isExhausted()) {
            nextDocument = null;
            return;
        }

        // Fetch next batch
        CompletableFuture<List<T>> future = new CompletableFuture<>();
        asyncCursor.next((batch, error) -> {
            if (error != null) {
                future.completeExceptionally(error);
            } else {
                future.complete(batch);
            }
        });

        List<T> batch;
        try {
            batch = future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Cursor operation interrupted", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw new RuntimeException(cause);
        }

        if (batch == null || batch.isEmpty()) {
            nextDocument = null;
        } else {
            currentBatchIterator = batch.iterator();
            nextDocument = currentBatchIterator.next();
        }
    }

    /**
     * Returns the next batch of documents.
     */
    public List<T> nextBatch() {
        CompletableFuture<List<T>> future = new CompletableFuture<>();
        asyncCursor.next((batch, error) -> {
            if (error != null) {
                future.completeExceptionally(error);
            } else {
                future.complete(batch);
            }
        });

        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Cursor operation interrupted", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw new RuntimeException(cause);
        }
    }

    @Override
    public void close() {
        asyncCursor.close();
    }
}

