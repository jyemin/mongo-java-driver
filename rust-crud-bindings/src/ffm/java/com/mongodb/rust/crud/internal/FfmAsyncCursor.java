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

import com.mongodb.MongoException;
import com.mongodb.internal.rust.crud.ffi.CursorResult;
import com.mongodb.internal.rust.crud.ffi.GetMoreResultCallback;
import com.mongodb.internal.rust.crud.ffi.MongoDbFfi;
import com.mongodb.lang.Nullable;
import com.mongodb.rust.crud.NativeAsyncCursor;
import com.mongodb.rust.crud.SingleResultCallback;
import org.bson.codecs.Decoder;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * FFM implementation of NativeAsyncCursor.
 *
 * <p>The cursor stores the first batch from the initial query result and returns it
 * on the first call to {@link #next}. Subsequent calls fetch more batches via getMore.
 *
 * @param <T> the document type
 */
public final class FfmAsyncCursor<T> implements NativeAsyncCursor<T> {

    private final MemorySegment clientPtr;
    private final MemorySegment cursorPtr;
    private final MemorySegment sessionPtr;
    private final Decoder<T> decoder;
    private final AtomicBoolean exhausted;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    @Nullable
    private List<T> firstBatch;

    /**
     * Creates a cursor from a CursorResult.
     *
     * <p>The first batch is decoded and stored in the cursor, to be returned on
     * the first call to {@link #next}.
     *
     * @param clientPtr the client pointer
     * @param cursorResult the CursorResult from find/aggregate
     * @param sessionPtr the session pointer (may be NULL)
     * @param decoder the decoder for documents
     * @param <T> the document type
     * @return the cursor with first batch stored internally
     */
    public static <T> FfmAsyncCursor<T> fromCursorResult(
            MemorySegment clientPtr,
            MemorySegment cursorResult,
            MemorySegment sessionPtr,
            Decoder<T> decoder) {

        MemorySegment cursorPtr = CursorResult.cursor(cursorResult);
        boolean isExhausted = CursorResult.exhausted(cursorResult);
        MemorySegment firstBatchStruct = CursorResult.first_batch(cursorResult);

        List<T> firstBatch = BsonMarshaller.fromBsonArrayStruct(firstBatchStruct, decoder);

        return new FfmAsyncCursor<>(clientPtr, cursorPtr, sessionPtr, decoder, isExhausted, firstBatch);
    }

    private FfmAsyncCursor(
            MemorySegment clientPtr,
            MemorySegment cursorPtr,
            MemorySegment sessionPtr,
            Decoder<T> decoder,
            boolean exhausted,
            List<T> firstBatch) {
        this.clientPtr = clientPtr;
        this.cursorPtr = cursorPtr;
        this.sessionPtr = sessionPtr;
        this.decoder = decoder;
        this.exhausted = new AtomicBoolean(exhausted);
        this.firstBatch = firstBatch;
    }

    @Override
    public void next(SingleResultCallback<List<T>> callback) {
        if (closed.get()) {
            callback.onResult(null, new MongoException("Cursor is closed"));
            return;
        }

        // Return the first batch if we have it
        if (firstBatch != null) {
            List<T> batch = firstBatch;
            firstBatch = null;
            callback.onResult(batch, null);
            return;
        }

        if (exhausted.get()) {
            callback.onResult(null, null);
            return;
        }

        Arena arena = Arena.ofShared();
        try {
            MemorySegment callbackPtr = GetMoreResultCallback.allocate(
                    (userdata, isExhausted, batchStruct, error) -> {
                        try {
                            if (error.address() != 0) {
                                callback.onResult(null, FfmErrorMapper.toException(error));
                            } else {
                                exhausted.set(isExhausted);
                                List<T> batch = BsonMarshaller.fromBsonArrayStruct(batchStruct, decoder);
                                callback.onResult(batch, null);
                            }
                        } catch (Throwable t) {
                            callback.onResult(null, t);
                        } finally {
                            arena.close();
                        }
                    },
                    arena);

            MongoDbFfi.mongo_cursor_get_more(
                    clientPtr,
                    cursorPtr,
                    sessionPtr,
                    MemorySegment.NULL,
                    callbackPtr);
        } catch (Exception e) {
            arena.close();
            callback.onResult(null, e);
        }
    }

    @Override
    public boolean isExhausted() {
        return firstBatch == null && exhausted.get();
    }

    @Override
    public void close(SingleResultCallback<Void> callback) {
        if (closed.compareAndSet(false, true)) {
            firstBatch = null;
            if (cursorPtr.address() != 0) {
                MongoDbFfi.mongo_cursor_close(cursorPtr);
            }
        }
        callback.onResult(null, null);
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            firstBatch = null;
            if (cursorPtr.address() != 0) {
                MongoDbFfi.mongo_cursor_close(cursorPtr);
            }
        }
    }
}

