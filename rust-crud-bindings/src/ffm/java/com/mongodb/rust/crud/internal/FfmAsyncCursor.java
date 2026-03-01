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

import com.mongodb.internal.rust.crud.ffi.GetMoreCallback;
import com.mongodb.internal.rust.crud.ffi.GetMoreResult;
import com.mongodb.internal.rust.crud.ffi.MongoDbFfi;
import com.mongodb.internal.rust.crud.ffi.VoidCallback;
import com.mongodb.rust.crud.NativeAsyncCursor;
import com.mongodb.rust.crud.SingleResultCallback;
import org.bson.BsonDocument;
import org.bson.codecs.Decoder;
import org.bson.codecs.DecoderContext;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * AsyncCursor implementation backed by Rust FFI cursor.
 */
public class FfmAsyncCursor<T> implements NativeAsyncCursor<T> {

    private final MemorySegment clientPtr;
    private final AtomicReference<MemorySegment> cursorPtr;
    private final AtomicBoolean exhausted;
    private final AtomicReference<List<BsonDocument>> pendingBatch;
    private final AtomicBoolean closed;
    private final Decoder<T> decoder;

    /**
     * Creates a cursor with a decoder for converting BsonDocuments to the target type.
     */
    public FfmAsyncCursor(MemorySegment clientPtr, CursorHandle handle, Decoder<T> decoder) {
        this.clientPtr = clientPtr;
        this.cursorPtr = new AtomicReference<>(handle.getCursorPtr());
        this.exhausted = new AtomicBoolean(handle.isExhausted());
        this.pendingBatch = new AtomicReference<>(handle.getFirstBatch());
        this.closed = new AtomicBoolean(false);
        this.decoder = decoder;
    }

    /**
     * Creates a cursor that returns raw BsonDocuments (no decoding).
     * This is a convenience constructor for when T is BsonDocument.
     */
    @SuppressWarnings("unchecked")
    public FfmAsyncCursor(MemorySegment clientPtr, CursorHandle handle) {
        this(clientPtr, handle, (Decoder<T>) BSON_DOCUMENT_DECODER);
    }

    private static final Decoder<BsonDocument> BSON_DOCUMENT_DECODER =
            new org.bson.codecs.BsonDocumentCodec();

    @Override
    public void next(SingleResultCallback<List<T>> callback) {
        if (closed.get()) {
            callback.completeExceptionally(new IllegalStateException("Cursor is closed"));
            return;
        }

        // If we have a pending batch (first batch or previous getMore), return it
        List<BsonDocument> batch = pendingBatch.getAndSet(null);
        if (batch != null && !batch.isEmpty()) {
            callback.complete(decodeBatch(batch));
            return;
        }

        // If exhausted, return null
        if (exhausted.get()) {
            callback.complete(null);
            return;
        }

        // Fetch next batch via getMore
        MemorySegment cursor = cursorPtr.get();
        if (cursor == null || cursor.equals(MemorySegment.NULL)) {
            exhausted.set(true);
            callback.complete(null);
            return;
        }

        Arena arena = Arena.ofShared();
        try {
            MemorySegment cb = GetMoreCallback.allocate((userdata, result, error) -> {
                try {
                    if (!error.equals(MemorySegment.NULL)) {
                        callback.completeExceptionally(ErrorConverter.toException(error));
                    } else {
                        boolean isExhausted = GetMoreResult.exhausted(result);
                        exhausted.set(isExhausted);

                        List<BsonDocument> nextBatch = CursorHandle.decodeBatch(GetMoreResult.batch(result));
                        callback.complete(nextBatch.isEmpty() ? null : decodeBatch(nextBatch));
                    }
                } finally {
                    arena.close();
                }
            }, arena);

            MongoDbFfi.mongo_cursor_get_more(clientPtr, cursor, MemorySegment.NULL, cb);
        } catch (Throwable t) {
            arena.close();
            callback.completeExceptionally(t);
        }
    }

    private List<T> decodeBatch(List<BsonDocument> bsonBatch) {
        List<T> decoded = new ArrayList<>(bsonBatch.size());
        DecoderContext decoderContext = DecoderContext.builder().build();
        for (BsonDocument doc : bsonBatch) {
            decoded.add(decoder.decode(doc.asBsonReader(), decoderContext));
        }
        return decoded;
    }

    @Override
    public boolean isExhausted() {
        return exhausted.get();
    }

    @Override
    public void close(SingleResultCallback<Void> callback) {
        if (!closed.compareAndSet(false, true)) {
            callback.complete(null);
            return;
        }

        MemorySegment cursor = cursorPtr.getAndSet(MemorySegment.NULL);
        if (cursor == null || cursor.equals(MemorySegment.NULL)) {
            callback.complete(null);
            return;
        }

        Arena arena = Arena.ofShared();
        try {
            MemorySegment cb = VoidCallback.allocate((userdata, error) -> {
                try {
                    if (!error.equals(MemorySegment.NULL)) {
                        callback.completeExceptionally(ErrorConverter.toException(error));
                    } else {
                        callback.complete(null);
                    }
                } finally {
                    arena.close();
                }
            }, arena);

            MongoDbFfi.mongo_cursor_close(clientPtr, cursor, MemorySegment.NULL, cb);
        } catch (Throwable t) {
            arena.close();
            callback.completeExceptionally(t);
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        MemorySegment cursor = cursorPtr.getAndSet(MemorySegment.NULL);
        if (cursor == null || cursor.equals(MemorySegment.NULL)) {
            return;
        }

        // Fire-and-forget close
        Arena arena = Arena.ofShared();
        try {
            MemorySegment cb = VoidCallback.allocate((userdata, error) -> {
                arena.close();
            }, arena);

            MongoDbFfi.mongo_cursor_close(clientPtr, cursor, MemorySegment.NULL, cb);
        } catch (Throwable t) {
            arena.close();
        }
    }
}

