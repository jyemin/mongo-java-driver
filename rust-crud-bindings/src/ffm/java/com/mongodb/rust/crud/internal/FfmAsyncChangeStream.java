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

import com.mongodb.internal.rust.crud.ffi.ChangeStreamGetMoreCallback;
import com.mongodb.internal.rust.crud.ffi.ChangeStreamGetMoreResult;
import com.mongodb.internal.rust.crud.ffi.MongoDbFfi;
import com.mongodb.internal.rust.crud.ffi.VoidCallback;
import com.mongodb.rust.crud.NativeAsyncChangeStream;
import com.mongodb.rust.crud.SingleResultCallback;
import org.bson.BsonDocument;
import org.bson.codecs.Decoder;
import org.bson.codecs.DecoderContext;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * AsyncChangeStream implementation backed by Rust FFI.
 */
public class FfmAsyncChangeStream<T> implements NativeAsyncChangeStream<T> {

    private final MemorySegment clientPtr;
    private final AtomicReference<MemorySegment> changeStreamPtr;
    private final AtomicReference<BsonDocument> resumeToken;
    private final AtomicBoolean closed;
    private final Decoder<T> decoder;

    public FfmAsyncChangeStream(MemorySegment clientPtr, MemorySegment changeStreamPtr, Decoder<T> decoder) {
        this.clientPtr = clientPtr;
        this.changeStreamPtr = new AtomicReference<>(changeStreamPtr);
        this.resumeToken = new AtomicReference<>();
        this.closed = new AtomicBoolean(false);
        this.decoder = decoder;
    }

    @Override
    public void next(SingleResultCallback<T> callback) {
        if (closed.get()) {
            callback.completeExceptionally(new IllegalStateException("Change stream is closed"));
            return;
        }

        MemorySegment csPtr = changeStreamPtr.get();
        if (csPtr == null || csPtr.equals(MemorySegment.NULL)) {
            callback.complete(null);
            return;
        }

        Arena arena = Arena.ofShared();
        try {
            MemorySegment cb = ChangeStreamGetMoreCallback.allocate((userdata, result, error) -> {
                try {
                    if (!error.equals(MemorySegment.NULL)) {
                        callback.completeExceptionally(ErrorConverter.toException(error));
                    } else {
                        // Update resume token
                        MemorySegment tokenPtr = ChangeStreamGetMoreResult.resume_token(result);
                        if (tokenPtr != null && !tokenPtr.equals(MemorySegment.NULL)) {
                            resumeToken.set(BsonMarshaller.fromBsonStruct(tokenPtr));
                        }

                        // Get the change document and decode it
                        BsonDocument doc = BsonMarshaller.fromBsonStruct(result);
                        if (doc != null) {
                            T decoded = decoder.decode(doc.asBsonReader(), DecoderContext.builder().build());
                            callback.complete(decoded);
                        } else {
                            callback.complete(null);
                        }
                    }
                } finally {
                    arena.close();
                }
            }, arena);

            MongoDbFfi.mongo_change_stream_get_more(clientPtr, csPtr, MemorySegment.NULL, cb);
        } catch (Throwable t) {
            arena.close();
            callback.completeExceptionally(t);
        }
    }

    @Override
    public BsonDocument getResumeToken() {
        return resumeToken.get();
    }

    @Override
    public void close(SingleResultCallback<Void> callback) {
        if (!closed.compareAndSet(false, true)) {
            callback.complete(null);
            return;
        }

        MemorySegment csPtr = changeStreamPtr.getAndSet(MemorySegment.NULL);
        if (csPtr == null || csPtr.equals(MemorySegment.NULL)) {
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

            MongoDbFfi.mongo_change_stream_close(clientPtr, csPtr, MemorySegment.NULL, cb);
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

        MemorySegment csPtr = changeStreamPtr.getAndSet(MemorySegment.NULL);
        if (csPtr == null || csPtr.equals(MemorySegment.NULL)) {
            return;
        }

        // Fire-and-forget close
        Arena arena = Arena.ofShared();
        try {
            MemorySegment cb = VoidCallback.allocate((userdata, error) -> {
                arena.close();
            }, arena);

            MongoDbFfi.mongo_change_stream_close(clientPtr, csPtr, MemorySegment.NULL, cb);
        } catch (Throwable t) {
            arena.close();
        }
    }
}

