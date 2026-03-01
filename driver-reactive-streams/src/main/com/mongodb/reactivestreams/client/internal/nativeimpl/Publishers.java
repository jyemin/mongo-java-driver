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
package com.mongodb.reactivestreams.client.internal.nativeimpl;

import com.mongodb.rust.crud.NativeAsyncCursor;
import com.mongodb.rust.crud.SingleResultCallback;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.Consumer;

/**
 * Utility methods for converting native async operations to Reactor Publishers.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
final class Publishers {

    /**
     * Converts a callback-based async operation to a Mono.
     *
     * @param operation a consumer that accepts a callback and initiates the async operation
     * @param <T> the result type
     * @return a Mono that completes with the operation result
     */
    static <T> Mono<T> toMono(Consumer<SingleResultCallback<T>> operation) {
        return Mono.create(sink -> operation.accept((result, error) -> {
            if (error != null) {
                sink.error(error);
            } else {
                sink.success(result);
            }
        }));
    }

    /**
     * Converts a callback-based async operation that returns void to a Mono.
     *
     * @param operation a consumer that accepts a callback and initiates the async operation
     * @return a Mono that completes when the operation completes
     */
    static Mono<Void> toMonoVoid(Consumer<SingleResultCallback<Void>> operation) {
        return Mono.create(sink -> operation.accept((result, error) -> {
            if (error != null) {
                sink.error(error);
            } else {
                sink.success();
            }
        }));
    }

    /**
     * Converts a NativeAsyncCursor to a Flux that emits all documents.
     *
     * @param cursorMono a Mono that provides the cursor
     * @param <T> the document type
     * @return a Flux that emits all documents from the cursor
     */
    static <T> Flux<T> cursorToFlux(Mono<NativeAsyncCursor<T>> cursorMono) {
        return cursorMono.flatMapMany(cursor ->
            Flux.<T>create(sink -> {
                sink.onDispose(() -> cursor.close());
                sink.onCancel(() -> cursor.close());

                fetchNext(cursor, sink);
            })
        );
    }

    private static <T> void fetchNext(NativeAsyncCursor<T> cursor, reactor.core.publisher.FluxSink<T> sink) {
        if (sink.isCancelled()) {
            cursor.close();
            return;
        }

        cursor.next((batch, error) -> {
            if (error != null) {
                sink.error(error);
                cursor.close();
            } else if (batch == null || batch.isEmpty()) {
                sink.complete();
                cursor.close();
            } else {
                for (T item : batch) {
                    if (sink.isCancelled()) {
                        cursor.close();
                        return;
                    }
                    sink.next(item);
                }
                fetchNext(cursor, sink);
            }
        });
    }

    private Publishers() {
    }
}

