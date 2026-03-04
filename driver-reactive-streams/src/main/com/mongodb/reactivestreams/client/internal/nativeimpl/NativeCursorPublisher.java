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

import com.mongodb.lang.Nullable;
import com.mongodb.rust.crud.NativeAsyncClient;
import com.mongodb.rust.crud.NativeAsyncClientSession;
import com.mongodb.rust.crud.NativeAsyncCursor;
import com.mongodb.rust.crud.NativeOperationContext;
import com.mongodb.rust.crud.SingleResultCallback;
import org.bson.codecs.configuration.CodecRegistry;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Base class for native publisher implementations that return cursor results.
 *
 * <p>This class handles common cursor-to-Flux conversion and provides a template
 * for subclasses to implement operation-specific behavior.</p>
 *
 * @param <TResult> the result type
 */
abstract class NativeCursorPublisher<TResult> implements Publisher<TResult> {

    private final NativeAsyncClient nativeClient;
    @Nullable private final NativeAsyncClientSession session;
    private final NativeOperationContext operationContext;
    private final CodecRegistry codecRegistry;
    private Integer batchSize;

    NativeCursorPublisher(NativeAsyncClient nativeClient, @Nullable NativeAsyncClientSession session,
                          NativeOperationContext operationContext, CodecRegistry codecRegistry) {
        this.nativeClient = nativeClient;
        this.session = session;
        this.operationContext = operationContext;
        this.codecRegistry = codecRegistry;
    }

    protected NativeAsyncClient getNativeClient() {
        return nativeClient;
    }

    @Nullable
    protected NativeAsyncClientSession getSession() {
        return session;
    }

    protected NativeOperationContext getOperationContext() {
        return operationContext;
    }

    protected CodecRegistry getCodecRegistry() {
        return codecRegistry;
    }

    @Nullable
    public Integer getBatchSize() {
        return batchSize;
    }

    protected void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    /**
     * Execute the operation and return a cursor.
     *
     * @param callback the callback to receive the cursor
     */
    protected abstract void executeAsync(SingleResultCallback<NativeAsyncCursor<TResult>> callback);

    /**
     * Returns a Publisher that emits only the first result.
     *
     * @return the first result publisher
     */
    public Publisher<TResult> first() {
        Integer originalBatchSize = this.batchSize;
        this.batchSize = 1;
        Mono<TResult> result = toFlux().next();
        this.batchSize = originalBatchSize;
        return result;
    }

    /**
     * Convert this publisher to a Flux.
     *
     * @return a Flux emitting all results
     */
    protected Flux<TResult> toFlux() {
        return Publishers.cursorToFlux(
            Publishers.toMono(this::executeAsync)
        );
    }

    @Override
    public void subscribe(Subscriber<? super TResult> subscriber) {
        toFlux().subscribe(subscriber);
    }
}

