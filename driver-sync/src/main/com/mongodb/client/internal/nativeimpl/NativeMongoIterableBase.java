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
package com.mongodb.client.internal.nativeimpl;

import com.mongodb.Function;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoIterable;
import com.mongodb.lang.Nullable;
import com.mongodb.rust.crud.NativeOperationContext;
import com.mongodb.rust.crud.NativeSyncClient;
import com.mongodb.rust.crud.NativeSyncClientSession;
import org.bson.codecs.Codec;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.Collection;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * Base class for native MongoIterable implementations.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public abstract class NativeMongoIterableBase<TResult> implements MongoIterable<TResult> {

    private final NativeSyncClient nativeClient;
    @Nullable
    private final NativeSyncClientSession nativeSession;
    private final NativeOperationContext operationContext;
    private final Class<TResult> resultClass;
    private final CodecRegistry codecRegistry;

    protected NativeMongoIterableBase(NativeSyncClient nativeClient,
                                      @Nullable NativeSyncClientSession nativeSession,
                                      NativeOperationContext operationContext,
                                      Class<TResult> resultClass,
                                      CodecRegistry codecRegistry) {
        this.nativeClient = notNull("nativeClient", nativeClient);
        this.nativeSession = nativeSession;
        this.operationContext = notNull("operationContext", operationContext);
        this.resultClass = notNull("resultClass", resultClass);
        this.codecRegistry = notNull("codecRegistry", codecRegistry);
    }

    protected NativeSyncClient getNativeClient() {
        return nativeClient;
    }

    @Nullable
    protected NativeSyncClientSession getNativeSession() {
        return nativeSession;
    }

    protected NativeOperationContext getOperationContext() {
        return operationContext;
    }

    protected Class<TResult> getResultClass() {
        return resultClass;
    }

    protected CodecRegistry getCodecRegistry() {
        return codecRegistry;
    }

    protected Codec<TResult> getCodec() {
        return codecRegistry.get(resultClass);
    }

    @Override
    public MongoCursor<TResult> iterator() {
        return cursor();
    }

    @Override
    @Nullable
    public TResult first() {
        try (MongoCursor<TResult> cursor = iterator()) {
            return cursor.hasNext() ? cursor.next() : null;
        }
    }

    @Override
    public <U> MongoIterable<U> map(Function<TResult, U> mapper) {
        return new NativeMappingIterable<>(this, mapper);
    }

    @Override
    public <A extends Collection<? super TResult>> A into(A target) {
        forEach(target::add);
        return target;
    }

    @Override
    public MongoIterable<TResult> batchSize(int batchSize) {
        // Subclasses should override to set batch size on their options
        return this;
    }
}

