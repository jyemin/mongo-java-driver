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

import java.util.Collection;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * MongoIterable that maps results from one type to another.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class NativeMappingIterable<T, U> implements MongoIterable<U> {

    private final MongoIterable<T> source;
    private final Function<T, U> mapper;

    public NativeMappingIterable(MongoIterable<T> source, Function<T, U> mapper) {
        this.source = notNull("source", source);
        this.mapper = notNull("mapper", mapper);
    }

    @Override
    public MongoCursor<U> iterator() {
        return new MappingMongoCursor<>(source.iterator(), mapper);
    }

    @Override
    public MongoCursor<U> cursor() {
        return iterator();
    }

    @Override
    @Nullable
    public U first() {
        T first = source.first();
        return first != null ? mapper.apply(first) : null;
    }

    @Override
    public <V> MongoIterable<V> map(Function<U, V> mapper) {
        return new NativeMappingIterable<>(this, mapper);
    }

    @Override
    public <A extends Collection<? super U>> A into(A target) {
        forEach(target::add);
        return target;
    }

    @Override
    public MongoIterable<U> batchSize(int batchSize) {
        source.batchSize(batchSize);
        return this;
    }

    /**
     * MongoCursor that maps results using a function.
     */
    private static final class MappingMongoCursor<T, U> implements MongoCursor<U> {
        private final MongoCursor<T> source;
        private final Function<T, U> mapper;

        MappingMongoCursor(MongoCursor<T> source, Function<T, U> mapper) {
            this.source = source;
            this.mapper = mapper;
        }

        @Override
        public void close() {
            source.close();
        }

        @Override
        public boolean hasNext() {
            return source.hasNext();
        }

        @Override
        public U next() {
            return mapper.apply(source.next());
        }

        @Override
        public int available() {
            return source.available();
        }

        @Override
        @Nullable
        public U tryNext() {
            T next = source.tryNext();
            return next != null ? mapper.apply(next) : null;
        }

        @Override
        @Nullable
        public com.mongodb.ServerCursor getServerCursor() {
            return source.getServerCursor();
        }

        @Override
        public com.mongodb.ServerAddress getServerAddress() {
            return source.getServerAddress();
        }
    }
}

