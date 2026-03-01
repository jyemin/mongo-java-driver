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

import com.mongodb.MongoNamespace;
import com.mongodb.client.DistinctIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.cursor.TimeoutMode;
import com.mongodb.client.model.Collation;
import com.mongodb.lang.Nullable;
import com.mongodb.rust.crud.NativeSyncClient;
import com.mongodb.rust.crud.NativeSyncClientSession;
import com.mongodb.rust.crud.NativeSyncCursor;
import com.mongodb.client.model.DistinctOptions;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWrapper;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * Native implementation of DistinctIterable using Rust FFI.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class NativeDistinctIterable<TResult>
        extends NativeMongoIterableBase<TResult>
        implements DistinctIterable<TResult> {

    private final MongoNamespace namespace;
    private final String fieldName;
    private final DistinctOptions options = new DistinctOptions();
    private Bson filter;

    public NativeDistinctIterable(NativeSyncClient nativeClient,
                                  @Nullable NativeSyncClientSession nativeSession,
                                  MongoNamespace namespace,
                                  String fieldName,
                                  Class<TResult> resultClass,
                                  CodecRegistry codecRegistry) {
        super(nativeClient, nativeSession, resultClass, codecRegistry);
        this.namespace = notNull("namespace", namespace);
        this.fieldName = notNull("fieldName", fieldName);
        this.filter = new BsonDocument();
    }

    @Override
    public DistinctIterable<TResult> filter(@Nullable Bson filter) {
        this.filter = filter != null ? filter : new BsonDocument();
        return this;
    }

    @Override
    public DistinctIterable<TResult> maxTime(long maxTime, TimeUnit timeUnit) {
        options.maxTime(maxTime, timeUnit);
        return this;
    }

    @Override
    public DistinctIterable<TResult> batchSize(int batchSize) {
        options.batchSize(batchSize);
        return this;
    }

    @Override
    public DistinctIterable<TResult> collation(@Nullable Collation collation) {
        options.collation(collation);
        return this;
    }

    @Override
    public DistinctIterable<TResult> comment(@Nullable String comment) {
        options.comment(comment != null ? new BsonString(comment) : null);
        return this;
    }

    @Override
    public DistinctIterable<TResult> comment(@Nullable BsonValue comment) {
        options.comment(comment);
        return this;
    }

    @Override
    public DistinctIterable<TResult> hint(@Nullable Bson hint) {
        options.hint(hint);
        return this;
    }

    @Override
    public DistinctIterable<TResult> hintString(@Nullable String hint) {
        options.hintString(hint);
        return this;
    }

    @Override
    public DistinctIterable<TResult> timeoutMode(TimeoutMode timeoutMode) {
        // TODO: Implement timeout mode
        return this;
    }

    @Override
    public MongoCursor<TResult> cursor() {
        BsonDocument filterDoc = BsonDocumentWrapper.asBsonDocument(filter, getCodecRegistry());
        NativeSyncCursor<TResult> nativeCursor = getNativeClient().distinct(
                namespace, fieldName, filterDoc, options, getCodec(), getNativeSession());
        return new NativeMongoCursor<>(nativeCursor);
    }
}

