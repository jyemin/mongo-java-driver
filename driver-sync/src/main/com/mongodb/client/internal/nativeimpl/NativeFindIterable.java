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

import com.mongodb.CursorType;
import com.mongodb.ExplainVerbosity;
import com.mongodb.MongoNamespace;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.cursor.TimeoutMode;
import com.mongodb.client.model.Collation;
import com.mongodb.client.model.FindOptions;
import com.mongodb.lang.Nullable;
import com.mongodb.rust.crud.NativeOperationContext;
import com.mongodb.rust.crud.NativeSyncClient;
import com.mongodb.rust.crud.NativeSyncClientSession;
import com.mongodb.rust.crud.NativeSyncCursor;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWrapper;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * Native implementation of FindIterable using Rust FFI.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class NativeFindIterable<TResult>
        extends NativeMongoIterableBase<TResult>
        implements FindIterable<TResult> {

    private final MongoNamespace namespace;
    private final FindOptions options = new FindOptions();
    private Bson filter;

    public NativeFindIterable(NativeSyncClient nativeClient,
                              @Nullable NativeSyncClientSession nativeSession,
                              NativeOperationContext operationContext,
                              MongoNamespace namespace,
                              Class<TResult> resultClass,
                              CodecRegistry codecRegistry) {
        super(nativeClient, nativeSession, operationContext, resultClass, codecRegistry);
        this.namespace = notNull("namespace", namespace);
        this.filter = new BsonDocument();
    }

    @Override
    public FindIterable<TResult> filter(@Nullable Bson filter) {
        this.filter = filter != null ? filter : new BsonDocument();
        return this;
    }

    @Override
    public FindIterable<TResult> limit(int limit) {
        options.limit(limit);
        return this;
    }

    @Override
    public FindIterable<TResult> skip(int skip) {
        options.skip(skip);
        return this;
    }

    @Override
    public FindIterable<TResult> maxTime(long maxTime, TimeUnit timeUnit) {
        options.maxTime(maxTime, timeUnit);
        return this;
    }

    @Override
    public FindIterable<TResult> maxAwaitTime(long maxAwaitTime, TimeUnit timeUnit) {
        options.maxAwaitTime(maxAwaitTime, timeUnit);
        return this;
    }

    @Override
    public FindIterable<TResult> projection(@Nullable Bson projection) {
        options.projection(projection);
        return this;
    }

    @Override
    public FindIterable<TResult> sort(@Nullable Bson sort) {
        options.sort(sort);
        return this;
    }

    @Override
    public FindIterable<TResult> noCursorTimeout(boolean noCursorTimeout) {
        options.noCursorTimeout(noCursorTimeout);
        return this;
    }

    @Override
    public FindIterable<TResult> partial(boolean partial) {
        options.partial(partial);
        return this;
    }

    @Override
    public FindIterable<TResult> cursorType(CursorType cursorType) {
        options.cursorType(cursorType);
        return this;
    }

    @Override
    public FindIterable<TResult> batchSize(int batchSize) {
        options.batchSize(batchSize);
        return this;
    }

    @Override
    public FindIterable<TResult> collation(@Nullable Collation collation) {
        options.collation(collation);
        return this;
    }

    @Override
    public FindIterable<TResult> comment(@Nullable String comment) {
        options.comment(comment != null ? new BsonString(comment) : null);
        return this;
    }

    @Override
    public FindIterable<TResult> comment(@Nullable BsonValue comment) {
        options.comment(comment);
        return this;
    }

    @Override
    public FindIterable<TResult> hint(@Nullable Bson hint) {
        options.hint(hint);
        return this;
    }

    @Override
    public FindIterable<TResult> hintString(@Nullable String hint) {
        options.hintString(hint);
        return this;
    }

    @Override
    public FindIterable<TResult> let(@Nullable Bson let) {
        options.let(let);
        return this;
    }

    @Override
    public FindIterable<TResult> max(@Nullable Bson max) {
        options.max(max);
        return this;
    }

    @Override
    public FindIterable<TResult> min(@Nullable Bson min) {
        options.min(min);
        return this;
    }

    @Override
    public FindIterable<TResult> returnKey(boolean returnKey) {
        options.returnKey(returnKey);
        return this;
    }

    @Override
    public FindIterable<TResult> showRecordId(boolean showRecordId) {
        options.showRecordId(showRecordId);
        return this;
    }

    @Override
    public FindIterable<TResult> allowDiskUse(@Nullable Boolean allowDiskUse) {
        options.allowDiskUse(allowDiskUse);
        return this;
    }

    @Override
    public FindIterable<TResult> timeoutMode(TimeoutMode timeoutMode) {
        // TODO: Implement timeout mode
        return this;
    }

    @Override
    public Document explain() {
        return explain(Document.class);
    }

    @Override
    public Document explain(ExplainVerbosity verbosity) {
        return explain(Document.class, verbosity);
    }

    @Override
    public <E> E explain(Class<E> explainResultClass) {
        // TODO: Implement explain
        throw new UnsupportedOperationException("explain not yet implemented");
    }

    @Override
    public <E> E explain(Class<E> explainResultClass, ExplainVerbosity verbosity) {
        // TODO: Implement explain with verbosity
        throw new UnsupportedOperationException("explain not yet implemented");
    }

    @Override
    public MongoCursor<TResult> cursor() {
        BsonDocument filterDoc = BsonDocumentWrapper.asBsonDocument(filter, getCodecRegistry());
        NativeSyncCursor<TResult> nativeCursor = getNativeClient().find(
                namespace, filterDoc, options, getCodec(), getOperationContext(), getNativeSession());
        return new NativeMongoCursor<>(nativeCursor);
    }

    @Nullable
    @Override
    public TResult first() {
        long origLimit = options.getLimit();
        try {
            options.limit(1);
            BsonDocument filterDoc = BsonDocumentWrapper.asBsonDocument(filter, getCodecRegistry());
            try (NativeSyncCursor<TResult> nativeCursor = getNativeClient().find(
                    namespace, filterDoc, options, getCodec(), getOperationContext(), getNativeSession())) {
                if (nativeCursor.hasNext()) {
                    return nativeCursor.next();
                }
                return null;
            }
        } finally {
            options.limit(origLimit);
        }
    }
}
