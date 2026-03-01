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

import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.ChangeStreamOptions;
import com.mongodb.client.model.Collation;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.FullDocumentBeforeChange;
import com.mongodb.lang.Nullable;
import com.mongodb.rust.crud.NativeSyncChangeStream;
import com.mongodb.rust.crud.NativeSyncClient;
import com.mongodb.rust.crud.NativeSyncClientSession;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.codecs.Codec;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * Native implementation of ChangeStreamIterable.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class NativeChangeStreamIterable<TResult>
        extends NativeMongoIterableBase<ChangeStreamDocument<TResult>>
        implements ChangeStreamIterable<TResult> {

    private final NativeSyncClient nativeClient;
    @Nullable
    private final NativeSyncClientSession nativeSession;
    private final List<BsonDocument> pipeline;
    private final Class<TResult> resultClass;
    private final CodecRegistry codecRegistry;
    private final ChangeStreamOptions options = new ChangeStreamOptions();

    // Watch level: client, database, or collection
    public enum WatchLevel { CLIENT, DATABASE, COLLECTION }
    private final WatchLevel watchLevel;
    @Nullable
    private final String databaseName;
    @Nullable
    private final String collectionName;

    public NativeChangeStreamIterable(NativeSyncClient nativeClient,
                                      @Nullable NativeSyncClientSession nativeSession,
                                      List<? extends Bson> pipeline,
                                      Class<TResult> resultClass,
                                      CodecRegistry codecRegistry,
                                      WatchLevel watchLevel,
                                      @Nullable String databaseName,
                                      @Nullable String collectionName) {
        super(nativeClient, nativeSession, 
                (Class<ChangeStreamDocument<TResult>>) (Class<?>) ChangeStreamDocument.class, codecRegistry);
        this.nativeClient = notNull("nativeClient", nativeClient);
        this.nativeSession = nativeSession;
        this.pipeline = toBsonDocumentList(pipeline, codecRegistry);
        this.resultClass = notNull("resultClass", resultClass);
        this.codecRegistry = notNull("codecRegistry", codecRegistry);
        this.watchLevel = notNull("watchLevel", watchLevel);
        this.databaseName = databaseName;
        this.collectionName = collectionName;
    }

    private List<BsonDocument> toBsonDocumentList(List<? extends Bson> pipeline, CodecRegistry codecRegistry) {
        List<BsonDocument> result = new ArrayList<>(pipeline.size());
        for (Bson stage : pipeline) {
            result.add(stage.toBsonDocument(BsonDocument.class, codecRegistry));
        }
        return result;
    }

    @Override
    public ChangeStreamIterable<TResult> fullDocument(FullDocument fullDocument) {
        options.fullDocument(fullDocument);
        return this;
    }

    @Override
    public ChangeStreamIterable<TResult> fullDocumentBeforeChange(FullDocumentBeforeChange fullDocumentBeforeChange) {
        options.fullDocumentBeforeChange(fullDocumentBeforeChange);
        return this;
    }

    @Override
    public ChangeStreamIterable<TResult> resumeAfter(BsonDocument resumeToken) {
        options.resumeAfter(resumeToken);
        return this;
    }

    @Override
    public ChangeStreamIterable<TResult> startAfter(BsonDocument startAfter) {
        options.startAfter(startAfter);
        return this;
    }

    @Override
    public ChangeStreamIterable<TResult> startAtOperationTime(BsonTimestamp startAtOperationTime) {
        options.startAtOperationTime(startAtOperationTime);
        return this;
    }

    @Override
    public ChangeStreamIterable<TResult> batchSize(int batchSize) {
        options.batchSize(batchSize);
        return this;
    }

    @Override
    public ChangeStreamIterable<TResult> maxAwaitTime(long maxAwaitTime, TimeUnit timeUnit) {
        options.maxAwaitTime(maxAwaitTime, timeUnit);
        return this;
    }

    @Override
    public ChangeStreamIterable<TResult> collation(@Nullable Collation collation) {
        options.collation(collation);
        return this;
    }

    @Override
    public ChangeStreamIterable<TResult> comment(@Nullable String comment) {
        options.comment(comment != null ? new BsonString(comment) : null);
        return this;
    }

    @Override
    public ChangeStreamIterable<TResult> comment(@Nullable BsonValue comment) {
        options.comment(comment);
        return this;
    }

    @Override
    public ChangeStreamIterable<TResult> showExpandedEvents(boolean showExpandedEvents) {
        // TODO: Add to ChangeStreamOptions when supported
        return this;
    }

    @Override
    public <TDocument> MongoIterable<TDocument> withDocumentClass(Class<TDocument> clazz) {
        // TODO: Implement withDocumentClass for native change streams
        throw new UnsupportedOperationException("withDocumentClass not yet implemented for native change streams");
    }

    @Override
    public MongoChangeStreamCursor<ChangeStreamDocument<TResult>> cursor() {
        Codec<ChangeStreamDocument<TResult>> codec = getChangeStreamDocumentCodec();
        NativeSyncChangeStream<ChangeStreamDocument<TResult>> nativeStream = createChangeStream(codec);
        return new NativeMongoChangeStreamCursor<>(nativeStream);
    }

    private Codec<ChangeStreamDocument<TResult>> getChangeStreamDocumentCodec() {
        // Use ChangeStreamDocument.createCodec which properly handles the full document decoding
        return ChangeStreamDocument.createCodec(resultClass, codecRegistry);
    }

    private NativeSyncChangeStream<ChangeStreamDocument<TResult>> createChangeStream(
            Codec<ChangeStreamDocument<TResult>> codec) {
        switch (watchLevel) {
            case CLIENT:
                return nativeClient.watchClient(pipeline, options, codec, nativeSession);
            case DATABASE:
                return nativeClient.watchDatabase(databaseName, pipeline, options, codec, nativeSession);
            case COLLECTION:
                return nativeClient.watchCollection(
                        new com.mongodb.MongoNamespace(databaseName, collectionName),
                        pipeline, options, codec, nativeSession);
            default:
                throw new IllegalStateException("Unknown watch level: " + watchLevel);
        }
    }

    @Override
    public MongoCursor<ChangeStreamDocument<TResult>> iterator() {
        return cursor();
    }
}

