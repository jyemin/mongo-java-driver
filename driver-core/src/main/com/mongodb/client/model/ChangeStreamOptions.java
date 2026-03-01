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

package com.mongodb.client.model;

import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.FullDocumentBeforeChange;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;

import java.util.concurrent.TimeUnit;

/**
 * The options to apply to a change stream operation.
 *
 * @since 5.4
 * @mongodb.driver.manual changeStreams/ Change Streams
 */
public final class ChangeStreamOptions {
    private FullDocument fullDocument;
    private FullDocumentBeforeChange fullDocumentBeforeChange;
    private BsonDocument resumeAfter;
    private BsonDocument startAfter;
    private BsonTimestamp startAtOperationTime;
    private int batchSize = -1;
    private long maxAwaitTimeMS = -1;
    private Collation collation;
    private BsonValue comment;

    /**
     * Gets the full document option.
     *
     * @return the full document option, or null if not set
     */
    @Nullable
    public FullDocument getFullDocument() {
        return fullDocument;
    }

    /**
     * Sets the full document option.
     *
     * @param fullDocument the full document option
     * @return this
     */
    public ChangeStreamOptions fullDocument(@Nullable final FullDocument fullDocument) {
        this.fullDocument = fullDocument;
        return this;
    }

    /**
     * Gets the full document before change option.
     *
     * @return the full document before change option, or null if not set
     */
    @Nullable
    public FullDocumentBeforeChange getFullDocumentBeforeChange() {
        return fullDocumentBeforeChange;
    }

    /**
     * Sets the full document before change option.
     *
     * @param fullDocumentBeforeChange the full document before change option
     * @return this
     */
    public ChangeStreamOptions fullDocumentBeforeChange(@Nullable final FullDocumentBeforeChange fullDocumentBeforeChange) {
        this.fullDocumentBeforeChange = fullDocumentBeforeChange;
        return this;
    }

    /**
     * Gets the resume token.
     *
     * @return the resume token, or null if not set
     */
    @Nullable
    public BsonDocument getResumeAfter() {
        return resumeAfter;
    }

    /**
     * Sets the resume token.
     *
     * @param resumeAfter the resume token
     * @return this
     */
    public ChangeStreamOptions resumeAfter(@Nullable final BsonDocument resumeAfter) {
        this.resumeAfter = resumeAfter;
        return this;
    }

    /**
     * Gets the start after token.
     *
     * @return the start after token, or null if not set
     */
    @Nullable
    public BsonDocument getStartAfter() {
        return startAfter;
    }

    /**
     * Sets the start after token.
     *
     * @param startAfter the start after token
     * @return this
     */
    public ChangeStreamOptions startAfter(@Nullable final BsonDocument startAfter) {
        this.startAfter = startAfter;
        return this;
    }

    /**
     * Gets the start at operation time.
     *
     * @return the start at operation time, or null if not set
     */
    @Nullable
    public BsonTimestamp getStartAtOperationTime() {
        return startAtOperationTime;
    }

    /**
     * Sets the start at operation time.
     *
     * @param startAtOperationTime the start at operation time
     * @return this
     */
    public ChangeStreamOptions startAtOperationTime(@Nullable final BsonTimestamp startAtOperationTime) {
        this.startAtOperationTime = startAtOperationTime;
        return this;
    }

    /**
     * Gets the batch size.
     *
     * @return the batch size, or -1 if not set
     */
    public int getBatchSize() {
        return batchSize;
    }

    /**
     * Sets the batch size.
     *
     * @param batchSize the batch size
     * @return this
     */
    public ChangeStreamOptions batchSize(final int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    /**
     * Gets the max await time in milliseconds.
     *
     * @return the max await time in milliseconds, or -1 if not set
     */
    public long getMaxAwaitTimeMS() {
        return maxAwaitTimeMS;
    }

    /**
     * Sets the max await time.
     *
     * @param maxAwaitTime the max await time
     * @param timeUnit the time unit
     * @return this
     */
    public ChangeStreamOptions maxAwaitTime(final long maxAwaitTime, final TimeUnit timeUnit) {
        this.maxAwaitTimeMS = TimeUnit.MILLISECONDS.convert(maxAwaitTime, timeUnit);
        return this;
    }

    /**
     * Gets the collation options.
     *
     * @return the collation, or null if not set
     */
    @Nullable
    public Collation getCollation() {
        return collation;
    }

    /**
     * Sets the collation options.
     *
     * @param collation the collation
     * @return this
     */
    public ChangeStreamOptions collation(@Nullable final Collation collation) {
        this.collation = collation;
        return this;
    }

    /**
     * Gets the comment.
     *
     * @return the comment, or null if not set
     */
    @Nullable
    public BsonValue getComment() {
        return comment;
    }

    /**
     * Sets the comment.
     *
     * @param comment the comment
     * @return this
     */
    public ChangeStreamOptions comment(@Nullable final BsonValue comment) {
        this.comment = comment;
        return this;
    }

    @Override
    public String toString() {
        return "ChangeStreamOptions{"
                + "fullDocument=" + fullDocument
                + ", fullDocumentBeforeChange=" + fullDocumentBeforeChange
                + ", resumeAfter=" + resumeAfter
                + ", startAfter=" + startAfter
                + ", startAtOperationTime=" + startAtOperationTime
                + ", batchSize=" + batchSize
                + ", maxAwaitTimeMS=" + maxAwaitTimeMS
                + ", collation=" + collation
                + ", comment=" + comment
                + '}';
    }
}

