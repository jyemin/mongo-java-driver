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

import com.mongodb.CursorType;
import com.mongodb.lang.Nullable;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.conversions.Bson;

import java.util.concurrent.TimeUnit;

/**
 * The options to apply to a find operation.
 *
 * @since 5.4
 * @mongodb.driver.manual tutorial/query-documents/ Find Tutorial
 * @mongodb.driver.manual reference/command/find/ Find Command
 */
public final class FindOptions {
    private Bson projection;
    private Bson sort;
    private long limit = -1;
    private long skip = -1;
    private int batchSize = -1;
    private Bson hint;
    private String hintString;
    private Collation collation;
    private BsonValue comment;
    private long maxTimeMS = -1;
    private long maxAwaitTimeMS = -1;
    private CursorType cursorType;
    private boolean noCursorTimeout;
    private boolean partial;
    private Bson let;
    private Bson max;
    private Bson min;
    private boolean returnKey;
    private boolean showRecordId;
    private Boolean allowDiskUse;

    /**
     * Gets the projection.
     *
     * @return the projection, or null if not set
     */
    @Nullable
    public Bson getProjection() {
        return projection;
    }

    /**
     * Sets the projection.
     *
     * @param projection the projection
     * @return this
     * @mongodb.driver.manual reference/method/db.collection.find/ Projection
     */
    public FindOptions projection(@Nullable final Bson projection) {
        this.projection = projection;
        return this;
    }

    /**
     * Gets the sort criteria.
     *
     * @return the sort criteria, or null if not set
     */
    @Nullable
    public Bson getSort() {
        return sort;
    }

    /**
     * Sets the sort criteria.
     *
     * @param sort the sort criteria
     * @return this
     */
    public FindOptions sort(@Nullable final Bson sort) {
        this.sort = sort;
        return this;
    }

    /**
     * Gets the limit.
     *
     * @return the limit, or -1 if not set
     */
    public long getLimit() {
        return limit;
    }

    /**
     * Sets the maximum number of documents to return.
     *
     * @param limit the limit
     * @return this
     */
    public FindOptions limit(final long limit) {
        this.limit = limit;
        return this;
    }

    /**
     * Gets the number of documents to skip.
     *
     * @return the skip value, or -1 if not set
     */
    public long getSkip() {
        return skip;
    }

    /**
     * Sets the number of documents to skip.
     *
     * @param skip the number of documents to skip
     * @return this
     */
    public FindOptions skip(final long skip) {
        this.skip = skip;
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
    public FindOptions batchSize(final int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    /**
     * Gets the hint for which index to use.
     *
     * @return the hint, or null if not set
     */
    @Nullable
    public Bson getHint() {
        return hint;
    }

    /**
     * Sets the hint for which index to use.
     *
     * @param hint the hint
     * @return this
     */
    public FindOptions hint(@Nullable final Bson hint) {
        this.hint = hint;
        return this;
    }

    /**
     * Gets the hint string for which index to use.
     *
     * @return the hint string, or null if not set
     */
    @Nullable
    public String getHintString() {
        return hintString;
    }

    /**
     * Sets the hint string for which index to use.
     *
     * @param hintString the hint string
     * @return this
     */
    public FindOptions hintString(@Nullable final String hintString) {
        this.hintString = hintString;
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
     * @mongodb.server.release 3.4
     */
    public FindOptions collation(@Nullable final Collation collation) {
        this.collation = collation;
        return this;
    }

    /**
     * Gets the comment for this operation.
     *
     * @return the comment, or null if not set
     * @mongodb.server.release 4.4
     */
    @Nullable
    public BsonValue getComment() {
        return comment;
    }

    /**
     * Sets the comment for this operation.
     *
     * @param comment the comment
     * @return this
     * @mongodb.server.release 4.4
     */
    public FindOptions comment(@Nullable final String comment) {
        this.comment = comment != null ? new BsonString(comment) : null;
        return this;
    }

    /**
     * Sets the comment for this operation.
     *
     * @param comment the comment
     * @return this
     * @mongodb.server.release 4.4
     */
    public FindOptions comment(@Nullable final BsonValue comment) {
        this.comment = comment;
        return this;
    }

    /**
     * Gets the maximum execution time in milliseconds.
     *
     * @return the max time in milliseconds, or -1 if not set
     */
    public long getMaxTimeMS() {
        return maxTimeMS;
    }

    /**
     * Sets the maximum execution time.
     *
     * @param maxTime the max time
     * @param timeUnit the time unit
     * @return this
     */
    public FindOptions maxTime(final long maxTime, final TimeUnit timeUnit) {
        this.maxTimeMS = TimeUnit.MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    /**
     * Gets the maximum await time in milliseconds.
     *
     * @return the max await time in milliseconds, or -1 if not set
     */
    public long getMaxAwaitTimeMS() {
        return maxAwaitTimeMS;
    }

    /**
     * Sets the maximum await time for tailable cursors.
     *
     * @param maxAwaitTime the max await time
     * @param timeUnit the time unit
     * @return this
     */
    public FindOptions maxAwaitTime(final long maxAwaitTime, final TimeUnit timeUnit) {
        this.maxAwaitTimeMS = TimeUnit.MILLISECONDS.convert(maxAwaitTime, timeUnit);
        return this;
    }

    /**
     * Gets the cursor type.
     *
     * @return the cursor type, or null if not set
     */
    @Nullable
    public CursorType getCursorType() {
        return cursorType;
    }

    /**
     * Sets the cursor type.
     *
     * @param cursorType the cursor type
     * @return this
     */
    public FindOptions cursorType(@Nullable final CursorType cursorType) {
        this.cursorType = cursorType;
        return this;
    }

    /**
     * Gets whether cursor timeout is disabled.
     *
     * @return true if cursor timeout is disabled
     */
    public boolean isNoCursorTimeout() {
        return noCursorTimeout;
    }

    /**
     * Sets whether cursor timeout is disabled.
     *
     * @param noCursorTimeout true to disable cursor timeout
     * @return this
     */
    public FindOptions noCursorTimeout(final boolean noCursorTimeout) {
        this.noCursorTimeout = noCursorTimeout;
        return this;
    }

    /**
     * Gets whether partial results are allowed for sharded clusters.
     *
     * @return true if partial results are allowed
     */
    public boolean isPartial() {
        return partial;
    }

    /**
     * Sets whether partial results are allowed for sharded clusters.
     *
     * @param partial true to allow partial results
     * @return this
     */
    public FindOptions partial(final boolean partial) {
        this.partial = partial;
        return this;
    }

    /**
     * Gets the let variables.
     *
     * @return the let variables, or null if not set
     */
    @Nullable
    public Bson getLet() {
        return let;
    }

    /**
     * Sets the let variables.
     *
     * @param let the let variables
     * @return this
     * @mongodb.server.release 5.0
     */
    public FindOptions let(@Nullable final Bson let) {
        this.let = let;
        return this;
    }

    /**
     * Gets the exclusive upper bound for a specific index.
     *
     * @return the max, or null if not set
     */
    @Nullable
    public Bson getMax() {
        return max;
    }

    /**
     * Sets the exclusive upper bound for a specific index.
     *
     * @param max the max
     * @return this
     */
    public FindOptions max(@Nullable final Bson max) {
        this.max = max;
        return this;
    }

    /**
     * Gets the minimum inclusive lower bound for a specific index.
     *
     * @return the min, or null if not set
     */
    @Nullable
    public Bson getMin() {
        return min;
    }

    /**
     * Sets the minimum inclusive lower bound for a specific index.
     *
     * @param min the min
     * @return this
     */
    public FindOptions min(@Nullable final Bson min) {
        this.min = min;
        return this;
    }

    /**
     * Gets whether to return only index keys.
     *
     * @return true if only index keys should be returned
     */
    public boolean isReturnKey() {
        return returnKey;
    }

    /**
     * Sets whether to return only index keys.
     *
     * @param returnKey true to return only index keys
     * @return this
     */
    public FindOptions returnKey(final boolean returnKey) {
        this.returnKey = returnKey;
        return this;
    }

    /**
     * Gets whether to show the record id.
     *
     * @return true if the record id should be shown
     */
    public boolean isShowRecordId() {
        return showRecordId;
    }

    /**
     * Sets whether to show the record id.
     *
     * @param showRecordId true to show the record id
     * @return this
     */
    public FindOptions showRecordId(final boolean showRecordId) {
        this.showRecordId = showRecordId;
        return this;
    }

    /**
     * Gets whether disk use is allowed.
     *
     * @return true if disk use is allowed, or null if not set
     */
    @Nullable
    public Boolean getAllowDiskUse() {
        return allowDiskUse;
    }

    /**
     * Sets whether disk use is allowed.
     *
     * @param allowDiskUse true to allow disk use
     * @return this
     * @mongodb.server.release 4.4
     */
    public FindOptions allowDiskUse(@Nullable final Boolean allowDiskUse) {
        this.allowDiskUse = allowDiskUse;
        return this;
    }

    @Override
    public String toString() {
        return "FindOptions{"
                + "projection=" + projection
                + ", sort=" + sort
                + ", limit=" + limit
                + ", skip=" + skip
                + ", batchSize=" + batchSize
                + ", hint=" + hint
                + ", hintString='" + hintString + '\''
                + ", collation=" + collation
                + ", comment=" + comment
                + ", maxTimeMS=" + maxTimeMS
                + ", maxAwaitTimeMS=" + maxAwaitTimeMS
                + ", cursorType=" + cursorType
                + ", noCursorTimeout=" + noCursorTimeout
                + ", partial=" + partial
                + ", let=" + let
                + ", max=" + max
                + ", min=" + min
                + ", returnKey=" + returnKey
                + ", showRecordId=" + showRecordId
                + ", allowDiskUse=" + allowDiskUse
                + '}';
    }
}

