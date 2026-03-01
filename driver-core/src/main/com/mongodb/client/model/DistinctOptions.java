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

import com.mongodb.lang.Nullable;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.conversions.Bson;

import java.util.concurrent.TimeUnit;

/**
 * The options to apply to a distinct operation.
 *
 * @since 5.4
 * @mongodb.driver.manual reference/command/distinct/ Distinct Command
 */
public final class DistinctOptions {
    private long maxTimeMS = -1;
    private int batchSize = -1;
    private Collation collation;
    private BsonValue comment;
    private Bson hint;
    private String hintString;

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
    public DistinctOptions maxTime(final long maxTime, final TimeUnit timeUnit) {
        this.maxTimeMS = TimeUnit.MILLISECONDS.convert(maxTime, timeUnit);
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
    public DistinctOptions collation(@Nullable final Collation collation) {
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
    public DistinctOptions comment(@Nullable final String comment) {
        this.comment = comment != null ? new BsonString(comment) : null;
        return this;
    }

    /**
     * Sets the comment.
     *
     * @param comment the comment
     * @return this
     */
    public DistinctOptions comment(@Nullable final BsonValue comment) {
        this.comment = comment;
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
    public DistinctOptions batchSize(final int batchSize) {
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
    public DistinctOptions hint(@Nullable final Bson hint) {
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
    public DistinctOptions hintString(@Nullable final String hintString) {
        this.hintString = hintString;
        return this;
    }

    @Override
    public String toString() {
        return "DistinctOptions{"
                + "maxTimeMS=" + maxTimeMS
                + ", batchSize=" + batchSize
                + ", collation=" + collation
                + ", comment=" + comment
                + ", hint=" + hint
                + ", hintString='" + hintString + '\''
                + '}';
    }
}

