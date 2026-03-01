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
import org.bson.BsonValue;
import org.bson.conversions.Bson;

import java.util.concurrent.TimeUnit;

/**
 * Options for listing collections.
 * Mirrors the options available on ListCollectionsIterable.
 */
public final class ListCollectionsOptions {
    private Bson filter;
    private int batchSize;
    private long maxTimeMS;
    private BsonValue comment;
    private Boolean authorizedCollections;

    /**
     * Creates a new instance with default values.
     */
    public ListCollectionsOptions() {
    }

    /**
     * Gets the filter to apply.
     * @return the filter, or null
     */
    @Nullable
    public Bson getFilter() {
        return filter;
    }

    /**
     * Sets the filter to apply.
     * @param filter the filter
     * @return this
     */
    public ListCollectionsOptions filter(@Nullable final Bson filter) {
        this.filter = filter;
        return this;
    }

    /**
     * Gets the batch size.
     * @return the batch size
     */
    public int getBatchSize() {
        return batchSize;
    }

    /**
     * Sets the batch size.
     * @param batchSize the batch size
     * @return this
     */
    public ListCollectionsOptions batchSize(final int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    /**
     * Gets the maximum execution time in milliseconds.
     * @return the max time in milliseconds
     */
    public long getMaxTimeMS() {
        return maxTimeMS;
    }

    /**
     * Sets the maximum execution time.
     * @param maxTime the max time
     * @param timeUnit the time unit
     * @return this
     */
    public ListCollectionsOptions maxTime(final long maxTime, final TimeUnit timeUnit) {
        this.maxTimeMS = TimeUnit.MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    /**
     * Gets the comment.
     * @return the comment, or null
     */
    @Nullable
    public BsonValue getComment() {
        return comment;
    }

    /**
     * Sets the comment.
     * @param comment the comment
     * @return this
     */
    public ListCollectionsOptions comment(@Nullable final BsonValue comment) {
        this.comment = comment;
        return this;
    }

    /**
     * Gets whether to only return collections the user is authorized to access.
     * @return authorizedCollections, or null if not set
     */
    @Nullable
    public Boolean getAuthorizedCollections() {
        return authorizedCollections;
    }

    /**
     * Sets whether to only return collections the user is authorized to access.
     * @param authorizedCollections true to only return authorized collections
     * @return this
     */
    public ListCollectionsOptions authorizedCollections(@Nullable final Boolean authorizedCollections) {
        this.authorizedCollections = authorizedCollections;
        return this;
    }
}

