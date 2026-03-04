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

package com.mongodb.rust.crud;

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.lang.Nullable;

/**
 * Context for native client operations containing operation-level settings.
 *
 * <p>This class encapsulates the settings that can be applied to individual operations,
 * including read preference, write concern, read concern, and timeout.</p>
 *
 * <p>Instances are immutable and should be created using the {@link Builder}.</p>
 */
public final class NativeOperationContext {

    @Nullable
    private final ReadPreference readPreference;
    @Nullable
    private final WriteConcern writeConcern;
    @Nullable
    private final ReadConcern readConcern;
    @Nullable
    private final Long timeoutMs;

    private NativeOperationContext(final Builder builder) {
        this.readPreference = builder.readPreference;
        this.writeConcern = builder.writeConcern;
        this.readConcern = builder.readConcern;
        this.timeoutMs = builder.timeoutMs;
    }

    /**
     * Gets the read preference for this operation.
     *
     * @return the read preference, or null to use the client default
     */
    @Nullable
    public ReadPreference getReadPreference() {
        return readPreference;
    }

    /**
     * Gets the write concern for this operation.
     *
     * @return the write concern, or null to use the client default
     */
    @Nullable
    public WriteConcern getWriteConcern() {
        return writeConcern;
    }

    /**
     * Gets the read concern for this operation.
     *
     * @return the read concern, or null to use the client default
     */
    @Nullable
    public ReadConcern getReadConcern() {
        return readConcern;
    }

    /**
     * Gets the timeout in milliseconds for this operation.
     *
     * @return the timeout in milliseconds, or null to use the client default
     */
    @Nullable
    public Long getTimeoutMs() {
        return timeoutMs;
    }

    /**
     * Creates a new builder.
     *
     * @return a new builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for {@link NativeOperationContext}.
     */
    public static final class Builder {
        @Nullable
        private ReadPreference readPreference;
        @Nullable
        private WriteConcern writeConcern;
        @Nullable
        private ReadConcern readConcern;
        @Nullable
        private Long timeoutMs;

        private Builder() {
        }

        /**
         * Sets the read preference.
         *
         * @param readPreference the read preference, or null to use client default
         * @return this
         */
        public Builder readPreference(@Nullable final ReadPreference readPreference) {
            this.readPreference = readPreference;
            return this;
        }

        /**
         * Sets the write concern.
         *
         * @param writeConcern the write concern, or null to use client default
         * @return this
         */
        public Builder writeConcern(@Nullable final WriteConcern writeConcern) {
            this.writeConcern = writeConcern;
            return this;
        }

        /**
         * Sets the read concern.
         *
         * @param readConcern the read concern, or null to use client default
         * @return this
         */
        public Builder readConcern(@Nullable final ReadConcern readConcern) {
            this.readConcern = readConcern;
            return this;
        }

        /**
         * Sets the timeout in milliseconds.
         *
         * @param timeoutMs the timeout in milliseconds, or null to use client default
         * @return this
         */
        public Builder timeoutMs(@Nullable final Long timeoutMs) {
            this.timeoutMs = timeoutMs;
            return this;
        }

        /**
         * Builds the operation context.
         *
         * @return the operation context
         */
        public NativeOperationContext build() {
            return new NativeOperationContext(this);
        }
    }
}

