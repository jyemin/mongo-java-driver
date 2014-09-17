/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package com.mongodb.protocol.message;

import com.mongodb.annotations.Immutable;

/**
 * The message settings
 *
 * @since 3.0
 */
@Immutable
public final class MessageSettings {
    private static final int DEFAULT_MAX_DOCUMENT_SIZE = 0x1000000;  // 16MB
    private static final int DEFAULT_MAX_MESSAGE_SIZE = 0x2000000;   // 32MB
    private static final int DEFAULT_MAX_WRITE_BATCH_SIZE = 1000;

    private final int maxDocumentSize;
    private final int maxMessageSize;
    private final int maxWriteBatchSize;

    /**
     * Gets the builder
     *
     * @return the builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * A MessageSettings builder.
     */
    public static final class Builder {
        private int maxDocumentSize = DEFAULT_MAX_DOCUMENT_SIZE;
        private int maxMessageSize = DEFAULT_MAX_MESSAGE_SIZE;
        private int maxWriteBatchSize = DEFAULT_MAX_WRITE_BATCH_SIZE;

        /**
         * Build it.
         *
         * @return the message settings
         */
        public MessageSettings build() {
            return new MessageSettings(this);
        }

        /**
         * Sets the maximum document size allowed.
         *
         * @param maxDocumentSize the maximum document size allowed
         * @return this
         */
        public Builder maxDocumentSize(final int maxDocumentSize) {
            this.maxDocumentSize = maxDocumentSize;
            return this;
        }

        /**
         * Sets the maximum message size allowed.
         *
         * @param maxMessageSize the maximum message size allowed
         * @return this
         */
        public Builder maxMessageSize(final int maxMessageSize) {
            this.maxMessageSize = maxMessageSize;
            return this;
        }

        /**
         * Sets the maximum write batch size allowed.
         *
         * @param maxWriteBatchSize the maximum write batch size allowed
         * @return this
         */
        public Builder maxWriteBatchSize(final int maxWriteBatchSize) {
            this.maxWriteBatchSize = maxWriteBatchSize;
            return this;
        }
    }

    /**
     * Gets the maximum document size allowed.
     *
     * @return the maximum document size allowed
     */
    public int getMaxDocumentSize() {
        return maxDocumentSize;
    }

    /**
     * Gets the maximum message size allowed.
     *
     * @return the maximum message size allowed
     */
    public int getMaxMessageSize() {
        return maxMessageSize;
    }

    /**
     * Gets the maximum write batch size allowed.
     *
     * @return the maximum write batch size allowed
     */
    public int getMaxWriteBatchSize() {
        return maxWriteBatchSize;
    }

    private MessageSettings(final Builder builder) {
        this.maxDocumentSize = builder.maxDocumentSize;
        this.maxMessageSize = builder.maxMessageSize;
        this.maxWriteBatchSize = builder.maxWriteBatchSize;
    }
}