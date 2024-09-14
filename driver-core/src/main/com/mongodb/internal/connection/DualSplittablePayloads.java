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

package com.mongodb.internal.connection;

import org.bson.BsonElement;
import org.bson.BsonWriter;
import org.bson.FieldNameValidator;
import org.bson.io.BsonOutput;

import java.util.List;

/**
 * This class is not part of the public API and may be removed or changed at any time.
 */
public abstract class DualSplittablePayloads extends OpMsgSequences {

    private final String firstSequenceId;
    private final FieldNameValidator firstFieldNameValidator;
    private final String secondSequenceId;
    private final FieldNameValidator secondFieldNameValidator;

    protected DualSplittablePayloads(final String firstSequenceId, final FieldNameValidator firstFieldNameValidator,
            final String secondSequenceId, final FieldNameValidator secondFieldNameValidator) {
        this.firstSequenceId = firstSequenceId;
        this.firstFieldNameValidator = firstFieldNameValidator;
        this.secondSequenceId = secondSequenceId;
        this.secondFieldNameValidator = secondFieldNameValidator;
    }

    public FieldNameValidator getFirstFieldNameValidator() {
        return firstFieldNameValidator;
    }

    public FieldNameValidator getSecondFieldNameValidator() {
        return secondFieldNameValidator;
    }

    public String getFirstSequenceId() {
        return firstSequenceId;
    }

    public String getSecondSequenceId() {
        return secondSequenceId;
    }

    public abstract EncodeResult encode(WritersProviderAndLimitsChecker writersProviderAndLimitsChecker);

    /**
     * @see #tryWrite(WriteAction)
     */
    public interface WritersProviderAndLimitsChecker {
        /**
         * Provides writers to the specified {@link WriteAction},
         * {@linkplain WriteAction#doAndGetBatchCount(OpsBsonWriters, BsonWriter) executes} it,
         * checks the {@linkplain MessageSettings limits}.
         */
        WriteResult tryWrite(WriteAction write);

        /**
         * @see #doAndGetBatchCount(OpsBsonWriters, BsonWriter)
         */
        interface WriteAction {
            /**.
             *
             * @return The resulting batch count}.
             */
            int doAndGetBatchCount(OpsBsonWriters firstWriter, BsonWriter secondWriter);
        }

        interface OpsBsonWriters {
            BsonWriter getWriter();

            /**
             * A {@link BsonWriter} to use for writing documents that are intended to be stored in a database.
             * Must write to the same {@linkplain BsonOutput output} as {@link #getWriter()} does.
             */
            BsonWriter getStoredDocumentWriter();
        }

        enum WriteResult {
            FAIL_LIMIT_EXCEEDED,
            OK_LIMIT_REACHED,
            OK_LIMIT_NOT_REACHED
        }
    }

    public static final class EncodeResult {
        private final boolean serverResponseRequired;
        private final List<BsonElement> extraElements;

        public EncodeResult(final boolean serverResponseRequired, final List<BsonElement> extraElements) {
            this.serverResponseRequired = serverResponseRequired;
            this.extraElements = extraElements;
        }

        public boolean isServerResponseRequired() {
            return serverResponseRequired;
        }

        public List<BsonElement> getExtraElements() {
            return extraElements;
        }
    }
}
