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

package org.bson.codecs;

import org.bson.BsonBinary;
import org.bson.BsonBinarySubType;
import org.bson.BsonInvalidOperationException;
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.BinaryVector;

/**
 * Encodes and decodes {@link BinaryVector} objects.
 *
 */
 final class BinaryVectorCodec implements Codec<BinaryVector> {

    @Override
    public void encode(final BsonWriter writer, final BinaryVector vectorToEncode, final EncoderContext encoderContext) {
        writer.writeBinaryData(new BsonBinary(vectorToEncode));
    }

    @Override
    public BinaryVector decode(final BsonReader reader, final DecoderContext decoderContext) {
        byte subType = reader.peekBinarySubType();

        if (subType != BsonBinarySubType.VECTOR.getValue()) {
            throw new BsonInvalidOperationException("Expected vector binary subtype " + BsonBinarySubType.VECTOR.getValue() + " but found " + subType);
        }

        return reader.readBinaryData()
                .asBinary()
                .asVector();
    }

    @Override
    public Class<BinaryVector> getEncoderClass() {
        return BinaryVector.class;
    }
}


