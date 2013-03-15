/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

package com.mongodb.serializers.json;

import org.bson.BSONBinarySubType;
import org.bson.BSONReader;
import org.bson.BSONWriter;
import org.bson.types.Binary;
import org.mongodb.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.UUID;

class UUIDSerializer implements Serializer<UUID> {

    @Override
    public void serialize(final BSONWriter bsonWriter, final UUID value) {
        bsonWriter.writeBinaryData(new Binary(BSONBinarySubType.UuidLegacy, convertToByteArray(value)));
    }

    @Override
    public UUID deserialize(final BSONReader reader) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Class<UUID> getSerializationClass() {
        return UUID.class;
    }

    private byte[] convertToByteArray(final UUID value) {
        ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
        bb.putLong(value.getMostSignificantBits());
        bb.putLong(value.getLeastSignificantBits());
        return bb.array();
    }
}
