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

package com.mongodb.rust.crud.internal;

import com.mongodb.internal.rust.crud.ffi.Bson;
import com.mongodb.internal.rust.crud.ffi.BsonValue;
import com.mongodb.lang.Nullable;
import org.bson.BsonBinaryReader;
import org.bson.BsonBinaryWriter;
import org.bson.BsonDocument;
import org.bson.RawBsonDocument;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;

/**
 * Utilities for marshalling BSON data between Java and FFI.
 */
public final class BsonMarshaller {

    private static final BsonDocumentCodec CODEC = new BsonDocumentCodec();

    private BsonMarshaller() {
    }

    /**
     * Encodes a BsonDocument to raw bytes.
     */
    public static byte[] encode(BsonDocument document) {
        try (BasicOutputBuffer buffer = new BasicOutputBuffer()) {
            encodeToBuffer(document, buffer);
            return buffer.toByteArray();
        }
    }

    private static void encodeToBuffer(BsonDocument document, BasicOutputBuffer buffer) {
        try (BsonBinaryWriter writer = new BsonBinaryWriter(buffer)) {
            CODEC.encode(writer, document, EncoderContext.builder().build());
        }
    }

    /**
     * Decodes raw bytes to a BsonDocument.
     */
    public static BsonDocument decode(byte[] bytes) {
        try (BsonBinaryReader reader = new BsonBinaryReader(ByteBuffer.wrap(bytes))) {
            return CODEC.decode(reader, DecoderContext.builder().build());
        }
    }

    /**
     * Decodes a BSON document from a MemorySegment.
     */
    public static BsonDocument decode(MemorySegment data, long len) {
        byte[] bytes = data.reinterpret(len).toArray(java.lang.foreign.ValueLayout.JAVA_BYTE);
        return decode(bytes);
    }

    /**
     * Allocates and populates a Bson struct in native memory.
     */
    public static MemorySegment toBsonStruct(Arena arena, BsonDocument document) {
        try (BasicOutputBuffer buffer = new BasicOutputBuffer()) {
            encodeToBuffer(document, buffer);
            int size = buffer.getSize();
            MemorySegment dataSegment = arena.allocate(size);
            dataSegment.copyFrom(MemorySegment.ofArray(buffer.getInternalBuffer()).asSlice(0, size));

            MemorySegment bsonStruct = Bson.allocate(arena);
            Bson.data(bsonStruct, dataSegment);
            Bson.len(bsonStruct, size);
            return bsonStruct;
        }
    }

    /**
     * Allocates and populates a Bson struct from a Bson interface (e.g., filter, update).
     */
    public static MemorySegment toBsonStruct(Arena arena, org.bson.conversions.Bson bson) {
        return toBsonStruct(arena, bson.toBsonDocument());
    }

    /**
     * Reads a BsonDocument from a Bson FFI struct.
     */
    @Nullable
    public static BsonDocument fromBsonStruct(MemorySegment bsonStruct) {
        if (bsonStruct == null || bsonStruct.equals(MemorySegment.NULL)) {
            return null;
        }
        MemorySegment data = Bson.data(bsonStruct);
        long len = Bson.len(bsonStruct);
        if (data.equals(MemorySegment.NULL) || len == 0) {
            return null;
        }
        return decode(data, len);
    }

    /**
     * Allocates and populates a BsonValue struct for a typed BSON value.
     */
    public static MemorySegment toBsonValueStruct(Arena arena, org.bson.BsonValue value) {
        // Wrap the value in a document to encode it
        BsonDocument wrapper = new BsonDocument("v", value);
        byte[] docBytes = encode(wrapper);
        
        // The value starts at offset 7: 4 (doc size) + 1 (type) + 2 ("v\0")
        int valueOffset = 7;
        int valueLen = docBytes.length - valueOffset - 1; // -1 for trailing null
        
        byte[] valueBytes = new byte[valueLen];
        System.arraycopy(docBytes, valueOffset, valueBytes, 0, valueLen);
        
        MemorySegment dataSegment = arena.allocate(valueBytes.length);
        dataSegment.copyFrom(MemorySegment.ofArray(valueBytes));
        
        MemorySegment bsonValueStruct = BsonValue.allocate(arena);
        BsonValue.data(bsonValueStruct, dataSegment);
        BsonValue.len(bsonValueStruct, valueBytes.length);
        BsonValue.bson_type(bsonValueStruct, (byte) value.getBsonType().getValue());
        return bsonValueStruct;
    }

    /**
     * Reads a BsonValue from a BsonValue FFI struct.
     */
    @Nullable
    public static org.bson.BsonValue fromBsonValueStruct(MemorySegment bsonValueStruct) {
        if (bsonValueStruct == null || bsonValueStruct.equals(MemorySegment.NULL)) {
            return null;
        }
        MemorySegment data = BsonValue.data(bsonValueStruct);
        long len = BsonValue.len(bsonValueStruct);
        byte bsonType = BsonValue.bson_type(bsonValueStruct);
        
        if (data.equals(MemorySegment.NULL) || len == 0) {
            return null;
        }
        
        // Read the raw value bytes and decode using RawBsonDocument.getValue
        byte[] valueBytes = data.reinterpret(len).toArray(java.lang.foreign.ValueLayout.JAVA_BYTE);
        return RawBsonDocument.getValue(bsonType, valueBytes);
    }
}

