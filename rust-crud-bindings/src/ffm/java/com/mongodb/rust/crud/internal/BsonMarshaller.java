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

import com.mongodb.internal.connection.ByteBufferBsonOutput;
import com.mongodb.internal.connection.PowerOfTwoBufferPool;
import com.mongodb.internal.rust.crud.ffi.Bson;
import com.mongodb.internal.rust.crud.ffi.BsonBatch;
import com.mongodb.internal.rust.crud.ffi.BsonValue;
import com.mongodb.lang.Nullable;
import org.bson.BsonArray;
import org.bson.BsonBinaryReader;
import org.bson.BsonBinaryWriter;
import org.bson.BsonDocument;
import org.bson.ByteBuf;
import org.bson.RawBsonDocument;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.io.BsonOutput;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Utilities for marshalling BSON data between Java and FFI.
 */
public final class BsonMarshaller {

    private static final BsonDocumentCodec CODEC = new BsonDocumentCodec();

    private BsonMarshaller() {
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
        try (ByteBufferBsonOutput buffer = new ByteBufferBsonOutput(PowerOfTwoBufferPool.DEFAULT)) {
            encodeToBuffer(document, buffer);
            int size = buffer.getSize();
            MemorySegment dataSegment = bsonOutputToSegment(arena, size, buffer);
            MemorySegment bsonStruct = Bson.allocate(arena);
            Bson.data(bsonStruct, dataSegment);
            Bson.len(bsonStruct, buffer.size());
            return bsonStruct;
        }
    }

    public static MemorySegment toBsonBatch(Arena arena, List<BsonDocument> documents) {
        try (ByteBufferBsonOutput bsonOutput = new ByteBufferBsonOutput(PowerOfTwoBufferPool.DEFAULT)) {
            int[] offsets = encodeBufferWithOffsets(documents, bsonOutput);
            MemorySegment bsonBatchStruct = BsonBatch.allocate(arena);
            BsonBatch.data(bsonBatchStruct, bsonOutputToSegment(arena, bsonOutput.getSize(), bsonOutput));
            BsonBatch.len(bsonBatchStruct, bsonOutput.size());
            BsonBatch.offsets(bsonBatchStruct, offsetsToSegment(arena, documents, offsets));
            BsonBatch.count(bsonBatchStruct, documents.size());

            return bsonBatchStruct;
        }
    }

    private static int[] encodeBufferWithOffsets(List<BsonDocument> documents, ByteBufferBsonOutput bsonOutput) {
        int[] offsets = new int[documents.size()];
        for (int i = 0; i < documents.size(); i++) {
            offsets[i] = bsonOutput.getPosition();
            encodeToBuffer(documents.get(i), bsonOutput);
        }
        return offsets;
    }

    private static MemorySegment offsetsToSegment(Arena arena, List<BsonDocument> documents, int[] offsets) {
        MemorySegment offsetsSegment = arena.allocate((long) documents.size() * Integer.BYTES);
        for (int i = 0; i < offsets.length; i++) {
            offsetsSegment.setAtIndex(java.lang.foreign.ValueLayout.JAVA_INT, i, offsets[i]);
        }
        return offsetsSegment;
    }

    private static MemorySegment bsonOutputToSegment(Arena arena, int size, ByteBufferBsonOutput buffer) {
        return byteBuffersToSegment(arena, size, buffer.getByteBuffers());
    }

    private static MemorySegment byteBuffersToSegment(Arena arena, int size, List<ByteBuf> buffers) {
        MemorySegment dataSegment = arena.allocate(size);
        int offset = 0;
        for (var cur : buffers) {
            ByteBuffer nio = cur.asNIO();
            int len = nio.remaining();
            dataSegment.asSlice(offset, len).copyFrom(MemorySegment.ofBuffer(nio));
            offset += len;
        }
        return dataSegment;
    }

    private static void encodeToBuffer(BsonDocument document, BsonOutput buffer) {
        try (BsonBinaryWriter writer = new BsonBinaryWriter(buffer)) {
            CODEC.encode(writer, document, EncoderContext.builder().build());
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

    private static final int EXCESS_BYTES_FROM_DOCUMENT_WRAPPER = 8;
    /**
     * Allocates and populates a BsonValue struct for a typed BSON value.
     */
    public static MemorySegment toBsonValueStruct(Arena arena, org.bson.BsonValue value) {
        try (ByteBufferBsonOutput bsonOutput = new ByteBufferBsonOutput(PowerOfTwoBufferPool.DEFAULT)) {
            MemorySegment dataSegment = toBsonValue(arena, value, bsonOutput);

            MemorySegment bsonValueStruct = BsonValue.allocate(arena);
            BsonValue.data(bsonValueStruct, dataSegment);
            BsonValue.len(bsonValueStruct, bsonOutput.size() - EXCESS_BYTES_FROM_DOCUMENT_WRAPPER);
            BsonValue.bson_type(bsonValueStruct, (byte) value.getBsonType().getValue());
            return bsonValueStruct;
        }
    }

    /**
     * Allocates and copies into a MemorySegment representing the bytes that encode the given BSON value.
     */
    public static MemorySegment toBsonValue(Arena arena, org.bson.BsonValue value) {
        try (ByteBufferBsonOutput bsonOutput = new ByteBufferBsonOutput(PowerOfTwoBufferPool.DEFAULT)) {
            return toBsonValue(arena, value, bsonOutput);
        }
    }

    private static MemorySegment toBsonValue(Arena arena, org.bson.BsonValue value, ByteBufferBsonOutput bsonOutput) {
        // Wrap the value in a document to encode it
        BsonDocument wrapper = new BsonDocument("v", value);
        encodeToBuffer(wrapper, bsonOutput);

        // The value starts at offset 7: 4 (doc size) + 1 (type) + 2 ("v\0")
        int valueOffset = 7;
        int valueLength = bsonOutput.size() - valueOffset - 1; // -1 for trailing null

        List<ByteBuf> byteBuffers = bsonOutput.getByteBuffers();
        // Assumes first buffer's limit is larger than 7
        byteBuffers.getFirst().asNIO().position(valueOffset);
        ByteBuffer lastBuffer = byteBuffers.getLast().asNIO();
        lastBuffer.limit(lastBuffer.limit() - 1);
        return byteBuffersToSegment(arena, valueLength, byteBuffers);
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

