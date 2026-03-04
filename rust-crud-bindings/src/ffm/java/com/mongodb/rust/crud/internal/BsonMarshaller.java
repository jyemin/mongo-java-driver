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
import com.mongodb.lang.Nullable;
import org.bson.BsonBinaryReader;
import org.bson.BsonBinaryWriter;
import org.bson.BsonDocument;
import org.bson.ByteBuf;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.Decoder;
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
     * Decodes raw bytes using the provided decoder.
     */
    public static <T> T decode(byte[] bytes, Decoder<T> decoder) {
        try (BsonBinaryReader reader = new BsonBinaryReader(ByteBuffer.wrap(bytes))) {
            return decoder.decode(reader, DecoderContext.builder().build());
        }
    }

    /**
     * Decodes a BSON document from a MemorySegment using the provided decoder.
     */
    public static <T> T decode(MemorySegment data, long len, Decoder<T> decoder) {
        byte[] bytes = data.reinterpret(len).toArray(java.lang.foreign.ValueLayout.JAVA_BYTE);
        return decode(bytes, decoder);
    }

    // ==================== FFI Struct Methods (STUBBED) ====================
    // TODO: Implement when FFI structs are regenerated

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
     * Allocates and populates a Bson struct from a BsonArray.
     * Wraps the array in a document with empty key "" since BSON requires a document at the root.
     * The FFI layer unwraps by getting the first field's value.
     */
    public static MemorySegment toBsonArrayStruct(Arena arena, org.bson.BsonArray array) {
        // BSON requires a document at the root, so wrap the array
        // Use empty key to match Rust FFI test convention
        BsonDocument wrapper = new BsonDocument("", array);
        return toBsonStruct(arena, wrapper);
    }

    public static MemorySegment toBsonBatch(Arena arena, List<BsonDocument> documents) {
        throw new UnsupportedOperationException("FFI: toBsonBatch not yet implemented");
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
        throw new UnsupportedOperationException("FFI: toBsonValueStruct not yet implemented");
    }

    /**
     * Allocates and copies into a MemorySegment representing the bytes that encode the given BSON value.
     */
    public static MemorySegment toBsonValue(Arena arena, org.bson.BsonValue value) {
        throw new UnsupportedOperationException("FFI: toBsonValue not yet implemented");
    }

    /**
     * Reads a BsonValue from a BsonValue FFI struct.
     */
    public static org.bson.BsonValue fromBsonValueStruct(MemorySegment bsonValueStruct) {
        throw new UnsupportedOperationException("FFI: fromBsonValueStruct not yet implemented");
    }

    // ==================== Private Helpers ====================

    private static MemorySegment bsonOutputToSegment(Arena arena, int size, ByteBufferBsonOutput buffer) {
        return byteBuffersToSegment(arena, size, buffer.getByteBuffers());
    }

    private static MemorySegment byteBuffersToSegment(Arena arena, int size, List<ByteBuf> buffers) {
        MemorySegment dataSegment = arena.allocate(size);
        int offset = 0;
        for (var cur : buffers) {
            java.nio.ByteBuffer nio = cur.asNIO();
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
}

