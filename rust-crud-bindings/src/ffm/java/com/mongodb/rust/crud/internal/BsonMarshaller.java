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
import org.bson.BsonArray;
import org.bson.BsonBinaryReader;
import org.bson.BsonBinaryWriter;
import org.bson.BsonDocument;
import org.bson.BsonValue;
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
     * Allocates and populates a Bson struct from a BsonArray.
     * Wraps the array in a document with empty key "" since BSON requires a document at the root.
     * The FFI layer unwraps by getting the first field's value.
     */
    public static MemorySegment toBsonArrayStruct(Arena arena, BsonArray array) {
        // BSON requires a document at the root, so wrap the array
        // Use empty key to match Rust FFI test convention
        BsonDocument wrapper = new BsonDocument("", array);
        return toBsonStruct(arena, wrapper);
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
     * The value is wrapped in a document {"": value} and the value bytes are extracted.
     */
    public static MemorySegment toBsonValueStruct(Arena arena, BsonValue value) {
        // Wrap in document to serialize
        BsonDocument wrapper = new BsonDocument("", value);
        try (ByteBufferBsonOutput buffer = new ByteBufferBsonOutput(PowerOfTwoBufferPool.DEFAULT)) {
            encodeToBuffer(wrapper, buffer);

            // Document structure: [4-byte len][type byte][key "\0"][value bytes][null terminator]
            // For key "", the key is just a single null byte
            // So: bytes[0..4] = length, bytes[4] = type, bytes[5] = '\0' (empty key), bytes[6..len-1] = value
            List<ByteBuf> byteBuffers = buffer.getByteBuffers();
            int totalSize = buffer.getSize();
            int valueLen = totalSize - 6 - 1; // Skip header (6 bytes) and trailing null (1 byte)

            ByteBuf firstBuf = byteBuffers.getFirst();
            ByteBuf lastBuf = byteBuffers.getLast();

            // Skip first 6 bytes (position the first buffer past the header)
            ByteBuffer firstNio = firstBuf.asNIO();
            firstNio.position(firstNio.position() + 6);

            // Exclude last byte (limit the last buffer to exclude trailing null)
            ByteBuffer lastNio = lastBuf.asNIO();
            lastNio.limit(lastNio.limit() - 1);

            // Get type byte at absolute position 4 from first buffer
            byte bsonType = firstNio.get(4);

            // Copy value bytes to segment
            MemorySegment dataSegment = arena.allocate(valueLen);
            int offset = 0;
            for (ByteBuf buf : byteBuffers) {
                ByteBuffer nio = buf.asNIO();
                int len = nio.remaining();
                if (len > 0) {
                    dataSegment.asSlice(offset, len).copyFrom(MemorySegment.ofBuffer(nio));
                    offset += len;
                }
            }

            MemorySegment bsonValueStruct = com.mongodb.internal.rust.crud.ffi.BsonValue.allocate(arena);
            com.mongodb.internal.rust.crud.ffi.BsonValue.data(bsonValueStruct, dataSegment);
            com.mongodb.internal.rust.crud.ffi.BsonValue.len(bsonValueStruct, valueLen);
            com.mongodb.internal.rust.crud.ffi.BsonValue.bson_type(bsonValueStruct, bsonType);
            return bsonValueStruct;
        }
    }

    /**
     * Reads a BsonValue from a BsonValue FFI struct.
     * The struct contains raw value bytes plus a type byte. We reconstruct the value
     * by wrapping it back in a document and decoding.
     */
    public static org.bson.BsonValue fromBsonValueStruct(MemorySegment bsonValueStruct) {
        if (bsonValueStruct == null || bsonValueStruct.equals(MemorySegment.NULL)) {
            return null;
        }

        MemorySegment dataPtr = com.mongodb.internal.rust.crud.ffi.BsonValue.data(bsonValueStruct);
        long len = com.mongodb.internal.rust.crud.ffi.BsonValue.len(bsonValueStruct);
        byte bsonType = com.mongodb.internal.rust.crud.ffi.BsonValue.bson_type(bsonValueStruct);

        if (dataPtr.equals(MemorySegment.NULL) || len == 0) {
            return null;
        }

        // Reconstruct a BSON document: [4-byte len][type][key "\0"][value][null]
        // Total length = 4 (length) + 1 (type) + 1 (empty key null) + len (value) + 1 (doc null)
        int docLen = 4 + 1 + 1 + (int) len + 1;
        byte[] docBytes = new byte[docLen];

        // Little-endian document length
        docBytes[0] = (byte) (docLen & 0xFF);
        docBytes[1] = (byte) ((docLen >> 8) & 0xFF);
        docBytes[2] = (byte) ((docLen >> 16) & 0xFF);
        docBytes[3] = (byte) ((docLen >> 24) & 0xFF);

        // Type byte
        docBytes[4] = bsonType;

        // Empty key (just null terminator)
        docBytes[5] = 0;

        // Copy value bytes
        dataPtr.reinterpret(len).asByteBuffer().get(docBytes, 6, (int) len);

        // Document null terminator
        docBytes[docLen - 1] = 0;

        // Decode and extract the value
        BsonDocument doc = decode(docBytes);
        return doc.get("");
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

