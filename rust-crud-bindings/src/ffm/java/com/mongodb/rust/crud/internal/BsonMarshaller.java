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
import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.Decoder;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.Encoder;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.io.BsonOutput;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;

/**
 * Utilities for marshalling BSON data between Java and FFI.
 */
public final class BsonMarshaller {

    private static final CodecRegistry REGISTRY = fromProviders(new BsonValueCodecProvider());
    private static final BsonDocumentCodec BSON_DOCUMENT_CODEC = new BsonDocumentCodec();

    private BsonMarshaller() {
    }

    /**
     * Decodes raw bytes to a BsonDocument.
     */
    public static BsonDocument decode(byte[] bytes) {
        try (BsonBinaryReader reader = new BsonBinaryReader(ByteBuffer.wrap(bytes))) {
            return BSON_DOCUMENT_CODEC.decode(reader, DecoderContext.builder().build());
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
     * Uses a direct ByteBuffer to avoid copying the bytes.
     */
    public static <T> T decode(MemorySegment data, long len, Decoder<T> decoder) {
        ByteBuffer byteBuffer = data.reinterpret(len).asByteBuffer().order(ByteOrder.LITTLE_ENDIAN);
        try (BsonBinaryReader reader = new BsonBinaryReader(byteBuffer)) {
            return decoder.decode(reader, DecoderContext.builder().build());
        }
    }

    // ==================== FFI Struct Methods (STUBBED) ====================
    // Timing for encode vs copy
    public static final boolean MARSHAL_TIMING = false;
    private static long encodeTime, copyTime, marshalCount;

    public static void printMarshalTimings() {
        if (!MARSHAL_TIMING || marshalCount == 0) return;
        System.out.printf("BsonMarshaller timings (avg over %d calls):%n", marshalCount);
        System.out.printf("  BSON encode:     %.4f ms%n", encodeTime / 1_000_000.0 / marshalCount);
        System.out.printf("  Copy to native:  %.4f ms%n", copyTime / 1_000_000.0 / marshalCount);
    }

    static {
        if (MARSHAL_TIMING) {
            Runtime.getRuntime().addShutdownHook(new Thread(BsonMarshaller::printMarshalTimings));
        }
    }

    /**
     * Allocates and populates a Bson struct in native memory for insert operations.
     * Uses isEncodingCollectibleDocument(true) to ensure _id is written first in the byte stream.
     */
    public static MemorySegment toBsonStructForInsert(Arena arena, BsonDocument document) {
        return toBsonStruct(arena, document, true);
    }

    /**
     * Allocates and populates a Bson struct in native memory.
     */
    public static MemorySegment toBsonStruct(Arena arena, BsonDocument document) {
        return toBsonStruct(arena, document, false);
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
     * Decodes documents from a BsonArray FFI struct using the provided decoder.
     *
     * <p>The BsonArray struct contains:
     * <ul>
     *   <li>{@code data} - pointer to an array of pointers to raw BSON documents</li>
     *   <li>{@code len} - number of documents</li>
     * </ul>
     *
     * @param bsonArrayStruct the BsonArray FFI struct (embedded in parent struct, not a pointer)
     * @param decoder the decoder to use for each document
     * @param <T> the target type
     * @return list of decoded documents
     */
    public static <T> List<T> fromBsonArrayStruct(MemorySegment bsonArrayStruct, Decoder<T> decoder) {
        long len = com.mongodb.internal.rust.crud.ffi.BsonArray.len(bsonArrayStruct);
        if (len == 0) {
            return List.of();
        }

        MemorySegment dataPtr = com.mongodb.internal.rust.crud.ffi.BsonArray.data(bsonArrayStruct);
        if (dataPtr.equals(MemorySegment.NULL)) {
            return List.of();
        }

        // data is a pointer to an array of pointers (uint8_t**)
        // Reinterpret to access the array of pointers
        MemorySegment pointerArray = dataPtr.reinterpret(len * ValueLayout.ADDRESS.byteSize());

        List<T> results = new ArrayList<>((int) len);

        for (int i = 0; i < len; i++) {
            // Read the pointer at index i
            MemorySegment docPtr = pointerArray.getAtIndex(ValueLayout.ADDRESS, i);

            // Read the BSON document size (first 4 bytes, little-endian int32)
            int docSize = docPtr.reinterpret(4).get(ValueLayout.JAVA_INT_UNALIGNED, 0);

            // Decode directly from native memory (no copy)
            results.add(decode(docPtr, docSize, decoder));
        }

        return results;
    }

    /**
     * Creates a BsonArray struct from a list of BsonDocuments.
     *
     * <p>The BsonArray struct contains:
     * <ul>
     *   <li>{@code data} - pointer to an array of pointers to raw BSON documents</li>
     *   <li>{@code len} - number of documents</li>
     * </ul>
     *
     * <p>This method is optimized to use a single native memory allocation for all document bytes,
     * with the pointer array pointing into offsets within that allocation.
     *
     * @param arena the arena for memory allocation
     * @param documents the documents to convert
     * @return the BsonArray struct
     */
    public static MemorySegment toDocumentBsonArrayStruct(Arena arena, List<BsonDocument> documents) {
        return toDocumentBsonArrayStruct(arena, documents, false);
    }

    /**
     * Creates a BsonArray struct from a list of BsonDocuments for insert operations.
     * Uses isEncodingCollectibleDocument(true) to ensure _id is written first in each document.
     */
    public static MemorySegment toDocumentBsonArrayStructForInsert(Arena arena, List<BsonDocument> documents) {
        return toDocumentBsonArrayStruct(arena, documents, true);
    }

    private static MemorySegment toDocumentBsonArrayStruct(Arena arena, List<BsonDocument> documents, boolean isCollectibleDocument) {
        MemorySegment bsonArray = com.mongodb.internal.rust.crud.ffi.BsonArray.allocate(arena);

        if (documents.isEmpty()) {
            com.mongodb.internal.rust.crud.ffi.BsonArray.data(bsonArray, MemorySegment.NULL);
            com.mongodb.internal.rust.crud.ffi.BsonArray.len(bsonArray, 0);
            return bsonArray;
        }

        // Encode all documents to a single buffer, tracking each document's start position
        int[] docOffsets = new int[documents.size()];
        try (ByteBufferBsonOutput buffer = new ByteBufferBsonOutput(PowerOfTwoBufferPool.DEFAULT)) {
            for (int i = 0; i < documents.size(); i++) {
                docOffsets[i] = buffer.getPosition();
                encodeToBuffer(documents.get(i), buffer, isCollectibleDocument);
            }

            // Allocate single native segment for all document bytes
            int totalSize = buffer.getSize();
            MemorySegment dataSegment = byteBuffersToSegment(arena, totalSize, buffer.getByteBuffers());

            // Allocate pointer array and point into the data segment
            MemorySegment pointerArray = arena.allocate(ValueLayout.ADDRESS, documents.size());
            for (int i = 0; i < documents.size(); i++) {
                MemorySegment docPtr = dataSegment.asSlice(docOffsets[i]);
                pointerArray.setAtIndex(ValueLayout.ADDRESS, i, docPtr);
            }

            com.mongodb.internal.rust.crud.ffi.BsonArray.data(bsonArray, pointerArray);
            com.mongodb.internal.rust.crud.ffi.BsonArray.len(bsonArray, documents.size());
        }

        return bsonArray;
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

    private static MemorySegment toBsonStruct(Arena arena, BsonDocument document, boolean isCollectibleDocument) {
        long t0 = MARSHAL_TIMING ? System.nanoTime() : 0;
        try (ByteBufferBsonOutput buffer = new ByteBufferBsonOutput(PowerOfTwoBufferPool.DEFAULT)) {
            encodeToBuffer(document, buffer, isCollectibleDocument);
            long t1 = MARSHAL_TIMING ? System.nanoTime() : 0;
            int size = buffer.getSize();
            MemorySegment dataSegment = bsonOutputToSegment(arena, size, buffer);
            MemorySegment bsonStruct = Bson.allocate(arena);
            Bson.data(bsonStruct, dataSegment);
            Bson.len(bsonStruct, size);
            if (MARSHAL_TIMING) {
                long t2 = System.nanoTime();
                encodeTime += (t1 - t0);
                copyTime += (t2 - t1);
                marshalCount++;
            }
            return bsonStruct;
        }
    }

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
        encodeToBuffer(document, buffer, false);
    }

    private static void encodeToBuffer(BsonDocument document, BsonOutput buffer, boolean isCollectibleDocument) {
        try (BsonBinaryWriter writer = new BsonBinaryWriter(buffer)) {
            @SuppressWarnings("unchecked")
            Encoder<BsonDocument> encoder = (Encoder<BsonDocument>) REGISTRY.get(document.getClass());
            encoder.encode(writer, document, EncoderContext.builder()
                    .isEncodingCollectibleDocument(isCollectibleDocument)
                    .build());
        }
    }
}

