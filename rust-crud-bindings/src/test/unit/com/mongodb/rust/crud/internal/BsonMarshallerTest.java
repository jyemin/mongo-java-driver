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
import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class BsonMarshallerTest {

    private static final BsonDocumentCodec CODEC = new BsonDocumentCodec();

    @Test
    void testDecodeBytes() {
        BsonDocument original = new BsonDocument("key", new BsonString("value"));
        byte[] encoded = encodeDocument(original);
        BsonDocument decoded = BsonMarshaller.decode(encoded);
        assertEquals(original, decoded);
    }

    @Test
    void testDecodeMemorySegment() {
        BsonDocument original = new BsonDocument("key", new BsonString("value"));
        byte[] encoded = encodeDocument(original);

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment segment = arena.allocate(encoded.length);
            segment.copyFrom(MemorySegment.ofArray(encoded));
            BsonDocument decoded = BsonMarshaller.decode(segment, encoded.length);
            assertEquals(original, decoded);
        }
    }

    // ==================== FFI-dependent tests ====================

    @Test
    void testToBsonStructSimpleDocument() {
        BsonDocument original = new BsonDocument("key", new BsonString("value"));
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment bsonStruct = BsonMarshaller.toBsonStruct(arena, original);

            MemorySegment data = Bson.data(bsonStruct);
            long len = Bson.len(bsonStruct);

            BsonDocument decoded = BsonMarshaller.decode(data, len);
            assertEquals(original, decoded);
        }
    }

    @Test
    void testFromBsonStructNull() {
        assertNull(BsonMarshaller.fromBsonStruct(null));
        assertNull(BsonMarshaller.fromBsonStruct(MemorySegment.NULL));
    }

    @Test
    void testFromBsonStructRoundTrip() {
        BsonDocument original = new BsonDocument("key", new BsonString("value"));
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment bsonStruct = BsonMarshaller.toBsonStruct(arena, original);
            BsonDocument decoded = BsonMarshaller.fromBsonStruct(bsonStruct);
            assertEquals(original, decoded);
        }
    }

    @Test
    void testToBsonStructComplexDocument() {
        BsonDocument original = new BsonDocument()
                .append("string", new BsonString("hello"))
                .append("int32", new BsonInt32(42))
                .append("int64", new BsonInt64(Long.MAX_VALUE))
                .append("double", new BsonDouble(3.14159))
                .append("boolean", BsonBoolean.TRUE)
                .append("objectId", new BsonObjectId(new ObjectId()))
                .append("dateTime", new BsonDateTime(System.currentTimeMillis()))
                .append("binary", new BsonBinary(new byte[]{1, 2, 3, 4, 5}))
                .append("array", new BsonArray(List.of(new BsonInt32(1), new BsonInt32(2))))
                .append("nested", new BsonDocument("inner", new BsonString("nested value")));

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment bsonStruct = BsonMarshaller.toBsonStruct(arena, original);
            BsonDocument decoded = BsonMarshaller.fromBsonStruct(bsonStruct);
            assertEquals(original, decoded);
        }
    }

    @Test
    void testToBsonBatchThrowsUnsupportedOperationException() {
        List<BsonDocument> documents = List.of(new BsonDocument("key", new BsonString("value")));
        try (Arena arena = Arena.ofConfined()) {
            assertThrows(UnsupportedOperationException.class, () ->
                BsonMarshaller.toBsonBatch(arena, documents));
        }
    }

    @Test
    void testToBsonValueStructThrowsUnsupportedOperationException() {
        org.bson.BsonString value = new BsonString("hello world");
        try (Arena arena = Arena.ofConfined()) {
            assertThrows(UnsupportedOperationException.class, () ->
                BsonMarshaller.toBsonValueStruct(arena, value));
        }
    }

    @Test
    void testToBsonValueThrowsUnsupportedOperationException() {
        BsonString value = new BsonString("hello world");
        try (Arena arena = Arena.ofConfined()) {
            assertThrows(UnsupportedOperationException.class, () ->
                BsonMarshaller.toBsonValue(arena, value));
        }
    }

    @Test
    void testFromBsonValueStructThrowsUnsupportedOperationException() {
        assertThrows(UnsupportedOperationException.class, () ->
            BsonMarshaller.fromBsonValueStruct(MemorySegment.NULL));
    }

    // Helper to encode a document to bytes for test setup
    private byte[] encodeDocument(BsonDocument document) {
        try (BasicOutputBuffer buffer = new BasicOutputBuffer()) {
            try (org.bson.BsonBinaryWriter writer = new org.bson.BsonBinaryWriter(buffer)) {
                CODEC.encode(writer, document, EncoderContext.builder().build());
            }
            return buffer.toByteArray();
        }
    }

    // ==================== Commented out FFI-dependent tests ====================
    /*
    @Test
    void testToBsonStructSimpleDocument() {
        BsonDocument original = new BsonDocument("key", new BsonString("value"));

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment bsonStruct = BsonMarshaller.toBsonStruct(arena, original);

            MemorySegment data = Bson.data(bsonStruct);
            long len = Bson.len(bsonStruct);

            BsonDocument decoded = BsonMarshaller.decode(data, len);
            assertEquals(original, decoded);
        }
    }

    @Test
    void testToBsonStructComplexDocument() {
        BsonDocument original = new BsonDocument()
                .append("string", new BsonString("hello"))
                .append("int32", new BsonInt32(42))
                .append("int64", new BsonInt64(Long.MAX_VALUE))
                .append("double", new BsonDouble(3.14159))
                .append("boolean", BsonBoolean.TRUE)
                .append("objectId", new BsonObjectId(new ObjectId()))
                .append("dateTime", new BsonDateTime(System.currentTimeMillis()))
                .append("binary", new BsonBinary(new byte[]{1, 2, 3, 4, 5}))
                .append("array", new BsonArray(List.of(new BsonInt32(1), new BsonInt32(2))))
                .append("nested", new BsonDocument("inner", new BsonString("nested value")));

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment bsonStruct = BsonMarshaller.toBsonStruct(arena, original);

            MemorySegment data = Bson.data(bsonStruct);
            long len = Bson.len(bsonStruct);

            BsonDocument decoded = BsonMarshaller.decode(data, len);
            assertEquals(original, decoded);
        }
    }

    @Test
    void testToBsonStructLargeDocument() {
        StringBuilder largeValue = new StringBuilder();
        for (int i = 0; i < 10000; i++) {
            largeValue.append("x");
        }
        BsonDocument original = new BsonDocument("largeField", new BsonString(largeValue.toString()));

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment bsonStruct = BsonMarshaller.toBsonStruct(arena, original);

            MemorySegment data = Bson.data(bsonStruct);
            long len = Bson.len(bsonStruct);

            BsonDocument decoded = BsonMarshaller.decode(data, len);
            assertEquals(original, decoded);
        }
    }

    @Test
    void testFromBsonStructNull() {
        assertNull(BsonMarshaller.fromBsonStruct(null));
        assertNull(BsonMarshaller.fromBsonStruct(MemorySegment.NULL));
    }

    @Test
    void testFromBsonStructEmptyData() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment bsonStruct = Bson.allocate(arena);
            Bson.data(bsonStruct, MemorySegment.NULL);
            Bson.len(bsonStruct, 0);
            assertNull(BsonMarshaller.fromBsonStruct(bsonStruct));
        }
    }

    @Test
    void testFromBsonStructRoundTrip() {
        BsonDocument original = new BsonDocument("key", new BsonString("value"));

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment bsonStruct = BsonMarshaller.toBsonStruct(arena, original);
            BsonDocument decoded = BsonMarshaller.fromBsonStruct(bsonStruct);
            assertEquals(original, decoded);
        }
    }

    @Test
    void testToBsonBatchSingleDocument() {
        BsonDocument doc = new BsonDocument("key", new BsonString("value"));
        List<BsonDocument> documents = List.of(doc);

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment batchStruct = BsonMarshaller.toBsonBatch(arena, documents);

            assertEquals(1, BsonBatch.count(batchStruct));

            MemorySegment data = BsonBatch.data(batchStruct);
            long len = BsonBatch.len(batchStruct);
            MemorySegment offsets = BsonBatch.offsets(batchStruct);

            int offset0 = offsets.getAtIndex(ValueLayout.JAVA_INT, 0);
            assertEquals(0, offset0);

            BsonDocument decoded = BsonMarshaller.decode(data.asSlice(offset0), len - offset0);
            assertEquals(doc, decoded);
        }
    }

    @Test
    void testToBsonBatchMultipleDocuments() {
        BsonDocument doc1 = new BsonDocument("key1", new BsonString("value1"));
        BsonDocument doc2 = new BsonDocument("key2", new BsonInt32(42));
        BsonDocument doc3 = new BsonDocument("key3", BsonBoolean.TRUE);
        List<BsonDocument> documents = List.of(doc1, doc2, doc3);

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment batchStruct = BsonMarshaller.toBsonBatch(arena, documents);

            assertEquals(3, BsonBatch.count(batchStruct));

            MemorySegment data = BsonBatch.data(batchStruct);
            long totalLen = BsonBatch.len(batchStruct);
            MemorySegment offsets = BsonBatch.offsets(batchStruct);

            List<BsonDocument> decoded = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                int offset = offsets.getAtIndex(ValueLayout.JAVA_INT, i);
                int nextOffset = (i < 2) ? offsets.getAtIndex(ValueLayout.JAVA_INT, i + 1) : (int) totalLen;
                int docLen = nextOffset - offset;
                decoded.add(BsonMarshaller.decode(data.asSlice(offset), docLen));
            }

            assertEquals(doc1, decoded.get(0));
            assertEquals(doc2, decoded.get(1));
            assertEquals(doc3, decoded.get(2));
        }
    }

    @Test
    void testToBsonBatchEmptyList() {
        List<BsonDocument> documents = List.of();

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment batchStruct = BsonMarshaller.toBsonBatch(arena, documents);
            assertEquals(0, BsonBatch.count(batchStruct));
        }
    }

    @Test
    void testToBsonBatchLargeDocuments() {
        StringBuilder largeValue = new StringBuilder();
        for (int i = 0; i < 5000; i++) {
            largeValue.append("x");
        }
        BsonDocument doc1 = new BsonDocument("large1", new BsonString(largeValue.toString()));
        BsonDocument doc2 = new BsonDocument("large2", new BsonString(largeValue.toString()));
        List<BsonDocument> documents = List.of(doc1, doc2);

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment batchStruct = BsonMarshaller.toBsonBatch(arena, documents);

            assertEquals(2, BsonBatch.count(batchStruct));

            MemorySegment data = BsonBatch.data(batchStruct);
            long totalLen = BsonBatch.len(batchStruct);
            MemorySegment offsets = BsonBatch.offsets(batchStruct);

            int offset0 = offsets.getAtIndex(ValueLayout.JAVA_INT, 0);
            int offset1 = offsets.getAtIndex(ValueLayout.JAVA_INT, 1);

            BsonDocument decoded1 = BsonMarshaller.decode(data.asSlice(offset0), offset1 - offset0);
            BsonDocument decoded2 = BsonMarshaller.decode(data.asSlice(offset1), totalLen - offset1);

            assertEquals(doc1, decoded1);
            assertEquals(doc2, decoded2);
        }
    }

    @Test
    void testToBsonValueStructString() {
        org.bson.BsonString value = new BsonString("hello world");

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment valueStruct = BsonMarshaller.toBsonValueStruct(arena, value);

            byte bsonType = BsonValue.bson_type(valueStruct);
            assertEquals(org.bson.BsonType.STRING.getValue(), bsonType);

            org.bson.BsonValue decoded = BsonMarshaller.fromBsonValueStruct(valueStruct);
            assertEquals(value, decoded);
        }
    }

    @Test
    void testToBsonValueStructInt32() {
        org.bson.BsonInt32 value = new BsonInt32(12345);

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment valueStruct = BsonMarshaller.toBsonValueStruct(arena, value);

            byte bsonType = BsonValue.bson_type(valueStruct);
            assertEquals(org.bson.BsonType.INT32.getValue(), bsonType);

            org.bson.BsonValue decoded = BsonMarshaller.fromBsonValueStruct(valueStruct);
            assertEquals(value, decoded);
        }
    }

    @Test
    void testToBsonValueStructObjectId() {
        org.bson.BsonObjectId value = new BsonObjectId(new ObjectId());

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment valueStruct = BsonMarshaller.toBsonValueStruct(arena, value);

            byte bsonType = BsonValue.bson_type(valueStruct);
            assertEquals(org.bson.BsonType.OBJECT_ID.getValue(), bsonType);

            org.bson.BsonValue decoded = BsonMarshaller.fromBsonValueStruct(valueStruct);
            assertEquals(value, decoded);
        }
    }

    @Test
    void testFromBsonValueStructNull() {
        assertNull(BsonMarshaller.fromBsonValueStruct(null));
        assertNull(BsonMarshaller.fromBsonValueStruct(MemorySegment.NULL));
    }

    @Test
    void testFromBsonValueStructEmptyData() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment valueStruct = BsonValue.allocate(arena);
            BsonValue.data(valueStruct, MemorySegment.NULL);
            BsonValue.len(valueStruct, 0);
            BsonValue.bson_type(valueStruct, (byte) org.bson.BsonType.STRING.getValue());
            assertNull(BsonMarshaller.fromBsonValueStruct(valueStruct));
        }
    }

    @Test
    void testToBsonValueString() {
        BsonString value = new BsonString("hello world");

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment segment = BsonMarshaller.toBsonValue(arena, value);

            int strLen = segment.get(ValueLayout.JAVA_INT_UNALIGNED, 0);
            assertEquals("hello world".length() + 1, strLen);

            byte[] strBytes = segment.asSlice(4, strLen - 1).toArray(ValueLayout.JAVA_BYTE);
            assertEquals("hello world", new String(strBytes));
        }
    }

    @Test
    void testToBsonValueInt32() {
        BsonInt32 value = new BsonInt32(42);

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment segment = BsonMarshaller.toBsonValue(arena, value);

            assertEquals(4, segment.byteSize());
            int decoded = segment.get(ValueLayout.JAVA_INT_UNALIGNED, 0);
            assertEquals(42, decoded);
        }
    }

    @Test
    void testToBsonValueInt64() {
        BsonInt64 value = new BsonInt64(Long.MAX_VALUE);

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment segment = BsonMarshaller.toBsonValue(arena, value);

            assertEquals(8, segment.byteSize());
            long decoded = segment.get(ValueLayout.JAVA_LONG_UNALIGNED, 0);
            assertEquals(Long.MAX_VALUE, decoded);
        }
    }

    @Test
    void testToBsonValueDouble() {
        BsonDouble value = new BsonDouble(3.14159);

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment segment = BsonMarshaller.toBsonValue(arena, value);

            assertEquals(8, segment.byteSize());
            double decoded = segment.get(ValueLayout.JAVA_DOUBLE_UNALIGNED, 0);
            assertEquals(3.14159, decoded, 0.00001);
        }
    }

    @Test
    void testToBsonValueBoolean() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment trueSegment = BsonMarshaller.toBsonValue(arena, BsonBoolean.TRUE);
            assertEquals(1, trueSegment.byteSize());
            assertEquals(1, trueSegment.get(ValueLayout.JAVA_BYTE, 0));

            MemorySegment falseSegment = BsonMarshaller.toBsonValue(arena, BsonBoolean.FALSE);
            assertEquals(1, falseSegment.byteSize());
            assertEquals(0, falseSegment.get(ValueLayout.JAVA_BYTE, 0));
        }
    }

    @Test
    void testToBsonValueObjectId() {
        ObjectId oid = new ObjectId();
        BsonObjectId value = new BsonObjectId(oid);

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment segment = BsonMarshaller.toBsonValue(arena, value);

            assertEquals(12, segment.byteSize());
            byte[] decoded = segment.toArray(ValueLayout.JAVA_BYTE);
            assertArrayEquals(oid.toByteArray(), decoded);
        }
    }

    @Test
    void testToBsonValueDocument() {
        BsonDocument value = new BsonDocument("nested", new BsonString("value"));

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment segment = BsonMarshaller.toBsonValue(arena, value);

            BsonDocument decoded = BsonMarshaller.decode(segment, segment.byteSize());
            assertEquals(value, decoded);
        }
    }

    @Test
    void testToBsonValueArray() {
        BsonArray value = new BsonArray(List.of(new BsonInt32(1), new BsonInt32(2), new BsonInt32(3)));

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment segment = BsonMarshaller.toBsonValue(arena, value);

            int size = segment.get(ValueLayout.JAVA_INT_UNALIGNED, 0);
            assertEquals(segment.byteSize(), size);
        }
    }

    @Test
    void testToBsonValueLargeString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 10000; i++) {
            sb.append("x");
        }
        BsonString value = new BsonString(sb.toString());

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment segment = BsonMarshaller.toBsonValue(arena, value);

            int strLen = segment.get(ValueLayout.JAVA_INT_UNALIGNED, 0);
            assertEquals(10001, strLen);

            byte[] strBytes = segment.asSlice(4, strLen - 1).toArray(ValueLayout.JAVA_BYTE);
            assertEquals(sb.toString(), new String(strBytes));
        }
    }

    // Helper to encode a document to bytes for test setup (FFI version)
    private byte[] encodeDocumentFFI(BsonDocument document) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment bsonStruct = BsonMarshaller.toBsonStruct(arena, document);
            MemorySegment data = Bson.data(bsonStruct);
            long len = Bson.len(bsonStruct);
            return data.reinterpret(len).toArray(ValueLayout.JAVA_BYTE);
        }
    }
    */
}

