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

import com.mongodb.internal.rust.crud.ffi.CursorResult;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class FfmAsyncCursorTest {

    private static final BsonDocumentCodec CODEC = new BsonDocumentCodec();

    @Test
    void testFromCursorResultWithEmptyBatchExhausted() {
        try (Arena arena = Arena.ofConfined()) {
            // Create empty BsonArray
            MemorySegment bsonArrayStruct = com.mongodb.internal.rust.crud.ffi.BsonArray.allocate(arena);
            com.mongodb.internal.rust.crud.ffi.BsonArray.data(bsonArrayStruct, MemorySegment.NULL);
            com.mongodb.internal.rust.crud.ffi.BsonArray.len(bsonArrayStruct, 0);

            // Create CursorResult with null cursor, exhausted=true, empty batch
            MemorySegment cursorResult = CursorResult.allocate(arena);
            CursorResult.cursor(cursorResult, MemorySegment.NULL);
            CursorResult.exhausted(cursorResult, true);
            // Copy the BsonArray into the first_batch field
            MemorySegment firstBatchField = CursorResult.first_batch(cursorResult);
            firstBatchField.copyFrom(bsonArrayStruct);

            FfmAsyncCursor<BsonDocument> cursor = FfmAsyncCursor.fromCursorResult(
                    MemorySegment.NULL,  // clientPtr
                    cursorResult,
                    MemorySegment.NULL,  // sessionPtr
                    CODEC);

            // First next() returns empty first batch
            var batch = new java.util.concurrent.atomic.AtomicReference<List<BsonDocument>>();
            cursor.next((result, error) -> batch.set(result));
            assertNotNull(batch.get());
            assertTrue(batch.get().isEmpty());

            // Cursor is now exhausted
            assertTrue(cursor.isExhausted());
        }
    }

    @Test
    void testFromCursorResultWithDocuments() {
        try (Arena arena = Arena.ofConfined()) {
            BsonDocument doc1 = new BsonDocument("name", new BsonString("Alice"));
            BsonDocument doc2 = new BsonDocument("name", new BsonString("Bob")).append("age", new BsonInt32(30));

            byte[] bytes1 = encodeDocument(doc1);
            byte[] bytes2 = encodeDocument(doc2);

            // Allocate document bytes
            MemorySegment seg1 = arena.allocate(bytes1.length);
            seg1.copyFrom(MemorySegment.ofArray(bytes1));
            MemorySegment seg2 = arena.allocate(bytes2.length);
            seg2.copyFrom(MemorySegment.ofArray(bytes2));

            // Create array of pointers
            MemorySegment pointerArray = arena.allocate(ValueLayout.ADDRESS, 2);
            pointerArray.setAtIndex(ValueLayout.ADDRESS, 0, seg1);
            pointerArray.setAtIndex(ValueLayout.ADDRESS, 1, seg2);

            // Create BsonArray struct
            MemorySegment bsonArrayStruct = com.mongodb.internal.rust.crud.ffi.BsonArray.allocate(arena);
            com.mongodb.internal.rust.crud.ffi.BsonArray.data(bsonArrayStruct, pointerArray);
            com.mongodb.internal.rust.crud.ffi.BsonArray.len(bsonArrayStruct, 2);

            // Create CursorResult - not exhausted (more batches available)
            MemorySegment cursorResult = CursorResult.allocate(arena);
            CursorResult.cursor(cursorResult, MemorySegment.ofAddress(0x12345678L)); // fake cursor ptr
            CursorResult.exhausted(cursorResult, false);
            MemorySegment firstBatchField = CursorResult.first_batch(cursorResult);
            firstBatchField.copyFrom(bsonArrayStruct);

            FfmAsyncCursor<BsonDocument> cursor = FfmAsyncCursor.fromCursorResult(
                    MemorySegment.NULL,
                    cursorResult,
                    MemorySegment.NULL,
                    CODEC);

            // Cursor not exhausted yet (has first batch)
            assertFalse(cursor.isExhausted());

            // First next() returns the first batch
            var batch = new java.util.concurrent.atomic.AtomicReference<List<BsonDocument>>();
            cursor.next((result, error) -> batch.set(result));
            assertEquals(2, batch.get().size());
            assertEquals(doc1, batch.get().get(0));
            assertEquals(doc2, batch.get().get(1));

            // Still not exhausted (server said there's more)
            assertFalse(cursor.isExhausted());
        }
    }

    @Test
    void testFromCursorResultExhausted() {
        try (Arena arena = Arena.ofConfined()) {
            BsonDocument doc = new BsonDocument("_id", new BsonInt32(1));
            byte[] bytes = encodeDocument(doc);

            MemorySegment seg = arena.allocate(bytes.length);
            seg.copyFrom(MemorySegment.ofArray(bytes));

            MemorySegment pointerArray = arena.allocate(ValueLayout.ADDRESS, 1);
            pointerArray.setAtIndex(ValueLayout.ADDRESS, 0, seg);

            MemorySegment bsonArrayStruct = com.mongodb.internal.rust.crud.ffi.BsonArray.allocate(arena);
            com.mongodb.internal.rust.crud.ffi.BsonArray.data(bsonArrayStruct, pointerArray);
            com.mongodb.internal.rust.crud.ffi.BsonArray.len(bsonArrayStruct, 1);

            // Create CursorResult - exhausted (this is the last batch)
            MemorySegment cursorResult = CursorResult.allocate(arena);
            CursorResult.cursor(cursorResult, MemorySegment.NULL); // no cursor when exhausted
            CursorResult.exhausted(cursorResult, true);
            MemorySegment firstBatchField = CursorResult.first_batch(cursorResult);
            firstBatchField.copyFrom(bsonArrayStruct);

            FfmAsyncCursor<BsonDocument> cursor = FfmAsyncCursor.fromCursorResult(
                    MemorySegment.NULL,
                    cursorResult,
                    MemorySegment.NULL,
                    CODEC);

            // Still has first batch, so not exhausted yet
            assertFalse(cursor.isExhausted());

            // First next() returns the first batch
            var batch = new java.util.concurrent.atomic.AtomicReference<List<BsonDocument>>();
            cursor.next((result, error) -> batch.set(result));
            assertEquals(1, batch.get().size());
            assertEquals(doc, batch.get().get(0));

            // Now exhausted
            assertTrue(cursor.isExhausted());
        }
    }

    private byte[] encodeDocument(BsonDocument document) {
        try (BasicOutputBuffer buffer = new BasicOutputBuffer()) {
            try (org.bson.BsonBinaryWriter writer = new org.bson.BsonBinaryWriter(buffer)) {
                CODEC.encode(writer, document, EncoderContext.builder().build());
            }
            return buffer.toByteArray();
        }
    }
}

