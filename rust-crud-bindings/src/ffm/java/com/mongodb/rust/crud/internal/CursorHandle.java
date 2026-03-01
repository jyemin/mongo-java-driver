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
import com.mongodb.internal.rust.crud.ffi.BsonBatch;
import org.bson.BsonDocument;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;

/**
 * Handle to an FFI cursor, wrapping the CursorResult.
 */
public final class CursorHandle {

    private final MemorySegment cursorPtr;
    private final boolean exhausted;
    private final List<BsonDocument> firstBatch;

    public CursorHandle(MemorySegment cursorResultPtr) {
        if (cursorResultPtr == null || cursorResultPtr.equals(MemorySegment.NULL)) {
            this.cursorPtr = MemorySegment.NULL;
            this.exhausted = true;
            this.firstBatch = List.of();
        } else {
            this.cursorPtr = CursorResult.cursor(cursorResultPtr);
            this.exhausted = CursorResult.exhausted(cursorResultPtr);
            this.firstBatch = decodeBatch(CursorResult.first_batch(cursorResultPtr));
        }
    }

    public MemorySegment getCursorPtr() {
        return cursorPtr;
    }

    public boolean isExhausted() {
        return exhausted;
    }

    public List<BsonDocument> getFirstBatch() {
        return firstBatch;
    }

    /**
     * Decodes a BsonBatch struct into a list of BsonDocuments.
     */
    public static List<BsonDocument> decodeBatch(MemorySegment batchPtr) {
        if (batchPtr == null || batchPtr.equals(MemorySegment.NULL)) {
            return List.of();
        }

        MemorySegment data = BsonBatch.data(batchPtr);
        long dataLen = BsonBatch.len(batchPtr);
        MemorySegment offsets = BsonBatch.offsets(batchPtr);
        int count = BsonBatch.count(batchPtr);

        if (data.equals(MemorySegment.NULL) || count == 0) {
            return List.of();
        }

        List<BsonDocument> documents = new ArrayList<>(count);
        MemorySegment dataSegment = data.reinterpret(dataLen);
        MemorySegment offsetsSegment = offsets.reinterpret((long) count * Integer.BYTES);

        for (int i = 0; i < count; i++) {
            int offset = offsetsSegment.getAtIndex(java.lang.foreign.ValueLayout.JAVA_INT, i);
            
            // Calculate document length (next offset - current offset, or remaining for last)
            int docLen;
            if (i < count - 1) {
                int nextOffset = offsetsSegment.getAtIndex(java.lang.foreign.ValueLayout.JAVA_INT, i + 1);
                docLen = nextOffset - offset;
            } else {
                docLen = (int) (dataLen - offset);
            }
            
            byte[] docBytes = dataSegment.asSlice(offset, docLen)
                    .toArray(java.lang.foreign.ValueLayout.JAVA_BYTE);
            documents.add(BsonMarshaller.decode(docBytes));
        }

        return documents;
    }
}

