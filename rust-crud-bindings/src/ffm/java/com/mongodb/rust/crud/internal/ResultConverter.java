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

import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.bulk.BulkWriteInsert;
import com.mongodb.bulk.BulkWriteUpsert;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.internal.rust.crud.ffi.*;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.codecs.Decoder;
import org.bson.codecs.DecoderContext;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Converts FFI result structs to Java result objects.
 */
public final class ResultConverter {

    private ResultConverter() {
    }

    /**
     * Converts FFI InsertOneResult to Java InsertOneResult.
     */
    public static InsertOneResult toInsertOneResult(MemorySegment resultPtr) {
        if (resultPtr == null || resultPtr.equals(MemorySegment.NULL)) {
            return InsertOneResult.unacknowledged();
        }
        
        MemorySegment insertedIdStruct = com.mongodb.internal.rust.crud.ffi.InsertOneResult.inserted_id(resultPtr);
        BsonValue insertedId = BsonMarshaller.fromBsonValueStruct(insertedIdStruct);
        return InsertOneResult.acknowledged(insertedId);
    }

    /**
     * Converts FFI InsertManyResult to Java InsertManyResult.
     */
    public static InsertManyResult toInsertManyResult(MemorySegment resultPtr) {
        if (resultPtr == null || resultPtr.equals(MemorySegment.NULL)) {
            return InsertManyResult.unacknowledged();
        }
        
        MemorySegment insertedIdsStruct = com.mongodb.internal.rust.crud.ffi.InsertManyResult.inserted_ids(resultPtr);
        BsonDocument idsDoc = BsonMarshaller.fromBsonStruct(insertedIdsStruct);
        
        Map<Integer, BsonValue> insertedIds = new HashMap<>();
        if (idsDoc != null) {
            for (String key : idsDoc.keySet()) {
                insertedIds.put(Integer.parseInt(key), idsDoc.get(key));
            }
        }
        return InsertManyResult.acknowledged(insertedIds);
    }

    /**
     * Converts FFI UpdateResult to Java UpdateResult.
     */
    public static UpdateResult toUpdateResult(MemorySegment resultPtr) {
        if (resultPtr == null || resultPtr.equals(MemorySegment.NULL)) {
            return UpdateResult.unacknowledged();
        }
        
        long matchedCount = com.mongodb.internal.rust.crud.ffi.UpdateResult.matched_count(resultPtr);
        long modifiedCount = com.mongodb.internal.rust.crud.ffi.UpdateResult.modified_count(resultPtr);
        
        MemorySegment upsertedIdPtr = com.mongodb.internal.rust.crud.ffi.UpdateResult.upserted_id(resultPtr);
        BsonValue upsertedId = null;
        if (upsertedIdPtr != null && !upsertedIdPtr.equals(MemorySegment.NULL)) {
            upsertedId = BsonMarshaller.fromBsonValueStruct(upsertedIdPtr);
        }
        
        return UpdateResult.acknowledged(matchedCount, modifiedCount, upsertedId);
    }

    /**
     * Converts FFI DeleteResult to Java DeleteResult.
     */
    public static DeleteResult toDeleteResult(MemorySegment resultPtr) {
        if (resultPtr == null || resultPtr.equals(MemorySegment.NULL)) {
            return DeleteResult.unacknowledged();
        }
        
        long deletedCount = com.mongodb.internal.rust.crud.ffi.DeleteResult.deleted_count(resultPtr);
        return DeleteResult.acknowledged(deletedCount);
    }

    /**
     * Converts FFI FindOneResult to Java BsonDocument.
     */
    @Nullable
    public static BsonDocument toFindOneResult(MemorySegment resultPtr) {
        if (resultPtr == null || resultPtr.equals(MemorySegment.NULL)) {
            return null;
        }
        
        MemorySegment documentPtr = FindOneResult.document(resultPtr);
        if (documentPtr == null || documentPtr.equals(MemorySegment.NULL)) {
            return null;
        }
        
        return BsonMarshaller.fromBsonStruct(documentPtr);
    }

    /**
     * Creates a CursorHandle from FFI CursorResult.
     */
    public static CursorHandle toCursorHandle(MemorySegment resultPtr) {
        return new CursorHandle(resultPtr);
    }

    /**
     * Converts a string result pointer to a Java String.
     */
    @Nullable
    public static String toString(MemorySegment resultPtr) {
        if (resultPtr == null || resultPtr.equals(MemorySegment.NULL)) {
            return null;
        }
        // Assuming the result pointer points directly to a C string
        return resultPtr.reinterpret(1024).getString(0);
    }

    /**
     * Converts FFI BulkWriteResult to Java BulkWriteResult.
     */
    public static BulkWriteResult toBulkWriteResult(MemorySegment resultPtr) {
        if (resultPtr == null || resultPtr.equals(MemorySegment.NULL)) {
            return BulkWriteResult.unacknowledged();
        }

        long insertedCount = com.mongodb.internal.rust.crud.ffi.BulkWriteResult.inserted_count(resultPtr);
        long matchedCount = com.mongodb.internal.rust.crud.ffi.BulkWriteResult.matched_count(resultPtr);
        long deletedCount = com.mongodb.internal.rust.crud.ffi.BulkWriteResult.deleted_count(resultPtr);
        long modifiedCount = com.mongodb.internal.rust.crud.ffi.BulkWriteResult.modified_count(resultPtr);

        // Parse inserted_ids: document where keys are indices and values are _ids
        List<BulkWriteInsert> inserts = new ArrayList<>();
        MemorySegment insertedIdsPtr = com.mongodb.internal.rust.crud.ffi.BulkWriteResult.inserted_ids(resultPtr);
        BsonDocument insertedIds = BsonMarshaller.fromBsonStruct(insertedIdsPtr);
        if (insertedIds != null) {
            for (String key : insertedIds.keySet()) {
                int index = Integer.parseInt(key);
                BsonValue id = insertedIds.get(key);
                inserts.add(new BulkWriteInsert(index, id));
            }
        }

        // Parse upserted_ids: document where keys are indices and values are _ids
        List<BulkWriteUpsert> upserts = new ArrayList<>();
        MemorySegment upsertedIdsPtr = com.mongodb.internal.rust.crud.ffi.BulkWriteResult.upserted_ids(resultPtr);
        BsonDocument upsertedIds = BsonMarshaller.fromBsonStruct(upsertedIdsPtr);
        if (upsertedIds != null) {
            for (String key : upsertedIds.keySet()) {
                int index = Integer.parseInt(key);
                BsonValue id = upsertedIds.get(key);
                upserts.add(new BulkWriteUpsert(index, id));
            }
        }

        return BulkWriteResult.acknowledged(
            (int) insertedCount, (int) matchedCount, (int) deletedCount, (int) modifiedCount,
            upserts, inserts);
    }

    /**
     * Decodes an array field from a BsonDocument using the provided decoder.
     *
     * @param doc the document containing the array
     * @param arrayFieldName the name of the array field
     * @param decoder the decoder to use for each element
     * @param <T> the result type
     * @return a list of decoded elements
     */
    public static <T> List<T> decodeArray(@Nullable BsonDocument doc, String arrayFieldName, Decoder<T> decoder) {
        List<T> results = new ArrayList<>();
        if (doc != null && doc.containsKey(arrayFieldName)) {
            DecoderContext decoderContext = DecoderContext.builder().build();
            for (BsonValue v : doc.getArray(arrayFieldName)) {
                BsonDocument elementDoc = v.asDocument();
                T decoded = decoder.decode(elementDoc.asBsonReader(), decoderContext);
                results.add(decoded);
            }
        }
        return results;
    }
}

