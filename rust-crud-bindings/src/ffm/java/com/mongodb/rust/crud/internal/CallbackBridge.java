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
import com.mongodb.internal.rust.crud.ffi.*;
import com.mongodb.rust.crud.NativeAsyncCursor;
import com.mongodb.rust.crud.SingleResultCallback;
import org.bson.BsonDocument;
import org.bson.BsonValue;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.List;
import java.util.function.BiConsumer;

/**
 * Creates FFI callback stubs that bridge to Java callbacks.
 * 
 * <p>FFI callbacks are invoked from Rust on a Rust thread. This bridge:
 * <ul>
 *   <li>Creates upcall stubs that can be passed to Rust</li>
 *   <li>Converts FFI results/errors to Java types</li>
 *   <li>Invokes the Java callback</li>
 * </ul>
 */
public final class CallbackBridge {

    private CallbackBridge() {
    }

    /**
     * Creates an InsertOneCallback upcall stub.
     */
    public static MemorySegment createInsertOneCallback(
            Arena arena, 
            SingleResultCallback<com.mongodb.client.result.InsertOneResult> callback) {
        
        BiConsumer<MemorySegment, MemorySegment> handler = (resultPtr, errorPtr) -> {
            try {
                if (!errorPtr.equals(MemorySegment.NULL)) {
                    callback.completeExceptionally(ErrorConverter.toException(errorPtr));
                } else {
                    callback.complete(ResultConverter.toInsertOneResult(resultPtr));
                }
            } catch (Throwable t) {
                callback.completeExceptionally(t);
            }
        };
        
        return InsertOneCallback.allocate((userdata, result, error) -> {
            handler.accept(result, error);
        }, arena);
    }

    /**
     * Creates an InsertManyCallback upcall stub.
     */
    public static MemorySegment createInsertManyCallback(
            Arena arena,
            SingleResultCallback<com.mongodb.client.result.InsertManyResult> callback) {

        return InsertManyCallback.allocate((userdata, result, error) -> {
            try {
                if (!error.equals(MemorySegment.NULL)) {
                    callback.completeExceptionally(ErrorConverter.toException(error));
                } else {
                    callback.complete(ResultConverter.toInsertManyResult(result));
                }
            } catch (Throwable t) {
                callback.completeExceptionally(t);
            }
        }, arena);
    }

    /**
     * Creates an UpdateCallback upcall stub.
     */
    public static MemorySegment createUpdateCallback(
            Arena arena,
            SingleResultCallback<com.mongodb.client.result.UpdateResult> callback) {

        return UpdateCallback.allocate((userdata, result, error) -> {
            try {
                if (!error.equals(MemorySegment.NULL)) {
                    callback.completeExceptionally(ErrorConverter.toException(error));
                } else {
                    callback.complete(ResultConverter.toUpdateResult(result));
                }
            } catch (Throwable t) {
                callback.completeExceptionally(t);
            }
        }, arena);
    }

    /**
     * Creates a DeleteCallback upcall stub.
     */
    public static MemorySegment createDeleteCallback(
            Arena arena,
            SingleResultCallback<com.mongodb.client.result.DeleteResult> callback) {

        return DeleteCallback.allocate((userdata, result, error) -> {
            try {
                if (!error.equals(MemorySegment.NULL)) {
                    callback.completeExceptionally(ErrorConverter.toException(error));
                } else {
                    callback.complete(ResultConverter.toDeleteResult(result));
                }
            } catch (Throwable t) {
                callback.completeExceptionally(t);
            }
        }, arena);
    }

    /**
     * Creates a FindOneCallback upcall stub.
     */
    public static MemorySegment createFindOneCallback(
            Arena arena,
            SingleResultCallback<org.bson.BsonDocument> callback) {

        return FindOneCallback.allocate((userdata, result, error) -> {
            try {
                if (!error.equals(MemorySegment.NULL)) {
                    callback.completeExceptionally(ErrorConverter.toException(error));
                } else {
                    callback.complete(ResultConverter.toFindOneResult(result));
                }
            } catch (Throwable t) {
                callback.completeExceptionally(t);
            }
        }, arena);
    }

    /**
     * Creates a CursorResultCallback upcall stub.
     */
    public static MemorySegment createCursorResultCallback(
            Arena arena,
            SingleResultCallback<CursorHandle> callback) {

        return CursorResultCallback.allocate((userdata, result, error) -> {
            try {
                if (!error.equals(MemorySegment.NULL)) {
                    callback.completeExceptionally(ErrorConverter.toException(error));
                } else {
                    callback.complete(ResultConverter.toCursorHandle(result));
                }
            } catch (Throwable t) {
                callback.completeExceptionally(t);
            }
        }, arena);
    }

    /**
     * Creates a CountCallback upcall stub.
     */
    public static MemorySegment createCountCallback(
            Arena arena,
            SingleResultCallback<Long> callback) {

        return CountCallback.allocate((userdata, result, error) -> {
            try {
                if (!error.equals(MemorySegment.NULL)) {
                    callback.completeExceptionally(ErrorConverter.toException(error));
                } else {
                    callback.complete(CountResult.count(result));
                }
            } catch (Throwable t) {
                callback.completeExceptionally(t);
            }
        }, arena);
    }

    /**
     * Creates a DistinctCallback upcall stub.
     */
    public static MemorySegment createDistinctCallback(
            Arena arena,
            SingleResultCallback<NativeAsyncCursor<BsonValue>> callback) {

        return DistinctCallback.allocate((userdata, result, error) -> {
            try {
                if (!error.equals(MemorySegment.NULL)) {
                    callback.completeExceptionally(ErrorConverter.toException(error));
                } else {
                    // DistinctResult contains values as a Bson array
                    MemorySegment bsonPtr = DistinctResult.values(result);
                    BsonDocument doc = BsonMarshaller.fromBsonStruct(bsonPtr);
                    List<BsonValue> values = new java.util.ArrayList<>();
                    if (doc != null && doc.containsKey("values")) {
                        values.addAll(doc.getArray("values").getValues());
                    } else if (doc != null) {
                        // Try to extract values from the document directly if it's an array wrapper
                        for (String key : doc.keySet()) {
                            values.add(doc.get(key));
                        }
                    }
                    callback.complete(new ListBackedCursor<>(values));
                }
            } catch (Throwable t) {
                callback.completeExceptionally(t);
            }
        }, arena);
    }

    /**
     * Creates a VoidCallback upcall stub.
     */
    public static MemorySegment createVoidCallback(
            Arena arena,
            SingleResultCallback<Void> callback) {

        return VoidCallback.allocate((userdata, error) -> {
            try {
                if (!error.equals(MemorySegment.NULL)) {
                    callback.completeExceptionally(ErrorConverter.toException(error));
                } else {
                    callback.complete(null);
                }
            } catch (Throwable t) {
                callback.completeExceptionally(t);
            }
        }, arena);
    }

    /**
     * Creates a CreateIndexCallback upcall stub that extracts the index name string.
     */
    public static MemorySegment createStringCallback(
            Arena arena,
            SingleResultCallback<String> callback) {

        return CreateIndexCallback.allocate((userdata, result, error) -> {
            try {
                if (!error.equals(MemorySegment.NULL)) {
                    callback.completeExceptionally(ErrorConverter.toException(error));
                } else {
                    MemorySegment namePtr = CreateIndexResult.index_name(result);
                    String name = namePtr.reinterpret(1024).getString(0);
                    callback.complete(name);
                }
            } catch (Throwable t) {
                callback.completeExceptionally(t);
            }
        }, arena);
    }

    /**
     * Creates a CreateIndexesCallback upcall stub that extracts the list of index names.
     */
    public static MemorySegment createStringListCallback(
            Arena arena,
            SingleResultCallback<List<String>> callback) {

        return CreateIndexesCallback.allocate((userdata, result, error) -> {
            try {
                if (!error.equals(MemorySegment.NULL)) {
                    callback.completeExceptionally(ErrorConverter.toException(error));
                } else {
                    // CreateIndexesResult contains index_names as a Bson document with array
                    MemorySegment bsonPtr = CreateIndexesResult.index_names(result);
                    BsonDocument doc = BsonMarshaller.fromBsonStruct(bsonPtr);
                    List<String> names = new java.util.ArrayList<>();
                    if (doc != null && doc.containsKey("values")) {
                        for (org.bson.BsonValue v : doc.getArray("values")) {
                            names.add(v.asString().getValue());
                        }
                    }
                    callback.complete(names);
                }
            } catch (Throwable t) {
                callback.completeExceptionally(t);
            }
        }, arena);
    }

    /**
     * Creates a ListCollectionNamesCallback upcall stub.
     */
    public static MemorySegment createListCollectionNamesCallback(
            Arena arena,
            SingleResultCallback<List<String>> callback) {

        return ListCollectionNamesCallback.allocate((userdata, result, error) -> {
            try {
                if (!error.equals(MemorySegment.NULL)) {
                    callback.completeExceptionally(ErrorConverter.toException(error));
                } else {
                    // ListCollectionNamesResult contains names as a Bson document with array
                    MemorySegment bsonPtr = ListCollectionNamesResult.names(result);
                    BsonDocument doc = BsonMarshaller.fromBsonStruct(bsonPtr);
                    List<String> names = new java.util.ArrayList<>();
                    if (doc != null && doc.containsKey("values")) {
                        for (org.bson.BsonValue v : doc.getArray("values")) {
                            names.add(v.asString().getValue());
                        }
                    }
                    callback.complete(names);
                }
            } catch (Throwable t) {
                callback.completeExceptionally(t);
            }
        }, arena);
    }

    /**
     * Creates a ListDatabaseNamesCallback upcall stub.
     */
    public static MemorySegment createListDatabaseNamesCallback(
            Arena arena,
            SingleResultCallback<List<String>> callback) {

        return ListDatabaseNamesCallback.allocate((userdata, result, error) -> {
            try {
                if (!error.equals(MemorySegment.NULL)) {
                    callback.completeExceptionally(ErrorConverter.toException(error));
                } else {
                    // ListDatabaseNamesResult contains names as a Bson document with array
                    MemorySegment bsonPtr = ListDatabaseNamesResult.names(result);
                    BsonDocument doc = BsonMarshaller.fromBsonStruct(bsonPtr);
                    List<String> names = new java.util.ArrayList<>();
                    if (doc != null && doc.containsKey("values")) {
                        for (org.bson.BsonValue v : doc.getArray("values")) {
                            names.add(v.asString().getValue());
                        }
                    }
                    callback.complete(names);
                }
            } catch (Throwable t) {
                callback.completeExceptionally(t);
            }
        }, arena);
    }

    /**
     * Creates a ListDatabasesCallback upcall stub.
     */
    public static MemorySegment createListDatabasesCallback(
            Arena arena,
            SingleResultCallback<BsonDocument> callback) {

        return ListDatabasesCallback.allocate((userdata, result, error) -> {
            try {
                if (!error.equals(MemorySegment.NULL)) {
                    callback.completeExceptionally(ErrorConverter.toException(error));
                } else {
                    // ListDatabasesResult contains databases as Bson and total_size
                    MemorySegment bsonPtr = ListDatabasesResult.databases(result);
                    BsonDocument doc = BsonMarshaller.fromBsonStruct(bsonPtr);
                    callback.complete(doc);
                }
            } catch (Throwable t) {
                callback.completeExceptionally(t);
            }
        }, arena);
    }

    /**
     * Creates a CommandCallback upcall stub for run_command.
     */
    public static MemorySegment createCommandCallback(
            Arena arena,
            SingleResultCallback<BsonDocument> callback) {

        return CommandCallback.allocate((userdata, result, error) -> {
            try {
                if (!error.equals(MemorySegment.NULL)) {
                    callback.completeExceptionally(ErrorConverter.toException(error));
                } else {
                    // CommandResult contains response as Bson
                    MemorySegment bsonPtr = CommandResult.response(result);
                    BsonDocument doc = BsonMarshaller.fromBsonStruct(bsonPtr);
                    callback.complete(doc);
                }
            } catch (Throwable t) {
                callback.completeExceptionally(t);
            }
        }, arena);
    }

    /**
     * Creates a ChangeStreamResultCallback upcall stub.
     */
    public static MemorySegment createChangeStreamCallback(
            Arena arena,
            SingleResultCallback<MemorySegment> callback) {

        return ChangeStreamResultCallback.allocate((userdata, result, error) -> {
            try {
                if (!error.equals(MemorySegment.NULL)) {
                    callback.completeExceptionally(ErrorConverter.toException(error));
                } else {
                    // ChangeStreamResult contains cursor pointer
                    MemorySegment cursorPtr = ChangeStreamResult.cursor(result);
                    callback.complete(cursorPtr);
                }
            } catch (Throwable t) {
                callback.completeExceptionally(t);
            }
        }, arena);
    }

    /**
     * Creates a BulkWriteCallback upcall stub.
     */
    public static MemorySegment createBulkWriteCallback(
            Arena arena,
            SingleResultCallback<BulkWriteResult> callback) {

        return BulkWriteCallback.allocate((userdata, result, error) -> {
            try {
                if (!error.equals(MemorySegment.NULL)) {
                    callback.completeExceptionally(ErrorConverter.toException(error));
                } else {
                    callback.complete(ResultConverter.toBulkWriteResult(result));
                }
            } catch (Throwable t) {
                callback.completeExceptionally(t);
            }
        }, arena);
    }

    /**
     * Creates a SessionCallback upcall stub.
     * The callback receives a session pointer (MemorySegment) on success.
     */
    public static MemorySegment createSessionCallback(
            Arena arena,
            BiConsumer<MemorySegment, Throwable> callback) {

        return SessionCallback.allocate((userdata, result, error) -> {
            try {
                if (!error.equals(MemorySegment.NULL)) {
                    callback.accept(null, ErrorConverter.toException(error));
                } else {
                    MemorySegment sessionPtr = SessionResult.session(result);
                    callback.accept(sessionPtr, null);
                }
            } catch (Throwable t) {
                callback.accept(null, t);
            }
        }, arena);
    }
}

