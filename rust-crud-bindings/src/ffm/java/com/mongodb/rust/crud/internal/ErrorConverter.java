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

import com.mongodb.MongoException;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.MongoSecurityException;
import com.mongodb.ServerAddress;
import com.mongodb.internal.rust.crud.ffi.Error_;
import com.mongodb.internal.rust.crud.ffi.ErrorUnion;
import com.mongodb.internal.rust.crud.ffi.ServerError;
import com.mongodb.internal.rust.crud.ffi.TimeoutError;
import com.mongodb.internal.rust.crud.ffi.AuthError;
import com.mongodb.internal.rust.crud.ffi.IoError;
import com.mongodb.internal.rust.crud.ffi.InvalidArgumentError;
import org.bson.BsonDocument;

import java.lang.foreign.MemorySegment;

/**
 * Converts FFI error structs to Java exceptions.
 */
public final class ErrorConverter {

    // Error type constants (from ErrorType enum in FFI)
    private static final byte SERVER = 0;
    private static final byte INSERT_MANY = 1;
    private static final byte BULK_WRITE = 2;
    private static final byte IO = 3;
    private static final byte SERVER_SELECTION = 4;
    private static final byte TIMEOUT = 5;
    private static final byte AUTH = 6;
    private static final byte INVALID_ARGUMENT = 7;
    private static final byte TRANSACTION = 8;
    private static final byte INCOMPATIBLE_SERVER = 9;
    private static final byte INVALID_RESPONSE = 10;
    private static final byte CHANGE_STREAM = 11;
    private static final byte SHUTDOWN = 12;

    private ErrorConverter() {
    }

    /**
     * Converts an FFI Error struct to a Java exception.
     */
    public static MongoException toException(MemorySegment errorPtr) {
        if (errorPtr == null || errorPtr.equals(MemorySegment.NULL)) {
            return new MongoException("Unknown error");
        }

        byte errorType = Error_.error_type(errorPtr);
        MemorySegment errorUnion = Error_.error(errorPtr);

        return switch (errorType) {
            case SERVER -> toServerException(ErrorUnion.server(errorUnion));
            case TIMEOUT -> toTimeoutException(ErrorUnion.timeout(errorUnion));
            case AUTH -> toAuthException(ErrorUnion.auth(errorUnion));
            case IO -> toIoException(ErrorUnion.io(errorUnion));
            case INVALID_ARGUMENT -> new MongoException(toInvalidArgumentException(ErrorUnion.invalid_argument(errorUnion)).getMessage());
            case SERVER_SELECTION -> toServerSelectionException(ErrorUnion.server_selection(errorUnion));
            default -> new MongoException("Error type: " + errorType);
        };
    }

    private static MongoCommandException toServerException(MemorySegment serverError) {
        int code = ServerError.code(serverError);
        String message = readString(ServerError.message(serverError));
        MemorySegment responsePtr = ServerError.server_response(serverError);
        BsonDocument response = BsonMarshaller.fromBsonStruct(responsePtr);
        if (response == null) {
            response = new BsonDocument();
        }
        return new MongoCommandException(response, new ServerAddress());
    }

    private static MongoTimeoutException toTimeoutException(MemorySegment timeoutError) {
        String message = readString(TimeoutError.message(timeoutError));
        return new MongoTimeoutException(message != null ? message : "Operation timed out");
    }

    private static MongoSecurityException toAuthException(MemorySegment authError) {
        String message = readString(AuthError.message(authError));
        return new MongoSecurityException(
            null, // credential
            message != null ? message : "Authentication failed"
        );
    }

    private static MongoException toIoException(MemorySegment ioError) {
        String message = readString(IoError.message(ioError));
        return new MongoException(message != null ? message : "I/O error");
    }

    private static IllegalArgumentException toInvalidArgumentException(MemorySegment invalidArgError) {
        String message = readString(InvalidArgumentError.message(invalidArgError));
        return new IllegalArgumentException(message != null ? message : "Invalid argument");
    }

    private static MongoException toServerSelectionException(MemorySegment serverSelectionError) {
        String message = readString(
            com.mongodb.internal.rust.crud.ffi.ServerSelectionError.message(serverSelectionError)
        );
        return new MongoException(message != null ? message : "Server selection failed");
    }

    /**
     * Reads a null-terminated string from a MemorySegment pointer.
     */
    private static String readString(MemorySegment ptr) {
        if (ptr == null || ptr.equals(MemorySegment.NULL)) {
            return null;
        }
        // Reinterpret as a large segment and find the null terminator
        MemorySegment unbounded = ptr.reinterpret(1024); // Assume max 1KB string
        return unbounded.getString(0);
    }
}

