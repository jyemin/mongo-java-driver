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

import com.mongodb.MongoCommandException;
import com.mongodb.MongoException;
import com.mongodb.MongoIncompatibleDriverException;
import com.mongodb.MongoSecurityException;
import com.mongodb.MongoSocketException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.ServerAddress;
import com.mongodb.internal.rust.crud.ffi.AuthError;
import com.mongodb.internal.rust.crud.ffi.ErrorUnion;
import com.mongodb.internal.rust.crud.ffi.Error_;
import com.mongodb.internal.rust.crud.ffi.IncompatibleServerError;
import com.mongodb.internal.rust.crud.ffi.InvalidArgumentError;
import com.mongodb.internal.rust.crud.ffi.InvalidResponseError;
import com.mongodb.internal.rust.crud.ffi.IoError;
import com.mongodb.internal.rust.crud.ffi.MongoDbFfi;
import com.mongodb.internal.rust.crud.ffi.ServerError;
import com.mongodb.internal.rust.crud.ffi.ServerSelectionError;
import com.mongodb.internal.rust.crud.ffi.ShutdownError;
import com.mongodb.internal.rust.crud.ffi.TimeoutError;
import com.mongodb.internal.rust.crud.ffi.TransactionError;
import org.bson.BsonDocument;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Maps FFI errors to Java driver exceptions.
 */
final class FfmErrorMapper {

    // Error type constants from Rust FFI
    private static final byte ERROR_TYPE_SERVER = 0;
    private static final byte ERROR_TYPE_INSERT_MANY = 1;
    private static final byte ERROR_TYPE_BULK_WRITE = 2;
    private static final byte ERROR_TYPE_IO = 3;
    private static final byte ERROR_TYPE_SERVER_SELECTION = 4;
    private static final byte ERROR_TYPE_TIMEOUT = 5;
    private static final byte ERROR_TYPE_AUTH = 6;
    private static final byte ERROR_TYPE_INVALID_ARGUMENT = 7;
    private static final byte ERROR_TYPE_TRANSACTION = 8;
    private static final byte ERROR_TYPE_INCOMPATIBLE_SERVER = 9;
    private static final byte ERROR_TYPE_INVALID_RESPONSE = 10;
    private static final byte ERROR_TYPE_CHANGE_STREAM = 11;
    private static final byte ERROR_TYPE_SHUTDOWN = 12;

    private FfmErrorMapper() {
    }

    /**
     * Maps an FFI error to a Java driver exception and frees the error.
     *
     * @param errorPtr pointer to the FFI Error struct (will be freed)
     * @return the appropriate MongoException subclass
     */
    static MongoException toException(MemorySegment errorPtr) {
        try {
            return mapError(errorPtr);
        } finally {
            MongoDbFfi.error_free(errorPtr);
        }
    }

    /**
     * Maps an FFI error to a Java driver exception without freeing.
     * Exposed for testing with mock error structs.
     *
     * @param errorPtr pointer to the FFI Error struct
     * @return the appropriate MongoException subclass
     */
    static MongoException mapError(MemorySegment errorPtr) {
        MemorySegment error = errorPtr.reinterpret(Error_.sizeof());
        byte errorType = Error_.error_type(error);
        MemorySegment errorUnion = Error_.error(error);

        return switch (errorType) {
            case ERROR_TYPE_SERVER -> mapServerError(errorUnion);
            case ERROR_TYPE_IO -> mapIoError(errorUnion);
            case ERROR_TYPE_SERVER_SELECTION -> mapServerSelectionError(errorUnion);
            case ERROR_TYPE_TIMEOUT -> mapTimeoutError(errorUnion);
            case ERROR_TYPE_AUTH -> mapAuthError(errorUnion);
            case ERROR_TYPE_INVALID_ARGUMENT -> mapInvalidArgumentError(errorUnion);
            case ERROR_TYPE_TRANSACTION -> mapTransactionError(errorUnion);
            case ERROR_TYPE_INCOMPATIBLE_SERVER -> mapIncompatibleServerError(errorUnion);
            case ERROR_TYPE_INVALID_RESPONSE -> mapInvalidResponseError(errorUnion);
            case ERROR_TYPE_SHUTDOWN -> mapShutdownError(errorUnion);
            // TODO: INSERT_MANY, BULK_WRITE, CHANGE_STREAM need special handling
            default -> new MongoException("Unknown error type: " + errorType);
        };
    }

    private static MongoException mapServerError(MemorySegment errorUnion) {
        MemorySegment serverErrorPtr = ErrorUnion.server(errorUnion);
        if (serverErrorPtr.address() == 0) {
            return new MongoException("Server error with null pointer");
        }
        MemorySegment serverError = serverErrorPtr.reinterpret(ServerError.sizeof());

        int code = ServerError.code(serverError);
        String codeName = getString(ServerError.code_name(serverError));
        String message = getString(ServerError.message(serverError));
        List<String> labels = getStringArray(ServerError.labels(serverError), ServerError.labels_len(serverError));

        // TODO: Extract server_response from ServerError.server_response (OwnedBson)
        // TODO: ServerAddress not available in FFI error - would need to track separately
        BsonDocument response = new BsonDocument()
                .append("ok", new org.bson.BsonInt32(0))
                .append("code", new org.bson.BsonInt32(code))
                .append("codeName", new org.bson.BsonString(codeName != null ? codeName : ""))
                .append("errmsg", new org.bson.BsonString(message != null ? message : ""));

        MongoCommandException ex = new MongoCommandException(response, new ServerAddress());
        labels.forEach(ex::addLabel);
        return ex;
    }

    private static MongoException mapIoError(MemorySegment errorUnion) {
        MemorySegment ioErrorPtr = ErrorUnion.io(errorUnion);
        if (ioErrorPtr.address() == 0) {
            return new MongoSocketException("IO error with null pointer", new ServerAddress(), null);
        }
        MemorySegment ioError = ioErrorPtr.reinterpret(IoError.sizeof());
        String message = getString(IoError.message(ioError));
        // TODO: ServerAddress not available in FFI IoError
        // TODO: Cause (Throwable) not available in FFI IoError
        return new MongoSocketException(message != null ? message : "IO error", new ServerAddress(), null);
    }

    private static MongoException mapServerSelectionError(MemorySegment errorUnion) {
        MemorySegment ssErrorPtr = ErrorUnion.server_selection(errorUnion);
        if (ssErrorPtr.address() == 0) {
            return new MongoTimeoutException("Server selection error with null pointer");
        }
        MemorySegment ssError = ssErrorPtr.reinterpret(ServerSelectionError.sizeof());
        String message = getString(ServerSelectionError.message(ssError));
        return new MongoTimeoutException(message != null ? message : "Server selection timeout");
    }

    private static MongoException mapTimeoutError(MemorySegment errorUnion) {
        MemorySegment timeoutErrorPtr = ErrorUnion.timeout(errorUnion);
        if (timeoutErrorPtr.address() == 0) {
            return new MongoTimeoutException("Timeout error with null pointer");
        }
        MemorySegment timeoutError = timeoutErrorPtr.reinterpret(TimeoutError.sizeof());
        String message = getString(TimeoutError.message(timeoutError));
        return new MongoTimeoutException(message != null ? message : "Operation timed out");
    }

    private static MongoException mapAuthError(MemorySegment errorUnion) {
        MemorySegment authErrorPtr = ErrorUnion.auth(errorUnion);
        if (authErrorPtr.address() == 0) {
            return new MongoSecurityException(null, "Authentication error with null pointer", null);
        }
        MemorySegment authError = authErrorPtr.reinterpret(AuthError.sizeof());
        String message = getString(AuthError.message(authError));
        // TODO: MongoCredential not available in FFI AuthError
        // TODO: Cause (Throwable) not available in FFI AuthError
        return new MongoSecurityException(null, message != null ? message : "Authentication failed", null);
    }

    private static MongoException mapInvalidArgumentError(MemorySegment errorUnion) {
        MemorySegment invArgErrorPtr = ErrorUnion.invalid_argument(errorUnion);
        if (invArgErrorPtr.address() == 0) {
            return new MongoException("Invalid argument error with null pointer");
        }
        MemorySegment invArgError = invArgErrorPtr.reinterpret(InvalidArgumentError.sizeof());
        String message = getString(InvalidArgumentError.message(invArgError));
        return new MongoException("Invalid argument: " + (message != null ? message : "unknown"));
    }

    private static MongoException mapTransactionError(MemorySegment errorUnion) {
        MemorySegment txnErrorPtr = ErrorUnion.transaction(errorUnion);
        if (txnErrorPtr.address() == 0) {
            return new MongoException("Transaction error with null pointer");
        }
        MemorySegment txnError = txnErrorPtr.reinterpret(TransactionError.sizeof());
        String message = getString(TransactionError.message(txnError));
        List<String> labels = getStringArray(TransactionError.labels(txnError), TransactionError.labels_len(txnError));

        MongoException ex = new MongoException(message != null ? message : "Transaction error");
        labels.forEach(ex::addLabel);
        return ex;
    }

    private static MongoException mapIncompatibleServerError(MemorySegment errorUnion) {
        MemorySegment incompatErrorPtr = ErrorUnion.incompatible_server(errorUnion);
        if (incompatErrorPtr.address() == 0) {
            return new MongoIncompatibleDriverException("Incompatible server error with null pointer", null);
        }
        MemorySegment incompatError = incompatErrorPtr.reinterpret(IncompatibleServerError.sizeof());
        String message = getString(IncompatibleServerError.message(incompatError));
        // TODO: ClusterDescription not available in FFI error
        return new MongoIncompatibleDriverException(message != null ? message : "Incompatible server", null);
    }

    private static MongoException mapInvalidResponseError(MemorySegment errorUnion) {
        MemorySegment invRespErrorPtr = ErrorUnion.invalid_response(errorUnion);
        if (invRespErrorPtr.address() == 0) {
            return new MongoException("Invalid response error with null pointer");
        }
        MemorySegment invRespError = invRespErrorPtr.reinterpret(InvalidResponseError.sizeof());
        String message = getString(InvalidResponseError.message(invRespError));
        return new MongoException("Invalid server response: " + (message != null ? message : "unknown"));
    }

    private static MongoException mapShutdownError(MemorySegment errorUnion) {
        return new MongoException("Client has been shut down");
    }

    private static String getString(MemorySegment ptr) {
        if (ptr.address() == 0) {
            return null;
        }
        // reinterpret with Long.MAX_VALUE allows getString to read until null terminator
        return ptr.reinterpret(Long.MAX_VALUE).getString(0);
    }

    private static List<String> getStringArray(MemorySegment arrayPtr, long length) {
        if (arrayPtr.address() == 0 || length == 0) {
            return Collections.emptyList();
        }
        List<String> result = new ArrayList<>((int) length);
        // arrayPtr is char** - array of pointers to strings
        MemorySegment array = arrayPtr.reinterpret(ValueLayout.ADDRESS.byteSize() * length);
        for (int i = 0; i < length; i++) {
            MemorySegment strPtr = array.getAtIndex(ValueLayout.ADDRESS, i);
            String str = getString(strPtr);
            if (str != null) {
                result.add(str);
            }
        }
        return result;
    }
}
