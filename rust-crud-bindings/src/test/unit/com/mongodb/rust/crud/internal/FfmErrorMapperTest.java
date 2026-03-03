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
import com.mongodb.internal.rust.crud.ffi.AuthError;
import com.mongodb.internal.rust.crud.ffi.ErrorUnion;
import com.mongodb.internal.rust.crud.ffi.Error_;
import com.mongodb.internal.rust.crud.ffi.IncompatibleServerError;
import com.mongodb.internal.rust.crud.ffi.InvalidArgumentError;
import com.mongodb.internal.rust.crud.ffi.IoError;
import com.mongodb.internal.rust.crud.ffi.ServerError;
import com.mongodb.internal.rust.crud.ffi.ServerSelectionError;
import com.mongodb.internal.rust.crud.ffi.TimeoutError;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for FfmErrorMapper using mock FFI error structs.
 */
class FfmErrorMapperTest {

    @Test
    void testMapServerError() {
        try (Arena arena = Arena.ofConfined()) {
            // Create ServerError struct
            MemorySegment serverError = ServerError.allocate(arena);
            ServerError.code(serverError, 11000); // duplicate key error
            ServerError.code_name(serverError, arena.allocateFrom("DuplicateKey"));
            ServerError.message(serverError, arena.allocateFrom("E11000 duplicate key error"));
            ServerError.labels(serverError, MemorySegment.NULL);
            ServerError.labels_len(serverError, 0);

            // Create Error_ struct
            MemorySegment error = createError(arena, (byte) 0, serverError);

            // Map and verify
            MongoException ex = FfmErrorMapper.mapError(error);
            assertInstanceOf(MongoCommandException.class, ex);
            MongoCommandException cmdEx = (MongoCommandException) ex;
            assertEquals(11000, cmdEx.getErrorCode());
        }
    }

    @Test
    void testMapServerErrorWithLabels() {
        try (Arena arena = Arena.ofConfined()) {
            // Create labels array (char**)
            MemorySegment label1 = arena.allocateFrom("TransientTransactionError");
            MemorySegment label2 = arena.allocateFrom("RetryableWriteError");
            MemorySegment labelsArray = arena.allocate(ValueLayout.ADDRESS, 2);
            labelsArray.setAtIndex(ValueLayout.ADDRESS, 0, label1);
            labelsArray.setAtIndex(ValueLayout.ADDRESS, 1, label2);

            // Create ServerError struct
            MemorySegment serverError = ServerError.allocate(arena);
            ServerError.code(serverError, 112);
            ServerError.code_name(serverError, arena.allocateFrom("WriteConflict"));
            ServerError.message(serverError, arena.allocateFrom("Write conflict"));
            ServerError.labels(serverError, labelsArray);
            ServerError.labels_len(serverError, 2);

            // Create Error_ struct
            MemorySegment error = createError(arena, (byte) 0, serverError);

            // Map and verify
            MongoException ex = FfmErrorMapper.mapError(error);
            assertInstanceOf(MongoCommandException.class, ex);
            assertTrue(ex.hasErrorLabel("TransientTransactionError"));
            assertTrue(ex.hasErrorLabel("RetryableWriteError"));
        }
    }

    @Test
    void testMapIoError() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment ioError = IoError.allocate(arena);
            IoError.message(ioError, arena.allocateFrom("Connection refused"));

            MemorySegment error = createError(arena, (byte) 3, ioError);

            MongoException ex = FfmErrorMapper.mapError(error);
            assertInstanceOf(MongoSocketException.class, ex);
            assertTrue(ex.getMessage().contains("Connection refused"));
        }
    }

    @Test
    void testMapServerSelectionError() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment ssError = ServerSelectionError.allocate(arena);
            ServerSelectionError.message(ssError, arena.allocateFrom("No servers available"));

            MemorySegment error = createError(arena, (byte) 4, ssError);

            MongoException ex = FfmErrorMapper.mapError(error);
            assertInstanceOf(MongoTimeoutException.class, ex);
            assertTrue(ex.getMessage().contains("No servers available"));
        }
    }

    @Test
    void testMapTimeoutError() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment timeoutError = TimeoutError.allocate(arena);
            TimeoutError.message(timeoutError, arena.allocateFrom("Operation timed out after 30s"));

            MemorySegment error = createError(arena, (byte) 5, timeoutError);

            MongoException ex = FfmErrorMapper.mapError(error);
            assertInstanceOf(MongoTimeoutException.class, ex);
            assertTrue(ex.getMessage().contains("30s"));
        }
    }

    @Test
    void testMapAuthError() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment authError = AuthError.allocate(arena);
            AuthError.message(authError, arena.allocateFrom("Authentication failed"));

            MemorySegment error = createError(arena, (byte) 6, authError);

            MongoException ex = FfmErrorMapper.mapError(error);
            assertInstanceOf(MongoSecurityException.class, ex);
        }
    }

    @Test
    void testMapInvalidArgumentError() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment invArgError = InvalidArgumentError.allocate(arena);
            InvalidArgumentError.message(invArgError, arena.allocateFrom("Invalid filter"));

            MemorySegment error = createError(arena, (byte) 7, invArgError);

            MongoException ex = FfmErrorMapper.mapError(error);
            assertTrue(ex.getMessage().contains("Invalid"));
        }
    }

    @Test
    void testMapIncompatibleServerError() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment incompatError = IncompatibleServerError.allocate(arena);
            IncompatibleServerError.message(incompatError, arena.allocateFrom("Server version too old"));

            MemorySegment error = createError(arena, (byte) 9, incompatError);

            MongoException ex = FfmErrorMapper.mapError(error);
            assertInstanceOf(MongoIncompatibleDriverException.class, ex);
        }
    }

    @Test
    void testMapUnknownErrorType() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment error = Error_.allocate(arena);
            Error_.error_type(error, (byte) 99);

            MongoException ex = FfmErrorMapper.mapError(error);
            assertTrue(ex.getMessage().contains("Unknown error type"));
        }
    }

    private MemorySegment createError(Arena arena, byte errorType, MemorySegment specificError) {
        MemorySegment error = Error_.allocate(arena);
        Error_.error_type(error, errorType);
        MemorySegment errorUnion = Error_.error(error);
        // All union fields are at offset 0, so we can just set the pointer
        errorUnion.set(java.lang.foreign.ValueLayout.ADDRESS, 0, specificError);
        return error;
    }
}

