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

import com.mongodb.ReadConcern;
import com.mongodb.internal.rust.crud.ffi.MongoDbFfi;
import com.mongodb.internal.rust.crud.ffi.ReadConcernOptions;
import com.mongodb.lang.Nullable;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

/**
 * Helper to create FFI ReadConcern handles from Java ReadConcern.
 */
final class FfmReadConcern {

    private FfmReadConcern() {
    }

    /**
     * Creates an FFI ReadConcern handle from a Java ReadConcern.
     *
     * @param arena the arena to allocate the options struct in
     * @param readConcern the Java ReadConcern (may be null)
     * @return the FFI handle, or NULL if readConcern is null or has no level
     */
    static MemorySegment create(Arena arena, @Nullable ReadConcern readConcern) {
        if (readConcern == null || readConcern.getLevel() == null) {
            return MemorySegment.NULL;
        }

        MemorySegment options = ReadConcernOptions.allocate(arena);
        ReadConcernOptions.level(options, arena.allocateFrom(readConcern.getLevel().getValue()));

        return MongoDbFfi.mongo_read_concern_create(options);
    }

    /**
     * Destroys an FFI ReadConcern handle.
     *
     * @param handle the handle to destroy (safe to call with NULL)
     */
    static void destroy(MemorySegment handle) {
        if (handle != null && !handle.equals(MemorySegment.NULL)) {
            MongoDbFfi.mongo_read_concern_destroy(handle);
        }
    }
}

