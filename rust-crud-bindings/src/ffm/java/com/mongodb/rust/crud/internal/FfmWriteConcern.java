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

import com.mongodb.WriteConcern;
import com.mongodb.internal.rust.crud.ffi.MongoDbFfi;
import com.mongodb.internal.rust.crud.ffi.WriteConcernOptions;
import com.mongodb.lang.Nullable;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.concurrent.TimeUnit;

/**
 * Helper to create FFI WriteConcern handles from Java WriteConcern.
 */
final class FfmWriteConcern {

    private FfmWriteConcern() {
    }

    /**
     * Creates an FFI WriteConcern handle from a Java WriteConcern.
     *
     * @param arena the arena to allocate the options struct in
     * @param writeConcern the Java WriteConcern (may be null)
     * @return the FFI handle, or NULL if writeConcern is null
     */
    static MemorySegment create(Arena arena, @Nullable WriteConcern writeConcern) {
        if (writeConcern == null) {
            return MemorySegment.NULL;
        }

        MemorySegment options = WriteConcernOptions.allocate(arena);

        // w value: -1 = not set, 0 = unacknowledged, 1+ = w value
        // If w_tag is set, w field is ignored
        Object wObject = writeConcern.getWObject();
        if (wObject instanceof Integer) {
            WriteConcernOptions.w(options, (Integer) wObject);
            WriteConcernOptions.w_tag(options, MemorySegment.NULL);
        } else if (wObject instanceof String) {
            WriteConcernOptions.w(options, -1); // ignored when w_tag is set
            WriteConcernOptions.w_tag(options, arena.allocateFrom((String) wObject));
        } else {
            WriteConcernOptions.w(options, -1);
            WriteConcernOptions.w_tag(options, MemorySegment.NULL);
        }

        // journal: -1 = not set, 0 = false, 1 = true
        Boolean journal = writeConcern.getJournal();
        if (journal == null) {
            WriteConcernOptions.journal(options, (byte) -1);
        } else {
            WriteConcernOptions.journal(options, (byte) (journal ? 1 : 0));
        }

        // w_timeout_ms: -1 = not set
        Integer wTimeoutMs = writeConcern.getWTimeout(TimeUnit.MILLISECONDS);
        if (wTimeoutMs == null) {
            WriteConcernOptions.w_timeout_ms(options, -1L);
        } else {
            WriteConcernOptions.w_timeout_ms(options, wTimeoutMs.longValue());
        }

        return MongoDbFfi.mongo_write_concern_create(options);
    }

    /**
     * Destroys an FFI WriteConcern handle.
     *
     * @param handle the handle to destroy (safe to call with NULL)
     */
    static void destroy(MemorySegment handle) {
        if (handle != null && !handle.equals(MemorySegment.NULL)) {
            MongoDbFfi.mongo_write_concern_destroy(handle);
        }
    }
}

