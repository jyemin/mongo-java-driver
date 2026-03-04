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

import com.mongodb.ReadPreference;
import com.mongodb.Tag;
import com.mongodb.TagSet;
import com.mongodb.TaggableReadPreference;
import com.mongodb.internal.rust.crud.ffi.MongoDbFfi;
import com.mongodb.internal.rust.crud.ffi.ReadPreferenceOptions;
import com.mongodb.lang.Nullable;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Helper to create FFI ReadPreference handles from Java ReadPreference.
 */
final class FfmReadPreference {

    // FFI read preference mode values
    private static final byte MODE_PRIMARY = 0;
    private static final byte MODE_PRIMARY_PREFERRED = 1;
    private static final byte MODE_SECONDARY = 2;
    private static final byte MODE_SECONDARY_PREFERRED = 3;
    private static final byte MODE_NEAREST = 4;

    private FfmReadPreference() {
    }

    /**
     * Creates an FFI ReadPreference handle from a Java ReadPreference.
     *
     * @param arena the arena to allocate the options struct in
     * @param readPreference the Java ReadPreference (may be null)
     * @return the FFI handle, or NULL if readPreference is null
     */
    static MemorySegment create(Arena arena, @Nullable ReadPreference readPreference) {
        if (readPreference == null) {
            return MemorySegment.NULL;
        }

        byte mode = toMode(readPreference);

        MemorySegment options = ReadPreferenceOptions.allocate(arena);

        // Only TaggableReadPreference has tags and max staleness (not primary)
        if (readPreference instanceof TaggableReadPreference) {
            TaggableReadPreference taggable = (TaggableReadPreference) readPreference;

            // Tags: BSON array of documents
            List<TagSet> tagSetList = taggable.getTagSetList();
            if (tagSetList.isEmpty()) {
                ReadPreferenceOptions.tags(options, MemorySegment.NULL);
            } else {
                ReadPreferenceOptions.tags(options, tagSetListToBson(arena, tagSetList));
            }

            // Max staleness in seconds, -1 = not set
            Long maxStalenessMs = taggable.getMaxStaleness(TimeUnit.MILLISECONDS);
            if (maxStalenessMs == null) {
                ReadPreferenceOptions.max_staleness_seconds(options, -1L);
            } else {
                ReadPreferenceOptions.max_staleness_seconds(options, maxStalenessMs / 1000);
            }
        } else {
            ReadPreferenceOptions.tags(options, MemorySegment.NULL);
            ReadPreferenceOptions.max_staleness_seconds(options, -1L);
        }

        // Hedge: BSON document, currently not supported
        // TODO: Support hedge options
        ReadPreferenceOptions.hedge(options, MemorySegment.NULL);

        return MongoDbFfi.mongo_read_preference_create(mode, options);
    }

    /**
     * Converts a list of TagSets to a BSON array memory segment.
     * Format: [{tag1: value1, tag2: value2}, {tag3: value3}]
     */
    private static MemorySegment tagSetListToBson(Arena arena, List<TagSet> tagSetList) {
        BsonArray array = new BsonArray();
        for (TagSet tagSet : tagSetList) {
            BsonDocument doc = new BsonDocument();
            for (Tag tag : tagSet) {
                doc.put(tag.getName(), new BsonString(tag.getValue()));
            }
            array.add(doc);
        }
        return BsonMarshaller.toBsonArrayStruct(arena, array);
    }

    /**
     * Converts a Java ReadPreference to an FFI mode byte.
     */
    private static byte toMode(ReadPreference readPreference) {
        String name = readPreference.getName();
        return switch (name) {
            case "primary" -> MODE_PRIMARY;
            case "primaryPreferred" -> MODE_PRIMARY_PREFERRED;
            case "secondary" -> MODE_SECONDARY;
            case "secondaryPreferred" -> MODE_SECONDARY_PREFERRED;
            case "nearest" -> MODE_NEAREST;
            default -> throw new IllegalArgumentException("Unknown read preference: " + name);
        };
    }

    /**
     * Destroys an FFI ReadPreference handle.
     *
     * @param handle the handle to destroy (safe to call with NULL)
     */
    static void destroy(MemorySegment handle) {
        if (handle != null && !handle.equals(MemorySegment.NULL)) {
            MongoDbFfi.mongo_read_preference_destroy(handle);
        }
    }
}

