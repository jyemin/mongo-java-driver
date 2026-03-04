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
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class FfmReadPreferenceTest {

    @Test
    void testCreateWithNull() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment result = FfmReadPreference.create(arena, null);
            assertEquals(MemorySegment.NULL, result);
        }
    }

    @Test
    void testCreateWithPrimary() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment result = FfmReadPreference.create(arena, ReadPreference.primary());
            assertNotEquals(MemorySegment.NULL, result);
            FfmReadPreference.destroy(result);
        }
    }

    @Test
    void testCreateWithPrimaryPreferred() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment result = FfmReadPreference.create(arena, ReadPreference.primaryPreferred());
            assertNotEquals(MemorySegment.NULL, result);
            FfmReadPreference.destroy(result);
        }
    }

    @Test
    void testCreateWithSecondary() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment result = FfmReadPreference.create(arena, ReadPreference.secondary());
            assertNotEquals(MemorySegment.NULL, result);
            FfmReadPreference.destroy(result);
        }
    }

    @Test
    void testCreateWithSecondaryPreferred() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment result = FfmReadPreference.create(arena, ReadPreference.secondaryPreferred());
            assertNotEquals(MemorySegment.NULL, result);
            FfmReadPreference.destroy(result);
        }
    }

    @Test
    void testCreateWithNearest() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment result = FfmReadPreference.create(arena, ReadPreference.nearest());
            assertNotEquals(MemorySegment.NULL, result);
            FfmReadPreference.destroy(result);
        }
    }

    @Test
    void testCreateWithMaxStaleness() {
        try (Arena arena = Arena.ofConfined()) {
            ReadPreference rp = ReadPreference.secondary()
                    .withMaxStalenessMS(120000L, TimeUnit.MILLISECONDS);
            MemorySegment result = FfmReadPreference.create(arena, rp);
            assertNotEquals(MemorySegment.NULL, result);
            FfmReadPreference.destroy(result);
        }
    }

    @Test
    void testDestroyWithNull() {
        // Should not throw
        FfmReadPreference.destroy(null);
        FfmReadPreference.destroy(MemorySegment.NULL);
    }
}

