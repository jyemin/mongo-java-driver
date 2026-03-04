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
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

import static org.junit.jupiter.api.Assertions.*;

class FfmReadConcernTest {

    @Test
    void testCreateWithNull() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment result = FfmReadConcern.create(arena, null);
            assertEquals(MemorySegment.NULL, result);
        }
    }

    @Test
    void testCreateWithDefault() {
        try (Arena arena = Arena.ofConfined()) {
            // Default ReadConcern has no level set
            MemorySegment result = FfmReadConcern.create(arena, ReadConcern.DEFAULT);
            assertEquals(MemorySegment.NULL, result);
        }
    }

    @Test
    void testCreateWithLocal() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment result = FfmReadConcern.create(arena, ReadConcern.LOCAL);
            assertNotEquals(MemorySegment.NULL, result);
            FfmReadConcern.destroy(result);
        }
    }

    @Test
    void testCreateWithMajority() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment result = FfmReadConcern.create(arena, ReadConcern.MAJORITY);
            assertNotEquals(MemorySegment.NULL, result);
            FfmReadConcern.destroy(result);
        }
    }

    @Test
    void testCreateWithSnapshot() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment result = FfmReadConcern.create(arena, ReadConcern.SNAPSHOT);
            assertNotEquals(MemorySegment.NULL, result);
            FfmReadConcern.destroy(result);
        }
    }

    @Test
    void testCreateWithLinearizable() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment result = FfmReadConcern.create(arena, ReadConcern.LINEARIZABLE);
            assertNotEquals(MemorySegment.NULL, result);
            FfmReadConcern.destroy(result);
        }
    }

    @Test
    void testDestroyWithNull() {
        // Should not throw
        FfmReadConcern.destroy(null);
        FfmReadConcern.destroy(MemorySegment.NULL);
    }
}

