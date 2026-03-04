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
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class FfmWriteConcernTest {

    @Test
    void testCreateWithNull() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment result = FfmWriteConcern.create(arena, null);
            assertEquals(MemorySegment.NULL, result);
        }
    }

    @Test
    void testCreateWithW1() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment result = FfmWriteConcern.create(arena, WriteConcern.W1);
            assertNotEquals(MemorySegment.NULL, result);
            FfmWriteConcern.destroy(result);
        }
    }

    @Test
    void testCreateWithW2() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment result = FfmWriteConcern.create(arena, WriteConcern.W2);
            assertNotEquals(MemorySegment.NULL, result);
            FfmWriteConcern.destroy(result);
        }
    }

    @Test
    void testCreateWithMajority() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment result = FfmWriteConcern.create(arena, WriteConcern.MAJORITY);
            assertNotEquals(MemorySegment.NULL, result);
            FfmWriteConcern.destroy(result);
        }
    }

    @Test
    void testCreateWithJournaled() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment result = FfmWriteConcern.create(arena, WriteConcern.JOURNALED);
            assertNotEquals(MemorySegment.NULL, result);
            FfmWriteConcern.destroy(result);
        }
    }

    @Test
    void testCreateWithUnacknowledged() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment result = FfmWriteConcern.create(arena, WriteConcern.UNACKNOWLEDGED);
            assertNotEquals(MemorySegment.NULL, result);
            FfmWriteConcern.destroy(result);
        }
    }

    @Test
    void testCreateWithWTimeout() {
        try (Arena arena = Arena.ofConfined()) {
            WriteConcern wc = WriteConcern.ACKNOWLEDGED.withWTimeout(5000, TimeUnit.MILLISECONDS);
            MemorySegment result = FfmWriteConcern.create(arena, wc);
            assertNotEquals(MemorySegment.NULL, result);
            FfmWriteConcern.destroy(result);
        }
    }

    @Test
    void testCreateWithAllOptions() {
        try (Arena arena = Arena.ofConfined()) {
            WriteConcern wc = new WriteConcern(2)
                    .withJournal(true)
                    .withWTimeout(3000, TimeUnit.MILLISECONDS);
            MemorySegment result = FfmWriteConcern.create(arena, wc);
            assertNotEquals(MemorySegment.NULL, result);
            FfmWriteConcern.destroy(result);
        }
    }

    @Test
    void testDestroyWithNull() {
        // Should not throw
        FfmWriteConcern.destroy(null);
        FfmWriteConcern.destroy(MemorySegment.NULL);
    }
}

