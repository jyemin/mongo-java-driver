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
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.Arrays;
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
    void testCreateWithSingleTagSet() {
        try (Arena arena = Arena.ofConfined()) {
            TagSet tagSet = new TagSet(Arrays.asList(
                    new Tag("dc", "east"),
                    new Tag("rack", "r1")
            ));
            ReadPreference rp = ReadPreference.secondary(tagSet);

            // Verify tags are actually in the read preference
            assertTrue(rp instanceof TaggableReadPreference);
            TaggableReadPreference trp = (TaggableReadPreference) rp;
            assertEquals(1, trp.getTagSetList().size());
            assertTrue(trp.getTagSetList().get(0).iterator().hasNext()); // has tags

            MemorySegment result = FfmReadPreference.create(arena, rp);
            assertNotEquals(MemorySegment.NULL, result);
            FfmReadPreference.destroy(result);
        }
    }

    @Test
    void testCreateWithMultipleTagSets() {
        try (Arena arena = Arena.ofConfined()) {
            TagSet tagSet1 = new TagSet(Arrays.asList(
                    new Tag("dc", "east"),
                    new Tag("rack", "r1")
            ));
            TagSet tagSet2 = new TagSet(Arrays.asList(
                    new Tag("dc", "west")
            ));
            ReadPreference rp = ReadPreference.nearest(Arrays.asList(tagSet1, tagSet2));
            MemorySegment result = FfmReadPreference.create(arena, rp);
            assertNotEquals(MemorySegment.NULL, result);
            FfmReadPreference.destroy(result);
        }
    }

    @Test
    void testCreateWithEmptyTagSet() {
        try (Arena arena = Arena.ofConfined()) {
            // Empty tag set - should work without tags
            ReadPreference rp = ReadPreference.secondaryPreferred(new TagSet());
            MemorySegment result = FfmReadPreference.create(arena, rp);
            assertNotEquals(MemorySegment.NULL, result);
            FfmReadPreference.destroy(result);
        }
    }

    @Test
    void testCreateWithTagsAndMaxStaleness() {
        try (Arena arena = Arena.ofConfined()) {
            TagSet tagSet = new TagSet(new Tag("region", "us-east-1"));
            ReadPreference rp = ReadPreference.secondary(tagSet)
                    .withMaxStalenessMS(90000L, TimeUnit.MILLISECONDS);
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

