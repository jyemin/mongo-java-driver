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

import com.mongodb.internal.rust.crud.ffi.FindCallback;
import com.mongodb.internal.rust.crud.ffi.RunCommandCallback;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests to reproduce the arena close bug in FfmAsyncClient.find().
 *
 * The bug manifests as:
 * java.lang.IllegalStateException: Session is acquired by 1 clients
 *
 * This happens when trying to close a shared arena while an upcall stub
 * allocated in that arena is still on the call stack.
 */
class FfmAsyncClientArenaCloseTest {

    /**
     * Reproduces the bug: closing an arena from within a FindCallback upcall.
     *
     * This test directly uses the FindCallback.allocate pattern from FfmAsyncClient.find()
     * to demonstrate that closing the arena from within the callback fails with
     * "Session is acquired by 1 clients".
     */
    @Test
    void testArenaCloseInsideFindCallbackFails() throws Exception {
        AtomicReference<Throwable> caughtException = new AtomicReference<>();
        CountDownLatch callbackInvoked = new CountDownLatch(1);

        Arena arena = Arena.ofShared();

        // Allocate a FindCallback in the arena - same pattern as FfmAsyncClient.find()
        MemorySegment callbackPtr = FindCallback.allocate(
                (userdata, result, error) -> {
                    try {
                        // This is the problematic pattern from FfmAsyncClient.find() line 604
                        // Trying to close the arena while we're still inside the upcall
                        arena.close();
                    } catch (IllegalStateException e) {
                        caughtException.set(e);
                    } finally {
                        callbackInvoked.countDown();
                    }
                },
                arena);

        // Invoke the callback directly (simulating what Rust does)
        FindCallback.invoke(callbackPtr, MemorySegment.NULL, MemorySegment.NULL, MemorySegment.NULL);

        assertTrue(callbackInvoked.await(5, TimeUnit.SECONDS), "Callback should be invoked");

        // Verify we got the expected exception
        assertNotNull(caughtException.get(), "Should have caught an exception");
        assertInstanceOf(IllegalStateException.class, caughtException.get());
        assertTrue(caughtException.get().getMessage().contains("acquired"),
                "Exception message should mention 'acquired': " + caughtException.get().getMessage());

        // Clean up - arena should still be open since close() failed
        assertTrue(arena.scope().isAlive(), "Arena should still be alive after failed close");
        arena.close();
    }

    /**
     * Tests the fix: defer arena close until after the upcall returns.
     *
     * The arena should be closed by a different mechanism, not from within
     * the callback itself.
     */
    @Test
    void testArenaCloseAfterCallbackReturnsSucceeds() throws Exception {
        AtomicReference<Arena> arenaRef = new AtomicReference<>();
        CountDownLatch callbackInvoked = new CountDownLatch(1);

        Arena arena = Arena.ofShared();
        arenaRef.set(arena);

        MemorySegment callbackPtr = FindCallback.allocate(
                (userdata, result, error) -> {
                    // Don't close the arena here - just do the work
                    // The arena will be closed after this callback returns
                    callbackInvoked.countDown();
                },
                arena);

        // Invoke the callback
        FindCallback.invoke(callbackPtr, MemorySegment.NULL, MemorySegment.NULL, MemorySegment.NULL);

        assertTrue(callbackInvoked.await(5, TimeUnit.SECONDS), "Callback should be invoked");

        // Now that we're outside the callback, closing should succeed
        assertTrue(arena.scope().isAlive(), "Arena should still be alive");
        assertDoesNotThrow(arena::close, "Should be able to close arena after callback returns");
    }

    /**
     * Test to understand the difference between find and runCommand.
     * Both use arena.close() in the finally block - do they both fail the same way?
     */
    @Test
    void testRunCommandCallbackPatternAlsoFails() throws Exception {
        AtomicReference<Throwable> caughtException = new AtomicReference<>();
        CountDownLatch callbackInvoked = new CountDownLatch(1);

        Arena arena = Arena.ofShared();

        // This mimics runCommand: just decode result and return
        MemorySegment callbackPtr = RunCommandCallback.allocate(
                (userdata, result, error) -> {
                    try {
                        // runCommand just decodes the result - no cursor created
                        if (result.address() != 0) {
                            // Simulate reading from result
                            // In real code: BsonMarshaller.decode(...)
                        }
                    } finally {
                        try {
                            arena.close();
                        } catch (IllegalStateException e) {
                            caughtException.set(e);
                        }
                        callbackInvoked.countDown();
                    }
                },
                arena);

        // Invoke the callback
        RunCommandCallback.invoke(callbackPtr, MemorySegment.NULL, MemorySegment.NULL, MemorySegment.NULL);

        assertTrue(callbackInvoked.await(5, TimeUnit.SECONDS), "Callback should be invoked");

        // RunCommand pattern should ALSO fail the same way as FindCallback
        assertNotNull(caughtException.get(),
                "RunCommand pattern should also fail with session acquired error");
        assertInstanceOf(IllegalStateException.class, caughtException.get());
        assertTrue(caughtException.get().getMessage().contains("acquired"),
                "Exception should mention 'acquired': " + caughtException.get().getMessage());
    }
}

