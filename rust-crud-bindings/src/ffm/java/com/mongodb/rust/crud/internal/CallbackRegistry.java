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

import java.lang.foreign.MemorySegment;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Registry for pending FFI callback operations. Allows sharing a single callback stub
 * across multiple operations by using the userdata parameter to dispatch to the
 * correct pending operation.
 *
 * <p>This eliminates the overhead of allocating a new upcall stub for each operation,
 * which can be significant (~10µs per allocation).</p>
 */
public final class CallbackRegistry {

    private static final AtomicLong nextOperationId = new AtomicLong();
    private static final ConcurrentHashMap<Long, PendingOperation<?>> pendingOperations = new ConcurrentHashMap<>();

    // Timing instrumentation (controlled by FfmAsyncClient.TIMING_ENABLED)
    private static long removeTime, dispatchCount;

    public static void printTimings() {
        if (!FfmAsyncClient.TIMING_ENABLED || dispatchCount == 0) return;
        System.out.printf("CallbackRegistry.dispatch() timings (avg over %d calls):%n", dispatchCount);
        System.out.printf("    Map remove:      %.4f ms%n", removeTime / 1_000_000.0 / dispatchCount);
    }

    private CallbackRegistry() {
        // Static utility class
    }

    /**
     * Registers a pending operation and returns an ID to be used as userdata.
     *
     * @param operation the pending operation
     * @return the operation ID (to be passed as userdata.address())
     */
    public static long register(PendingOperation<?> operation) {
        long id = nextOperationId.incrementAndGet();
        pendingOperations.put(id, operation);
        return id;
    }

    /**
     * Creates a MemorySegment to pass as userdata for the given operation ID.
     * The segment's address() will return the operation ID.
     *
     * @param operationId the operation ID from register()
     * @return a MemorySegment whose address is the operation ID
     */
    public static MemorySegment toUserdata(long operationId) {
        return MemorySegment.ofAddress(operationId);
    }

    /**
     * Dispatches a callback result to the pending operation identified by userdata.
     * This should be called from the shared callback stub.
     *
     * @param userdata the userdata passed to the callback (contains operation ID as address)
     * @param result the result from native code (may be NULL)
     * @param error the error from native code (may be NULL)
     */
    public static void dispatch(MemorySegment userdata, MemorySegment result, MemorySegment error) {
        long t0 = FfmAsyncClient.TIMING_ENABLED ? System.nanoTime() : 0;
        long operationId = userdata.address();
        PendingOperation<?> operation = pendingOperations.remove(operationId);
        if (FfmAsyncClient.TIMING_ENABLED) {
            removeTime += (System.nanoTime() - t0);
            dispatchCount++;
        }

        if (operation != null) {
            operation.complete(result, error);
        }
        // If operation is null, it was already completed or never registered - ignore
    }

    /**
     * Dispatches a callback for void operations (no result, only error).
     * This should be called from callback stubs for operations like drop that don't return a result.
     *
     * @param userdata the userdata passed to the callback (contains operation ID as address)
     * @param error the error from native code (may be NULL for success)
     */
    public static void dispatchVoid(MemorySegment userdata, MemorySegment error) {
        dispatch(userdata, MemorySegment.NULL, error);
    }

    /**
     * Returns the number of pending operations. Useful for testing/debugging.
     */
    public static int getPendingCount() {
        return pendingOperations.size();
    }
}

