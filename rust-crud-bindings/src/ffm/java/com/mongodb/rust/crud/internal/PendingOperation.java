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

import com.mongodb.rust.crud.SingleResultCallback;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.function.Function;

/**
 * Represents a pending FFI operation waiting for a callback response.
 *
 * @param <T> the result type expected by the callback
 */
public final class PendingOperation<T> {

    // Timing instrumentation
    private static long rustTime, decodeTime, onResultTime, completeCount;

    public static void printTimings() {
        if (!FfmAsyncClient.TIMING_ENABLED || completeCount == 0) return;
        System.out.printf("PendingOperation.complete() timings (avg over %d calls):%n", completeCount);
        if (rustTime > 0) {
            System.out.printf("    Rust/MongoDB:    %.4f ms%n", rustTime / 1_000_000.0 / completeCount);
        }
        System.out.printf("    Decode:          %.4f ms%n", decodeTime / 1_000_000.0 / completeCount);
        System.out.printf("    onResult:        %.4f ms%n", onResultTime / 1_000_000.0 / completeCount);
        System.out.printf("    (Arena.ofAuto - no explicit close)%n");
    }

    private final SingleResultCallback<T> callback;
    private final Function<MemorySegment, T> resultDecoder;
    private final Arena arena;
    private long ffiDispatchEndTime;  // Set after FFI call returns (only used if TIMING_ENABLED)

    /**
     * Creates a new pending operation.
     *
     * @param callback the user's callback to invoke with the result
     * @param resultDecoder function to decode the native result into a Java object
     * @param arena the per-operation arena to close after completion
     */
    public PendingOperation(
            SingleResultCallback<T> callback,
            Function<MemorySegment, T> resultDecoder,
            Arena arena) {
        this.callback = callback;
        this.resultDecoder = resultDecoder;
        this.arena = arena;
    }

    /**
     * Records when the FFI dispatch call returned (for timing instrumentation).
     */
    public void setFfiDispatchEndTime(long nanoTime) {
        this.ffiDispatchEndTime = nanoTime;
    }

    /**
     * Completes this operation with the result or error from native code.
     * Closes the arena after invoking the callback.
     *
     * @param result the result from native code (NULL address if error)
     * @param error the error from native code (NULL address if success)
     */
    void complete(MemorySegment result, MemorySegment error) {
        long t0 = FfmAsyncClient.TIMING_ENABLED ? System.nanoTime() : 0;
        if (FfmAsyncClient.TIMING_ENABLED && ffiDispatchEndTime != 0) {
            rustTime += (t0 - ffiDispatchEndTime);
        }
        T decodedResult = null;
        Throwable exception = null;
        try {
            if (error.address() != 0) {
                exception = FfmErrorMapper.toException(error);
            } else {
                // result may be NULL for void operations (e.g., drop) - decoder handles this
                decodedResult = resultDecoder.apply(result);
            }
        } catch (Throwable t) {
            exception = t;
        }
        long t1 = FfmAsyncClient.TIMING_ENABLED ? System.nanoTime() : 0;
        if (FfmAsyncClient.TIMING_ENABLED) decodeTime += (t1 - t0);

        callback.onResult(decodedResult, exception);

        if (FfmAsyncClient.TIMING_ENABLED) {
            long t2 = System.nanoTime();
            onResultTime += (t2 - t1);
            completeCount++;
        }
    }
}

