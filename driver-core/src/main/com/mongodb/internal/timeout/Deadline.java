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

package com.mongodb.internal.timeout;

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.isTrueArgument;

public final class Deadline {
    private static final Deadline INFINITE = new Deadline(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
    private final long deadlineNanos;

    public static Deadline infinite() {
        return INFINITE;
    }

    public static Deadline finite(final long timeout, final TimeUnit timeUnit) {
        return new Deadline(timeout, timeUnit);
    }

    // 0 is infinite, < 0 is error, > 0 is finite
    public static Deadline of(final long timeout, final TimeUnit timeUnit) {
        return timeout == 0 ? infinite() : finite(timeout, timeUnit);
    }

    public static Deadline min(final Deadline first, final Deadline second) {
       return first.deadlineNanos < second.deadlineNanos ? first : second;
    }

    private Deadline(final long timeout, final TimeUnit timeUnit) {
        isTrueArgument("timeout >= 0", timeout >= 0);
        long curTimeNanos = System.nanoTime();
        long timeoutNanos = TimeUnit.NANOSECONDS.convert(timeout, timeUnit);
        this.deadlineNanos = curTimeNanos + timeoutNanos >= 0 ? curTimeNanos + timeoutNanos : Long.MAX_VALUE;
    }

    public boolean isExpired() {
        return deadlineNanos <= System.nanoTime();
    }

    public long getDeadline(final TimeUnit timeUnit) {
        return timeUnit.convert(deadlineNanos, TimeUnit.NANOSECONDS);
    }

    // If the deadline has expired, this will return a non-positive number
    public long getTimeRemaining(final TimeUnit timeUnit) {
        long curTimeNanos = System.nanoTime();
        if (deadlineNanos <= curTimeNanos) {
            return 0;
        } else {
            return timeUnit.convert(deadlineNanos - curTimeNanos, TimeUnit.NANOSECONDS);
        }
    }

    public boolean isInfinite() {
        return deadlineNanos == Long.MAX_VALUE;
    }
}
