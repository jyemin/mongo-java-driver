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
package com.mongodb.internal;

import com.mongodb.MongoOperationTimeoutException;
import com.mongodb.internal.time.StartTime;
import com.mongodb.internal.time.Timeout;
import com.mongodb.lang.Nullable;

import java.util.Objects;
import java.util.function.LongConsumer;

import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.internal.time.Timeout.ZeroSemantics.ZERO_DURATION_MEANS_INFINITE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Timeout Context.
 *
 * <p>The context for handling timeouts in relation to the Client Side Operation Timeout specification.</p>
 */
public class TimeoutContext {
    private static final int NO_ROUND_TRIP_TIME_MS = 0;
    private final TimeoutSettings timeoutSettings;
    @Nullable
    private final Timeout timeout;
    @Nullable
    private final MaxTimeSupplier maxTimeSupplier;
    private final boolean isMaintenanceContext;
    private final long minRoundTripTimeMS;

    public static MongoOperationTimeoutException createMongoRoundTripTimeoutException() {
        return createMongoTimeoutException("Remaining timeoutMS is less than or equal to the server's minimum round trip time.");
    }

    public static MongoOperationTimeoutException createMongoTimeoutException(final String message) {
        return new MongoOperationTimeoutException(message);
    }

    public static <T> T throwMongoTimeoutException(final String message) {
        throw new MongoOperationTimeoutException(message);
    }

    public static MongoOperationTimeoutException createMongoTimeoutException(final Throwable cause) {
        return createMongoTimeoutException("Operation exceeded the timeout limit: " + cause.getMessage(), cause);
    }

    public static MongoOperationTimeoutException createMongoTimeoutException(final String message, @Nullable final Throwable cause) {
        if (cause instanceof MongoOperationTimeoutException) {
            return (MongoOperationTimeoutException) cause;
        }
        return new MongoOperationTimeoutException(message, cause);
    }

    public TimeoutContext(final TimeoutSettings timeoutSettings) {
        this(false, timeoutSettings, startTimeout(timeoutSettings.getTimeoutMS()));
    }

    private TimeoutContext(final boolean isMaintenanceContext,
                           final TimeoutSettings timeoutSettings,
                           @Nullable final Timeout timeout) {
        this(isMaintenanceContext,
                NO_ROUND_TRIP_TIME_MS,
                timeoutSettings,
                null,
                timeout);
    }

    private TimeoutContext(final boolean isMaintenanceContext,
                           final long minRoundTripTimeMS,
                           final TimeoutSettings timeoutSettings,
                           @Nullable final MaxTimeSupplier maxTimeSupplier,
                           @Nullable final Timeout timeout) {
        this.isMaintenanceContext = isMaintenanceContext;
        this.timeoutSettings = timeoutSettings;
        this.minRoundTripTimeMS = minRoundTripTimeMS;
        this.maxTimeSupplier = maxTimeSupplier;
        this.timeout = timeout;
    }

    /**
     * Allows for the differentiation between users explicitly setting a global operation timeout via {@code timeoutMS}.
     *
     * @return true if a timeout has been set.
     */
    public boolean hasTimeoutMS() {
        return timeoutSettings.getTimeoutMS() != null;
    }

    @Nullable
    public Timeout timeoutIncludingRoundTrip() {
        return timeout == null ? null : timeout.shortenBy(minRoundTripTimeMS, MILLISECONDS);
    }

    public TimeoutSettings getTimeoutSettings() {
        return timeoutSettings;
    }

    @Nullable
    public Timeout getTimeout() {
        return timeout;
    }

    /**
     * The override will be provided as the remaining value in
     * {@link #runMaxTimeMS}, where 0 is ignored.
     *
     * <p>
     * NOTE: Suitable for static user-defined values only (i.e MaxAwaitTimeMS),
     * not for running timeouts that adjust dynamically (CSOT).
     * <p>
     * If remaining CSOT timeout is less than this static timeout, then CSOT timeout will be used.
     *
     */
    public TimeoutContext withMaxTimeOverride(final long maxTimeMS) {
        return new TimeoutContext(
                isMaintenanceContext,
                minRoundTripTimeMS,
                timeoutSettings,
                () -> maxTimeMS,
                timeout);
    }


    @Override
    public String toString() {
        return "TimeoutContext{"
                + "isMaintenanceContext=" + isMaintenanceContext
                + ", timeoutSettings=" + timeoutSettings
                + ", timeout=" + timeout
                + ", minRoundTripTimeMS=" + minRoundTripTimeMS
                + '}';
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final TimeoutContext that = (TimeoutContext) o;
        return isMaintenanceContext == that.isMaintenanceContext
                && minRoundTripTimeMS == that.minRoundTripTimeMS
                && Objects.equals(timeoutSettings, that.timeoutSettings)
                && Objects.equals(timeout, that.timeout);
    }

    @Override
    public int hashCode() {
        return Objects.hash(isMaintenanceContext, timeoutSettings, timeout, minRoundTripTimeMS);
    }

    @Nullable
    public static Timeout startTimeout(@Nullable final Long timeoutMS) {
        if (timeoutMS != null) {
            return Timeout.expiresIn(timeoutMS, MILLISECONDS, ZERO_DURATION_MEANS_INFINITE);
        }
        return null;
    }

    /**
     * Returns the computed server selection timeout
     *
     * <p>Caches the computed server selection timeout if:
     * <ul>
     *     <li>not in a maintenance context</li>
     *     <li>there is a timeoutMS, so to keep the same legacy behavior.</li>
     *     <li>the server selection timeout is less than the remaining overall timeout.</li>
     * </ul>
     *
     * @return the timeout context
     */
    public Timeout computeServerSelectionTimeout() {
        if (hasTimeoutMS()) {
            return assertNotNull(timeout);
        }

        return StartTime.now().timeoutAfterOrInfiniteIfNegative(getTimeoutSettings().getServerSelectionTimeoutMS(), MILLISECONDS);
    }

    public void runMaxTimeMS(final LongConsumer onRemaining) {
        if (maxTimeSupplier != null) {
            long maxTimeMS = maxTimeSupplier.get();
            if (maxTimeMS > 0) {
                runMinTimeout(onRemaining, maxTimeMS);
            }
            return;
        }
        if (timeout == null) {
            runWithFixedTimeout(timeoutSettings.getMaxTimeMS(), onRemaining);
            return;
        }
        assertNotNull(timeoutIncludingRoundTrip())
                .run(MILLISECONDS,
                        () -> {},
                        onRemaining,
                        () -> {
                            throw createMongoRoundTripTimeoutException();
                        });

    }

    private void runMinTimeout(final LongConsumer onRemaining, final long fixedMs) {
        Timeout timeout = timeoutIncludingRoundTrip();
        if (timeout != null) {
            timeout.run(MILLISECONDS, () -> {
                        onRemaining.accept(fixedMs);
                    },
                    (renamingMs) -> {
                        onRemaining.accept(Math.min(renamingMs, fixedMs));
                    }, () -> {
                        throwMongoTimeoutException("The operation exceeded the timeout limit.");
                    });
        } else {
            onRemaining.accept(fixedMs);
        }
    }

    private static void runWithFixedTimeout(final long ms, final LongConsumer onRemaining) {
        if (ms != 0) {
            onRemaining.accept(ms);
        }
    }

    public interface MaxTimeSupplier {
        long get();
    }
}
