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

package com.mongodb.internal.observability.micrometer;

import com.mongodb.lang.Nullable;
import com.mongodb.observability.ObservabilitySettings;
import com.mongodb.observability.micrometer.MicrometerObservabilitySettings;
import io.micrometer.observation.ObservationRegistry;

import static com.mongodb.internal.observability.micrometer.MongodbObservation.LowCardinalityKeyNames.SYSTEM;
import static java.lang.System.getenv;

/**
 * Manages tracing spans for MongoDB driver activities.
 * <p>
 * This class provides methods to create and manage spans for commands, operations and transactions.
 * It integrates with a {@link Tracer} to propagate tracing information and record telemetry.
 * </p>
 */
public class TracingManager {
    /**
     * A no-op instance of the TracingManager used when tracing is disabled.
     */
    public static final TracingManager NO_OP = new TracingManager(null);
    private final Tracer tracer;
    private final boolean enableCommandPayload;

    /**
     * If set, this will enable/disable tracing even when an observationRegistry has been passed
     */
    public static final String ENV_OBSERVABILITY_ENABLED = "OBSERVABILITY_MONGODB_ENABLED";

    /**
     * If set, this will truncate the command payload captured in the tracing span to the specified length.
     */
    public static final String ENV_OBSERVABILITY_QUERY_TEXT_MAX_LENGTH = "OBSERVABILITY_MONGODB_QUERY_TEXT_MAX_LENGTH";

    /**
     * Constructs a new TracingManager with the specified observation registry.
     * @param observabilitySettings The observation registry to use for tracing operations, may be null.
     */
    public TracingManager(@Nullable final ObservabilitySettings observabilitySettings) {
        if (observabilitySettings == null) {
            tracer = Tracer.NO_OP;
            enableCommandPayload = false;

        } else {
            MicrometerObservabilitySettings settings;
            if (observabilitySettings instanceof MicrometerObservabilitySettings) {
                settings = (MicrometerObservabilitySettings) observabilitySettings;
            } else {
                throw new IllegalArgumentException("Only Micrometer based observability is currently supported");
            }

            String envOtelInstrumentationEnabled = getenv(ENV_OBSERVABILITY_ENABLED);
            boolean enableTracing = true;
            if (envOtelInstrumentationEnabled != null) {
                enableTracing = Boolean.parseBoolean(envOtelInstrumentationEnabled);
            }

            ObservationRegistry observationRegistry = settings.getObservationRegistry();
            tracer = enableTracing && observationRegistry != null
                    ? new MicrometerTracer(observationRegistry, settings.isEnableCommandPayloadTracing(), settings.getMaxQueryTextLength())
                    : Tracer.NO_OP;

            this.enableCommandPayload = tracer.includeCommandPayload();
        }
    }

    /**
     * Creates a new transaction span for the specified server session.
     *
     * @return The created transaction span.
     */
    public Span addTransactionSpan() {
        Span span = tracer.nextSpan("transaction", null, null);
        span.tagLowCardinality(SYSTEM.withValue("mongodb"));
        return span;
    }

    /**
     * Checks whether tracing is enabled.
     *
     * @return True if tracing is enabled, false otherwise.
     */
    public boolean isEnabled() {
        return tracer.isEnabled();
    }

    /**
     * Checks whether command payload tracing is enabled.
     *
     * @return True if command payload tracing is enabled, false otherwise.
     */
    public boolean isCommandPayloadEnabled() {
        return enableCommandPayload;
    }


}
