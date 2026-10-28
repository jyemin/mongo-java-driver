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
package com.mongodb.internal.connection;

import com.mongodb.internal.VisibleForTesting;
import com.mongodb.lang.Nullable;

import java.util.concurrent.ConcurrentHashMap;

import static com.mongodb.internal.VisibleForTesting.AccessModifier.PRIVATE;

/**
 * A registry of {@link WireMessageInterceptor}s, keyed by the description of the cluster, which is the application name.
 * It works the same way as the registry of logging interceptors in {@code StructuredLogger}.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class WireMessageInterceptors {
    private static final ConcurrentHashMap<String, WireMessageInterceptor> INTERCEPTORS = new ConcurrentHashMap<>();

    @VisibleForTesting(otherwise = PRIVATE)
    public static void add(final String clusterDescription, final WireMessageInterceptor interceptor) {
        INTERCEPTORS.put(clusterDescription, interceptor);
    }

    @VisibleForTesting(otherwise = PRIVATE)
    public static void remove(final String clusterDescription) {
        INTERCEPTORS.remove(clusterDescription);
    }

    @Nullable
    static WireMessageInterceptor get(@Nullable final String clusterDescription) {
        return clusterDescription == null ? null : INTERCEPTORS.get(clusterDescription);
    }

    private WireMessageInterceptors() {
    }
}
