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

package com.mongodb.rust.crud;

import com.mongodb.lang.Nullable;

/**
 * Callback interface for async operations.
 *
 * <p>Exactly one of {@code result} or {@code error} will be non-null when
 * {@link #onResult} is invoked.</p>
 *
 * @param <T> the result type
 */
@FunctionalInterface
public interface SingleResultCallback<T> {

    /**
     * Called when the operation completes.
     *
     * @param result the result, or null if an error occurred
     * @param error the error, or null if the operation succeeded
     */
    void onResult(@Nullable T result, @Nullable Throwable error);

    /**
     * Complete this callback with a successful result.
     */
    default void complete(@Nullable T result) {
        onResult(result, null);
    }

    /**
     * Complete this callback with an error.
     */
    default void completeExceptionally(Throwable error) {
        onResult(null, error);
    }
}

