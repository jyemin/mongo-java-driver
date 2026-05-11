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

package com.mongodb.client;

import com.mongodb.annotations.Alpha;
import com.mongodb.annotations.Reason;

import java.util.concurrent.TimeUnit;

/**
 * Iterable for the results of an MQLv2 query executed via one of the
 * {@link MongoDatabase#mqlv2(String)} overloads.
 *
 * <p>This interface is part of an experimental MQLv2 driver API. The MQLv2 query language is
 * itself experimental and subject to change.</p>
 *
 * @param <TResult> the type of each result document.
 * @since EXPERIMENTAL
 */
@Alpha(Reason.CLIENT)
public interface Mqlv2Iterable<TResult> extends MongoIterable<TResult> {

    /**
     * Set the number of documents to return per batch.
     *
     * @param batchSize the batch size
     * @return this
     */
    @Override
    Mqlv2Iterable<TResult> batchSize(int batchSize);

    /**
     * Set the maximum execution time on the server for this operation.
     *
     * @param maxTime  the max time
     * @param timeUnit the time unit, which may not be null
     * @return this
     */
    Mqlv2Iterable<TResult> maxTime(long maxTime, TimeUnit timeUnit);
}
