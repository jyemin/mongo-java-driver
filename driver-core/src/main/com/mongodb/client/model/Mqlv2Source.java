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

package com.mongodb.client.model;

import com.mongodb.annotations.Alpha;
import com.mongodb.annotations.Reason;

/**
 * A source of MQLv2 query text. Implemented by facade pipeline builders so that callers can pass
 * a builder directly to the {@code mqlv2(Mqlv2Source)} overloads on {@code MongoDatabase}
 * (or the equivalent async type) without an explicit {@code .toMqlv2()} call at the call site.
 *
 * <p>This interface is part of an experimental MQLv2 driver API. The MQLv2 query language is itself
 * experimental and subject to change.</p>
 *
 * @since EXPERIMENTAL
 */
@Alpha(Reason.CLIENT)
public interface Mqlv2Source {
    /**
     * Serialize this source to canonical MQLv2 query text.
     *
     * @return the MQLv2 text to send to the server.
     */
    String toMqlv2();
}
