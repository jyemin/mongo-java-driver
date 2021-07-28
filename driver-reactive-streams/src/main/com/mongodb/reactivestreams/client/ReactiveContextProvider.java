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

package com.mongodb.reactivestreams.client;

import com.mongodb.ContextProvider;
import com.mongodb.RequestContext;
import org.reactivestreams.Subscriber;

/**
 * Putting this in reactive-streams module so there is no API dependency on reactive streams in driver-core
 * @since 4.4
 */
public interface ReactiveContextProvider extends ContextProvider {
    /**
     * Get the request context from the subscriber.
     *
     * @param subscriber the subscriber for the operation
     * @return the request context
     */
    @SuppressWarnings("rawtypes") // TODO: a bit weird to have to take a raw type and suppress warnings
    RequestContext getContext(Subscriber subscriber);
}