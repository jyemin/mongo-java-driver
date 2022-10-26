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

package com.mongodb.internal.binding;


/**
 * A factory of connection sources to servers that can be read from or written to.
 *
 * <p>This is part of an internal package and is not a stable part of the API</p>
 */
public interface ReadWriteBinding extends ReadBinding, WriteBinding, ReferenceCounted {
    @Override
    ReadWriteBinding retain();
}
