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
package com.mongodb.reactivestreams.client.unified;

import org.junit.jupiter.params.provider.Arguments;

import java.util.Collection;

import static util.JsonPoweredTestHelper.getTestDocuments;

/**
 * POC: runs the draft handshake tests in {@code testing/resources/handshake-poc}, which are not part of the specifications submodule.
 */
final class UnifiedHandshakePocTest extends UnifiedReactiveStreamsTest {
    private static Collection<Arguments> data() {
        return getTestData("handshake-poc", getTestDocuments("/handshake-poc"), true, Language.JAVA);
    }
}
