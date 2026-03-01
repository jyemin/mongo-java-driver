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

package com.mongodb.reactivestreams.client.internal.crypt;

import com.mongodb.internal.crypt.capi.MongoKeyDecryptor;
import com.mongodb.internal.time.Timeout;
import com.mongodb.lang.Nullable;
import reactor.core.publisher.Mono;

import javax.net.ssl.SSLContext;
import java.io.Closeable;
import java.util.Map;

import static org.bson.assertions.Assertions.assertTrue;

// TODO: This class needs to be reimplemented - the old implementation relied on deleted connection classes
class KeyManagementService implements Closeable {
    private final Map<String, SSLContext> kmsProviderSslContextMap;
    private final int timeoutMillis;

    KeyManagementService(final Map<String, SSLContext> kmsProviderSslContextMap, final int timeoutMillis) {
        assertTrue("timeoutMillis > 0", timeoutMillis > 0);
        this.kmsProviderSslContextMap = kmsProviderSslContextMap;
        this.timeoutMillis = timeoutMillis;
    }

    public void close() {
        // no-op
    }

    Mono<Void> decryptKey(final MongoKeyDecryptor keyDecryptor, @Nullable final Timeout operationTimeout) {
        return Mono.error(new UnsupportedOperationException("KeyManagementService not yet implemented"));
    }
}
