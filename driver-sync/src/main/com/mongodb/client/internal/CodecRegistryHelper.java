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

package com.mongodb.client.internal;

import com.mongodb.MongoClientException;
import org.bson.UuidRepresentation;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.internal.UuidRepresentationOverridingCodecRegistry;

final class CodecRegistryHelper {

    static CodecRegistry createRegistry(final CodecRegistry codecRegistry, final UuidRepresentation uuidRepresentation) {
        CodecRegistry retVal = codecRegistry;
        if (uuidRepresentation != UuidRepresentation.JAVA_LEGACY) {
            if (codecRegistry instanceof CodecProvider) {
                retVal = new UuidRepresentationOverridingCodecRegistry((CodecProvider) codecRegistry, uuidRepresentation);
            } else {
                throw new MongoClientException("Changing the default UuidRepresentation requires a CodecRegistry that also implements the" +
                        " CodecProvider interface");
            }
        }
        return retVal;
    }

    private CodecRegistryHelper() {
    }
}
