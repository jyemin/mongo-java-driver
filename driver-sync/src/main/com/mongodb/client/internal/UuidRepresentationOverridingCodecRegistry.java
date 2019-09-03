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

import org.bson.UuidRepresentation;
import org.bson.codecs.Codec;
import org.bson.codecs.UuidRepresentationOverridingCodec;
import org.bson.codecs.configuration.ChildCodecRegistry;
import org.bson.internal.CodecCache;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CycleDetectingCodecRegistry;

import static com.mongodb.assertions.Assertions.notNull;

public class UuidRepresentationOverridingCodecRegistry implements CycleDetectingCodecRegistry {

    private final CodecProvider wrapped;
    private final CodecCache codecCache = new CodecCache();
    private final UuidRepresentation uuidRepresentation;

    UuidRepresentationOverridingCodecRegistry(final CodecProvider wrapped, final UuidRepresentation uuidRepresentation) {
        this.uuidRepresentation = notNull("uuidRepresentation", uuidRepresentation);
        this.wrapped = wrapped;
    }

    @Override
    public <T> Codec<T> get(final Class<T> clazz) {
        return get(new ChildCodecRegistry<T>(this, clazz));
    }

    @Override
    @SuppressWarnings({"unchecked"})
    public <T> Codec<T> get(final ChildCodecRegistry context) {
        if (!codecCache.containsKey(context.getCodecClass())) {
            Codec<T> codec = wrapped.get(context.getCodecClass(), context);
            if (codec instanceof UuidRepresentationOverridingCodec) {
                codec = ((UuidRepresentationOverridingCodec<T>) codec).withUuidRepresentation(uuidRepresentation);
            }
            codecCache.put(context.getCodecClass(), codec);
        }
        return codecCache.getOrThrow(context.getCodecClass());
    }
}
