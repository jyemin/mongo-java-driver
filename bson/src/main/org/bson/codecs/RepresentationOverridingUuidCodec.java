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

package org.bson.codecs;

import org.bson.UuidRepresentation;

import java.util.UUID;

/**
 *
 * @since 3.12
 */
public class RepresentationOverridingUuidCodec extends UuidCodec implements UuidRepresentationOverridingCodec<UUID> {

    /**
     *
     */
    public RepresentationOverridingUuidCodec() {
    }

    /**
     *
     * @param uuidRepresentation
     */
    public RepresentationOverridingUuidCodec(final UuidRepresentation uuidRepresentation) {
        super(uuidRepresentation);
    }

    @Override
    public Codec<UUID> withUuidRepresentation(final UuidRepresentation uuidRepresentation) {
        return new RepresentationOverridingUuidCodec(uuidRepresentation);
    }
}
