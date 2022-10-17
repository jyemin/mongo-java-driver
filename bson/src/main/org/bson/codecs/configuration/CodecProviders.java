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

package org.bson.codecs.configuration;

import org.bson.UuidRepresentation;
import org.bson.codecs.Codec;
import org.bson.internal.MapOfCodecsProvider;
import org.bson.internal.OverridableUuidRepresentationCodecProvider;
import org.bson.internal.ProvidersCodecProvider;

import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

/**
 * A helper class for creating {@link CodecProvider} instances from {@link Codec}s or other {@link CodecProvider}s.
 *
 * @since 4.8
 */
public final class CodecProviders {

    /**
     * Creates a {@link CodecProvider} from the provided list of {@code CodecProvider} instances.
     *
     * @param providers the codec providers
     * @return a {@code CodecProvider} with the ordered list of {@code CodecProvider} instances.
     */
    public static CodecProvider fromProviders(final CodecProvider... providers) {
        return fromProviders(asList(providers));
    }

    /**
     * Creates a {@link CodecProvider} from the provided list of {@code CodecProvider} instances.
     **
     * @param providers the codec providers
     * @return a {@code CodecProvider} with the ordered list of {@code CodecProvider} instances.
     */
    public static CodecProvider fromProviders(final List<? extends CodecProvider> providers) {
        return new ProvidersCodecProvider(providers);
    }

    /**
     * Creates a {@link CodecProvider} from the provided list of {@link  Codec} instances.
     *
     * <p>This provider can then be used alongside other providers. Typically used when adding extra codecs to existing codecs with the
     * {@link #fromProviders(List)} helper.</p>
     *
     * @param codecs the {@code Codec}s to create a provider for
     * @return a {@code CodecProvider} for the given list of {@code Codec} instances.
     */
    public static CodecProvider fromCodecs(final Codec<?>... codecs) {
        return fromCodecs(asList(codecs));
    }

    /**
     * Creates a {@link CodecProvider} from the provided list of {@code Codec} instances.
     *
     * <p>This provider can then be used alongside other providers.  Typically used when adding extra codecs to existing codecs with the
     * {@link #fromProviders(List)} helper.</p>
     *
     * @param codecs the {@code Codec} to create a provider for
     * @return a {@code CodecProvider} for the given list of {@code Codec} instances.
     */
    public static CodecProvider fromCodecs(final List<? extends Codec<?>> codecs) {
        return fromProviders(singletonList(new MapOfCodecsProvider(codecs)));
    }

    /**
     * Apply given {@link UuidRepresentation} to the given {@link CodecProvider}.
     *
     * @param codecProvider the code provider
     * @param uuidRepresentation the uuid representation
     * @return a {@code CodecRegistry} with the given {@code UuidRepresentation} applied to the given {@code CodecRegistry}
     */
    public static CodecProvider withUuidRepresentation(final CodecProvider codecProvider, final UuidRepresentation uuidRepresentation) {
        return fromProviders(singletonList(new OverridableUuidRepresentationCodecProvider(codecProvider, uuidRepresentation)));
    }

    private CodecProviders() {
    }
}
