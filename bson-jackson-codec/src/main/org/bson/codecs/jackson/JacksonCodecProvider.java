/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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
 *
 */

package org.bson.codecs.jackson;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.undercouch.bson4jackson.BsonFactory;
import de.undercouch.bson4jackson.BsonParser;
import org.bson.codecs.Codec;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;

/**
 * TODO
 */
public class JacksonCodecProvider implements CodecProvider {

    private final ObjectMapper objectMapper;

    public JacksonCodecProvider() {
        this(new ObjectMapper(getBsonFactory()));
    }

    public static BsonFactory getBsonFactory() {
        BsonFactory bsonFactory = new BsonFactory();
        bsonFactory.enable(BsonParser.Feature.HONOR_DOCUMENT_LENGTH);
        return bsonFactory;
    }

    public JacksonCodecProvider(final ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }



    @Override
    public <T> Codec<T> get(final Class<T> clazz, final CodecRegistry registry) {
        return new JacksonCodec<>(clazz, objectMapper);
    }
}
