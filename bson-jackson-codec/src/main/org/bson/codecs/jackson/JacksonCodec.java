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
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.RawBsonDocument;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.RawBsonDocumentCodec;

import java.io.IOException;
import java.io.UncheckedIOException;

final class JacksonCodec<T> implements Codec<T> {

    private final RawBsonDocumentCodec rawBsonDocumentCodec = new RawBsonDocumentCodec();
    private final Class<T> clazz;
    private final ObjectMapper objectMapper;


    public JacksonCodec(final Class<T> clazz, final ObjectMapper objectMapper) {
        this.clazz = clazz;
        this.objectMapper = objectMapper;
    }

    @Override
    public T decode(final BsonReader reader, final DecoderContext decoderContext) {
        RawBsonDocument rawBsonDocument = rawBsonDocumentCodec.decode(reader, decoderContext);
        try {
            return objectMapper.readValue(rawBsonDocument.getByteBuffer().array(), clazz); // TODO: is this safe to call array?
        } catch (IOException e) {
            throw new RuntimeException(e);  // TODO
        }
    }

    @Override
    public void encode(final BsonWriter writer, final T value, final EncoderContext encoderContext) {
        try {
            byte[] data = objectMapper.writeValueAsBytes(value);
            rawBsonDocumentCodec.encode(writer, new RawBsonDocument(data), encoderContext);
        } catch (IOException e) {
            throw new UncheckedIOException(e); // TODO
        }
    }

    @Override
    public Class<T> getEncoderClass() {
        return clazz;
    }
}
