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
 */

package com.mongodb.serializers.json;

import org.bson.BSONReader;
import org.bson.BSONWriter;

@SuppressWarnings("rawtypes")
class IterableSerializer extends ContainerSerializer<Iterable> {

    protected IterableSerializer(final SerializerProvider serializerProvider) {
        super(serializerProvider);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void serialize(final BSONWriter bsonWriter, final Iterable iterable) {
        bsonWriter.writeStartArray();
        for (Object o : iterable) {
            serializerProvider.findSerializer(o.getClass()).serialize(bsonWriter, o);
        }
        bsonWriter.writeEndArray();
    }

    @Override
    public Iterable deserialize(final BSONReader reader) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Class<Iterable> getSerializationClass() {
        return Iterable.class;
    }
}
