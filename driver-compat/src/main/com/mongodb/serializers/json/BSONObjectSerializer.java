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

import org.bson.BSONObject;
import org.bson.BSONReader;
import org.bson.BSONWriter;

class BSONObjectSerializer extends ContainerSerializer<BSONObject> {

    protected BSONObjectSerializer(final SerializerProvider serializerProvider) {
        super(serializerProvider);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void serialize(final BSONWriter bsonWriter, final BSONObject bsonObject) {
        bsonWriter.writeStartDocument();
        for (String name : bsonObject.keySet()) {
            bsonWriter.writeName(name);
            serializerProvider.findSerializer(bsonObject.get(name).getClass()).serialize(bsonWriter, bsonObject.get(name));
        }
        bsonWriter.writeEndDocument();
    }

    @Override
    public BSONObject deserialize(final BSONReader reader) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Class<BSONObject> getSerializationClass() {
        return BSONObject.class;
    }
}
