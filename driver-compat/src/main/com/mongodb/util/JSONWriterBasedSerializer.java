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

package com.mongodb.util;

import com.mongodb.serializers.json.SerializerProvider;
import org.bson.BSONWriter;
import org.mongodb.json.JSONWriter;
import org.mongodb.json.JSONWriterSettings;

import java.io.StringWriter;
import java.io.Writer;

class JSONWriterBasedSerializer implements ObjectSerializer {

    private final JSONWriterSettings writerSettings;

    public JSONWriterBasedSerializer(final JSONWriterSettings writerSettings) {
        this.writerSettings = writerSettings;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void serialize(final Object obj, final StringBuilder buf) {
        final Writer writer = new StringWriter();
        final BSONWriter bsonWriter = new JSONWriter(writer, writerSettings);
        final SerializerProvider serializerProvider = SerializerProvider.newInstanceDefault();
        serializerProvider.findSerializer(obj.getClass()).serialize(bsonWriter, obj);
        buf.append(writer.toString());
    }

    @Override
    public String serialize(final Object obj) {
        final StringBuilder builder = new StringBuilder();
        serialize(obj, builder);
        return builder.toString();
    }
}
