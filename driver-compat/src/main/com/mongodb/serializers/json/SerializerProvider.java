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

import org.bson.BSONType;
import org.mongodb.serialization.Serializer;
import org.mongodb.serialization.serializers.BinarySerializer;
import org.mongodb.serialization.serializers.BooleanSerializer;
import org.mongodb.serialization.serializers.ByteArraySerializer;
import org.mongodb.serialization.serializers.ByteSerializer;
import org.mongodb.serialization.serializers.CodeSerializer;
import org.mongodb.serialization.serializers.DateSerializer;
import org.mongodb.serialization.serializers.DoubleSerializer;
import org.mongodb.serialization.serializers.FloatSerializer;
import org.mongodb.serialization.serializers.IntegerSerializer;
import org.mongodb.serialization.serializers.LongSerializer;
import org.mongodb.serialization.serializers.MaxKeySerializer;
import org.mongodb.serialization.serializers.MinKeySerializer;
import org.mongodb.serialization.serializers.NullSerializer;
import org.mongodb.serialization.serializers.ObjectIdSerializer;
import org.mongodb.serialization.serializers.PatternSerializer;
import org.mongodb.serialization.serializers.ShortSerializer;
import org.mongodb.serialization.serializers.StringSerializer;
import org.mongodb.serialization.serializers.TimestampSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@SuppressWarnings("rawtypes")
public class SerializerProvider {

    private SerializerProvider() {
    }

    private final Map<Class<?>, Serializer<?>> classSerializerMap = new ConcurrentHashMap<Class<?>, Serializer<?>>();
    private final Map<BSONType, Serializer<?>> bsonTypeSerializerMap = new HashMap<BSONType, Serializer<?>>();

    public SerializerProvider registerSerializer(final Serializer<?> serializer) {
        if (serializer == null) {
            throw new IllegalArgumentException("Serializer can't be null");
        }

        if (serializer.getSerializationClass() == null) {
            classSerializerMap.put(Null.class, serializer);
        } else {
            classSerializerMap.put(serializer.getSerializationClass(), serializer);
        }

        return this;
    }

    public SerializerProvider registerSerializer(final BSONType bsonType, final Serializer<?> serializer) {

        registerSerializer(serializer);
        bsonTypeSerializerMap.put(bsonType, serializer);

        return this;
    }

    public Serializer findSerializer(final Class<?> cls) {
        if (cls == null) {
            return classSerializerMap.get(Null.class);
        }

        if (classSerializerMap.containsKey(cls)) {
            return classSerializerMap.get(cls);
        } else {
            return findApplicableSerializer(cls);
        }
    }

    public Serializer findSerializer(final BSONType bsonType) {
        return bsonTypeSerializerMap.get(bsonType);
    }

    private Serializer findApplicableSerializer(Class<?> cls) {

        for (Class<?> key : classSerializerMap.keySet()) {
            if (key.isAssignableFrom(cls)) {
                classSerializerMap.put(cls, classSerializerMap.get(key));
                return classSerializerMap.get(key);
            }
        }

        throw new IllegalArgumentException(String.format("No applicable serializer for %s", cls.getName()));
    }

    public static SerializerProvider newInstance() {
        return new SerializerProvider();
    }

    public static SerializerProvider newInstanceDefault() {
        final SerializerProvider serializerProvider = SerializerProvider.newInstance();

        serializerProvider
                .registerSerializer(BSONType.OBJECT_ID, new ObjectIdSerializer())
                .registerSerializer(new IntegerSerializer())
                .registerSerializer(new LongSerializer())
                .registerSerializer(new StringSerializer())
                .registerSerializer(new DoubleSerializer())
                .registerSerializer(new BinarySerializer())
                .registerSerializer(new DateSerializer())
                .registerSerializer(new DateSerializer())
                .registerSerializer(new TimestampSerializer())
                .registerSerializer(new BooleanSerializer())
                .registerSerializer(new PatternSerializer())
                .registerSerializer(new MinKeySerializer())
                .registerSerializer(new MaxKeySerializer())
                .registerSerializer(new CodeSerializer())
                .registerSerializer(new NullSerializer())
                .registerSerializer(new FloatSerializer())
                .registerSerializer(new ShortSerializer())
                .registerSerializer(new ByteSerializer())
                .registerSerializer(new ByteArraySerializer())
                .registerSerializer(new UUIDSerializer())
                .registerSerializer(new CodeWScopeSerializer(serializerProvider))
                .registerSerializer(new BSONObjectSerializer(serializerProvider))
                .registerSerializer(new DBRefBaseSerializer(serializerProvider))
                .registerSerializer(new IterableSerializer(serializerProvider))
                .registerSerializer(new MapSerializer(serializerProvider))
                .registerSerializer(new ObjectArraySerializer(serializerProvider));

        return serializerProvider;
    }

    private static class Null {
    }
}
