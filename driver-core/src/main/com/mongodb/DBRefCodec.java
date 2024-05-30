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

package com.mongodb;

import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonWriter;
import org.bson.Transformer;
import org.bson.UuidRepresentation;
import org.bson.codecs.BsonTypeClassMap;
import org.bson.codecs.BsonTypeCodecMap;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.OverridableUuidRepresentationCodec;
import org.bson.codecs.configuration.CodecRegistry;

import static com.mongodb.assertions.Assertions.notNull;
import static org.bson.internal.ContainerCodecHelper.readValue;

/**
 * A Codec for DBRef instances.
 *
 * @since 3.0
 */
public class DBRefCodec implements Codec<DBRef>, OverridableUuidRepresentationCodec<DBRef> {
    private final CodecRegistry registry;
    private final UuidRepresentation uuidRepresentation;
    private final Transformer valueTransformer = value -> value;
    private final BsonTypeCodecMap bsonTypeCodecMap;

    /**
     * Construct an instance with the given registry, which is used to encode the id of the referenced document.
     *
     * @param registry the non-null codec registry
     */
    public DBRefCodec(final CodecRegistry registry) {
        this(registry, UuidRepresentation.UNSPECIFIED);
    }

    public DBRefCodec(final CodecRegistry registry, final UuidRepresentation uuidRepresentation) {
        this.registry = notNull("registry", registry);
        this.uuidRepresentation = uuidRepresentation;
        bsonTypeCodecMap = new BsonTypeCodecMap(new BsonTypeClassMap(), registry);
    }

    @Override
    public Codec<DBRef> withUuidRepresentation(final UuidRepresentation uuidRepresentation) {
        return new DBRefCodec(this.registry, uuidRepresentation);
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void encode(final BsonWriter writer, final DBRef value, final EncoderContext encoderContext) {
        writer.writeStartDocument();
        writer.writeString("$ref", value.getCollectionName());
        writer.writeName("$id");
        Codec codec = registry.get(value.getId().getClass());
        codec.encode(writer, value.getId(), encoderContext);
        if (value.getDatabaseName() != null) {
            writer.writeString("$db", value.getDatabaseName());
        }
        writer.writeEndDocument();
    }

    @Override
    public Class<DBRef> getEncoderClass() {
        return DBRef.class;
    }

    @Override
    public DBRef decode(final BsonReader reader, final DecoderContext decoderContext) {
        String databaseName = null;
        String collectionName = null;
        Object id = null;

        reader.readStartDocument();

        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            String fieldName = reader.readName();
            switch (fieldName) {
                case "$ref":
                    collectionName = reader.readString();
                    break;
                case "$id":
                    id = readValue(reader, decoderContext, bsonTypeCodecMap, uuidRepresentation, registry, valueTransformer);
                    break;
                case "$db":
                    databaseName = reader.readString();
                    break;
                default:
                    throw new RuntimeException("TODO");  // TODO
            }
        }

        reader.readEndDocument();

        if (collectionName == null || id == null) {
            throw new RuntimeException("TODO");  // TODO
        }

        return new DBRef(databaseName, collectionName, id);
    }
}
