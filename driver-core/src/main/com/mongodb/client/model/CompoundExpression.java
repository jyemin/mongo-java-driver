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

package com.mongodb.client.model;

import org.bson.BsonDocument;
import org.bson.BsonDocumentWriter;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.Map;
import java.util.Objects;

class CompoundExpression<TExpression> implements Bson {
    private final Map<String, TExpression> expressions;

    CompoundExpression(final Map<String, TExpression> expressions) {
        this.expressions = expressions;
    }

    @Override
    public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> documentClass, final CodecRegistry codecRegistry) {
        BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());

        writer.writeStartDocument();
        expressions.forEach((s, expression) -> {
            writer.writeName(s);
            BuildersHelper.encodeValue(writer, expression, codecRegistry);
        });
        writer.writeEndDocument();

        return writer.getDocument();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        CompoundExpression<?> that = (CompoundExpression<?>) o;

        return Objects.equals(expressions, that.expressions);
    }

    @Override
    public int hashCode() {
        return expressions.hashCode();
    }

    @Override
    public String toString() {
        return "Expression{"
                + ", expressions=" + expressions
                + '}';
    }
}
