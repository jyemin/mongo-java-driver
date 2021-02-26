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

package com.mongodb.client.model.expressions;

import com.mongodb.annotations.Immutable;
import com.mongodb.lang.Nullable;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.Objects;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * An expression representing a reference to a variable.
 *
 * @see Expressions#currentRef()
 * @see Expressions#rootRef()
 * @see Expressions#ref(String)
 * @since 4.?
 */
@Immutable
public class VariableReferenceExpression implements Expression {
    private final String name;
    @Nullable
    private final String fieldPath;

    VariableReferenceExpression(final String name, @Nullable final String fieldPath) {
        this.name = notNull("name", name);
        this.fieldPath = fieldPath;
    }

    /**
     * Gets the field path to append to the variable reference.
     *
     *  * @param path the field path to append to the variable reference.  This may be null
     * @return this
     */
    @Nullable
    public VariableReferenceExpression fieldPath(final String path) {
        return new VariableReferenceExpression(name, path);
    }

    /**
     * Gets the name of the variable.
     *
     * @return the variable name
     */
    public String getName() {
        return name;
    }

    /**
     * Gets the optional field path.
     *
     * @return the field path
     */
    @Nullable
    public String getFieldPath() {
        return fieldPath;
    }

    @Override
    public BsonValue toBsonValue(final CodecRegistry codecRegistry) {
        return new BsonString("$$" + name + (fieldPath == null ? "" : ("." + fieldPath)));
    }

    @Override
    public String toString() {
        return "VariableReferenceExpression{"
                + "name='" + name + '\''
                + (fieldPath != null ? ", fieldPath='" + fieldPath + '\'' : "")
                + '}';
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        VariableReferenceExpression that = (VariableReferenceExpression) o;
        return Objects.equals(name, that.name) && Objects.equals(fieldPath, that.fieldPath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, fieldPath);
    }
}
