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
import org.bson.types.CodeWScope;

public class CodeWScopeSerializer extends ContainerSerializer<CodeWScope> {

    protected CodeWScopeSerializer(final SerializerProvider serializerProvider) {
        super(serializerProvider);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void serialize(final BSONWriter bsonWriter, final CodeWScope codeWScope) {
        bsonWriter.writeJavaScriptWithScope(codeWScope.getCode());
        final BSONObject scope = codeWScope.getScope();
        if (scope != null) {
            serializerProvider.findSerializer(scope.getClass()).serialize(bsonWriter, scope);
        }
    }

    @Override
    public CodeWScope deserialize(final BSONReader reader) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Class<CodeWScope> getSerializationClass() {
        return CodeWScope.class;
    }
}
