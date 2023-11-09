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

import com.mongodb.MongoNamespace;

import java.util.Collections;
import java.util.List;

public class NamespaceWriteModelsPair {
    private final MongoNamespace namespace;
    private final List<? extends WriteModel<?>> writeModels;

    public NamespaceWriteModelsPair(final MongoNamespace namespace, final WriteModel<?> writeModels) {
        this.namespace = namespace;
        this.writeModels = Collections.singletonList(writeModels);
    }

    public NamespaceWriteModelsPair(final MongoNamespace namespace, final List<? extends WriteModel<?>> writeModels) {
        this.namespace = namespace;
        this.writeModels = writeModels;
    }

    public MongoNamespace getNamespace() {
        return namespace;
    }

    public List<WriteModel<?>> getWriteModels() {
        return writeModels;
    }
}
