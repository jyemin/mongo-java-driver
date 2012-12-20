/*
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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

package org.mongodb.operation;

public abstract class MongoUpdateBase extends MongoWrite {
    protected final MongoQueryFilter filter;
    private boolean isUpsert = false;

    public MongoUpdateBase(final MongoQueryFilter filter) {
        this.filter = filter;
    }

    public MongoQueryFilter getFilter() {
        return filter;
    }

    public boolean isUpsert() {
        return isUpsert;
    }

    public MongoUpdateBase isUpsert(final boolean isUpsert) {
        this.isUpsert = isUpsert;
        return this;
    }

    public abstract boolean isMulti();
}
