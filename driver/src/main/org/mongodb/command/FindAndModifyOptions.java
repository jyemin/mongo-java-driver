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

package org.mongodb.command;

import org.mongodb.Document;

public final class FindAndModifyOptions {
    private Document filter;
    private Document selector;
    private Document sortCriteria;
    private Document update;
    private String collectionName;
    private boolean returnNew;
    private boolean upsert;
    private boolean remove;

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private Document filter;
        private Document selector;
        private Document sortCriteria;
        private Document update;
        private boolean returnNew;
        private boolean upsert;
        private boolean remove;
        private String collectionName;

        public Builder filter(final Document aFilter) {
            this.filter = aFilter;
            return this;
        }

        public Builder selector(final Document aSelector) {
            this.selector = aSelector;
            return this;
        }

        public Builder sortCriteria(final Document aSortCriteria) {
            this.sortCriteria = aSortCriteria;
            return this;
        }

        public Builder update(final Document aUpdate) {
            this.update = aUpdate;
            return this;
        }

        public Builder returnNew(final boolean aReturnNew) {
            this.returnNew = aReturnNew;
            return this;
        }

        public Builder upsert(final boolean aUpsert) {
            this.upsert = aUpsert;
            return this;
        }

        public Builder remove(final boolean aRemove) {
            this.remove = aRemove;
            return this;
        }

        public Builder collectionName(final String aCollectionName) {
            this.collectionName = aCollectionName;
            return this;
        }

        public FindAndModifyOptions build() {
            return new FindAndModifyOptions(this);
        }
    }

    private FindAndModifyOptions(final Builder builder) {
        this.filter = builder.filter;
        this.selector = builder.selector;
        this.sortCriteria = builder.sortCriteria;
        this.update = builder.update;
        this.returnNew = builder.returnNew;
        this.upsert = builder.upsert;
        this.remove = builder.remove;
        this.collectionName = builder.collectionName;
    }

    Document toDocument() {
        final Document cmd = new Document("findandmodify", collectionName);
        if (filter != null) {
            cmd.put("query", filter);
        }
        if (selector != null) {
            cmd.put("fields", selector);
        }
        if (sortCriteria != null) {
            cmd.put("sort", sortCriteria);
        }
        if (remove) {
            cmd.put("remove", true);
        } else {
            if (update != null) {
                cmd.put("update", update);
            }
            if (returnNew) {
                cmd.put("new", true);
            }
            if (upsert) {
                cmd.put("upsert", true);
            }
        }
        return cmd;
    }

}
