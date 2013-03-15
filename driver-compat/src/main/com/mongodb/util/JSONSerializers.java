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

import org.mongodb.json.JSONMode;
import org.mongodb.json.JSONWriterSettings;

public final class JSONSerializers {


    private JSONSerializers() {
    }

    /**
     * Returns an <code>ObjectSerializer</code> that mostly conforms to the strict JSON format defined in
     * <a href="http://www.mongodb.org/display/DOCS/Mongo+Extended+JSON", but with a few differences to keep
     * compatibility with previous versions of the driver.  Clients should generally prefer
     * <code>getStrict</code> in preference to this method.
     *
     * @return object serializer
     * @see #getStrict()
     */
    public static ObjectSerializer getLegacy() {
        return new JSONWriterBasedSerializer(new JSONWriterSettings(JSONMode.Strict));
    }

    /**
     * Returns an <code>ObjectSerializer</code> that conforms to the strict JSON format defined in
     * <a href="http://www.mongodb.org/display/DOCS/Mongo+Extended+JSON".
     *
     * @return object serializer
     */
    public static ObjectSerializer getStrict() {
        return new JSONWriterBasedSerializer(new JSONWriterSettings(JSONMode.Strict));
    }

}
