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

import org.bson.BSONCallback;

/**
 * Helper methods for JSON serialization and de-serialization
 */
public class JSON {

    /**
     * Serializes an object into its JSON form.
     * <p/>
     * This method delegates serialization to <code>JSONSerializers.getLegacy</code>
     *
     * @param o object to serialize
     * @return String containing JSON form of the object
     * @see com.mongodb.util.JSONSerializers#getLegacy()
     */
    public static String serialize(Object o) {
        StringBuilder buf = new StringBuilder();
        serialize(o, buf);
        return buf.toString();
    }

    /**
     * Serializes an object into its JSON form.
     * <p/>
     * This method delegates serialization to <code>JSONSerializers.getLegacy</code>
     *
     * @param o   object to serialize
     * @param buf StringBuilder containing the JSON representation under construction
     * @return String containing JSON form of the object
     * @see com.mongodb.util.JSONSerializers#getLegacy()
     */
    public static void serialize(Object o, StringBuilder buf) {
        JSONSerializers.getLegacy().serialize(o, buf);
    }

    /**
     * Parses a JSON string representing a JSON value
     *
     * @param s the string to parse
     * @return the object
     */
    public static Object parse(String s) {
        return parse(s, null);
    }

    /**
     * Parses a JSON string representing a JSON value
     *
     * @param s the string to parse
     * @return the object
     */
    public static Object parse(final String s, final BSONCallback c) {
        return null;
    }
}