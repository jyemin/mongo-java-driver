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

package org.bson;

import com.mongodb.BasicDBObject;
import org.junit.Test;

import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;

public class BSONTest {

    @Test
    public void testRegexFlags() {
        assertEquals(256, BSON.regexFlags("g"));
        assertEquals(52, BSON.regexFlags("stx"));
        assertEquals(266, BSON.regexFlags("img"));
    }


    @Test(expected = IllegalArgumentException.class)
    public void testRegexInvalidModifier() {
        BSON.regexFlags("ilk");
    }

    @Test
    public void testConversionFromInt() {
        assertEquals("ix", BSON.regexFlags(Pattern.CASE_INSENSITIVE | Pattern.COMMENTS));
        assertEquals("t", BSON.regexFlags(Pattern.LITERAL));
        assertEquals("", BSON.regexFlags(0));
    }

    @Test
    public void testEncodingDecode() {
        final BasicDBObject inputDoc = new BasicDBObject("_id", 1);
        final byte[] encoded = BSON.encode(inputDoc);
        assertEquals(inputDoc, BSON.decode(encoded));
    }

}
