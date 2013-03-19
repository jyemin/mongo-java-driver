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

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.DBRef;
import org.bson.BasicBSONObject;
import org.bson.types.BSONTimestamp;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.CodeWScope;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;
import org.junit.Test;

import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;

public class JSONTest {

    @Test
    public void testSerializeInt() {
        assertEquals("321", JSON.serialize(321));
        assertEquals("-12", JSON.serialize(-12));
    }

    @Test
    public void testSerializeLong() {
        assertEquals("-9223372036854775808", JSON.serialize(Long.MIN_VALUE));
        assertEquals("654", JSON.serialize(654L));
    }

    @Test
    public void testSerializeDouble() {
        assertEquals("1.02", JSON.serialize(1.02));
        assertEquals("1.0E100", JSON.serialize(1.0E100));
        assertEquals("1.22E-9", JSON.serialize(12.2E-10));
    }

    @Test
    public void testSerializeBoolean() {
        assertEquals("true", JSON.serialize(true));
        assertEquals("false", JSON.serialize(false));
    }

    @Test
    public void testSerializeString() {
        assertEquals("\"somestring\"", JSON.serialize("somestring"));
        assertEquals("\"\"", JSON.serialize(""));
        assertEquals("\"\\t\"", JSON.serialize("\t"));
    }

    @Test
    public void testSerializePattern() {
        Pattern pattern = Pattern.compile("^a\\s+patte\\w$", Pattern.CASE_INSENSITIVE);
        assertEquals(
                "{ \"$regex\" : \"^a\\\\s+patte\\\\w$\", \"$options\" : \"i\" }",
                JSON.serialize(pattern)
        );
    }


    @Test
    public void testSerializeDate() {
        assertEquals("{ \"$date\" : 0 }", JSON.serialize(new Date(0)));
        assertEquals("{ \"$date\" : 9223372036854775807 }", JSON.serialize(new Date(Long.MAX_VALUE)));
    }

    @Test
    public void testSerializeObjectArray() {
        Object[] objects = new Object[]{false, 12, new BasicBSONObject(), "string"};
        assertEquals("[false, 12, { }, \"string\"]", JSON.serialize(objects));
    }

    @Test
    public void testSerializeDBObject() {
        DBObject object = new BasicDBObject();
        object.put("i1", 12);
        object.put("s2", "st");
        object.put("b3", true);
        assertEquals("{ \"i1\" : 12, \"s2\" : \"st\", \"b3\" : true }", JSON.serialize(object));
    }

    @Test
    public void testSerializeCode() {
        Code code = new Code("var i");
        assertEquals("{ \"$code\" : \"var i\" }", JSON.serialize(code));
    }

    @Test
    public void testSerializeCodeWScope() {
        CodeWScope code = new CodeWScope("var i", new BasicBSONObject("i", 2));
        assertEquals("{ \"$code\" : \"var i\", \"$scope\" : { \"i\" : 2 } }", JSON.serialize(code));
    }

    @Test
    public void testSerializeObjectId() {
        ObjectId objectId = new ObjectId("5136efb318c65ff2360c3db6");
        assertEquals("{ \"$oid\" : \"5136efb318c65ff2360c3db6\" }", JSON.serialize(objectId));
    }

    @Test
    public void testSerializeDBRef() {
        ObjectId objectId = new ObjectId("5136efb318c65ff2360c3db6");
        DBObject object = new BasicDBObject("ref", new DBRef(null, "test.foo", objectId));
        assertEquals(
                "{ \"ref\" : { \"$ref\" : \"test.foo\", \"$id\" : { \"$oid\" : \"5136efb318c65ff2360c3db6\" } } }",
                JSON.serialize(object)
        );

    }

    @Test
    public void testSerializeIterable() {
        Set<Integer> set = new TreeSet<Integer>();
        set.add(3);
        set.add(2);
        set.add(1);
        assertEquals("[1, 2, 3]", JSON.serialize(set));
    }

    @Test
    public void testSerializeMap() {
        Map<String, Boolean> map = new TreeMap<String, Boolean>();
        map.put("i", true);
        map.put("k", false);
        assertEquals("{ \"i\" : true, \"k\" : false }", JSON.serialize(map));
    }

    @Test
    public void testSerializeMaxKey() {
        assertEquals("{ \"$maxKey\" : 1 }", JSON.serialize(new MaxKey()));
    }

    @Test
    public void testSerializeMinKey() {
        assertEquals("{ \"$minKey\" : 1 }", JSON.serialize(new MinKey()));
    }

    @Test
    public void testSerializeBinary() {
        Binary binary = new Binary((byte) 3, "false".getBytes());
        assertEquals("{ \"$binary\" : \"ZmFsc2U=\", \"$type\" : \"3\" }", JSON.serialize(binary));
    }

    @Test
    public void testSerializeUUID() {
        UUID uuid = UUID.fromString("60f65152-6d4a-4f11-9c9b-590b575da7b5");
//        assertEquals("{ \"$uuid\" : \"60f65152-6d4a-4f11-9c9b-590b575da7b5\"}", JSON.serialize(uuid));
        assertEquals("{ \"$binary\" : \"YPZRUm1KTxGcm1kLV12ntQ==\", \"$type\" : \"3\" }", JSON.serialize(uuid));
    }

    @Test
    public void testSerializeBSONTimestamp() {
        BSONTimestamp timestamp = new BSONTimestamp(1300474885, 10);
        assertEquals("{ \"$timestamp\" : { \"t\" : 1300474885, \"i\" : 10 } }", JSON.serialize(timestamp));
    }

    @Test
    public void testSerializeByteArray() {
        byte[] bytes = new byte[]{102, 97, 108, 115, 101};
        assertEquals("{ \"$binary\" : \"ZmFsc2U=\", \"$type\" : \"0\" }", JSON.serialize(bytes));
    }


//    @Test
//    public void testParseInt() {
//        assertEquals("321", JSON.parse("321"));
//        assertEquals("-12", JSON.parse("-12"));
//    }
//
//    @Test
//    public void testParseLong() {
//        assertEquals("-9223372036854775808", JSON.serialize(Long.MIN_VALUE));
//        assertEquals(654L, JSON.parse("654"));
//    }
//
//    @Test
//    public void testParseDouble() {
//        assertEquals(1.02, JSON.parse("1.02"));
//        assertEquals(1.0E100, JSON.parse("1.0E100"));
//        assertEquals(12.2E-10, JSON.parse("1.22E-9"));
//    }
//
//    @Test
//    public void testParseBoolean() {
//        assertEquals(Boolean.TRUE, JSON.parse("true"));
//        assertEquals(Boolean.FALSE, JSON.parse("false"));
//    }
//
//    @Test
//    public void testParseString() {
//        assertEquals("\"somestring\"", JSON.parse("somestring"));
//        assertEquals("\"\"", JSON.parse(""));
//        assertEquals("\"\\t\"", JSON.parse("\t"));
//    }
//
//    @Test
//    public void testParsePattern() {
//        Pattern expected = Pattern.compile("^a\\s+patte\\w$", Pattern.CASE_INSENSITIVE);
//
//        Object o = JSON.parse("{ \"$regex\" : \"^a\\\\s+patte\\\\w$\", \"$options\" : \"i\" }");
//        assertThat(o, instanceOf(Pattern.class));
//
//        Pattern actual = (Pattern) o;
//        assertEquals(expected.pattern(), actual.pattern());
//        assertEquals(expected.flags(), actual.flags());
//    }
//
//
//    @Test
//    public void testParseDate() {
//        assertEquals(new Date(0), JSON.parse("{ \"$date\" : 0 }"));
//        assertEquals(new Date(Long.MAX_VALUE), JSON.parse("{ \"$date\" : 9223372036854775807 }"));
//    }
//
//    @Test
//    public void testParseObjectArray() {
//        Object[] objects = new Object[]{false, 12, new BasicBSONObject(), "string"};
//        assertEquals("b", JSON.parse("[ false , 12 , { } , \"string\"]"));
//    }
//
//    @Test
//    public void testParseDBObject() {
//        DBObject object = new BasicDBObject();
//        object.put("i1", 12);
//        object.put("s2", "st");
//        object.put("b3", true);
//        assertEquals(object, JSON.parse("{ \"i1\" : 12, \"s2\" : \"st\", \"b3\" : true }"));
//    }
//
//    @Test
//    public void testParseCode() {
//        Code code = new Code("var i");
//        assertEquals(code, JSON.parse("{ \"$code\" : \"var i\" }"));
//    }
//
//    @Test
//    public void testParseCodeWScope() {
//        CodeWScope code = new CodeWScope("var i", new BasicBSONObject("i", 2));
//        assertEquals(code, JSON.parse("{ \"$code\" : \"var i\", \"$scope\" : { \"i\" : 2 } }"));
//    }
//
//    @Test
//    public void testParseObjectId() {
//        ObjectId objectId = new ObjectId("5136efb318c65ff2360c3db6");
//        assertEquals(objectId, JSON.parse("{ \"$oid\" : \"5136efb318c65ff2360c3db6\" }"));
//    }
//
//    @Test
//    public void testParseDBRef() {
//        ObjectId objectId = new ObjectId("5136efb318c65ff2360c3db6");
//        DBObject object = new BasicDBObject("ref", new DBRef(null, "test.foo", objectId));
//        assertEquals(
//                object,
//                JSON.parse("{ \"ref\" : { \"$ref\" : \"test.foo\", \"$id\" : { \"$oid\" : \"5136efb318c65ff2360c3db6\" } } }")
//        );
//
//    }
//
//    @Test
//    public void testParseIterable() {
//        assertEquals("a", JSON.parse("[1, 2, 3]"));
//    }
//
//    @Test
//    public void testParseMap() {
////        Map<String, Boolean> map = new TreeMap<String, Boolean>();
////        map.put("i", true);
////        map.put("k", false);
//        assertEquals("b", JSON.parse("{ \"i\" : true, \"k\" : false }"));
//    }
//
//    @Test
//    public void testParseMaxKey() {
//        assertEquals(new MaxKey(), JSON.parse("{ \"$maxKey\" : 1 }"));
//    }
//
//    @Test
//    public void testParseMinKey() {
//        assertEquals(new MinKey(), JSON.parse("{ \"$minKey\" : 1 }"));
//    }
//
//    @Test
//    public void testParseBinary() {
//        Binary binary = new Binary((byte) 3, "false".getBytes());
//        assertEquals(binary, JSON.parse("{ \"$binary\" : \"ZmFsc2U=\", \"$type\" : \"3\" }"));
//    }
//
//    @Test
//    public void testParseUUID() {
//        UUID uuid = UUID.fromString("60f65152-6d4a-4f11-9c9b-590b575da7b5");
//        assertEquals(uuid, JSON.parse("{ \"$uuid\" : \"60f65152-6d4a-4f11-9c9b-590b575da7b5\"}"));
//    }
//
//    @Test
//    public void testParseBSONTimestamp() {
//        BSONTimestamp timestamp = new BSONTimestamp(1300474885, 10);
//        assertEquals(timestamp, JSON.parse("{ \"$timestamp\" : { \"t\" : 1300474885, \"i\" : 10 } }"));
//    }
//
//    @Test
//    public void testParseByteArray() {
//        byte[] bytes = new byte[]{102, 97, 108, 115, 101};
//        assertEquals(bytes, JSON.parse("{ \"$binary\" : \"ZmFsc2U=\", \"$type\" : \"0\" }"));
//    }


}
