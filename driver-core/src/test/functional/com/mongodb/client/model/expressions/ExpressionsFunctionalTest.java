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

import com.mongodb.MongoNamespace;
import com.mongodb.client.model.Field;
import com.mongodb.client.test.CollectionHelper;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.types.Decimal128;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static com.mongodb.ClusterFixture.getDefaultDatabaseName;
import static com.mongodb.client.model.Aggregates.addFields;
import static com.mongodb.client.model.expressions.Expressions.add;
import static com.mongodb.client.model.expressions.Expressions.array;
import static com.mongodb.client.model.expressions.Expressions.branch;
import static com.mongodb.client.model.expressions.Expressions.currentRef;
import static com.mongodb.client.model.expressions.Expressions.document;
import static com.mongodb.client.model.expressions.Expressions.fieldPath;
import static com.mongodb.client.model.expressions.Expressions.gte;
import static com.mongodb.client.model.expressions.Expressions.literal;
import static com.mongodb.client.model.expressions.Expressions.of;
import static com.mongodb.client.model.expressions.Expressions.ref;
import static com.mongodb.client.model.expressions.Expressions.rootRef;
import static com.mongodb.client.model.expressions.Expressions.switchExpr;
import static com.mongodb.client.model.expressions.Expressions.unparsedLiteral;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.bson.BsonDocument.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

public class ExpressionsFunctionalTest {

    private CollectionHelper<BsonDocument> helper;

    @Before
    public void setUp() {
        helper = new CollectionHelper<>(new BsonDocumentCodec(),
                new MongoNamespace(getDefaultDatabaseName(), getClass().getName()));
        helper.insertDocuments(new BsonDocument("_id", new BsonInt32(1)));
    }

    @After
    public void tearDown() {
        helper.drop();
    }

    @Test
    public void addTest() {
        // given
        AddExpression expr = add(literal(1), literal(2));

        // unit
        assertEquals(asList(literal(1), literal(2)), expr.getNumbers());
        assertEquals(new BsonDocument("$add", new BsonArray(asList(new BsonInt32(1), new BsonInt32(2)))),
                expr.toBsonValue());

        // functional
        assertEquals(3, eval(expr).asNumber().intValue());
    }

    @Test
    public void gteTest() {
        // given
        GreaterThanOrEqualExpression expr = gte(literal(7), literal(6));

        // unit
        assertEquals(literal(7), expr.getFirst());
        assertEquals(literal(6), expr.getSecond());
        assertEquals(new BsonDocument("$gte", new BsonArray(asList(new BsonInt32(7), new BsonInt32(6)))),
                expr.toBsonValue());

        // functional
        assertTrue(eval(expr).asBoolean().getValue());
    }

    @Test
    public void switchExprTest() {
        SwitchExpression expr = switchExpr(
                branch(literal(false), literal(1)),
                branch(literal(true), literal(2)));

        // unit
        assertNull(expr.getDefault());
        assertEquals(asList(branch(literal(false), literal(1)), branch(literal(true), literal(2))),
                expr.getBranches());
        assertEquals(new BsonDocument("$switch",
                        new BsonDocument("branches",
                                new BsonArray(asList(
                                        new BsonDocument("case", BsonBoolean.FALSE).append("then", new BsonInt32(1)),
                                        new BsonDocument("case", BsonBoolean.TRUE).append("then", new BsonInt32(2)))))),
                expr.toBsonValue());

        // functional
        assertEquals(2, eval(expr).asNumber().intValue());
    }

    @Test
    public void switchExprWithDefaultTest() {
        // given
        SwitchExpression expr = switchExpr(
                branch(literal(false), literal(1)),
                branch(literal(false), literal(2)))
                .defaultExpr(literal(3));

        // unit
        assertEquals(literal(3), expr.getDefault());
        assertEquals(asList(branch(literal(false), literal(1)), branch(literal(false), literal(2))),
                expr.getBranches());
        assertEquals(new BsonDocument("$switch",
                        new BsonDocument("branches",
                                new BsonArray(asList(
                                        new BsonDocument("case", BsonBoolean.FALSE).append("then", new BsonInt32(1)),
                                        new BsonDocument("case", BsonBoolean.FALSE).append("then", new BsonInt32(2)))))
                                .append("default", new BsonInt32(3))),
                expr.toBsonValue());

        // functional
        assertEquals(3, eval(expr).asNumber().intValue());
    }

    @Test
    public void literalStringTest() {
        // given
        LiteralExpression expr = literal("str");

        // unit
        assertTrue(expr.isParsed());
        assertEquals(new BsonString("str"), expr.getValue());
        assertEquals(new BsonString("str"), expr.toBsonValue());

        // functional
        assertEquals("str", eval(expr).asString().getValue());
    }

    @Test
    public void literalIntTest() {
        // given
        LiteralExpression expr = literal(42);

        // unit
        assertTrue(expr.isParsed());
        assertEquals(new BsonInt32(42), expr.getValue());
        assertEquals(new BsonInt32(42), expr.toBsonValue());

        // functional
        assertEquals(42, eval(expr).asInt32().intValue());
    }

    @Test
    public void literalLongTest() {
        // given
        LiteralExpression expr = literal(42L);

        // unit
        assertTrue(expr.isParsed());
        assertEquals(new BsonInt64(42L), expr.getValue());
        assertEquals(new BsonInt64(42L), expr.toBsonValue());

        // functional
        assertEquals(42L, eval(expr).asInt64().longValue());
    }

    @Test
    public void literalDoubleTest() {
        // given
        LiteralExpression expr = literal(42.0);

        // unit
        assertTrue(expr.isParsed());
        assertEquals(new BsonDouble(42.0), expr.getValue());
        assertEquals(new BsonDouble(42.0), expr.toBsonValue());

        // functional
        assertEquals(42.0, eval(expr).asDouble().doubleValue());
    }

    @Test
    public void literalDecimal128Test() {
        // given
        Decimal128 value = Decimal128.parse("42.900");
        LiteralExpression expr = literal(value);

        // unit
        assertTrue(expr.isParsed());
        assertEquals(new BsonDecimal128(value), expr.getValue());
        assertEquals(new BsonDecimal128(value), expr.toBsonValue());

        // functional
        assertEquals(value, eval(expr).asDecimal128().decimal128Value());
    }

    @Test
    public void literalBooleanTest() {
        // given
        LiteralExpression expr = literal(true);

        // unit
        assertTrue(expr.isParsed());
        assertEquals(BsonBoolean.TRUE, expr.getValue());
        assertEquals(BsonBoolean.TRUE, expr.toBsonValue());

        // functional
        assertTrue(eval(expr).asBoolean().getValue());
    }

    @Test
    public void literalDateTest() {
        // given
        Date date = new Date();
        LiteralExpression expr = literal(date);

        // unit
        assertTrue(expr.isParsed());
        assertEquals(new BsonDateTime(date.getTime()), expr.getValue());
        assertEquals(new BsonDateTime(date.getTime()), expr.toBsonValue());

        // functional
        assertEquals(date.getTime(), eval(expr).asDateTime().getValue());
    }

    @Test
    public void literalBsonValueTest() {
        // given
        LiteralExpression expr = literal(new BsonString("str"));

        // unit
        assertTrue(expr.isParsed());
        assertEquals(new BsonString("str"), expr.getValue());
        assertEquals(new BsonString("str"), expr.toBsonValue());

        // functional
        assertEquals("str", eval(expr).asString().getValue());
    }

    @Test
    public void literalObjectTest() {
        // given
        LiteralExpression expr = literal((Object) "str");

        // unit
        assertTrue(expr.isParsed());
        assertEquals("str", expr.getValue()); // TODO: it's weird that this returns something different than in literalStringTest above
        assertEquals(new BsonString("str"), expr.toBsonValue());

        // functional
        assertEquals("str", eval(expr).asString().getValue());
    }

    @Test
    public void literalNullObjectTest() {
        // given
        LiteralExpression expr = literal((Object) null);  // TODO: should parameter be @Nullable

        // unit
        assertTrue(expr.isParsed());
        assertNull(expr.getValue());  // TODO: should parameter be @Nullable?
        assertEquals(BsonNull.VALUE, expr.toBsonValue());

        // functional
        assertEquals(BsonNull.VALUE, eval(expr));
    }

     @Test
    public void unparsedLiteralIntTest() {
        // given
        LiteralExpression expr = unparsedLiteral(42);

        // unit
        assertFalse(expr.isParsed());
        assertEquals(new BsonInt32(42), expr.getValue());
        assertEquals(new BsonDocument("$literal", new BsonInt32(42)), expr.toBsonValue());

        // functional
        assertEquals(42, eval(expr).asInt32().intValue());
    }

    @Test
    public void unparsedLiteralLongTest() {
        // given
        LiteralExpression expr = unparsedLiteral(42L);

        // unit
        assertFalse(expr.isParsed());
        assertEquals(new BsonInt64(42L), expr.getValue());
        assertEquals(new BsonDocument("$literal", new BsonInt64(42L)), expr.toBsonValue());

        // functional
        assertEquals(42L, eval(expr).asInt64().longValue());
    }

    @Test
    public void unparsedLiteralDoubleTest() {
        // given
        LiteralExpression expr = unparsedLiteral(42.0);

        // unit
        assertFalse(expr.isParsed());
        assertEquals(new BsonDouble(42.0), expr.getValue());
        assertEquals(new BsonDocument("$literal", new BsonDouble(42.0)), expr.toBsonValue());

        // functional
        assertEquals(42.0, eval(expr).asDouble().doubleValue());
    }

    @Test
    public void unparsedLiteralDecimal128Test() {
        // given
        Decimal128 value = Decimal128.parse("42.900");
        LiteralExpression expr = unparsedLiteral(value);

        // unit
        assertFalse(expr.isParsed());
        assertEquals(new BsonDecimal128(value), expr.getValue());
        assertEquals(new BsonDocument("$literal", new BsonDecimal128(value)), expr.toBsonValue());

        // functional
        assertEquals(value, eval(expr).asDecimal128().decimal128Value());
    }

    @Test
    public void unparsedLiteralBooleanTest() {
        // given
        LiteralExpression expr = unparsedLiteral(true);

        // unit
        assertFalse(expr.isParsed());
        assertEquals(BsonBoolean.TRUE, expr.getValue());
        assertEquals(new BsonDocument("$literal", BsonBoolean.TRUE), expr.toBsonValue());

        // functional
        assertTrue(eval(expr).asBoolean().getValue());
    }

    @Test
    public void unparsedLiteralDateTest() {
        // given
        Date date = new Date();
        LiteralExpression expr = unparsedLiteral(date);

        // unit
        assertFalse(expr.isParsed());
        assertEquals(new BsonDateTime(date.getTime()), expr.getValue());
        assertEquals(new BsonDocument("$literal", new BsonDateTime(date.getTime())), expr.toBsonValue());

        // functional
        assertEquals(date.getTime(), eval(expr).asDateTime().getValue());
    }

    @Test
    public void unparsedLiteralBsonValueTest() {
        // given
        LiteralExpression expr = unparsedLiteral(new BsonString("str"));

        // unit
        assertFalse(expr.isParsed());
        assertEquals(new BsonString("str"), expr.getValue());
        assertEquals(new BsonDocument("$literal", new BsonString("str")), expr.toBsonValue());

        // functional
        assertEquals("str", eval(expr).asString().getValue());
    }

    @Test
    public void unparsedLiteralObjectTest() {
        // given
        LiteralExpression expr = unparsedLiteral((Object) "str");

        // unit
        assertFalse(expr.isParsed());
        assertEquals("str", expr.getValue());
        assertEquals(new BsonDocument("$literal", new BsonString("str")), expr.toBsonValue());

        // functional
        assertEquals("str", eval(expr).asString().getValue());
    }

    @Test
    public void unparsedLiteralThatLooksLikeAPathTest() {
        // given
        LiteralExpression expr = unparsedLiteral("$$path");

        // unit
        assertFalse(expr.isParsed());
        assertEquals(new BsonString("$$path"), expr.getValue());
        assertEquals(new BsonDocument("$literal", new BsonString("$$path")), expr.toBsonValue());

        // functional
        assertEquals("$$path", eval(expr).asString().getValue());
    }

    @Test
    public void documentTest() {
        // given
        Map<String, Expression> map = new HashMap<>();
        map.put("key", add(literal(1), literal(2)));
        DocumentExpression expr = document(map);

        // unit
        assertEquals(map, expr.getElements());
        assertEquals(new BsonDocument("key",
                        new BsonDocument("$add", new BsonArray(asList(new BsonInt32(1), new BsonInt32(2))))),
                expr.toBsonValue());

        // functional
        assertEquals(parse("{key: 3}"), eval(expr));
    }

    @Test
    public void arrayTest() {
        // given
        ArrayExpression expr = array(asList(add(literal(2), literal(1)), add(literal(7), literal(3))));

        // unit
        assertEquals(asList(add(literal(2), literal(1)), add(literal(7), literal(3))), expr.getElements());
        assertEquals(new BsonArray(asList(
                new BsonDocument("$add", new BsonArray(asList(new BsonInt32(2), new BsonInt32(1)))),
                new BsonDocument("$add", new BsonArray(asList(new BsonInt32(7), new BsonInt32(3)))))),
                expr.toBsonValue());

        // functional
        assertEquals(new BsonArray(asList(new BsonInt32(3), new BsonInt32(10))), eval(expr));
    }

    @Test
    public void fieldPathTest() {
        // given
        FieldPathExpression expr = fieldPath("_id");

        // unit
        assertEquals("_id", expr.getFieldPath());
        assertEquals(new BsonString("$_id"), expr.toBsonValue());

        // functional
        assertEquals(1, eval(expr).asNumber().intValue());
    }

    @Test
    public void currentRefTest() {
        // given
        VariableReferenceExpression expr = currentRef();

        // unit
        assertEquals("CURRENT", expr.getName());
        assertNull(expr.getFieldPath());
        assertEquals(new BsonString("$$CURRENT"), expr.toBsonValue());

        // functional
        assertEquals(parse("{_id: 1}"), eval(expr));
    }

    @Test
    public void currentRefWithFieldPathTest() {
        // given
        VariableReferenceExpression expr = currentRef().fieldPath("_id");

        // unit
        assertEquals("CURRENT", expr.getName());
        assertEquals("_id", expr.getFieldPath());
        assertEquals(new BsonString("$$CURRENT._id"), expr.toBsonValue());

        // functional
        assertEquals(1, eval(expr).asNumber().intValue());
    }

    @Test
    public void rootRefTest() {
        // given
        VariableReferenceExpression expr = rootRef();

        // unit
        assertEquals("ROOT", expr.getName());
        assertNull(expr.getFieldPath());
        assertEquals(new BsonString("$$ROOT"), expr.toBsonValue());

        // functional
        assertEquals(parse("{_id: 1}"), eval(expr));
    }

    @Test
    public void rootRefWithFieldPathTest() {
        // given
        VariableReferenceExpression expr = rootRef().fieldPath("_id");

        // unit
        assertEquals("ROOT", expr.getName());
        assertEquals("_id", expr.getFieldPath());
        assertEquals(new BsonString("$$ROOT._id"), expr.toBsonValue());

        // functional
        assertEquals(1, eval(expr).asNumber().intValue());
    }

    @Test
    public void refTest() {
        // given
        VariableReferenceExpression expr = ref("ROOT");

        // unit
        assertEquals("ROOT", expr.getName());
        assertNull(expr.getFieldPath());
        assertEquals(new BsonString("$$ROOT"), expr.toBsonValue());

        // functional
        assertEquals(parse("{_id: 1}"), eval(expr));
    }

    @Test
    public void refWithFieldPathTest() {
        // given
        VariableReferenceExpression expr = ref("ROOT").fieldPath("_id");

        // unit
        assertEquals("ROOT", expr.getName());
        assertEquals("_id", expr.getFieldPath());
        assertEquals(new BsonString("$$ROOT._id"), expr.toBsonValue());

        // functional
        assertEquals(1, eval(expr).asNumber().intValue());
    }

    @Test
    public void ofTest() {
        // given
        CustomExpression expr = of(parse("{$and: [true, true]}"));

        // unit
        assertEquals(parse("{$and: [true, true]}"), expr.getExpression());
        assertEquals(new BsonDocument("$and", new BsonArray(asList(BsonBoolean.TRUE, BsonBoolean.TRUE))), expr.toBsonValue());

        // functional
        assertTrue(eval(expr).asBoolean().getValue());
    }

    private BsonValue eval(final Expression expression) {
        return helper.aggregate(singletonList(
                addFields(new Field<>("val", expression.toBsonValue()))))
                .get(0).get("val");
    }
}
