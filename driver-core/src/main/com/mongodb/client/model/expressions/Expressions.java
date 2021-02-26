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

import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDecimal128;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.conversions.Bson;
import org.bson.types.Decimal128;

import java.util.Date;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;

/**
 * Builders for aggregate expressions.
 *
 * @mongodb.driver.manual reference/operator/aggregation/
 * @since 4.?
 */
public interface Expressions {

    //
    //  Literal expressions
    //

    // The regular literal expressions will _not_ wrap the values in $literal. This is the normal case so making these
    // method names shorter ones

    /**
     * An expression representing the given literal boolean value.  The value will _not_ be wrapped in a {@code $literal}.
     *
     * @param value the boolean value of the expression
     * @return a literal expression representing the given boolean value
     * @see #unparsedLiteral(boolean)
     * @mongodb.driver.manual meta/aggregation-quick-reference/#literals
     */
    static LiteralExpression literal(final boolean value) {
        return new LiteralExpression(BsonBoolean.valueOf(value), true);
    }

    /**
     * An expression representing the given literal int value.  The value will _not_ be wrapped in a {@code $literal}.
     *
     * @param value the boolean value of the expression
     * @return a literal expression representing the given int value
     * @see #unparsedLiteral(int)
     * @mongodb.driver.manual meta/aggregation-quick-reference/#literals
     */
    static LiteralExpression literal(final int value) {
        return new LiteralExpression(new BsonInt32(value), true);
    }

    /**
     * An expression representing the given literal long value.  The value will _not_ be wrapped in a {@code $literal}.
     *
     * @param value the long value of the expression
     * @return a literal expression representing the given long value
     * @see #unparsedLiteral(long)
     * @mongodb.driver.manual meta/aggregation-quick-reference/#literals
     */
    static LiteralExpression literal(final long value) {
        return new LiteralExpression(new BsonInt64(value), true);
    }

    /**
     * An expression representing the given literal Date value.  The value will _not_ be wrapped in a {@code $literal}.
     *
     * @param value the Date value of the expression
     * @return a literal expression representing the given Date value
     * @see #unparsedLiteral(Date)
     * @mongodb.driver.manual meta/aggregation-quick-reference/#literals
     */
    static LiteralExpression literal(final Date value) {
        return new LiteralExpression(new BsonDateTime(value.getTime()), true);
    }

    /**
     * An expression representing the given literal String value.  The value will _not_ be wrapped in a {@code $literal}.
     *
     * @param value the String value of the expression
     * @return a literal expression representing the given String value
     * @see #unparsedLiteral(String)
     * @mongodb.driver.manual meta/aggregation-quick-reference/#literals
     */
    static LiteralExpression literal(final String value) {
        return new LiteralExpression(new BsonString(value), true);
    }

    /**
     * An expression representing the given literal double value.  The value will _not_ be wrapped in a {@code $literal}.
     *
     * @param value the double value of the expression
     * @return a literal expression representing the given double value
     * @see #unparsedLiteral(double)
     * @mongodb.driver.manual meta/aggregation-quick-reference/#literals
     */
    static LiteralExpression literal(final double value) {
        return new LiteralExpression(new BsonDouble(value), true);
    }

    /**
     * An expression representing the given literal boolean value.  The value will _not_ be wrapped in a {@code $literal}.
     *
     * @param value the boolean value of the expression
     * @return a literal expression representing the given boolean value
     * @see #unparsedLiteral(boolean)
     * @mongodb.driver.manual meta/aggregation-quick-reference/#literals
     */
    static LiteralExpression literal(final Decimal128 value) {
        return new LiteralExpression(new BsonDecimal128(value), true);
    }

    /**
     * An expression representing the given literal value.  The value will _not_ be wrapped in a {@code $literal}.
     *
     * @param value the value of the expression
     * @return a literal expression representing the given boolean value
     * @see #unparsedLiteral(boolean)
     * @mongodb.driver.manual meta/aggregation-quick-reference/#literals
     */
    static LiteralExpression literal(final BsonValue value) {
        return new LiteralExpression(value, true);
    }

    /**
     * An expression representing the given value. The value will be parsed by the expression evaluator.
     *
     * @param value the value of the expression, which can be any type that can be represented as a BSON value
     *
     * @return a literal expression representing the given value
     * @see #unparsedLiteral(Object)
     * @mongodb.driver.manual meta/aggregation-quick-reference/#literals
     */
    static LiteralExpression literal(final Object value) {
        return new LiteralExpression(value, true);
    }


    /**
     * An expression representing the given literal boolean value.  The value will be wrapped in a {@code $literal}.
     *
     * @param value the boolean value of the expression
     * @return a literal expression representing the given boolean value
     * @see #unparsedLiteral(boolean)
     * @mongodb.driver.manual meta/aggregation-quick-reference/#literals
     */
    static LiteralExpression unparsedLiteral(final boolean value) {
        return new LiteralExpression(BsonBoolean.valueOf(value), false);
    }

    /**
     * An expression representing the given literal int value.  The value will be wrapped in a {@code $literal}.
     *
     * @param value the boolean value of the expression
     * @return a literal expression representing the given int value
     * @see #literal(int)
     * @mongodb.driver.manual meta/aggregation-quick-reference/#literals
     */
    static LiteralExpression unparsedLiteral(final int value) {
        return new LiteralExpression(new BsonInt32(value), false);
    }

    /**
     * An expression representing the given literal long value.  The value will be wrapped in a {@code $literal}.
     *
     * @param value the long value of the expression
     * @return a literal expression representing the given long value
     * @see #literal(long)
     * @mongodb.driver.manual meta/aggregation-quick-reference/#literals
     */
    static LiteralExpression unparsedLiteral(final long value) {
        return new LiteralExpression(new BsonInt64(value), false);
    }

    /**
     * An expression representing the given literal Date value.  The value will be wrapped in a {@code $literal}.
     *
     * @param value the Date value of the expression
     * @return a literal expression representing the given Date value
     * @see #literal(int)
     * @mongodb.driver.manual meta/aggregation-quick-reference/#literals
     */
    static LiteralExpression unparsedLiteral(final Date value) {
        return new LiteralExpression(new BsonDateTime(value.getTime()), false);
    }

    /**
     * An expression representing the given literal String value.  The value will be wrapped in a {@code $literal}.
     *
     * @param value the String value of the expression
     * @return a literal expression representing the given int value
     * @see #literal(String)
     * @mongodb.driver.manual meta/aggregation-quick-reference/#literals
     */
    static LiteralExpression unparsedLiteral(final String value) {
        return new LiteralExpression(new BsonString(value), false);
    }

    /**
     * An expression representing the given literal double value.  The value will be wrapped in a {@code $literal}.
     *
     * @param value the double value of the expression
     * @return a literal expression representing the given double value
     * @see #literal(double)
     * @mongodb.driver.manual meta/aggregation-quick-reference/#literals
     */
    static LiteralExpression unparsedLiteral(final double value) {
        return new LiteralExpression(new BsonDouble(value), false);
    }

    /**
     * An expression representing the given literal Decimal128 value.  The value will be wrapped in a {@code $literal}.
     *
     * @param value the Decimal128 value of the expression
     * @return a literal expression representing the given Decimal128 value
     * @see #literal(Decimal128)
     * @mongodb.driver.manual meta/aggregation-quick-reference/#literals
     */
    static LiteralExpression unparsedLiteral(final Decimal128 value) {
        return new LiteralExpression(new BsonDecimal128(value), false);
    }

    /**
     * An expression representing the given literal value.  The value will be wrapped in a {@code $literal}.
     *
     * @param value the value of the expression
     * @return a literal expression representing the given int value
     * @see #literal(BsonValue)
     * @mongodb.driver.manual meta/aggregation-quick-reference/#literals
     */
    static LiteralExpression unparsedLiteral(final BsonValue value) {
        return new LiteralExpression(value, false);
    }

    /**
     * An expression representing the given value. The value will not be parsed by the expression evaluator.
     *
     * @param value the value of the expression, which can be any type that can be represented as a BSON value
     *
     * @return a literal expression representing the given value
     * @see #literal(Object) (Object)
     * @mongodb.driver.manual meta/aggregation-quick-reference/#literals
     * @mongodb.driver.manual reference/operator/aggregation/literal/#exp._S_literal
     */
    static LiteralExpression unparsedLiteral(final Object value) {
        return new LiteralExpression(value, false);
    }

    // object and array expressions

    /**
     * An expression representing a document, where the value of each field is evaluated as an expression.
     *
     * @param elements the elements of the document
     * @return the document expression
     * @mongodb.driver.manual meta/aggregation-quick-reference/#agg-quick-ref-expression-objects
     */
    static DocumentExpression document(Map<String, Expression> elements) {
        return new DocumentExpression(elements);
    }

    /**
     * An expression representing an array, where each element of the array is evaluated as an expression.
     *
     * @param elements the elements of the array
     * @return the array expression
     */
    static ArrayExpression array(List<Expression> elements) {
        return new ArrayExpression(elements);
    }

    //
    //  Path expressions
    //

    /**
     * An expression representing a field path, to access fields in the input documents. To specify a field path, specify the field name or
     * the dotted field name (if the field is in an embedded document).  Do not prefix the field path with a {@code "$"}.
     *
     * @param fieldPath the path to the field, which should not be prefixed with a @code "$"}
     * @return the field path expression
     * @mongodb.driver.manual meta/aggregation-quick-reference/#agg-quick-ref-field-paths
     */
    static FieldPathExpression fieldPath(final String fieldPath) {
        return new FieldPathExpression(fieldPath);
    }

    /**
     * An expression representing a reference to the CURRENT system variable.
     *
     * @return the variable expression
     * @mongodb.driver.manual reference/aggregation-variables/#variable.CURRENT
     */
    static VariableReferenceExpression currentRef() {
        return ref("CURRENT");
    }

    /**
     * An expression representing a reference to the ROOT system variable.
     *
     * @return the variable expression
     * @mongodb.driver.manual reference/aggregation-variables/#variable.ROOT
     */
    static VariableReferenceExpression rootRef() {
        return ref("ROOT");
    }

    /**
     * An expression representing a reference to the variable with the given name.  Do not prefix the variable name with {@code "$"}.
     *
     * @param name the variable name, which should not be prefixed with with {@code "$"}
     * @return the variable expression
     */
    static VariableReferenceExpression ref(final String name) {
        return new VariableReferenceExpression(name, null);
    }

    /**
     * An expression represented by the given BSON document.  This can be used as an escape hatch if you need to use an expression for
     * which there is not yet builder support in the release of the driver that is currently in use.
     *
     * @param expression the expression
     * @return the custom expression
     */
    static CustomExpression of(final Bson expression) {
        return new CustomExpression(expression);
    }

    /**
     * An expression that adds numbers together or adds numbers and a date. If one of the arguments is a date, $add treats the other
     * arguments as milliseconds to add to the date.
     *
     * @param numbers The arguments can be any valid expression as long as they resolve to either all numbers or to numbers and a date.
     * @return a $add expression
     * @mongodb.driver.manual reference/operator/aggregation/add/
     */
    static AddExpression add(final Expression... numbers) {
        return new AddExpression(asList(numbers));
    }

    /**
     * An expression that compares two values and returns true when the first value is greater than or equivalent to the second value,
     * and false when the first value is less than the second value.
     *
     * @param first the first expression
     * @param second the second expression
     * @return a $gte expression
     * @mongodb.driver.manual reference/operator/aggregation/gte/
     */
    static GreaterThanOrEqualExpression gte(final Expression first, final Expression second) {
        return new GreaterThanOrEqualExpression(first, second);
    }

    /**
     * An expression that evaluates a series of case expressions. When it finds an expression which evaluates to true, $switch executes a
     * specified expression and breaks out of the control flow.
     *
     * @param branches the branches
     * @return a $switch expression
     * @see #branch(Expression, Expression)
     * @mongodb.driver.manual reference/operator/aggregation/switch/
     */
    static SwitchExpression switchExpr(final Branch... branches) {
        return new SwitchExpression(asList(branches), null);
    }

    /**
     * A branch of a switch expression.
     *
     * @param caseExpr an expression representing the case of the branch
     * @param thenExpr an expression that is evaluated if this is the first branch whose case evaluates to true
     * @return a branch of switch expression
     * @see #switchExpr(Branch...)
     * @mongodb.driver.manual reference/operator/aggregation/switch/
     */
    static Branch branch(final Expression caseExpr, final Expression thenExpr) {
        return new Branch(caseExpr, thenExpr);
    }
}
