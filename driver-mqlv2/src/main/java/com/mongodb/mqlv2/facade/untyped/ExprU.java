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
package com.mongodb.mqlv2.facade.untyped;

import com.mongodb.mqlv2.ast.BinaryOpType;
import com.mongodb.mqlv2.ast.Expr;
import com.mongodb.mqlv2.ast.UnaryOpType;

/**
 * Fluent wrapper around an {@link Expr}. Instances are immutable; every operator returns a new
 * {@code ExprU} wrapping the new {@link Expr} node.
 *
 * <p>Instances are constructed via the static factory methods on
 * {@link Untyped}.</p>
 */
public final class ExprU {

    private final Expr expr;

    ExprU(final Expr expr) {
        this.expr = expr;
    }

    public Expr toExpr() {
        return expr;
    }

    // --- Binary operators ---

    public ExprU eq(final ExprU other) {
        return new ExprU(new Expr.BinaryOp(BinaryOpType.EQ, expr, other.expr));
    }

    public ExprU ne(final ExprU other) {
        return new ExprU(new Expr.BinaryOp(BinaryOpType.NE, expr, other.expr));
    }

    public ExprU is(final ExprU other) {
        return new ExprU(new Expr.BinaryOp(BinaryOpType.IS, expr, other.expr));
    }

    public ExprU lt(final ExprU other) {
        return new ExprU(new Expr.BinaryOp(BinaryOpType.LT, expr, other.expr));
    }

    public ExprU le(final ExprU other) {
        return new ExprU(new Expr.BinaryOp(BinaryOpType.LE, expr, other.expr));
    }

    public ExprU gt(final ExprU other) {
        return new ExprU(new Expr.BinaryOp(BinaryOpType.GT, expr, other.expr));
    }

    public ExprU ge(final ExprU other) {
        return new ExprU(new Expr.BinaryOp(BinaryOpType.GE, expr, other.expr));
    }

    public ExprU and(final ExprU other) {
        return new ExprU(new Expr.BinaryOp(BinaryOpType.AND, expr, other.expr));
    }

    public ExprU or(final ExprU other) {
        return new ExprU(new Expr.BinaryOp(BinaryOpType.OR, expr, other.expr));
    }

    public ExprU add(final ExprU other) {
        return new ExprU(new Expr.BinaryOp(BinaryOpType.ADD, expr, other.expr));
    }

    public ExprU sub(final ExprU other) {
        return new ExprU(new Expr.BinaryOp(BinaryOpType.SUB, expr, other.expr));
    }

    public ExprU mul(final ExprU other) {
        return new ExprU(new Expr.BinaryOp(BinaryOpType.MUL, expr, other.expr));
    }

    public ExprU div(final ExprU other) {
        return new ExprU(new Expr.BinaryOp(BinaryOpType.DIV, expr, other.expr));
    }

    // --- Unary operator ---

    public ExprU not() {
        return new ExprU(new Expr.UnaryOp(UnaryOpType.NOT, expr));
    }

    // --- Chain / accessor operators ---

    /**
     * Chained field access. {@code field("a").field("b")} produces
     * {@code FieldAccess(FieldAccess(CurrentValue, "a"), "b")}.
     *
     * @param name the next field segment.
     * @return new {@code ExprU} wrapping a {@code FieldAccess}.
     */
    public ExprU field(final String name) {
        return new ExprU(new Expr.FieldAccess(expr, name));
    }

    /**
     * Arrow operator: {@code field("a").arrow("b")} produces {@code a->b} (the unwind-then-map
     * sugar).
     *
     * @param name the field to map onto.
     * @return new {@code ExprU} wrapping an {@code ArrowOp}.
     */
    public ExprU arrow(final String name) {
        return new ExprU(new Expr.ArrowOp(expr, name));
    }

    /**
     * Array indexing. {@code field("a").at(lit(0))} produces {@code a[0]}.
     *
     * @param index the index expression.
     * @return new {@code ExprU} wrapping an {@code ArrayIndex}.
     */
    public ExprU at(final ExprU index) {
        return new ExprU(new Expr.ArrayIndex(expr, index.expr));
    }

    /**
     * Postfix unwind operator: {@code field("a").unwind()} produces {@code a*}.
     *
     * @return new {@code ExprU} wrapping an {@code UnwindExpr}.
     */
    public ExprU unwind() {
        return new ExprU(new Expr.UnwindExpr(expr));
    }

    /**
     * The {@code any} predicate. {@code field("a").unwind().any(current().eq(lit(2)))} produces
     * {@code a* any ($ == 2)}.
     *
     * @param predicate the predicate evaluated against each unwound element.
     * @return new {@code ExprU} wrapping an {@code Any}.
     */
    public ExprU any(final ExprU predicate) {
        return new ExprU(new Expr.Any(expr, predicate.expr));
    }
}
