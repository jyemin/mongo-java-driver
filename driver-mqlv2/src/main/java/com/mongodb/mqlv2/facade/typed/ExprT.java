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
package com.mongodb.mqlv2.facade.typed;

import com.mongodb.mqlv2.ast.BinaryOpType;
import com.mongodb.mqlv2.ast.Expr;
import com.mongodb.mqlv2.ast.UnaryOpType;

/**
 * Phantom-typed fluent wrapper around an {@link Expr}. {@code T} is the result type of the
 * expression. The {@code T} parameter is enforced by Java's type system at compile time, but is
 * unrepresented at runtime (the wrapper carries only an {@link Expr}). MQLv2 itself is dynamically
 * typed — {@code T} is a driver-side convenience that catches a class of mistakes at compile time.
 *
 * <p>Type-system policy:
 * <ul>
 *   <li>Comparisons ({@code eq}, {@code ne}, {@code is}, {@code lt}, {@code le}, {@code gt},
 *       {@code ge}): parametric on the other side; return {@code ExprT<Boolean>}.</li>
 *   <li>Boolean ops ({@code and}, {@code or}): other side strict {@code ExprT<Boolean>}, receiver
 *       loose; return {@code ExprT<Boolean>}.</li>
 *   <li>{@code not()}: returns {@code ExprT<Boolean>}.</li>
 *   <li>Arithmetic ({@code add}, {@code sub}, {@code mul}, {@code div}): strict — both sides must
 *       be the same {@code T}; returns {@code ExprT<T>}.</li>
 *   <li>{@code field}/{@code arrow}/{@code at}/{@code unwind}: untyped overload returns
 *       {@code ExprT<Object>}; typed overload with {@code Class<T>} witness returns
 *       {@code ExprT<T>}.</li>
 *   <li>{@code any(ExprT<Boolean>)}: returns {@code ExprT<Boolean>}.</li>
 * </ul>
 * </p>
 *
 * @param <T> the result type of the expression.
 */
public final class ExprT<T> {

    private final Expr expr;

    ExprT(final Expr expr) {
        this.expr = expr;
    }

    public Expr toExpr() {
        return expr;
    }

    // --- Comparisons (parametric on the other side, return Boolean) ---

    public <U> ExprT<Boolean> eq(final ExprT<U> other) {
        return new ExprT<>(new Expr.BinaryOp(BinaryOpType.EQ, expr, other.expr));
    }

    public <U> ExprT<Boolean> ne(final ExprT<U> other) {
        return new ExprT<>(new Expr.BinaryOp(BinaryOpType.NE, expr, other.expr));
    }

    public <U> ExprT<Boolean> is(final ExprT<U> other) {
        return new ExprT<>(new Expr.BinaryOp(BinaryOpType.IS, expr, other.expr));
    }

    public <U> ExprT<Boolean> lt(final ExprT<U> other) {
        return new ExprT<>(new Expr.BinaryOp(BinaryOpType.LT, expr, other.expr));
    }

    public <U> ExprT<Boolean> le(final ExprT<U> other) {
        return new ExprT<>(new Expr.BinaryOp(BinaryOpType.LE, expr, other.expr));
    }

    public <U> ExprT<Boolean> gt(final ExprT<U> other) {
        return new ExprT<>(new Expr.BinaryOp(BinaryOpType.GT, expr, other.expr));
    }

    public <U> ExprT<Boolean> ge(final ExprT<U> other) {
        return new ExprT<>(new Expr.BinaryOp(BinaryOpType.GE, expr, other.expr));
    }

    // --- Boolean ops (other side strict-Boolean, receiver loose) ---

    public ExprT<Boolean> and(final ExprT<Boolean> other) {
        return new ExprT<>(new Expr.BinaryOp(BinaryOpType.AND, expr, other.expr));
    }

    public ExprT<Boolean> or(final ExprT<Boolean> other) {
        return new ExprT<>(new Expr.BinaryOp(BinaryOpType.OR, expr, other.expr));
    }

    public ExprT<Boolean> not() {
        return new ExprT<>(new Expr.UnaryOp(UnaryOpType.NOT, expr));
    }

    // --- Arithmetic (strict: both sides must be the same T) ---

    public ExprT<T> add(final ExprT<T> other) {
        return new ExprT<>(new Expr.BinaryOp(BinaryOpType.ADD, expr, other.expr));
    }

    public ExprT<T> sub(final ExprT<T> other) {
        return new ExprT<>(new Expr.BinaryOp(BinaryOpType.SUB, expr, other.expr));
    }

    public ExprT<T> mul(final ExprT<T> other) {
        return new ExprT<>(new Expr.BinaryOp(BinaryOpType.MUL, expr, other.expr));
    }

    public ExprT<T> div(final ExprT<T> other) {
        return new ExprT<>(new Expr.BinaryOp(BinaryOpType.DIV, expr, other.expr));
    }

    // --- Chain / accessor (untyped + typed overloads) ---

    public ExprT<Object> field(final String name) {
        return new ExprT<>(new Expr.FieldAccess(expr, name));
    }

    public <U> ExprT<U> field(final String name, final Class<U> type) {
        return new ExprT<>(new Expr.FieldAccess(expr, name));
    }

    public ExprT<Object> arrow(final String name) {
        return new ExprT<>(new Expr.ArrowOp(expr, name));
    }

    public <U> ExprT<U> arrow(final String name, final Class<U> type) {
        return new ExprT<>(new Expr.ArrowOp(expr, name));
    }

    public ExprT<Object> at(final ExprT<Long> index) {
        return new ExprT<>(new Expr.ArrayIndex(expr, index.expr));
    }

    public <U> ExprT<U> at(final ExprT<Long> index, final Class<U> type) {
        return new ExprT<>(new Expr.ArrayIndex(expr, index.expr));
    }

    public ExprT<Object> unwind() {
        return new ExprT<>(new Expr.UnwindExpr(expr));
    }

    public <E> ExprT<E> unwind(final Class<E> elementType) {
        return new ExprT<>(new Expr.UnwindExpr(expr));
    }

    public ExprT<Boolean> any(final ExprT<Boolean> predicate) {
        return new ExprT<>(new Expr.Any(expr, predicate.expr));
    }
}
