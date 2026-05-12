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
package com.mongodb.mqlv2.facade.subtyped;

import com.mongodb.mqlv2.ast.BinaryOpType;
import com.mongodb.mqlv2.ast.Expr;

/**
 * Root of the subtyped facade hierarchy. Carries a single {@link Expr} payload via
 * {@link #toExpr()}. Comparisons are declared here because MQLv2 cross-type equality is
 * always legal (returns false on type mismatch). The {@code asXxx} methods are pure
 * compile-time refinements — they rewrap the same {@link Expr} in a wrapper of a narrower
 * interface type and do not change the underlying AST.
 */
public sealed interface ExprT
        permits NumExprT, BoolExprT, StrExprT, DateExprT, DocExprT, ArrExprT, ExprImpl {

    Expr toExpr();

    // --- Comparisons (parametric on the other side, return Boolean) ---

    default BoolExprT eq(final ExprT other) {
        return new ExprImpl<>(new Expr.BinaryOp(BinaryOpType.EQ, toExpr(), other.toExpr()));
    }

    default BoolExprT ne(final ExprT other) {
        return new ExprImpl<>(new Expr.BinaryOp(BinaryOpType.NE, toExpr(), other.toExpr()));
    }

    default BoolExprT is(final ExprT other) {
        return new ExprImpl<>(new Expr.BinaryOp(BinaryOpType.IS, toExpr(), other.toExpr()));
    }

    default BoolExprT lt(final ExprT other) {
        return new ExprImpl<>(new Expr.BinaryOp(BinaryOpType.LT, toExpr(), other.toExpr()));
    }

    default BoolExprT le(final ExprT other) {
        return new ExprImpl<>(new Expr.BinaryOp(BinaryOpType.LE, toExpr(), other.toExpr()));
    }

    default BoolExprT gt(final ExprT other) {
        return new ExprImpl<>(new Expr.BinaryOp(BinaryOpType.GT, toExpr(), other.toExpr()));
    }

    default BoolExprT ge(final ExprT other) {
        return new ExprImpl<>(new Expr.BinaryOp(BinaryOpType.GE, toExpr(), other.toExpr()));
    }

    // --- Pure-cast refiners (no AST change) ---

    default NumExprT asNum() {
        return new ExprImpl<>(toExpr());
    }

    default IntExprT asInt() {
        return new ExprImpl<>(toExpr());
    }

    default StrExprT asStr() {
        return new ExprImpl<>(toExpr());
    }

    default BoolExprT asBool() {
        return new ExprImpl<>(toExpr());
    }

    default DateExprT asDate() {
        return new ExprImpl<>(toExpr());
    }

    default DocExprT asDoc() {
        return new ExprImpl<>(toExpr());
    }

    default <E extends ExprT> ArrExprT<E> asArr() {
        return new ExprImpl<>(toExpr());
    }

    // --- Arrow path-walk (permissive: legal on any expression in MQLv2) ---

    default ExprT arrow(final String name) {
        return new ExprImpl<>(new Expr.ArrowOp(toExpr(), name));
    }

    default IntExprT intArrow(final String name) {
        return new ExprImpl<>(new Expr.ArrowOp(toExpr(), name));
    }

    default NumExprT numArrow(final String name) {
        return new ExprImpl<>(new Expr.ArrowOp(toExpr(), name));
    }

    default StrExprT strArrow(final String name) {
        return new ExprImpl<>(new Expr.ArrowOp(toExpr(), name));
    }

    default BoolExprT boolArrow(final String name) {
        return new ExprImpl<>(new Expr.ArrowOp(toExpr(), name));
    }

    default DateExprT dateArrow(final String name) {
        return new ExprImpl<>(new Expr.ArrowOp(toExpr(), name));
    }

    default DocExprT docArrow(final String name) {
        return new ExprImpl<>(new Expr.ArrowOp(toExpr(), name));
    }

    default ArrExprT<ExprT> arrArrow(final String name) {
        return new ExprImpl<>(new Expr.ArrowOp(toExpr(), name));
    }
}
