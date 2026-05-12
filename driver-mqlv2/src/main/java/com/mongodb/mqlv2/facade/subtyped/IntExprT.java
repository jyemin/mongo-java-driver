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
 * Integer expression. Arithmetic overloads stay in {@link IntExprT} when both sides are
 * statically integer; mixed operations widen to {@link NumExprT} via the inherited methods.
 */
public sealed interface IntExprT extends NumExprT permits ExprImpl {

    default IntExprT add(final IntExprT other) {
        return new ExprImpl<>(new Expr.BinaryOp(BinaryOpType.ADD, toExpr(), other.toExpr()));
    }

    default IntExprT sub(final IntExprT other) {
        return new ExprImpl<>(new Expr.BinaryOp(BinaryOpType.SUB, toExpr(), other.toExpr()));
    }

    default IntExprT mul(final IntExprT other) {
        return new ExprImpl<>(new Expr.BinaryOp(BinaryOpType.MUL, toExpr(), other.toExpr()));
    }

    default IntExprT div(final IntExprT other) {
        return new ExprImpl<>(new Expr.BinaryOp(BinaryOpType.DIV, toExpr(), other.toExpr()));
    }
}
