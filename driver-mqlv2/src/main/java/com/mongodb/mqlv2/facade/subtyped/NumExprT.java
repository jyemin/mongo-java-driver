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
 * Numeric expression. Sub-interface of {@link ExprT} that gates arithmetic.
 * {@link IntExprT} narrows further so that {@code Int op Int} stays {@code Int} via
 * covariant-return overloads, while mixed {@code Int op Num} widens to {@code Num}.
 */
public sealed interface NumExprT extends ExprT permits IntExprT, ExprImpl {

    default NumExprT add(final NumExprT other) {
        return new ExprImpl<>(new Expr.BinaryOp(BinaryOpType.ADD, toExpr(), other.toExpr()));
    }

    default NumExprT sub(final NumExprT other) {
        return new ExprImpl<>(new Expr.BinaryOp(BinaryOpType.SUB, toExpr(), other.toExpr()));
    }

    default NumExprT mul(final NumExprT other) {
        return new ExprImpl<>(new Expr.BinaryOp(BinaryOpType.MUL, toExpr(), other.toExpr()));
    }

    default NumExprT div(final NumExprT other) {
        return new ExprImpl<>(new Expr.BinaryOp(BinaryOpType.DIV, toExpr(), other.toExpr()));
    }
}
