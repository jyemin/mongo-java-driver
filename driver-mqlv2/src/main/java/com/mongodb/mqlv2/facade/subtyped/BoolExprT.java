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
import com.mongodb.mqlv2.ast.UnaryOpType;

/** Boolean expression. Gates {@code and}/{@code or}/{@code not}. */
public sealed interface BoolExprT extends ExprT permits ExprImpl {

    default BoolExprT and(final BoolExprT other) {
        return new ExprImpl<>(new Expr.BinaryOp(BinaryOpType.AND, toExpr(), other.toExpr()));
    }

    default BoolExprT or(final BoolExprT other) {
        return new ExprImpl<>(new Expr.BinaryOp(BinaryOpType.OR, toExpr(), other.toExpr()));
    }

    default BoolExprT not() {
        return new ExprImpl<>(new Expr.UnaryOp(UnaryOpType.NOT, toExpr()));
    }
}
