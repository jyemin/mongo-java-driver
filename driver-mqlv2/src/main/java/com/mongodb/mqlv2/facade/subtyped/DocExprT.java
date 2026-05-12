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

import com.mongodb.mqlv2.ast.Expr;

/**
 * Document expression. Provides typed field accessor methods (no {@code Class} witness).
 * The {@code field} method returns base {@link ExprT}; the typed variants
 * ({@code intField}, {@code strField}, ...) commit to a specific subtype.
 * Arrow accessors live on base {@link ExprT}.
 */
public sealed interface DocExprT extends ExprT permits ExprImpl {

    default ExprT field(final String name) {
        return new ExprImpl<>(new Expr.FieldAccess(toExpr(), name));
    }

    default IntExprT intField(final String name) {
        return new ExprImpl<>(new Expr.FieldAccess(toExpr(), name));
    }

    default NumExprT numField(final String name) {
        return new ExprImpl<>(new Expr.FieldAccess(toExpr(), name));
    }

    default StrExprT strField(final String name) {
        return new ExprImpl<>(new Expr.FieldAccess(toExpr(), name));
    }

    default BoolExprT boolField(final String name) {
        return new ExprImpl<>(new Expr.FieldAccess(toExpr(), name));
    }

    default DateExprT dateField(final String name) {
        return new ExprImpl<>(new Expr.FieldAccess(toExpr(), name));
    }

    default DocExprT docField(final String name) {
        return new ExprImpl<>(new Expr.FieldAccess(toExpr(), name));
    }

    default ArrExprT<ExprT> arrField(final String name) {
        return new ExprImpl<>(new Expr.FieldAccess(toExpr(), name));
    }
}
