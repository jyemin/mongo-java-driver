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
 * Array expression with statically-known element type {@code E}.
 *
 * @param <E> element type.
 */
public sealed interface ArrExprT<E extends ExprT> extends ExprT permits ExprImpl {

    default E elementAt(final IntExprT index) {
        @SuppressWarnings("unchecked")
        E result = (E) new ExprImpl<>(new Expr.ArrayIndex(toExpr(), index.toExpr()));
        return result;
    }

    default E unwind() {
        @SuppressWarnings("unchecked")
        E result = (E) new ExprImpl<>(new Expr.UnwindExpr(toExpr()));
        return result;
    }

    default BoolExprT any(final BoolExprT predicate) {
        return new ExprImpl<>(new Expr.Any(toExpr(), predicate.toExpr()));
    }
}
