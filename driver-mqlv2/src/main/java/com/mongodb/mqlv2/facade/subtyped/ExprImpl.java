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
 * Single package-private concrete record that implements every facade interface. The record
 * component {@code toExpr} doubles as the {@link ExprT#toExpr()} accessor. Factories return
 * this typed as the narrowest applicable interface; all behavior is inherited from default
 * methods on the interfaces themselves.
 *
 * @param toExpr underlying AST node.
 * @param <E>    element-type binding for {@link ArrExprT} (ignored by all other interfaces).
 */
record ExprImpl<E extends ExprT>(Expr toExpr)
        implements ExprT, NumExprT, IntExprT, BoolExprT, StrExprT, DateExprT, DocExprT, ArrExprT<E> {
}
