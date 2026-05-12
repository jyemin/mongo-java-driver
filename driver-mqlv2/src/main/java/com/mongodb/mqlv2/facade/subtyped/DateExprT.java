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

import java.util.List;

/** Date expression. Date-component extractors all return {@link IntExprT}. */
public sealed interface DateExprT extends ExprT permits ExprImpl {

    default IntExprT year() {
        return fn("year");
    }

    default IntExprT month() {
        return fn("month");
    }

    default IntExprT dayOfMonth() {
        return fn("dayOfMonth");
    }

    default IntExprT dayOfYear() {
        return fn("dayOfYear");
    }

    default IntExprT dayOfWeek() {
        return fn("dayOfWeek");
    }

    default IntExprT hour() {
        return fn("hour");
    }

    default IntExprT minute() {
        return fn("minute");
    }

    default IntExprT second() {
        return fn("second");
    }

    default IntExprT millisecond() {
        return fn("millisecond");
    }

    private IntExprT fn(final String name) {
        return new ExprImpl<>(new Expr.FunctionCall(name, List.of(toExpr())));
    }
}
