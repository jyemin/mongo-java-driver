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
package com.mongodb.mqlv2.ast;

import java.util.List;
import java.util.Map;

/**
 * Expression node taxonomy. Mirrors every constructor of ParseExpr in
 * src/third_party/mqlv2/src/MQLv2/ParseTree.hs, renamed neutrally
 * (the P-prefix and Haskell-isms are stripped).
 */
public sealed interface Expr {

    record ValueLit(Value value) implements Expr {}

    record CurrentValue() implements Expr {}

    record VarRef(String name) implements Expr {}

    record BinaryOp(BinaryOpType op, Expr left, Expr right) implements Expr {}

    record UnaryOp(UnaryOpType op, Expr arg) implements Expr {}

    record FieldAccess(Expr target, String field) implements Expr {}

    record ArrowOp(Expr target, String field) implements Expr {}

    record ArrayIndex(Expr array, Expr index) implements Expr {}

    record UnwindExpr(Expr arg) implements Expr {}

    record BagConstructor(List<Expr> elements) implements Expr {}

    record ArrayConstructor(List<Expr> elements) implements Expr {}

    record DocumentConstructor(List<Map.Entry<Expr, Expr>> fields) implements Expr {}

    record Any(Expr sequence, Expr predicate) implements Expr {}

    /**
     * Untyped function call. The function name is validated against the registry at construction
     * time by the builder layer (in a future facade plan), not here.
     *
     * @param name the function name (must appear in the function registry).
     * @param args the arguments to the function.
     */
    record FunctionCall(String name, List<Expr> args) implements Expr {}

    record SubPipelineExpr(Stage pipeline) implements Expr {}

    record LetExpr(List<Map.Entry<String, Expr>> bindings, Expr body) implements Expr {}
}
