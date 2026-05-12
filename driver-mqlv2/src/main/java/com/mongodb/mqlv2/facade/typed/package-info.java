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

/**
 * Phantom-typed fluent facade for constructing MQLv2 pipelines. Expressions carry a Java type
 * parameter ({@link com.mongodb.mqlv2.facade.typed.ExprT}); the pipeline builder
 * ({@link com.mongodb.mqlv2.facade.typed.PipelineBuilderT}) requires {@code ExprT<Boolean>}
 * for {@code match()} predicates so wrong-type predicates fail at compile time.
 *
 * <p>Type-system policy summary:</p>
 * <ul>
 *   <li><b>Comparisons</b> ({@code eq}, {@code ne}, {@code is}, {@code lt}, {@code le},
 *       {@code gt}, {@code ge}) are parametric on the other side and return {@code ExprT<Boolean>}.
 *       Comparing mismatched types is permitted by the facade and produces a runtime answer.</li>
 *   <li><b>Boolean ops</b> ({@code and}, {@code or}) require the other side to be
 *       {@code ExprT<Boolean>}; the receiver's {@code T} is unconstrained.</li>
 *   <li><b>Arithmetic</b> ({@code add}, {@code sub}, {@code mul}, {@code div}) is strict:
 *       both sides must be the same {@code T}. This forces explicit typing for arithmetic
 *       expressions, catching unintentional cross-type adds at compile time.</li>
 *   <li><b>Field / arrow / index / current / var</b> have untyped overloads returning
 *       {@code ExprT<Object>}, and typed overloads taking a {@code Class<T>} witness returning
 *       {@code ExprT<T>}.</li>
 *   <li><b>Function calls</b> are typed per-function (e.g. {@code count(any)} → {@code Long},
 *       {@code sum(T)} → {@code T}, {@code avg(any)} → {@code Double}).</li>
 * </ul>
 */
package com.mongodb.mqlv2.facade.typed;
