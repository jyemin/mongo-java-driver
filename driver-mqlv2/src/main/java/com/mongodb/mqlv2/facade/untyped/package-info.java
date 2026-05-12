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
 * Untyped fluent facade for constructing MQLv2 pipelines. The primary entry points are
 * {@link com.mongodb.mqlv2.facade.untyped.Untyped} (static factory methods, typically
 * star-imported) and the returned {@link com.mongodb.mqlv2.facade.untyped.PipelineBuilder}
 * (which implements {@link com.mongodb.client.model.Mqlv2Source}).
 *
 * <p>Expressions are wrapped in {@link com.mongodb.mqlv2.facade.untyped.ExprU} so that fluent
 * operator methods chain naturally. There is no compile-time typing of expressions — all
 * {@code ExprU}s look alike to the compiler. Mistakes surface at server-execution time.</p>
 */
package com.mongodb.mqlv2.facade.untyped;
