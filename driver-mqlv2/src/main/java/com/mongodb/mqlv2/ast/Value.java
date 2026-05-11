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
 * Literal values carried inside ValueLit nodes. Mirrors the Value type variants from
 * src/third_party/mqlv2/src/MQLv2/DataModel.hs.
 *
 * <p>VDocument and VSequence are present for completeness but are typically constructed at the
 * AST level via DocumentConstructor / BagConstructor / ArrayConstructor expression nodes
 * rather than as ValueLits.</p>
 */
public sealed interface Value {
    record VNull() implements Value {}
    record VMissing() implements Value {}
    record VUndefined() implements Value {}
    record VBool(boolean value) implements Value {}
    record VInt(long value) implements Value {}
    record VDouble(double value) implements Value {}
    record VString(String value) implements Value {}
    record VDate(long millisSinceEpoch) implements Value {}
    record VDocument(Map<String, Value> fields) implements Value {}
    record VSequence(List<Value> elements, boolean ordered) implements Value {}
}
