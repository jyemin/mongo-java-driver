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
 * Recursive trie used by project / unset / set / group stages. Interior nodes carry an ordered
 * list of (segment-name, subtree) pairs preserving insertion order. Leaves carry an optional
 * value expression — present for set/group, absent (null) for project/unset.
 */
public sealed interface FieldPathTree {

    record Interior(List<Map.Entry<String, FieldPathTree>> children) implements FieldPathTree {}

    /**
     * @param value the value expression for set/group, or {@code null} for project/unset leaves.
     */
    record Leaf(Expr value) implements FieldPathTree {}
}
