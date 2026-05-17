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
 * Stage node taxonomy. Mirrors every constructor of PipelineParseStage in
 * src/third_party/mqlv2/src/MQLv2/ParseTree.hs, plus the two ParsedFrom variants exposed
 * as distinct stage nodes (FromStageSimple / FromStageNested).
 */
public sealed interface Stage {

    // ---- Data sources ----

    record FromStageSimple(Expr source) implements Stage {}

    record FromStageNested(List<Map.Entry<String, Expr>> sources) implements Stage {}

    // ---- Core transforms ----

    record MatchStage(Stage prev, Expr predicate) implements Stage {}

    record FormatStage(Stage prev, Expr expr) implements Stage {}

    record AggStage(Stage prev, Expr expr) implements Stage {}

    record ProjectStage(Stage prev, FieldPathTree tree) implements Stage {}

    record LimitStage(Stage prev, Expr count) implements Stage {}

    record SortStage(Stage prev, List<SortSpec> specs) implements Stage {}

    record GroupStage(Stage prev, List<Assignment> groupKeys, List<Assignment> aggKeys) implements Stage {}

    record SetStage(Stage prev, List<Assignment> assignments) implements Stage {}

    record UnsetStage(Stage prev, FieldPathTree tree) implements Stage {}

    record DistinctStage(Stage prev) implements Stage {}

    record CountStage(Stage prev) implements Stage {}

    // ---- Unwind ----

    record UnwindSimpleStage(Stage prev, Expr expr) implements Stage {}

    record UnwindComplexStage(Stage prev, String varName, Expr sourceExpr, Expr bodyExpr) implements Stage {}

    // ---- Join (joinType is optional per the grammar; null means InnerJoin) ----

    record JoinStage(Stage prev, JoinType joinType, String varName, Expr source, Expr condition) implements Stage {}
}
