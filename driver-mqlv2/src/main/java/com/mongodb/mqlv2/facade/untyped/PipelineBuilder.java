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
package com.mongodb.mqlv2.facade.untyped;

import com.mongodb.client.model.Mqlv2Source;
import com.mongodb.mqlv2.Serializer;
import com.mongodb.mqlv2.ast.Assignment;
import com.mongodb.mqlv2.ast.FieldPathTree;
import com.mongodb.mqlv2.ast.JoinType;
import com.mongodb.mqlv2.ast.SortSpec;
import com.mongodb.mqlv2.ast.Stage;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Fluent wrapper around a {@link Stage}. Every stage method returns a new {@code PipelineBuilder}
 * with the new stage layered on top, so the chain remains immutable and serialisable at any point.
 *
 * <p>Implements {@link Mqlv2Source} so an in-progress pipeline can be passed directly to
 * {@code MongoDatabase.mqlv2(Mqlv2Source)}.</p>
 */
public final class PipelineBuilder implements Mqlv2Source {

    private final Stage stage;

    PipelineBuilder(final Stage stage) {
        this.stage = stage;
    }

    public Stage stage() {
        return stage;
    }

    // --- Core transforms ---

    public PipelineBuilder match(final ExprU predicate) {
        return new PipelineBuilder(new Stage.MatchStage(stage, predicate.toExpr()));
    }

    public PipelineBuilder format(final ExprU expr) {
        return new PipelineBuilder(new Stage.FormatStage(stage, expr.toExpr()));
    }

    public PipelineBuilder agg(final ExprU expr) {
        return new PipelineBuilder(new Stage.AggStage(stage, expr.toExpr()));
    }

    public PipelineBuilder limit(final int count) {
        return new PipelineBuilder(new Stage.LimitStage(stage, count));
    }

    public PipelineBuilder sort(final SortSpec... specs) {
        return new PipelineBuilder(new Stage.SortStage(stage, List.of(specs)));
    }

    public PipelineBuilder distinct() {
        return new PipelineBuilder(new Stage.DistinctStage(stage));
    }

    public PipelineBuilder count() {
        return new PipelineBuilder(new Stage.CountStage(stage));
    }

    // --- Project / unset (FieldPathTree from dot-paths) ---

    public PipelineBuilder project(final String... dotPaths) {
        return new PipelineBuilder(new Stage.ProjectStage(stage, buildTree(dotPaths)));
    }

    public PipelineBuilder unset(final String... dotPaths) {
        return new PipelineBuilder(new Stage.UnsetStage(stage, buildTree(dotPaths)));
    }

    // --- Set / group ---

    public PipelineBuilder set(final Assignment... assignments) {
        return new PipelineBuilder(new Stage.SetStage(stage, List.of(assignments)));
    }

    public PipelineBuilder group(final List<Assignment> groupKeys, final List<Assignment> aggKeys) {
        return new PipelineBuilder(new Stage.GroupStage(stage, groupKeys, aggKeys));
    }

    // --- Unwind ---

    public PipelineBuilder unwind(final ExprU expr) {
        return new PipelineBuilder(new Stage.UnwindSimpleStage(stage, expr.toExpr()));
    }

    public PipelineBuilder unwindAs(final String varName, final ExprU sourceExpr, final ExprU bodyExpr) {
        return new PipelineBuilder(new Stage.UnwindComplexStage(
                stage, varName, sourceExpr.toExpr(), bodyExpr.toExpr()));
    }

    // --- Join ---

    /**
     * Join stage with optional join type.
     *
     * @param joinType  the join type, or {@code null} for default inner.
     * @param varName   the binding name introduced by the join.
     * @param source    the right-hand source expression.
     * @param condition the join predicate (evaluates against the cross-product current value).
     * @return new {@code PipelineBuilder} wrapping the join stage.
     */
    public PipelineBuilder join(final JoinType joinType, final String varName,
                                final ExprU source, final ExprU condition) {
        return new PipelineBuilder(new Stage.JoinStage(
                stage, joinType, varName, source.toExpr(), condition.toExpr()));
    }

    // --- Mqlv2Source ---

    @Override
    public String toMqlv2() {
        return new Serializer().serialize(stage);
    }

    // --- FieldPathTree construction from dot-paths ---

    static FieldPathTree buildTree(final String[] dotPaths) {
        List<Map.Entry<String, FieldPathTree>> children = new ArrayList<>();
        for (String path : dotPaths) {
            String[] segs = path.split("\\.");
            insert(children, segs, 0);
        }
        return new FieldPathTree.Interior(children);
    }

    private static void insert(final List<Map.Entry<String, FieldPathTree>> children,
                               final String[] segs, final int idx) {
        String head = segs[idx];
        boolean last = idx == segs.length - 1;
        for (int i = 0; i < children.size(); i++) {
            Map.Entry<String, FieldPathTree> existing = children.get(i);
            if (existing.getKey().equals(head)) {
                if (last) {
                    return;
                }
                FieldPathTree subtree = existing.getValue();
                if (subtree instanceof FieldPathTree.Leaf) {
                    List<Map.Entry<String, FieldPathTree>> grand = new ArrayList<>();
                    insert(grand, segs, idx + 1);
                    children.set(i, new AbstractMap.SimpleImmutableEntry<>(head,
                            new FieldPathTree.Interior(grand)));
                } else {
                    FieldPathTree.Interior interior = (FieldPathTree.Interior) subtree;
                    List<Map.Entry<String, FieldPathTree>> grand = new ArrayList<>(interior.children());
                    insert(grand, segs, idx + 1);
                    children.set(i, new AbstractMap.SimpleImmutableEntry<>(head,
                            new FieldPathTree.Interior(grand)));
                }
                return;
            }
        }
        if (last) {
            children.add(new AbstractMap.SimpleImmutableEntry<>(head, new FieldPathTree.Leaf(null)));
        } else {
            List<Map.Entry<String, FieldPathTree>> grand = new ArrayList<>();
            insert(grand, segs, idx + 1);
            children.add(new AbstractMap.SimpleImmutableEntry<>(head, new FieldPathTree.Interior(grand)));
        }
    }
}
