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
 * Fluent stage chain for the subtyped facade. Method-for-method parallel of
 * {@link com.mongodb.mqlv2.facade.typed.PipelineBuilderT}, accepting subtyped
 * {@link ExprT} values rather than phantom-typed {@code ExprT<?>} values.
 */
public final class PipelineBuilderS implements Mqlv2Source {

    private final Stage stage;

    PipelineBuilderS(final Stage stage) {
        this.stage = stage;
    }

    public Stage stage() {
        return stage;
    }

    public PipelineBuilderS match(final BoolExprT predicate) {
        return new PipelineBuilderS(new Stage.MatchStage(stage, predicate.toExpr()));
    }

    public PipelineBuilderS format(final ExprT expr) {
        return new PipelineBuilderS(new Stage.FormatStage(stage, expr.toExpr()));
    }

    public PipelineBuilderS agg(final ExprT expr) {
        return new PipelineBuilderS(new Stage.AggStage(stage, expr.toExpr()));
    }

    public PipelineBuilderS limit(final ExprT expr) {
        return new PipelineBuilderS(new Stage.LimitStage(stage, expr.toExpr()));
    }

    public PipelineBuilderS sort(final SortSpec... specs) {
        return new PipelineBuilderS(new Stage.SortStage(stage, List.of(specs)));
    }

    public PipelineBuilderS distinct() {
        return new PipelineBuilderS(new Stage.DistinctStage(stage));
    }

    public PipelineBuilderS count() {
        return new PipelineBuilderS(new Stage.CountStage(stage));
    }

    public PipelineBuilderS project(final String... dotPaths) {
        return new PipelineBuilderS(new Stage.ProjectStage(stage, buildTree(dotPaths)));
    }

    public PipelineBuilderS unset(final String... dotPaths) {
        return new PipelineBuilderS(new Stage.UnsetStage(stage, buildTree(dotPaths)));
    }

    public PipelineBuilderS set(final Assignment... assignments) {
        return new PipelineBuilderS(new Stage.SetStage(stage, List.of(assignments)));
    }

    public PipelineBuilderS group(final List<Assignment> groupKeys, final List<Assignment> aggKeys) {
        return new PipelineBuilderS(new Stage.GroupStage(stage, groupKeys, aggKeys));
    }

    public PipelineBuilderS unwind(final ExprT expr) {
        return new PipelineBuilderS(new Stage.UnwindSimpleStage(stage, expr.toExpr()));
    }

    public PipelineBuilderS unwindAs(final String varName, final ExprT sourceExpr, final ExprT bodyExpr) {
        return new PipelineBuilderS(new Stage.UnwindComplexStage(
                stage, varName, sourceExpr.toExpr(), bodyExpr.toExpr()));
    }

    /**
     * Join stage with optional join type.
     *
     * @param joinType  the join type, or {@code null} for default inner.
     * @param varName   the binding name introduced by the join.
     * @param source    the right-hand source expression.
     * @param condition the join predicate.
     * @return new {@code PipelineBuilderS} wrapping the join stage.
     */
    public PipelineBuilderS join(final JoinType joinType, final String varName,
                                 final ExprT source, final BoolExprT condition) {
        return new PipelineBuilderS(new Stage.JoinStage(
                stage, joinType, varName, source.toExpr(), condition.toExpr()));
    }

    @Override
    public String toMqlv2() {
        return new Serializer().serialize(stage);
    }

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
