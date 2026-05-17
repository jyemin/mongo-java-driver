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
package com.mongodb.mqlv2;

import com.mongodb.mqlv2.ast.Assignment;
import com.mongodb.mqlv2.ast.BinaryOpType;
import com.mongodb.mqlv2.ast.Expr;
import com.mongodb.mqlv2.ast.FieldPathTree;
import com.mongodb.mqlv2.ast.SortDirection;
import com.mongodb.mqlv2.ast.SortSpec;
import com.mongodb.mqlv2.ast.Stage;
import com.mongodb.mqlv2.ast.Value;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SerializerTest {

    private final Serializer s = new Serializer();

    @Test
    void simpleMatch() {
        // from <<1, 2, 3>> | match $ == 2
        Stage ast =
                new Stage.MatchStage(
                        new Stage.FromStageSimple(new Expr.BagConstructor(List.of(
                                new Expr.ValueLit(new Value.VInt(1)),
                                new Expr.ValueLit(new Value.VInt(2)),
                                new Expr.ValueLit(new Value.VInt(3))))),
                        new Expr.BinaryOp(BinaryOpType.EQ,
                                new Expr.CurrentValue(),
                                new Expr.ValueLit(new Value.VInt(2))));
        assertEquals("from <<1, 2, 3>> | match ($ == 2)", s.serialize(ast));
    }

    @Test
    void fieldAccessBareIdent() {
        Expr fa = new Expr.FieldAccess(new Expr.CurrentValue(), "a");
        Stage ast =
                new Stage.MatchStage(
                        new Stage.FromStageSimple(new Expr.BagConstructor(List.of())),
                        new Expr.BinaryOp(BinaryOpType.EQ, fa, new Expr.ValueLit(new Value.VInt(2))));
        assertEquals("from <<>> | match (a == 2)", s.serialize(ast));
    }

    @Test
    void sortAscOmittedDescExplicit() {
        Stage ast = new Stage.SortStage(
                new Stage.FromStageSimple(new Expr.BagConstructor(List.of())),
                List.of(
                        new SortSpec(new Expr.FieldAccess(new Expr.CurrentValue(), "a"), SortDirection.DESC),
                        new SortSpec(new Expr.FieldAccess(new Expr.CurrentValue(), "b"), SortDirection.ASC)));
        assertEquals("from <<>> | sort a desc, b", s.serialize(ast));
    }

    @Test
    void projectBranching() {
        FieldPathTree tree = new FieldPathTree.Interior(List.of(
                Map.entry("a", new FieldPathTree.Leaf(null)),
                Map.entry("x", new FieldPathTree.Interior(List.of(
                        Map.entry("y", new FieldPathTree.Leaf(null)),
                        Map.entry("z", new FieldPathTree.Leaf(null))))),
                Map.entry("b", new FieldPathTree.Leaf(null))));
        Stage ast = new Stage.ProjectStage(
                new Stage.FromStageSimple(new Expr.BagConstructor(List.of())),
                tree);
        assertEquals("from <<>> | project a, x.{y, z}, b", s.serialize(ast));
    }

    @Test
    void unwindComplex() {
        Expr aStar = new Expr.UnwindExpr(new Expr.FieldAccess(new Expr.CurrentValue(), "a"));
        Expr body = new Expr.DocumentConstructor(List.of(
                Map.entry(new Expr.ValueLit(new Value.VString("idx")), new Expr.VarRef("i"))));
        Stage ast = new Stage.UnwindComplexStage(
                new Stage.FromStageSimple(new Expr.BagConstructor(List.of())),
                "i", aStar, body);
        assertEquals("from <<>> | unwind $i=a* in {idx: $i}", s.serialize(ast));
    }

    @Test
    void groupWithSumArrow() {
        Expr a = new Expr.FieldAccess(new Expr.CurrentValue(), "a");
        Expr sumExpr = new Expr.FunctionCall("sum", List.of(
                new Expr.ArrowOp(new Expr.CurrentValue(), "b")));
        Stage ast = new Stage.GroupStage(
                new Stage.FromStageSimple(new Expr.BagConstructor(List.of(
                        new Expr.DocumentConstructor(List.of(
                                Map.entry(new Expr.ValueLit(new Value.VString("a")), new Expr.ValueLit(new Value.VInt(1))),
                                Map.entry(new Expr.ValueLit(new Value.VString("b")), new Expr.ValueLit(new Value.VInt(2)))))))),
                List.of(new Assignment(List.of("k"), a)),
                List.of(new Assignment(List.of("s"), sumExpr)));
        assertEquals(
                "from <<{a: 1, b: 2}>> | group (k = a) (s = sum($->b))",
                s.serialize(ast));
    }

    @Test
    void serializeExprStandalone() {
        Serializer s = new Serializer();
        Expr e = new Expr.BinaryOp(BinaryOpType.EQ,
                new Expr.FieldAccess(new Expr.CurrentValue(), "sku"),
                new Expr.ValueLit(new Value.VString("WIDGET-1")));
        assertEquals("(sku == \"WIDGET-1\")", s.serialize(e));
    }
}
