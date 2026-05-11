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

import com.mongodb.client.Fixture;
import com.mongodb.client.MongoDatabase;
import com.mongodb.mqlv2.ast.Assignment;
import com.mongodb.mqlv2.ast.BinaryOpType;
import com.mongodb.mqlv2.ast.Expr;
import com.mongodb.mqlv2.ast.FieldPathTree;
import com.mongodb.mqlv2.ast.JoinType;
import com.mongodb.mqlv2.ast.SortDirection;
import com.mongodb.mqlv2.ast.SortSpec;
import com.mongodb.mqlv2.ast.Stage;
import com.mongodb.mqlv2.ast.UnaryOpType;
import com.mongodb.mqlv2.ast.Value;
import org.bson.BsonDocument;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

class Mqlv2ConformanceTest {

    private final MongoDatabase db = Fixture.getDefaultDatabase();

    // --- AST helpers ---

    private static Expr lit(final long n) {
        return new Expr.ValueLit(new Value.VInt(n));
    }
    private static Expr lit(final String s) {
        return new Expr.ValueLit(new Value.VString(s));
    }
    private static Expr field(final String name) {
        return new Expr.FieldAccess(new Expr.CurrentValue(), name);
    }
    private static Expr eq(final Expr l, final Expr r) {
        return new Expr.BinaryOp(BinaryOpType.EQ, l, r);
    }
    private static Expr add(final Expr l, final Expr r) {
        return new Expr.BinaryOp(BinaryOpType.ADD, l, r);
    }
    private static Expr mul(final Expr l, final Expr r) {
        return new Expr.BinaryOp(BinaryOpType.MUL, l, r);
    }
    private static Expr bag(final Expr... es) {
        return new Expr.BagConstructor(List.of(es));
    }
    @SafeVarargs
    private static Expr doc(final Map.Entry<String, Expr>... fields) {
        List<Map.Entry<Expr, Expr>> entries = new ArrayList<>();
        for (Map.Entry<String, Expr> f : fields) {
            entries.add(Map.entry(lit(f.getKey()), f.getValue()));
        }
        return new Expr.DocumentConstructor(entries);
    }
    private static Map.Entry<String, Expr> kv(final String k, final Expr v) {
        return Map.entry(k, v);
    }

    // --- Expected helpers ---

    private static BsonDocument bd(final String json) {
        return BsonDocument.parse(json);
    }
    private static Set<BsonDocument> bset(final BsonDocument... docs) {
        return new HashSet<>(Arrays.asList(docs));
    }
    private static Set<BsonDocument> asSet(final List<BsonDocument> docs) {
        return new HashSet<>(docs);
    }

    private List<BsonDocument> run(final Stage ast) {
        return db.mqlv2(new Pipeline(ast), BsonDocument.class).into(new ArrayList<>());
    }

    @Test
    void matchSimple() {
        Stage ast = new Stage.MatchStage(
                new Stage.FromStageSimple(bag(lit(1), lit(2), lit(3))),
                eq(new Expr.CurrentValue(), lit(2)));
        assertEquals(List.of(bd("{\"value\": 2}")), run(ast));
    }

    @Test
    void formatWithDocAndMul() {
        Stage ast = new Stage.FormatStage(
                new Stage.FromStageSimple(bag(doc(kv("a", lit(1))), doc(kv("a", lit(2))))),
                doc(kv("doubled", mul(field("a"), lit(2)))));
        assertEquals(
                bset(bd("{\"doubled\": 2}"), bd("{\"doubled\": 4}")),
                asSet(run(ast)));
    }

    @Test
    void sortDescThenAsc() {
        Stage ast = new Stage.SortStage(
                new Stage.FromStageSimple(bag(
                        doc(kv("a", lit(3)), kv("b", lit(2))),
                        doc(kv("a", lit(1)), kv("b", lit(7))),
                        doc(kv("a", lit(5)), kv("b", lit(2))),
                        doc(kv("a", lit(5)), kv("b", lit(1))))),
                List.of(
                        new SortSpec(field("a"), SortDirection.DESC),
                        new SortSpec(field("b"), SortDirection.ASC)));
        assertEquals(
                List.of(
                        bd("{\"a\": 5, \"b\": 1}"),
                        bd("{\"a\": 5, \"b\": 2}"),
                        bd("{\"a\": 3, \"b\": 2}"),
                        bd("{\"a\": 1, \"b\": 7}")),
                run(ast));
    }

    @Test
    void limit() {
        Stage ast = new Stage.LimitStage(
                new Stage.SortStage(
                        new Stage.FromStageSimple(bag(
                                lit(3), lit(1), lit(4), lit(1), lit(5), lit(9), lit(2), lit(6))),
                        List.of(new SortSpec(new Expr.CurrentValue(), SortDirection.ASC))),
                3);
        assertEquals(
                List.of(bd("{\"value\": 1}"), bd("{\"value\": 1}"), bd("{\"value\": 2}")),
                run(ast));
    }

    @Test
    void projectBranching() {
        FieldPathTree tree = new FieldPathTree.Interior(List.of(
                Map.entry("a", new FieldPathTree.Interior(List.of(
                        Map.entry("x", new FieldPathTree.Leaf(null)),
                        Map.entry("z", new FieldPathTree.Leaf(null))))),
                Map.entry("b", new FieldPathTree.Leaf(null))));
        Stage ast = new Stage.ProjectStage(
                new Stage.FromStageSimple(bag(doc(
                        kv("a", doc(kv("x", lit(1)), kv("y", lit(2)), kv("z", lit(3)))),
                        kv("b", lit(9))))),
                tree);
        assertEquals(
                List.of(bd("{\"a\": {\"x\": 1, \"z\": 3}, \"b\": 9}")),
                run(ast));
    }

    @Test
    void setWithArithmetic() {
        Stage ast = new Stage.SetStage(
                new Stage.FromStageSimple(bag(doc(kv("a", lit(1))), doc(kv("a", lit(2))))),
                List.of(new Assignment(List.of("z", "add1"), add(field("a"), lit(1)))));
        assertEquals(
                bset(
                        bd("{\"a\": 1, \"z\": {\"add1\": 2}}"),
                        bd("{\"a\": 2, \"z\": {\"add1\": 3}}")),
                asSet(run(ast)));
    }

    @Test
    void unset() {
        FieldPathTree tree = new FieldPathTree.Interior(List.of(
                Map.entry("b", new FieldPathTree.Leaf(null))));
        Stage ast = new Stage.UnsetStage(
                new Stage.FromStageSimple(bag(doc(kv("a", lit(1)), kv("b", lit(2))))),
                tree);
        assertEquals(List.of(bd("{\"a\": 1}")), run(ast));
    }

    @Test
    void distinctAndCount() {
        Stage distinct = new Stage.DistinctStage(
                new Stage.FromStageSimple(bag(lit(1), lit(1), lit(2), lit(3))));
        assertEquals(
                bset(bd("{\"value\": 1}"), bd("{\"value\": 2}"), bd("{\"value\": 3}")),
                asSet(run(distinct)));

        Stage count = new Stage.CountStage(
                new Stage.FromStageSimple(bag(lit(1), lit(2), lit(3), lit(4), lit(5))));
        assertEquals(List.of(bd("{\"value\": 5}")), run(count));
    }

    @Test
    void unwindSimple() {
        Stage ast = new Stage.UnwindSimpleStage(
                new Stage.FromStageSimple(new Expr.BagConstructor(List.of(
                        new Expr.ArrayConstructor(List.of(lit(1), lit(2), lit(3)))))),
                new Expr.CurrentValue());
        assertEquals(
                bset(bd("{\"value\": 1}"), bd("{\"value\": 2}"), bd("{\"value\": 3}")),
                asSet(run(ast)));
    }

    @Test
    void anyExpressionAndArrow() {
        Expr arrow = new Expr.ArrowOp(field("a"), "b");
        Expr predicate = new Expr.Any(arrow, eq(new Expr.CurrentValue(), lit(2)));
        Stage ast = new Stage.MatchStage(
                new Stage.FromStageSimple(bag(
                        doc(kv("a", new Expr.ArrayConstructor(List.of(
                                doc(kv("b", lit(1))),
                                doc(kv("b", lit(2))))))),
                        doc(kv("a", new Expr.ArrayConstructor(List.of(
                                doc(kv("b", lit(3))))))))),
                predicate);
        assertEquals(
                List.of(bd("{\"a\": [{\"b\": 1}, {\"b\": 2}]}")),
                run(ast));
    }

    @Test
    void groupWithSumArrow() {
        Stage ast = new Stage.GroupStage(
                new Stage.FromStageSimple(bag(
                        doc(kv("a", lit(1)), kv("b", lit(2))),
                        doc(kv("a", lit(1)), kv("b", lit(3))),
                        doc(kv("a", lit(2)), kv("b", lit(4))))),
                List.of(new Assignment(List.of("k"), field("a"))),
                List.of(new Assignment(List.of("s"), new Expr.FunctionCall("sum",
                        List.of(new Expr.ArrowOp(new Expr.CurrentValue(), "b"))))));
        assertEquals(
                bset(
                        bd("{\"k\": 1, \"s\": 5}"),
                        bd("{\"k\": 2, \"s\": 4}")),
                asSet(run(ast)));
    }

    @Test
    void letExpr() {
        Stage ast = new Stage.FromStageSimple(
                new Expr.LetExpr(
                        List.of(Map.entry("x", lit(2))),
                        add(new Expr.VarRef("x"), lit(3))));
        assertEquals(List.of(bd("{\"value\": 5}")), run(ast));
    }

    @Test
    void topLevelAggViaFunctionCall() {
        Stage ast = new Stage.FromStageSimple(
                new Expr.FunctionCall("sum", List.of(bag(lit(1), lit(2), lit(3), lit(4)))));
        assertEquals(List.of(bd("{\"value\": 10}")), run(ast));
    }

    @Test
    void notAndIsNullish() {
        Expr isNullish = new Expr.FunctionCall("isNullish", List.of(field("a")));
        Stage ast = new Stage.MatchStage(
                new Stage.FromStageSimple(bag(
                        doc(kv("a", lit(1))),
                        doc(kv("a", new Expr.ValueLit(new Value.VNull()))),
                        doc(kv("a", new Expr.ValueLit(new Value.VMissing()))))),
                new Expr.UnaryOp(UnaryOpType.NOT, isNullish));
        assertEquals(List.of(bd("{\"a\": 1}")), run(ast));
    }

    @Test
    void crossProductFromNested() {
        Stage ast = new Stage.FromStageNested(List.of(
                Map.entry("s", bag(lit(1), lit(2), lit(3))),
                Map.entry("s2", bag(lit(10), lit(20)))));
        assertEquals(
                bset(
                        bd("{\"s\": 1, \"s2\": 10}"),
                        bd("{\"s\": 1, \"s2\": 20}"),
                        bd("{\"s\": 2, \"s2\": 10}"),
                        bd("{\"s\": 2, \"s2\": 20}"),
                        bd("{\"s\": 3, \"s2\": 10}"),
                        bd("{\"s\": 3, \"s2\": 20}")),
                asSet(run(ast)));
    }

    @Test
    void joinDefaultInner() {
        Expr cBag = bag(
                doc(kv("id", lit(1)), kv("items", lit(10))),
                doc(kv("id", lit(2)), kv("items", lit(5))));
        Expr oBag = bag(
                doc(kv("id", lit(1)), kv("items", lit(10))),
                doc(kv("id", lit(2)), kv("items", lit(5))));
        Expr cId = new Expr.FieldAccess(new Expr.FieldAccess(new Expr.CurrentValue(), "c"), "id");
        Expr oId = new Expr.FieldAccess(new Expr.FieldAccess(new Expr.CurrentValue(), "o"), "id");
        Stage ast = new Stage.JoinStage(
                new Stage.FromStageNested(List.of(Map.entry("c", cBag))),
                null,
                "o",
                oBag,
                eq(cId, oId));
        assertEquals(
                bset(
                        bd("{\"c\": {\"id\": 1, \"items\": 10}, \"o\": {\"id\": 1, \"items\": 10}}"),
                        bd("{\"c\": {\"id\": 2, \"items\": 5}, \"o\": {\"id\": 2, \"items\": 5}}")),
                asSet(run(ast)));
    }

    @Test
    void joinLeftOuter() {
        Expr cBag = bag(
                doc(kv("id", lit(1))),
                doc(kv("id", lit(2))),
                doc(kv("id", lit(3))));
        Expr oBag = bag(
                doc(kv("id", lit(1)), kv("v", lit("x"))),
                doc(kv("id", lit(3)), kv("v", lit("z"))));
        Expr cId = new Expr.FieldAccess(new Expr.FieldAccess(new Expr.CurrentValue(), "c"), "id");
        Expr oId = new Expr.FieldAccess(new Expr.FieldAccess(new Expr.CurrentValue(), "o"), "id");
        Stage ast = new Stage.JoinStage(
                new Stage.FromStageNested(List.of(Map.entry("c", cBag))),
                JoinType.LEFT_OUTER,
                "o",
                oBag,
                eq(cId, oId));
        assertEquals(
                bset(
                        bd("{\"c\": {\"id\": 1}, \"o\": {\"id\": 1, \"v\": \"x\"}}"),
                        bd("{\"c\": {\"id\": 2}}"),
                        bd("{\"c\": {\"id\": 3}, \"o\": {\"id\": 3, \"v\": \"z\"}}")),
                asSet(run(ast)));
    }
}
