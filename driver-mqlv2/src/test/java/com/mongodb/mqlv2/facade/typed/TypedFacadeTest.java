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
package com.mongodb.mqlv2.facade.typed;

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

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.mongodb.mqlv2.facade.typed.Typed.arr;
import static com.mongodb.mqlv2.facade.typed.Typed.asc;
import static com.mongodb.mqlv2.facade.typed.Typed.assign;
import static com.mongodb.mqlv2.facade.typed.Typed.bag;
import static com.mongodb.mqlv2.facade.typed.Typed.current;
import static com.mongodb.mqlv2.facade.typed.Typed.desc;
import static com.mongodb.mqlv2.facade.typed.Typed.doc;
import static com.mongodb.mqlv2.facade.typed.Typed.entry;
import static com.mongodb.mqlv2.facade.typed.Typed.field;
import static com.mongodb.mqlv2.facade.typed.Typed.from;
import static com.mongodb.mqlv2.facade.typed.Typed.isNullish;
import static com.mongodb.mqlv2.facade.typed.Typed.letIn;
import static com.mongodb.mqlv2.facade.typed.Typed.lit;
import static com.mongodb.mqlv2.facade.typed.Typed.dayOfMonth;
import static com.mongodb.mqlv2.facade.typed.Typed.dayOfWeek;
import static com.mongodb.mqlv2.facade.typed.Typed.dayOfYear;
import static com.mongodb.mqlv2.facade.typed.Typed.hour;
import static com.mongodb.mqlv2.facade.typed.Typed.litDate;
import static com.mongodb.mqlv2.facade.typed.Typed.millisecond;
import static com.mongodb.mqlv2.facade.typed.Typed.minute;
import static com.mongodb.mqlv2.facade.typed.Typed.month;
import static com.mongodb.mqlv2.facade.typed.Typed.second;
import static com.mongodb.mqlv2.facade.typed.Typed.year;
import static com.mongodb.mqlv2.facade.typed.Typed.litMissing;
import static com.mongodb.mqlv2.facade.typed.Typed.litNull;
import static com.mongodb.mqlv2.facade.typed.Typed.sum;
import static com.mongodb.mqlv2.facade.typed.Typed.var;
import static org.junit.jupiter.api.Assertions.assertEquals;

class TypedFacadeTest {

    private final MongoDatabase db = Fixture.getDefaultDatabase();

    // --- Bare AST helpers ---

    private static Expr bLit(final long n) {
        return new Expr.ValueLit(new Value.VInt(n));
    }

    private static Expr bLit(final String s) {
        return new Expr.ValueLit(new Value.VString(s));
    }

    private static Expr bField(final String name) {
        return new Expr.FieldAccess(new Expr.CurrentValue(), name);
    }

    private static Expr bEq(final Expr l, final Expr r) {
        return new Expr.BinaryOp(BinaryOpType.EQ, l, r);
    }

    private static Expr bAdd(final Expr l, final Expr r) {
        return new Expr.BinaryOp(BinaryOpType.ADD, l, r);
    }

    private static Expr bMul(final Expr l, final Expr r) {
        return new Expr.BinaryOp(BinaryOpType.MUL, l, r);
    }

    private static Expr bFn(final String name, final Expr... args) {
        return new Expr.FunctionCall(name, List.of(args));
    }

    private static Expr bBag(final Expr... es) {
        return new Expr.BagConstructor(List.of(es));
    }

    @SafeVarargs
    private static Expr bDoc(final Map.Entry<String, Expr>... fields) {
        List<Map.Entry<Expr, Expr>> entries = new ArrayList<>();
        for (Map.Entry<String, Expr> f : fields) {
            entries.add(Map.entry(bLit(f.getKey()), f.getValue()));
        }
        return new Expr.DocumentConstructor(entries);
    }

    private static Map.Entry<String, Expr> bKv(final String k, final Expr v) {
        return Map.entry(k, v);
    }

    // --- Expected-document helpers ---

    private static BsonDocument bd(final String json) {
        return BsonDocument.parse(json);
    }

    private static Set<BsonDocument> bset(final BsonDocument... docs) {
        return new HashSet<>(Arrays.asList(docs));
    }

    private static Set<BsonDocument> asSet(final List<BsonDocument> docs) {
        return new HashSet<>(docs);
    }

    private List<BsonDocument> run(final PipelineBuilderT pipeline) {
        return db.mqlv2(pipeline, BsonDocument.class).into(new ArrayList<>());
    }

    private static void assertSameAst(final Stage facadeAst, final Stage bareAst) {
        assertEquals(bareAst, facadeAst, "typed facade AST diverges from bare AST");
    }

    // --- Tests ---

    @Test
    void matchSimple() {
        // from <<1, 2, 3>> | match $ == 2
        PipelineBuilderT facade =
                from(bag(lit(1L), lit(2L), lit(3L)))
                        .match(current().eq(lit(2L)));
        Stage bare = new Stage.MatchStage(
                new Stage.FromStageSimple(bBag(bLit(1), bLit(2), bLit(3))),
                bEq(new Expr.CurrentValue(), bLit(2)));

        assertSameAst(facade.stage(), bare);
        assertEquals(List.of(bd("{\"value\": 2}")), run(facade));
    }

    @Test
    void formatWithDocAndMul() {
        // from <<{a:1},{a:2}>> | format {doubled: a * 2}
        PipelineBuilderT facade =
                from(bag(doc(entry("a", lit(1L))), doc(entry("a", lit(2L)))))
                        .format(doc(entry("doubled", field("a", Long.class).mul(lit(2L)))));
        Stage bare = new Stage.FormatStage(
                new Stage.FromStageSimple(bBag(bDoc(bKv("a", bLit(1))), bDoc(bKv("a", bLit(2))))),
                bDoc(bKv("doubled", bMul(bField("a"), bLit(2)))));

        assertSameAst(facade.stage(), bare);
        assertEquals(
                bset(bd("{\"doubled\": 2}"), bd("{\"doubled\": 4}")),
                asSet(run(facade)));
    }

    @Test
    void sortDescThenAsc() {
        // from <<{a:3,b:2},{a:1,b:7},{a:5,b:2},{a:5,b:1}>> | sort a desc, b
        PipelineBuilderT facade =
                from(bag(
                        doc(entry("a", lit(3L)), entry("b", lit(2L))),
                        doc(entry("a", lit(1L)), entry("b", lit(7L))),
                        doc(entry("a", lit(5L)), entry("b", lit(2L))),
                        doc(entry("a", lit(5L)), entry("b", lit(1L)))))
                        .sort(desc(field("a")), asc(field("b")));
        Stage bare = new Stage.SortStage(
                new Stage.FromStageSimple(bBag(
                        bDoc(bKv("a", bLit(3)), bKv("b", bLit(2))),
                        bDoc(bKv("a", bLit(1)), bKv("b", bLit(7))),
                        bDoc(bKv("a", bLit(5)), bKv("b", bLit(2))),
                        bDoc(bKv("a", bLit(5)), bKv("b", bLit(1))))),
                List.of(
                        new SortSpec(bField("a"), SortDirection.DESC),
                        new SortSpec(bField("b"), SortDirection.ASC)));

        assertSameAst(facade.stage(), bare);
        assertEquals(
                List.of(
                        bd("{\"a\": 5, \"b\": 1}"),
                        bd("{\"a\": 5, \"b\": 2}"),
                        bd("{\"a\": 3, \"b\": 2}"),
                        bd("{\"a\": 1, \"b\": 7}")),
                run(facade));
    }

    @Test
    void limit() {
        // from <<3,1,4,1,5,9,2,6>> | sort $ | limit 3
        PipelineBuilderT facade =
                from(bag(lit(3L), lit(1L), lit(4L), lit(1L), lit(5L), lit(9L), lit(2L), lit(6L)))
                        .sort(asc(current()))
                        .limit(3);
        Stage bare = new Stage.LimitStage(
                new Stage.SortStage(
                        new Stage.FromStageSimple(bBag(
                                bLit(3), bLit(1), bLit(4), bLit(1), bLit(5), bLit(9), bLit(2), bLit(6))),
                        List.of(new SortSpec(new Expr.CurrentValue(), SortDirection.ASC))),
                3);

        assertSameAst(facade.stage(), bare);
        assertEquals(
                List.of(bd("{\"value\": 1}"), bd("{\"value\": 1}"), bd("{\"value\": 2}")),
                run(facade));
    }

    @Test
    void projectBranching() {
        // from <<{a:{x:1,y:2,z:3}, b:9}>> | project a.x, a.z, b
        PipelineBuilderT facade =
                from(bag(doc(
                        entry("a", doc(entry("x", lit(1L)), entry("y", lit(2L)), entry("z", lit(3L)))),
                        entry("b", lit(9L)))))
                        .project("a.x", "a.z", "b");
        Stage bare = new Stage.ProjectStage(
                new Stage.FromStageSimple(bBag(bDoc(
                        bKv("a", bDoc(bKv("x", bLit(1)), bKv("y", bLit(2)), bKv("z", bLit(3)))),
                        bKv("b", bLit(9))))),
                new FieldPathTree.Interior(List.of(
                        Map.entry("a", new FieldPathTree.Interior(List.of(
                                Map.entry("x", new FieldPathTree.Leaf(null)),
                                Map.entry("z", new FieldPathTree.Leaf(null))))),
                        Map.entry("b", new FieldPathTree.Leaf(null)))));

        assertSameAst(facade.stage(), bare);
        assertEquals(
                List.of(bd("{\"a\": {\"x\": 1, \"z\": 3}, \"b\": 9}")),
                run(facade));
    }

    @Test
    void setWithArithmetic() {
        // from <<{a:1},{a:2}>> | set z.add1 = a + 1
        PipelineBuilderT facade =
                from(bag(doc(entry("a", lit(1L))), doc(entry("a", lit(2L)))))
                        .set(assign("z.add1", field("a", Long.class).add(lit(1L))));
        Stage bare = new Stage.SetStage(
                new Stage.FromStageSimple(bBag(bDoc(bKv("a", bLit(1))), bDoc(bKv("a", bLit(2))))),
                List.of(new Assignment(List.of("z", "add1"), bAdd(bField("a"), bLit(1)))));

        assertSameAst(facade.stage(), bare);
        assertEquals(
                bset(
                        bd("{\"a\": 1, \"z\": {\"add1\": 2}}"),
                        bd("{\"a\": 2, \"z\": {\"add1\": 3}}")),
                asSet(run(facade)));
    }

    @Test
    void unset() {
        // from <<{a:1,b:2}>> | unset b
        PipelineBuilderT facade =
                from(bag(doc(entry("a", lit(1L)), entry("b", lit(2L)))))
                        .unset("b");
        Stage bare = new Stage.UnsetStage(
                new Stage.FromStageSimple(bBag(bDoc(bKv("a", bLit(1)), bKv("b", bLit(2))))),
                new FieldPathTree.Interior(List.of(
                        Map.entry("b", new FieldPathTree.Leaf(null)))));

        assertSameAst(facade.stage(), bare);
        assertEquals(List.of(bd("{\"a\": 1}")), run(facade));
    }

    @Test
    void distinctAndCount() {
        PipelineBuilderT facadeDistinct =
                from(bag(lit(1L), lit(1L), lit(2L), lit(3L)))
                        .distinct();
        Stage bareDistinct = new Stage.DistinctStage(
                new Stage.FromStageSimple(bBag(bLit(1), bLit(1), bLit(2), bLit(3))));

        assertSameAst(facadeDistinct.stage(), bareDistinct);
        assertEquals(
                bset(bd("{\"value\": 1}"), bd("{\"value\": 2}"), bd("{\"value\": 3}")),
                asSet(run(facadeDistinct)));

        PipelineBuilderT facadeCount =
                from(bag(lit(1L), lit(2L), lit(3L), lit(4L), lit(5L)))
                        .count();
        Stage bareCount = new Stage.CountStage(
                new Stage.FromStageSimple(bBag(bLit(1), bLit(2), bLit(3), bLit(4), bLit(5))));

        assertSameAst(facadeCount.stage(), bareCount);
        assertEquals(List.of(bd("{\"value\": 5}")), run(facadeCount));
    }

    @Test
    void unwindSimple() {
        // from <<[1,2,3]>> | unwind $
        PipelineBuilderT facade =
                from(bag(arr(lit(1L), lit(2L), lit(3L))))
                        .unwind(current());
        Stage bare = new Stage.UnwindSimpleStage(
                new Stage.FromStageSimple(new Expr.BagConstructor(List.of(
                        new Expr.ArrayConstructor(List.of(bLit(1), bLit(2), bLit(3)))))),
                new Expr.CurrentValue());

        assertSameAst(facade.stage(), bare);
        assertEquals(
                bset(bd("{\"value\": 1}"), bd("{\"value\": 2}"), bd("{\"value\": 3}")),
                asSet(run(facade)));
    }

    @Test
    void anyExpressionAndArrow() {
        // from <<{a:[{b:1},{b:2}]},{a:[{b:3}]}>> | match a->b any ($ == 2)
        PipelineBuilderT facade =
                from(bag(
                        doc(entry("a", arr(doc(entry("b", lit(1L))), doc(entry("b", lit(2L)))))),
                        doc(entry("a", arr(doc(entry("b", lit(3L))))))))
                        .match(field("a").arrow("b").any(current().eq(lit(2L))));
        Stage bare = new Stage.MatchStage(
                new Stage.FromStageSimple(bBag(
                        bDoc(bKv("a", new Expr.ArrayConstructor(List.of(
                                bDoc(bKv("b", bLit(1))),
                                bDoc(bKv("b", bLit(2))))))),
                        bDoc(bKv("a", new Expr.ArrayConstructor(List.of(
                                bDoc(bKv("b", bLit(3))))))))),
                new Expr.Any(new Expr.ArrowOp(bField("a"), "b"),
                        bEq(new Expr.CurrentValue(), bLit(2))));

        assertSameAst(facade.stage(), bare);
        assertEquals(
                List.of(bd("{\"a\": [{\"b\": 1}, {\"b\": 2}]}")),
                run(facade));
    }

    @Test
    void groupWithSumArrow() {
        // from <<{a:1,b:2},{a:1,b:3},{a:2,b:4}>> | group (k=a) (s=sum($->b))
        PipelineBuilderT facade =
                from(bag(
                        doc(entry("a", lit(1L)), entry("b", lit(2L))),
                        doc(entry("a", lit(1L)), entry("b", lit(3L))),
                        doc(entry("a", lit(2L)), entry("b", lit(4L)))))
                        .group(
                                List.of(assign("k", field("a"))),
                                List.of(assign("s", sum(current().arrow("b")))));
        Stage bare = new Stage.GroupStage(
                new Stage.FromStageSimple(bBag(
                        bDoc(bKv("a", bLit(1)), bKv("b", bLit(2))),
                        bDoc(bKv("a", bLit(1)), bKv("b", bLit(3))),
                        bDoc(bKv("a", bLit(2)), bKv("b", bLit(4))))),
                List.of(new Assignment(List.of("k"), bField("a"))),
                List.of(new Assignment(List.of("s"), new Expr.FunctionCall("sum",
                        List.of(new Expr.ArrowOp(new Expr.CurrentValue(), "b"))))));

        assertSameAst(facade.stage(), bare);
        assertEquals(
                bset(bd("{\"k\": 1, \"s\": 5}"), bd("{\"k\": 2, \"s\": 4}")),
                asSet(run(facade)));
    }

    @Test
    void letExpr() {
        // from let $x = 2 in $x + 3
        PipelineBuilderT facade = from(letIn(var("x", Long.class).add(lit(3L)), entry("x", lit(2L))));
        Stage bare = new Stage.FromStageSimple(
                new Expr.LetExpr(
                        List.of(Map.entry("x", bLit(2))),
                        new Expr.BinaryOp(BinaryOpType.ADD, new Expr.VarRef("x"), bLit(3))));

        assertSameAst(facade.stage(), bare);
        assertEquals(List.of(bd("{\"value\": 5}")), run(facade));
    }

    @Test
    void topLevelAggViaFunctionCall() {
        // from sum(<<1,2,3,4>>)
        PipelineBuilderT facade = from(sum(bag(lit(1L), lit(2L), lit(3L), lit(4L))));
        Stage bare = new Stage.FromStageSimple(
                new Expr.FunctionCall("sum", List.of(bBag(bLit(1), bLit(2), bLit(3), bLit(4)))));

        assertSameAst(facade.stage(), bare);
        assertEquals(List.of(bd("{\"value\": 10}")), run(facade));
    }

    @Test
    void notAndIsNullish() {
        // from <<{a:1},{a:null},{a:missing}>> | match not isNullish(a)
        PipelineBuilderT facade =
                from(bag(
                        doc(entry("a", lit(1L))),
                        doc(entry("a", litNull())),
                        doc(entry("a", litMissing()))))
                        .match(isNullish(field("a")).not());
        Stage bare = new Stage.MatchStage(
                new Stage.FromStageSimple(bBag(
                        bDoc(bKv("a", bLit(1))),
                        bDoc(bKv("a", new Expr.ValueLit(new Value.VNull()))),
                        bDoc(bKv("a", new Expr.ValueLit(new Value.VMissing()))))),
                new Expr.UnaryOp(UnaryOpType.NOT,
                        new Expr.FunctionCall("isNullish", List.of(bField("a")))));

        assertSameAst(facade.stage(), bare);
        assertEquals(List.of(bd("{\"a\": 1}")), run(facade));
    }

    @Test
    void crossProductFromNested() {
        // from s=<<1,2,3>>, s2=<<10,20>>
        PipelineBuilderT facade = from(
                entry("s", bag(lit(1L), lit(2L), lit(3L))),
                entry("s2", bag(lit(10L), lit(20L))));
        Stage bare = new Stage.FromStageNested(List.of(
                Map.entry("s", bBag(bLit(1), bLit(2), bLit(3))),
                Map.entry("s2", bBag(bLit(10), bLit(20)))));

        assertSameAst(facade.stage(), bare);
        assertEquals(
                bset(
                        bd("{\"s\": 1, \"s2\": 10}"),
                        bd("{\"s\": 1, \"s2\": 20}"),
                        bd("{\"s\": 2, \"s2\": 10}"),
                        bd("{\"s\": 2, \"s2\": 20}"),
                        bd("{\"s\": 3, \"s2\": 10}"),
                        bd("{\"s\": 3, \"s2\": 20}")),
                asSet(run(facade)));
    }

    @Test
    void joinDefaultInner() {
        PipelineBuilderT facade =
                from(entry("c", bag(
                        doc(entry("id", lit(1L)), entry("items", lit(10L))),
                        doc(entry("id", lit(2L)), entry("items", lit(5L))))))
                        .join(
                                null,
                                "o",
                                bag(
                                        doc(entry("id", lit(1L)), entry("items", lit(10L))),
                                        doc(entry("id", lit(2L)), entry("items", lit(5L)))),
                                field("c").field("id").eq(field("o").field("id")));
        Stage bare = new Stage.JoinStage(
                new Stage.FromStageNested(List.of(Map.entry("c", bBag(
                        bDoc(bKv("id", bLit(1)), bKv("items", bLit(10))),
                        bDoc(bKv("id", bLit(2)), bKv("items", bLit(5))))))),
                null,
                "o",
                bBag(
                        bDoc(bKv("id", bLit(1)), bKv("items", bLit(10))),
                        bDoc(bKv("id", bLit(2)), bKv("items", bLit(5)))),
                bEq(
                        new Expr.FieldAccess(bField("c"), "id"),
                        new Expr.FieldAccess(bField("o"), "id")));

        assertSameAst(facade.stage(), bare);
        assertEquals(
                bset(
                        bd("{\"c\": {\"id\": 1, \"items\": 10}, \"o\": {\"id\": 1, \"items\": 10}}"),
                        bd("{\"c\": {\"id\": 2, \"items\": 5}, \"o\": {\"id\": 2, \"items\": 5}}")),
                asSet(run(facade)));
    }

    @Test
    void joinLeftOuter() {
        PipelineBuilderT facade =
                from(entry("c", bag(
                        doc(entry("id", lit(1L))),
                        doc(entry("id", lit(2L))),
                        doc(entry("id", lit(3L))))))
                        .join(
                                JoinType.LEFT_OUTER,
                                "o",
                                bag(
                                        doc(entry("id", lit(1L)), entry("v", lit("x"))),
                                        doc(entry("id", lit(3L)), entry("v", lit("z")))),
                                field("c").field("id").eq(field("o").field("id")));
        Stage bare = new Stage.JoinStage(
                new Stage.FromStageNested(List.of(Map.entry("c", bBag(
                        bDoc(bKv("id", bLit(1))),
                        bDoc(bKv("id", bLit(2))),
                        bDoc(bKv("id", bLit(3))))))),
                JoinType.LEFT_OUTER,
                "o",
                bBag(
                        bDoc(bKv("id", bLit(1)), bKv("v", bLit("x"))),
                        bDoc(bKv("id", bLit(3)), bKv("v", bLit("z")))),
                bEq(
                        new Expr.FieldAccess(bField("c"), "id"),
                        new Expr.FieldAccess(bField("o"), "id")));

        assertSameAst(facade.stage(), bare);
        assertEquals(
                bset(
                        bd("{\"c\": {\"id\": 1}, \"o\": {\"id\": 1, \"v\": \"x\"}}"),
                        bd("{\"c\": {\"id\": 2}}"),
                        bd("{\"c\": {\"id\": 3}, \"o\": {\"id\": 3, \"v\": \"z\"}}")),
                asSet(run(facade)));
    }

    @Test
    void dateExtractors() {
        // 2024-01-15T10:30:45.123Z (Monday), dayOfWeek=2 (Sun=1 convention)
        long dtMillis = Instant.parse("2024-01-15T10:30:45.123Z").toEpochMilli();

        PipelineBuilderT facade =
                from(bag(doc(entry("dt", litDate(dtMillis)))))
                        .format(doc(
                                entry("y",  year(field("dt"))),
                                entry("m",  month(field("dt"))),
                                entry("dm", dayOfMonth(field("dt"))),
                                entry("dy", dayOfYear(field("dt"))),
                                entry("dw", dayOfWeek(field("dt"))),
                                entry("h",  hour(field("dt"))),
                                entry("mn", minute(field("dt"))),
                                entry("s",  second(field("dt"))),
                                entry("ms", millisecond(field("dt")))));

        Expr bDt = new Expr.ValueLit(new Value.VDate(dtMillis));
        Stage bare = new Stage.FormatStage(
                new Stage.FromStageSimple(bBag(bDoc(bKv("dt", bDt)))),
                bDoc(
                        bKv("y",  bFn("year",        bField("dt"))),
                        bKv("m",  bFn("month",       bField("dt"))),
                        bKv("dm", bFn("dayOfMonth",  bField("dt"))),
                        bKv("dy", bFn("dayOfYear",   bField("dt"))),
                        bKv("dw", bFn("dayOfWeek",   bField("dt"))),
                        bKv("h",  bFn("hour",        bField("dt"))),
                        bKv("mn", bFn("minute",      bField("dt"))),
                        bKv("s",  bFn("second",      bField("dt"))),
                        bKv("ms", bFn("millisecond", bField("dt")))));

        assertSameAst(facade.stage(), bare);
        assertEquals(
                List.of(bd("{\"y\":2024,\"m\":1,\"dm\":15,\"dy\":15,\"dw\":2,\"h\":10,\"mn\":30,\"s\":45,\"ms\":123}")),
                run(facade));
    }

    @Test
    void dateComparisons() {
        long jan1  = Instant.parse("2024-01-01T00:00:00Z").toEpochMilli();
        long dec31 = Instant.parse("2024-12-31T23:59:59Z").toEpochMilli();
        long mid   = Instant.parse("2024-06-15T10:30:00Z").toEpochMilli();

        // litDate(jan1) < litDate(dec31)  => true
        PipelineBuilderT facadeLt = from(litDate(jan1).lt(litDate(dec31)));
        Stage bareLt = new Stage.FromStageSimple(
                new Expr.BinaryOp(BinaryOpType.LT,
                        new Expr.ValueLit(new Value.VDate(jan1)),
                        new Expr.ValueLit(new Value.VDate(dec31))));
        assertSameAst(facadeLt.stage(), bareLt);
        assertEquals(List.of(bd("{\"value\": true}")), run(facadeLt));

        // litDate(mid) == litDate(mid)  => true
        PipelineBuilderT facadeEqTrue = from(litDate(mid).eq(litDate(mid)));
        Stage bareEqTrue = new Stage.FromStageSimple(
                new Expr.BinaryOp(BinaryOpType.EQ,
                        new Expr.ValueLit(new Value.VDate(mid)),
                        new Expr.ValueLit(new Value.VDate(mid))));
        assertSameAst(facadeEqTrue.stage(), bareEqTrue);
        assertEquals(List.of(bd("{\"value\": true}")), run(facadeEqTrue));

        // litDate(jan1) == litDate(dec31)  => false
        PipelineBuilderT facadeEqFalse = from(litDate(jan1).eq(litDate(dec31)));
        Stage bareEqFalse = new Stage.FromStageSimple(
                new Expr.BinaryOp(BinaryOpType.EQ,
                        new Expr.ValueLit(new Value.VDate(jan1)),
                        new Expr.ValueLit(new Value.VDate(dec31))));
        assertSameAst(facadeEqFalse.stage(), bareEqFalse);
        assertEquals(List.of(bd("{\"value\": false}")), run(facadeEqFalse));

        // litDate(mid) is litDate(mid)  => true
        PipelineBuilderT facadeIs = from(litDate(mid).is(litDate(mid)));
        Stage bareIs = new Stage.FromStageSimple(
                new Expr.BinaryOp(BinaryOpType.IS,
                        new Expr.ValueLit(new Value.VDate(mid)),
                        new Expr.ValueLit(new Value.VDate(mid))));
        assertSameAst(facadeIs.stage(), bareIs);
        assertEquals(List.of(bd("{\"value\": true}")), run(facadeIs));
    }
}
