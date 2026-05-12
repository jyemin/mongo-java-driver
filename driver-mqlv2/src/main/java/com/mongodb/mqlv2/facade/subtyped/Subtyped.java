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

import com.mongodb.mqlv2.ast.Assignment;
import com.mongodb.mqlv2.ast.Expr;
import com.mongodb.mqlv2.ast.SortDirection;
import com.mongodb.mqlv2.ast.SortSpec;
import com.mongodb.mqlv2.ast.Stage;
import com.mongodb.mqlv2.ast.Value;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Static factory methods for the subtyped MQLv2 facade. Star-import:
 * {@code import static com.mongodb.mqlv2.facade.subtyped.Subtyped.*;}
 */
public final class Subtyped {

    private Subtyped() {
    }

    // --- Literals ---

    public static IntExprT intLit(final long n) {
        return new ExprImpl<>(new Expr.ValueLit(new Value.VInt(n)));
    }

    public static NumExprT numLit(final double d) {
        return new ExprImpl<>(new Expr.ValueLit(new Value.VDouble(d)));
    }

    public static StrExprT strLit(final String s) {
        return new ExprImpl<>(new Expr.ValueLit(new Value.VString(s)));
    }

    public static BoolExprT boolLit(final boolean b) {
        return new ExprImpl<>(new Expr.ValueLit(new Value.VBool(b)));
    }

    public static DateExprT dateLit(final long millisSinceEpoch) {
        return new ExprImpl<>(new Expr.ValueLit(new Value.VDate(millisSinceEpoch)));
    }

    public static ExprT litNull() {
        return new ExprImpl<>(new Expr.ValueLit(new Value.VNull()));
    }

    public static ExprT litMissing() {
        return new ExprImpl<>(new Expr.ValueLit(new Value.VMissing()));
    }

    public static ExprT litUndefined() {
        return new ExprImpl<>(new Expr.ValueLit(new Value.VUndefined()));
    }

    // --- Current value (typed variants) ---

    public static ExprT current() {
        return new ExprImpl<>(new Expr.CurrentValue());
    }

    public static IntExprT intCurrent() {
        return new ExprImpl<>(new Expr.CurrentValue());
    }

    public static NumExprT numCurrent() {
        return new ExprImpl<>(new Expr.CurrentValue());
    }

    public static StrExprT strCurrent() {
        return new ExprImpl<>(new Expr.CurrentValue());
    }

    public static BoolExprT boolCurrent() {
        return new ExprImpl<>(new Expr.CurrentValue());
    }

    public static DateExprT dateCurrent() {
        return new ExprImpl<>(new Expr.CurrentValue());
    }

    public static DocExprT docCurrent() {
        return new ExprImpl<>(new Expr.CurrentValue());
    }

    public static ArrExprT<ExprT> arrCurrent() {
        return new ExprImpl<>(new Expr.CurrentValue());
    }

    // --- Variables (typed variants) ---

    public static ExprT var(final String name) {
        return new ExprImpl<>(new Expr.VarRef(name));
    }

    public static IntExprT intVar(final String name) {
        return new ExprImpl<>(new Expr.VarRef(name));
    }

    public static NumExprT numVar(final String name) {
        return new ExprImpl<>(new Expr.VarRef(name));
    }

    public static StrExprT strVar(final String name) {
        return new ExprImpl<>(new Expr.VarRef(name));
    }

    public static BoolExprT boolVar(final String name) {
        return new ExprImpl<>(new Expr.VarRef(name));
    }

    public static DateExprT dateVar(final String name) {
        return new ExprImpl<>(new Expr.VarRef(name));
    }

    public static DocExprT docVar(final String name) {
        return new ExprImpl<>(new Expr.VarRef(name));
    }

    public static ArrExprT<ExprT> arrVar(final String name) {
        return new ExprImpl<>(new Expr.VarRef(name));
    }

    // --- Top-level field shorthand (= current().xxxField(name)) ---

    public static ExprT field(final String name) {
        return new ExprImpl<>(new Expr.FieldAccess(new Expr.CurrentValue(), name));
    }

    public static IntExprT intField(final String name) {
        return new ExprImpl<>(new Expr.FieldAccess(new Expr.CurrentValue(), name));
    }

    public static NumExprT numField(final String name) {
        return new ExprImpl<>(new Expr.FieldAccess(new Expr.CurrentValue(), name));
    }

    public static StrExprT strField(final String name) {
        return new ExprImpl<>(new Expr.FieldAccess(new Expr.CurrentValue(), name));
    }

    public static BoolExprT boolField(final String name) {
        return new ExprImpl<>(new Expr.FieldAccess(new Expr.CurrentValue(), name));
    }

    public static DateExprT dateField(final String name) {
        return new ExprImpl<>(new Expr.FieldAccess(new Expr.CurrentValue(), name));
    }

    public static DocExprT docField(final String name) {
        return new ExprImpl<>(new Expr.FieldAccess(new Expr.CurrentValue(), name));
    }

    public static ArrExprT<ExprT> arrField(final String name) {
        return new ExprImpl<>(new Expr.FieldAccess(new Expr.CurrentValue(), name));
    }

    // --- Constructors ---

    public static ArrExprT<ExprT> bag(final ExprT... elements) {
        return new ExprImpl<>(new Expr.BagConstructor(toExprList(elements)));
    }

    public static ArrExprT<ExprT> arr(final ExprT... elements) {
        return new ExprImpl<>(new Expr.ArrayConstructor(toExprList(elements)));
    }

    @SafeVarargs
    public static DocExprT doc(final Map.Entry<String, ExprT>... entries) {
        List<Map.Entry<Expr, Expr>> fields = new ArrayList<>();
        for (Map.Entry<String, ExprT> e : entries) {
            fields.add(Map.entry(new Expr.ValueLit(new Value.VString(e.getKey())), e.getValue().toExpr()));
        }
        return new ExprImpl<>(new Expr.DocumentConstructor(fields));
    }

    public static Map.Entry<String, ExprT> entry(final String key, final ExprT value) {
        return Map.entry(key, value);
    }

    // --- From / pipeline source ---

    public static PipelineBuilderS from(final ExprT source) {
        return new PipelineBuilderS(new Stage.FromStageSimple(source.toExpr()));
    }

    @SafeVarargs
    public static PipelineBuilderS fromNested(final Map.Entry<String, ExprT>... sources) {
        List<Map.Entry<String, Expr>> entries = new ArrayList<>();
        for (Map.Entry<String, ExprT> s : sources) {
            entries.add(Map.entry(s.getKey(), s.getValue().toExpr()));
        }
        return new PipelineBuilderS(new Stage.FromStageNested(entries));
    }

    // --- Sort / assignment helpers ---

    public static SortSpec asc(final ExprT expr) {
        return new SortSpec(expr.toExpr(), SortDirection.ASC);
    }

    public static SortSpec desc(final ExprT expr) {
        return new SortSpec(expr.toExpr(), SortDirection.DESC);
    }

    /**
     * Build an {@link Assignment} from a dot-separated path and a value.
     *
     * @param dotPath the dot-decomposed field path (e.g. "a.b.c").
     * @param value   the expression assigned to that path.
     * @return a new {@link Assignment}.
     */
    public static Assignment assign(final String dotPath, final ExprT value) {
        return new Assignment(Arrays.asList(dotPath.split("\\.")), value.toExpr());
    }

    // --- Sub-pipeline + let ---

    public static ExprT sub(final PipelineBuilderS pipeline) {
        return new ExprImpl<>(new Expr.SubPipelineExpr(pipeline.stage()));
    }

    /**
     * Let expression. Reads as: {@code let bindings... in body}. Return type is the
     * type of {@code body}, preserved via unchecked cast (safe because {@link ExprImpl}
     * implements every facade interface).
     *
     * @param body     the body expression of the let.
     * @param bindings the (name, value) pairs introduced by the let.
     * @param <T>      the result type (inferred from {@code body}).
     * @return new wrapper of the same narrow type as {@code body}.
     */
    @SafeVarargs
    public static <T extends ExprT> T letIn(final T body, final Map.Entry<String, ExprT>... bindings) {
        List<Map.Entry<String, Expr>> binds = new ArrayList<>();
        for (Map.Entry<String, ExprT> b : bindings) {
            binds.add(Map.entry(b.getKey(), b.getValue().toExpr()));
        }
        @SuppressWarnings("unchecked")
        T result = (T) new ExprImpl<>(new Expr.LetExpr(binds, body.toExpr()));
        return result;
    }

    // --- Function calls (typed returns) ---

    public static IntExprT count(final ExprT x) {
        return new ExprImpl<>(new Expr.FunctionCall("count", List.of(x.toExpr())));
    }

    public static NumExprT sum(final NumExprT x) {
        return new ExprImpl<>(new Expr.FunctionCall("sum", List.of(x.toExpr())));
    }

    public static IntExprT sum(final IntExprT x) {
        return new ExprImpl<>(new Expr.FunctionCall("sum", List.of(x.toExpr())));
    }

    public static NumExprT min(final NumExprT x) {
        return new ExprImpl<>(new Expr.FunctionCall("min", List.of(x.toExpr())));
    }

    public static IntExprT min(final IntExprT x) {
        return new ExprImpl<>(new Expr.FunctionCall("min", List.of(x.toExpr())));
    }

    public static NumExprT max(final NumExprT x) {
        return new ExprImpl<>(new Expr.FunctionCall("max", List.of(x.toExpr())));
    }

    public static IntExprT max(final IntExprT x) {
        return new ExprImpl<>(new Expr.FunctionCall("max", List.of(x.toExpr())));
    }

    public static NumExprT avg(final NumExprT x) {
        return new ExprImpl<>(new Expr.FunctionCall("avg", List.of(x.toExpr())));
    }

    public static BoolExprT isNullish(final ExprT x) {
        return new ExprImpl<>(new Expr.FunctionCall("isNullish", List.of(x.toExpr())));
    }

    public static BoolExprT notNullish(final ExprT x) {
        return new ExprImpl<>(new Expr.FunctionCall("notNullish", List.of(x.toExpr())));
    }

    public static ExprT fillNullish(final ExprT a, final ExprT b) {
        return new ExprImpl<>(new Expr.FunctionCall("fillNullish", List.of(a.toExpr(), b.toExpr())));
    }

    public static BoolExprT regexMatch(final StrExprT a, final StrExprT b) {
        return new ExprImpl<>(new Expr.FunctionCall("regexMatch", List.of(a.toExpr(), b.toExpr())));
    }


    public static IntExprT year(final DateExprT x) {
        return new ExprImpl<>(new Expr.FunctionCall("year", List.of(x.toExpr())));
    }

    public static IntExprT month(final DateExprT x) {
        return new ExprImpl<>(new Expr.FunctionCall("month", List.of(x.toExpr())));
    }

    public static IntExprT dayOfMonth(final DateExprT x) {
        return new ExprImpl<>(new Expr.FunctionCall("dayOfMonth", List.of(x.toExpr())));
    }

    public static IntExprT dayOfYear(final DateExprT x) {
        return new ExprImpl<>(new Expr.FunctionCall("dayOfYear", List.of(x.toExpr())));
    }

    public static IntExprT dayOfWeek(final DateExprT x) {
        return new ExprImpl<>(new Expr.FunctionCall("dayOfWeek", List.of(x.toExpr())));
    }

    public static IntExprT hour(final DateExprT x) {
        return new ExprImpl<>(new Expr.FunctionCall("hour", List.of(x.toExpr())));
    }

    public static IntExprT minute(final DateExprT x) {
        return new ExprImpl<>(new Expr.FunctionCall("minute", List.of(x.toExpr())));
    }

    public static IntExprT second(final DateExprT x) {
        return new ExprImpl<>(new Expr.FunctionCall("second", List.of(x.toExpr())));
    }

    public static IntExprT millisecond(final DateExprT x) {
        return new ExprImpl<>(new Expr.FunctionCall("millisecond", List.of(x.toExpr())));
    }

    // --- Helpers ---

    private static List<Expr> toExprList(final ExprT[] xs) {
        List<Expr> out = new ArrayList<>(xs.length);
        for (ExprT x : xs) {
            out.add(x.toExpr());
        }
        return out;
    }
}
