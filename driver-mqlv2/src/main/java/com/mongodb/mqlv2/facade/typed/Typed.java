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
 * Static factory methods for the typed MQLv2 facade. Star-import these:
 * {@code import static com.mongodb.mqlv2.facade.typed.Typed.*;}
 */
public final class Typed {

    private Typed() {
    }

    // --- Values / literals ---

    public static ExprT<Long> lit(final long n) {
        return new ExprT<>(new Expr.ValueLit(new Value.VInt(n)));
    }

    public static ExprT<Double> lit(final double d) {
        return new ExprT<>(new Expr.ValueLit(new Value.VDouble(d)));
    }

    public static ExprT<String> lit(final String s) {
        return new ExprT<>(new Expr.ValueLit(new Value.VString(s)));
    }

    public static ExprT<Boolean> lit(final boolean b) {
        return new ExprT<>(new Expr.ValueLit(new Value.VBool(b)));
    }

    public static ExprT<Object> litNull() {
        return new ExprT<>(new Expr.ValueLit(new Value.VNull()));
    }

    public static ExprT<Object> litMissing() {
        return new ExprT<>(new Expr.ValueLit(new Value.VMissing()));
    }

    public static ExprT<Object> litUndefined() {
        return new ExprT<>(new Expr.ValueLit(new Value.VUndefined()));
    }

    public static ExprT<Long> litDate(final long millisSinceEpoch) {
        return new ExprT<>(new Expr.ValueLit(new Value.VDate(millisSinceEpoch)));
    }

    // --- Current value / variables / top-level field access ---

    public static ExprT<Object> current() {
        return new ExprT<>(new Expr.CurrentValue());
    }

    public static <T> ExprT<T> current(final Class<T> type) {
        return new ExprT<>(new Expr.CurrentValue());
    }

    public static ExprT<Object> var(final String name) {
        return new ExprT<>(new Expr.VarRef(name));
    }

    public static <T> ExprT<T> var(final String name, final Class<T> type) {
        return new ExprT<>(new Expr.VarRef(name));
    }

    public static ExprT<Object> field(final String name) {
        return new ExprT<>(new Expr.FieldAccess(new Expr.CurrentValue(), name));
    }

    public static <T> ExprT<T> field(final String name, final Class<T> type) {
        return new ExprT<>(new Expr.FieldAccess(new Expr.CurrentValue(), name));
    }

    // --- Constructors (bag, array, document) ---

    public static ExprT<Object> bag(final ExprT<?>... elements) {
        return new ExprT<>(new Expr.BagConstructor(toExprList(elements)));
    }

    public static ExprT<Object> arr(final ExprT<?>... elements) {
        return new ExprT<>(new Expr.ArrayConstructor(toExprList(elements)));
    }

    @SafeVarargs
    public static ExprT<Object> doc(final Map.Entry<String, ExprT<?>>... entries) {
        List<Map.Entry<Expr, Expr>> fields = new ArrayList<>();
        for (Map.Entry<String, ExprT<?>> e : entries) {
            fields.add(Map.entry(new Expr.ValueLit(new Value.VString(e.getKey())), e.getValue().toExpr()));
        }
        return new ExprT<>(new Expr.DocumentConstructor(fields));
    }

    public static Map.Entry<String, ExprT<?>> entry(final String key, final ExprT<?> value) {
        return Map.entry(key, value);
    }

    // --- From / pipeline source ---

    public static PipelineBuilderT from(final ExprT<?> source) {
        return new PipelineBuilderT(new Stage.FromStageSimple(source.toExpr()));
    }

    @SafeVarargs
    public static PipelineBuilderT from(final Map.Entry<String, ExprT<?>>... sources) {
        List<Map.Entry<String, Expr>> entries = new ArrayList<>();
        for (Map.Entry<String, ExprT<?>> s : sources) {
            entries.add(Map.entry(s.getKey(), s.getValue().toExpr()));
        }
        return new PipelineBuilderT(new Stage.FromStageNested(entries));
    }

    // --- Sort / assignment helpers ---

    public static SortSpec asc(final ExprT<?> expr) {
        return new SortSpec(expr.toExpr(), SortDirection.ASC);
    }

    public static SortSpec desc(final ExprT<?> expr) {
        return new SortSpec(expr.toExpr(), SortDirection.DESC);
    }

    /**
     * Build an {@link Assignment} from a dot-separated path and a value.
     *
     * @param dotPath the dot-decomposed field path (e.g. "a.b.c").
     * @param value   the expression assigned to that path.
     * @return a new {@link Assignment}.
     */
    public static Assignment assign(final String dotPath, final ExprT<?> value) {
        return new Assignment(Arrays.asList(dotPath.split("\\.")), value.toExpr());
    }

    // --- Sub-pipeline + let ---

    public static ExprT<Object> sub(final PipelineBuilderT pipeline) {
        return new ExprT<>(new Expr.SubPipelineExpr(pipeline.stage()));
    }

    /**
     * Let expression. Reads as: {@code let bindings... in body}.
     *
     * @param body     the body expression of the let.
     * @param bindings the (name, value) pairs introduced by the let.
     * @param <T>      the result type (inferred from {@code body}).
     * @return new {@code ExprT<T>} wrapping a {@code LetExpr}.
     */
    @SafeVarargs
    public static <T> ExprT<T> letIn(final ExprT<T> body, final Map.Entry<String, ExprT<?>>... bindings) {
        List<Map.Entry<String, Expr>> binds = new ArrayList<>();
        for (Map.Entry<String, ExprT<?>> b : bindings) {
            binds.add(Map.entry(b.getKey(), b.getValue().toExpr()));
        }
        return new ExprT<>(new Expr.LetExpr(binds, body.toExpr()));
    }

    // --- Function calls ---

    private static Expr fnExpr(final String name, final ExprT<?>... args) {
        return new Expr.FunctionCall(name, toExprList(args));
    }

    public static ExprT<Long> count(final ExprT<?> x) {
        return new ExprT<>(fnExpr("count", x));
    }
    public static <T> ExprT<T> sum(final ExprT<T> x) {
        return new ExprT<>(fnExpr("sum", x));
    }
    public static <T> ExprT<T> min(final ExprT<T> x) {
        return new ExprT<>(fnExpr("min", x));
    }
    public static <T> ExprT<T> max(final ExprT<T> x) {
        return new ExprT<>(fnExpr("max", x));
    }
    public static ExprT<Double> avg(final ExprT<?> x) {
        return new ExprT<>(fnExpr("avg", x));
    }
    public static ExprT<Boolean> isNullish(final ExprT<?> x) {
        return new ExprT<>(fnExpr("isNullish", x));
    }
    public static ExprT<Boolean> notNullish(final ExprT<?> x) {
        return new ExprT<>(fnExpr("notNullish", x));
    }
    public static ExprT<Long> year(final ExprT<?> x) {
        return new ExprT<>(fnExpr("year", x));
    }
    public static ExprT<Long> month(final ExprT<?> x) {
        return new ExprT<>(fnExpr("month", x));
    }
    public static ExprT<Long> dayOfYear(final ExprT<?> x) {
        return new ExprT<>(fnExpr("dayOfYear", x));
    }
    public static ExprT<Long> dayOfMonth(final ExprT<?> x) {
        return new ExprT<>(fnExpr("dayOfMonth", x));
    }
    public static ExprT<Long> dayOfWeek(final ExprT<?> x) {
        return new ExprT<>(fnExpr("dayOfWeek", x));
    }
    public static ExprT<Long> hour(final ExprT<?> x) {
        return new ExprT<>(fnExpr("hour", x));
    }
    public static ExprT<Long> minute(final ExprT<?> x) {
        return new ExprT<>(fnExpr("minute", x));
    }
    public static ExprT<Long> second(final ExprT<?> x) {
        return new ExprT<>(fnExpr("second", x));
    }
    public static ExprT<Long> millisecond(final ExprT<?> x) {
        return new ExprT<>(fnExpr("millisecond", x));
    }
    public static <T> ExprT<T> fillNullish(final ExprT<T> a, final ExprT<T> b) {
        return new ExprT<>(fnExpr("fillNullish", a, b));
    }
    public static ExprT<Boolean> regexMatch(final ExprT<String> a, final ExprT<String> b) {
        return new ExprT<>(fnExpr("regexMatch", a, b));
    }

    // --- Helpers ---

    private static List<Expr> toExprList(final ExprT<?>[] xs) {
        List<Expr> out = new ArrayList<>(xs.length);
        for (ExprT<?> x : xs) {
            out.add(x.toExpr());
        }
        return out;
    }
}
