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
 * Static factory methods for the untyped MQLv2 facade. Star-import these:
 * {@code import static com.mongodb.mqlv2.facade.untyped.Untyped.*;}
 */
public final class Untyped {

    private Untyped() {
    }

    // --- Values / literals ---

    public static ExprU lit(final long n) {
        return new ExprU(new Expr.ValueLit(new Value.VInt(n)));
    }

    public static ExprU lit(final double d) {
        return new ExprU(new Expr.ValueLit(new Value.VDouble(d)));
    }

    public static ExprU lit(final String s) {
        return new ExprU(new Expr.ValueLit(new Value.VString(s)));
    }

    public static ExprU lit(final boolean b) {
        return new ExprU(new Expr.ValueLit(new Value.VBool(b)));
    }

    public static ExprU litNull() {
        return new ExprU(new Expr.ValueLit(new Value.VNull()));
    }

    public static ExprU litMissing() {
        return new ExprU(new Expr.ValueLit(new Value.VMissing()));
    }

    public static ExprU litUndefined() {
        return new ExprU(new Expr.ValueLit(new Value.VUndefined()));
    }

    public static ExprU litDate(final long millisSinceEpoch) {
        return new ExprU(new Expr.ValueLit(new Value.VDate(millisSinceEpoch)));
    }

    // --- Current value / variables / top-level field access ---

    public static ExprU current() {
        return new ExprU(new Expr.CurrentValue());
    }

    public static ExprU var(final String name) {
        return new ExprU(new Expr.VarRef(name));
    }

    /**
     * Field access on the current value: {@code field("a")} produces
     * {@code FieldAccess(CurrentValue, "a")}, serializing as the bare identifier {@code a}.
     *
     * @param name the field name.
     * @return new {@code ExprU} wrapping a {@code FieldAccess}.
     */
    public static ExprU field(final String name) {
        return new ExprU(new Expr.FieldAccess(new Expr.CurrentValue(), name));
    }

    // --- Constructors (bag, array, document) ---

    public static ExprU bag(final ExprU... elements) {
        return new ExprU(new Expr.BagConstructor(toExprList(elements)));
    }

    public static ExprU arr(final ExprU... elements) {
        return new ExprU(new Expr.ArrayConstructor(toExprList(elements)));
    }

    @SafeVarargs
    public static ExprU doc(final Map.Entry<String, ExprU>... entries) {
        List<Map.Entry<Expr, Expr>> fields = new ArrayList<>();
        for (Map.Entry<String, ExprU> e : entries) {
            fields.add(Map.entry(new Expr.ValueLit(new Value.VString(e.getKey())), e.getValue().toExpr()));
        }
        return new ExprU(new Expr.DocumentConstructor(fields));
    }

    /**
     * Tiny alias for {@link Map#entry(Object, Object)} so {@code doc(entry("a", lit(1)))} reads
     * naturally without an import.
     *
     * @param key   the field name.
     * @param value the field value expression.
     * @return a {@code Map.Entry} suitable for {@link #doc(Map.Entry[])}.
     */
    public static Map.Entry<String, ExprU> entry(final String key, final ExprU value) {
        return Map.entry(key, value);
    }

    // --- From / pipeline source ---

    public static PipelineBuilder from(final ExprU source) {
        return new PipelineBuilder(new Stage.FromStageSimple(source.toExpr()));
    }

    @SafeVarargs
    public static PipelineBuilder fromNested(final Map.Entry<String, ExprU>... sources) {
        List<Map.Entry<String, Expr>> entries = new ArrayList<>();
        for (Map.Entry<String, ExprU> s : sources) {
            entries.add(Map.entry(s.getKey(), s.getValue().toExpr()));
        }
        return new PipelineBuilder(new Stage.FromStageNested(entries));
    }

    // --- Sort / assignment helpers ---

    public static SortSpec asc(final ExprU expr) {
        return new SortSpec(expr.toExpr(), SortDirection.ASC);
    }

    public static SortSpec desc(final ExprU expr) {
        return new SortSpec(expr.toExpr(), SortDirection.DESC);
    }

    /**
     * Build an {@link Assignment} from a dot-separated path and a value.
     *
     * @param dotPath the dot-decomposed field path (e.g. "a.b.c").
     * @param value   the expression assigned to that path.
     * @return a new {@link Assignment}.
     */
    public static Assignment assign(final String dotPath, final ExprU value) {
        return new Assignment(Arrays.asList(dotPath.split("\\.")), value.toExpr());
    }

    // --- Sub-pipeline + let ---

    public static ExprU sub(final PipelineBuilder pipeline) {
        return new ExprU(new Expr.SubPipelineExpr(pipeline.stage()));
    }

    /**
     * Let expression. Reads as: {@code let bindings... in body}.
     *
     * @param body     the body expression of the let.
     * @param bindings the (name, value) pairs introduced by the let.
     * @return new {@code ExprU} wrapping a {@code LetExpr}.
     */
    @SafeVarargs
    public static ExprU letIn(final ExprU body, final Map.Entry<String, ExprU>... bindings) {
        List<Map.Entry<String, Expr>> binds = new ArrayList<>();
        for (Map.Entry<String, ExprU> b : bindings) {
            binds.add(Map.entry(b.getKey(), b.getValue().toExpr()));
        }
        return new ExprU(new Expr.LetExpr(binds, body.toExpr()));
    }

    // --- Function calls ---

    private static ExprU fn(final String name, final ExprU... args) {
        return new ExprU(new Expr.FunctionCall(name, toExprList(args)));
    }

    public static ExprU count(final ExprU x) {
        return fn("count", x);
    }
    public static ExprU sum(final ExprU x) {
        return fn("sum", x);
    }
    public static ExprU min(final ExprU x) {
        return fn("min", x);
    }
    public static ExprU max(final ExprU x) {
        return fn("max", x);
    }
    public static ExprU avg(final ExprU x) {
        return fn("avg", x);
    }
    public static ExprU isNullish(final ExprU x) {
        return fn("isNullish", x);
    }
    public static ExprU notNullish(final ExprU x) {
        return fn("notNullish", x);
    }
    public static ExprU year(final ExprU x) {
        return fn("year", x);
    }
    public static ExprU month(final ExprU x) {
        return fn("month", x);
    }
    public static ExprU dayOfYear(final ExprU x) {
        return fn("dayOfYear", x);
    }
    public static ExprU dayOfMonth(final ExprU x) {
        return fn("dayOfMonth", x);
    }
    public static ExprU dayOfWeek(final ExprU x) {
        return fn("dayOfWeek", x);
    }
    public static ExprU hour(final ExprU x) {
        return fn("hour", x);
    }
    public static ExprU minute(final ExprU x) {
        return fn("minute", x);
    }
    public static ExprU second(final ExprU x) {
        return fn("second", x);
    }
    public static ExprU millisecond(final ExprU x) {
        return fn("millisecond", x);
    }
    public static ExprU fillNullish(final ExprU a, final ExprU b) {
        return fn("fillNullish", a, b);
    }
    public static ExprU regexMatch(final ExprU a, final ExprU b) {
        return fn("regexMatch", a, b);
    }

    // --- Helpers ---

    private static List<Expr> toExprList(final ExprU[] xs) {
        List<Expr> out = new ArrayList<>(xs.length);
        for (ExprU x : xs) {
            out.add(x.toExpr());
        }
        return out;
    }
}
