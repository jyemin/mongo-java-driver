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
import com.mongodb.mqlv2.ast.Expr;
import com.mongodb.mqlv2.ast.FieldPathTree;
import com.mongodb.mqlv2.ast.SortDirection;
import com.mongodb.mqlv2.ast.SortSpec;
import com.mongodb.mqlv2.ast.Stage;
import com.mongodb.mqlv2.ast.Value;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * Hand-written serializer: AST -> MQLv2 surface text.
 *
 * <p>Rendering rules follow {@code src/third_party/mqlv2/schema/language.yaml}.</p>
 */
public class Serializer {

    public String serialize(final Stage stage) {
        if (stage instanceof Stage.FromStageSimple s) {
            return "from " + ser(s.source());
        } else if (stage instanceof Stage.FromStageNested s) {
            return "from " + s.sources().stream()
                    .map(e -> e.getKey() + "=" + ser(e.getValue()))
                    .collect(Collectors.joining(", "));
        } else if (stage instanceof Stage.MatchStage s) {
            return serialize(s.prev()) + " | match " + ser(s.predicate());
        } else if (stage instanceof Stage.FormatStage s) {
            return serialize(s.prev()) + " | format " + ser(s.expr());
        } else if (stage instanceof Stage.AggStage s) {
            return serialize(s.prev()) + " | agg " + ser(s.expr());
        } else if (stage instanceof Stage.ProjectStage s) {
            return serialize(s.prev()) + " | project " + renderTreeTopLevel(s.tree());
        } else if (stage instanceof Stage.LimitStage s) {
            return serialize(s.prev()) + " | limit " + ser(s.count());
        } else if (stage instanceof Stage.SortStage s) {
            return serialize(s.prev()) + " | sort " + s.specs().stream()
                    .map(this::serSortSpec).collect(Collectors.joining(", "));
        } else if (stage instanceof Stage.GroupStage s) {
            return serialize(s.prev()) + " | group ("
                    + s.groupKeys().stream().map(this::serAssign).collect(Collectors.joining(", "))
                    + ") ("
                    + s.aggKeys().stream().map(this::serAssign).collect(Collectors.joining(", "))
                    + ")";
        } else if (stage instanceof Stage.SetStage s) {
            return serialize(s.prev()) + " | set "
                    + s.assignments().stream().map(this::serAssign).collect(Collectors.joining(", "));
        } else if (stage instanceof Stage.UnsetStage s) {
            return serialize(s.prev()) + " | unset " + renderTreeTopLevel(s.tree());
        } else if (stage instanceof Stage.DistinctStage s) {
            return serialize(s.prev()) + " | distinct";
        } else if (stage instanceof Stage.CountStage s) {
            return serialize(s.prev()) + " | count";
        } else if (stage instanceof Stage.UnwindSimpleStage s) {
            return serialize(s.prev()) + " | unwind " + ser(s.expr());
        } else if (stage instanceof Stage.UnwindComplexStage s) {
            return serialize(s.prev()) + " | unwind $" + s.varName()
                    + "=" + ser(s.sourceExpr()) + " in " + ser(s.bodyExpr());
        } else if (stage instanceof Stage.JoinStage s) {
            String joinTypeFragment = s.joinType() == null ? "" : s.joinType().surface() + " ";
            return serialize(s.prev()) + " | join " + joinTypeFragment
                    + s.varName() + "=" + ser(s.source()) + " (" + ser(s.condition()) + ")";
        }
        throw new IllegalArgumentException("Unhandled Stage: " + stage.getClass().getName());
    }

    private String ser(final Expr expr) {
        if (expr instanceof Expr.ValueLit e) {
            return serValue(e.value());
        } else if (expr instanceof Expr.CurrentValue) {
            return "$";
        } else if (expr instanceof Expr.VarRef e) {
            return "$" + e.name();
        } else if (expr instanceof Expr.BinaryOp e) {
            return "(" + ser(e.left()) + " " + e.op().surface() + " " + ser(e.right()) + ")";
        } else if (expr instanceof Expr.UnaryOp e) {
            return "(" + e.op().surface() + " " + ser(e.arg()) + ")";
        } else if (expr instanceof Expr.FieldAccess e) {
            return (e.target() instanceof Expr.CurrentValue) ? e.field() : ser(e.target()) + "." + e.field();
        } else if (expr instanceof Expr.ArrowOp e) {
            return ser(e.target()) + "->" + e.field();
        } else if (expr instanceof Expr.ArrayIndex e) {
            return ser(e.array()) + "[" + ser(e.index()) + "]";
        } else if (expr instanceof Expr.UnwindExpr e) {
            return ser(e.arg()) + "*";
        } else if (expr instanceof Expr.BagConstructor e) {
            return "<<" + e.elements().stream().map(this::ser).collect(Collectors.joining(", ")) + ">>";
        } else if (expr instanceof Expr.ArrayConstructor e) {
            return "[" + e.elements().stream().map(this::ser).collect(Collectors.joining(", ")) + "]";
        } else if (expr instanceof Expr.DocumentConstructor e) {
            return "{" + e.fields().stream()
                    .map(p -> ser(p.getKey()) + ": " + ser(p.getValue()))
                    .collect(Collectors.joining(", ")) + "}";
        } else if (expr instanceof Expr.Any e) {
            return ser(e.sequence()) + " any (" + ser(e.predicate()) + ")";
        } else if (expr instanceof Expr.FunctionCall e) {
            return e.name() + "("
                    + e.args().stream().map(this::ser).collect(Collectors.joining(", "))
                    + ")";
        } else if (expr instanceof Expr.SubPipelineExpr e) {
            return "(" + serialize(e.pipeline()) + ")";
        } else if (expr instanceof Expr.LetExpr e) {
            String binds = e.bindings().stream()
                    .map(b -> "$" + b.getKey() + " = " + ser(b.getValue()))
                    .collect(Collectors.joining(", "));
            return "let " + binds + " in " + ser(e.body());
        }
        throw new IllegalArgumentException("Unhandled Expr: " + expr.getClass().getName());
    }

    private String serValue(final Value v) {
        if (v instanceof Value.VNull) {
            return "null";
        }
        if (v instanceof Value.VMissing) {
            return "missing";
        }
        if (v instanceof Value.VUndefined) {
            return "undefined()";
        }
        if (v instanceof Value.VBool b) {
            return Boolean.toString(b.value());
        }
        if (v instanceof Value.VInt i) {
            return Long.toString(i.value());
        }
        if (v instanceof Value.VDouble d) {
            return Double.toString(d.value());
        }
        if (v instanceof Value.VString s) {
            return "\"" + escapeString(s.value()) + "\"";
        }
        if (v instanceof Value.VDate d) {
            return "date(\"" + java.time.Instant.ofEpochMilli(d.millisSinceEpoch()) + "\")";
        }
        if (v instanceof Value.VDocument doc) {
            return "{" + doc.fields().entrySet().stream()
                    .map(e -> e.getKey() + ": " + serValue(e.getValue()))
                    .collect(Collectors.joining(", ")) + "}";
        }
        if (v instanceof Value.VSequence seq) {
            String body = seq.elements().stream()
                    .map(this::serValue).collect(Collectors.joining(", "));
            return seq.ordered() ? "[" + body + "]" : "<<" + body + ">>";
        }
        throw new IllegalArgumentException("Unhandled Value: " + v.getClass().getName());
    }

    private String escapeString(final String s) {
        StringBuilder sb = new StringBuilder(s.length() + 2);
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"':  sb.append("\\\""); break;
                case '\\': sb.append("\\\\"); break;
                case '\n': sb.append("\\n");  break;
                case '\r': sb.append("\\r");  break;
                case '\t': sb.append("\\t");  break;
                default:   sb.append(c);
            }
        }
        return sb.toString();
    }

    private String serSortSpec(final SortSpec s) {
        if (s.direction() == SortDirection.ASC) {
            return ser(s.expr());
        }
        return ser(s.expr()) + " " + s.direction().surface();
    }

    private String serAssign(final Assignment a) {
        return String.join(".", a.path()) + " = " + ser(a.value());
    }

    private String renderTreeTopLevel(final FieldPathTree tree) {
        if (tree instanceof FieldPathTree.Leaf) {
            return "";
        }
        FieldPathTree.Interior interior = (FieldPathTree.Interior) tree;
        return interior.children().stream()
                .map(e -> renderPath(e.getKey(), e.getValue()))
                .collect(Collectors.joining(", "));
    }

    private String renderPath(final String name, final FieldPathTree subtree) {
        if (subtree instanceof FieldPathTree.Leaf) {
            return name;
        }
        FieldPathTree.Interior interior = (FieldPathTree.Interior) subtree;
        if (interior.children().size() == 1) {
            Map.Entry<String, FieldPathTree> only = interior.children().get(0);
            return name + "." + renderPath(only.getKey(), only.getValue());
        }
        return name + ".{"
                + interior.children().stream()
                        .map(e -> renderPath(e.getKey(), e.getValue()))
                        .collect(Collectors.joining(", "))
                + "}";
    }
}
