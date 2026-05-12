# driver-mqlv2 (Experimental)

Experimental Java driver API for MongoDB's new MQLv2 query language. The module is a
**skunkworks** — not for production, not on the public driver release path. Its purpose is
to explore what driver-side APIs over MQLv2 could feel like *if* MQLv2 ever becomes real.

The public `MongoDatabase.mqlv2(...)` methods (added in `driver-sync`) are marked
`@Alpha(Reason.CLIENT)`. The schema and API can change at any time.

## What's here

1. A hand-written **AST taxonomy** mirroring the MQLv2 surface syntax (sealed interfaces +
   records, Java 17).
2. A **serializer** that turns an AST into canonical MQLv2 text.
3. A **`Pipeline`** wrapper that implements `Mqlv2Source` so an AST root can be passed
   directly to `MongoDatabase.mqlv2(...)`.
4. Two **builder facades** on top of the AST — one untyped, one typed via phantom type
   parameters. Same AST underneath; different ergonomics on top.

## Architecture

```
       User code (one of three styles)
                 │
                 ▼
   ┌─────────────────────────────────────┐
   │  Facades (optional)                 │
   │    com.mongodb.mqlv2.facade.untyped │  ExprU, PipelineBuilder, Untyped
   │    com.mongodb.mqlv2.facade.typed   │  ExprT<T>, PipelineBuilderT, Typed
   └─────────────────────────────────────┘
                 │
                 ▼ build
   ┌─────────────────────────────────────┐
   │  AST (canonical)                    │
   │    com.mongodb.mqlv2.ast.{Stage,    │  sealed interfaces + records
   │      Expr, Value, FieldPathTree,    │
   │      SortSpec, Assignment, ...}     │
   └─────────────────────────────────────┘
                 │
                 ▼ serialize
   ┌─────────────────────────────────────┐
   │  Serializer (single class)          │  AST → MQLv2 surface text
   │    com.mongodb.mqlv2.Serializer     │
   └─────────────────────────────────────┘
                 │
                 ▼ wrapped by
   ┌─────────────────────────────────────┐
   │  Pipeline / PipelineBuilder(T)      │  implements Mqlv2Source
   └─────────────────────────────────────┘
                 │
                 ▼ db.mqlv2(...)
        ┌───────────────────┐
        │  Mqlv2Operation   │  driver-core, in driver-core/internal/operation
        │  (wire-level)     │  builds {mqlv2: "<text>", $db: ...}
        └───────────────────┘
                 │
                 ▼  wire
            ┌──────────┐
            │  mongod  │  experimental `mqlv2` command
            └──────────┘
```

The AST is the load-bearing layer. Both facades produce the same AST shapes (the
conformance tests assert this — `facade.stage().equals(bareAst)` for every test case).
Anything that compiles via a facade serializes to text the server accepts.

## The AST

`com.mongodb.mqlv2.ast` — hand-written from the schema at
`src/third_party/mqlv2/schema/language.yaml` in the mongodb/mongo repository.

- **`Stage`** (sealed) — 16 record subtypes: `FromStageSimple`, `FromStageNested`,
  `MatchStage`, `FormatStage`, `AggStage`, `ProjectStage`, `LimitStage`, `SortStage`,
  `GroupStage`, `SetStage`, `UnsetStage`, `DistinctStage`, `CountStage`,
  `UnwindSimpleStage`, `UnwindComplexStage`, `JoinStage`.
- **`Expr`** (sealed) — 16 record subtypes: `ValueLit`, `CurrentValue`, `VarRef`,
  `BinaryOp`, `UnaryOp`, `FieldAccess`, `ArrowOp`, `ArrayIndex`, `UnwindExpr`,
  `BagConstructor`, `ArrayConstructor`, `DocumentConstructor`, `Any`, `FunctionCall`,
  `SubPipelineExpr`, `LetExpr`.
- **`Value`** (sealed) — 10 literal variants (`VNull`, `VMissing`, `VUndefined`, `VBool`,
  `VInt`, `VDouble`, `VString`, `VDate`, `VDocument`, `VSequence`).
- **Helpers** — `FieldPathTree` (sealed `Interior`/`Leaf`), `SortSpec`, `Assignment`.
- **Enums** — `BinaryOpType` (13 ops with surface forms), `UnaryOpType` (`NOT`),
  `SortDirection` (`ASC`/`DESC`), `JoinType` (4), `DatePart` (9).

## The serializer

`com.mongodb.mqlv2.Serializer` is a single class (~180 LOC) that turns any `Stage` into
canonical MQLv2 surface text via `instanceof` pattern matching. Stylistic choices baked in:

- `BinaryOp` and `UnaryOp` always wrap in parens (safe, never ambiguous).
- `FieldAccess` from `CurrentValue` emits the bare identifier (`a`, not `$.a`).
- `DocumentConstructor` keys are emitted as quoted strings (`{"a": 1}`).
- `SortSpec` omits `asc` (the default direction); `desc` is always explicit.
- `JoinStage`'s `joinType` segment is omitted when null (defaults to inner on the server).
- `BagConstructor`, `ArrayConstructor`: `<<...>>` and `[...]`.

The serializer is proven correct against the live `mqlv2` server command — for every
conformance test, the serialized text round-trips through the server and produces the
expected result documents.

## The facades

### Bare AST (no facade)

Construct AST records directly. Verbose, unambiguous, and the baseline against which the
facades are measured.

```java
new Stage.MatchStage(
    new Stage.FromStageSimple(new Expr.BagConstructor(List.of(
        new Expr.ValueLit(new Value.VInt(1)),
        new Expr.ValueLit(new Value.VInt(2)),
        new Expr.ValueLit(new Value.VInt(3))))),
    new Expr.BinaryOp(BinaryOpType.EQ,
        new Expr.CurrentValue(),
        new Expr.ValueLit(new Value.VInt(2))))
```

Used directly in `Mqlv2ConformanceTest` — the reference shape every facade has to match.

### Untyped facade — `com.mongodb.mqlv2.facade.untyped`

`ExprU` wraps `Expr` with fluent operator methods (`.eq`, `.ne`, `.lt`, ..., `.and`,
`.or`, `.not`, `.add`, `.sub`, `.mul`, `.div`, `.field`, `.arrow`, `.at`, `.unwind`,
`.any`). `PipelineBuilder` wraps `Stage` with fluent stage methods (`.match`, `.format`,
`.sort`, `.limit`, ...). `Untyped` is a final class of static factory methods intended to
be star-imported.

```java
import static com.mongodb.mqlv2.facade.untyped.Untyped.*;

from(bag(lit(1), lit(2), lit(3)))
    .match(current().eq(lit(2)))
```

All `ExprU` look the same to the compiler — no type checks. Mistakes surface at server
execution time.

### Typed phantom facade — `com.mongodb.mqlv2.facade.typed`

`ExprT<T>` wraps `Expr` with a phantom Java type parameter — `T` expresses the result
type of the expression for compile-time checking. `PipelineBuilderT.match()` requires
`ExprT<Boolean>`. Arithmetic is strict (both sides must be the same `T`); comparisons
are parametric on the other side; `Class<T>` witnesses on `field`/`var`/`current`/`arrow`
let callers commit to a type when inference can't.

```java
import static com.mongodb.mqlv2.facade.typed.Typed.*;

from(bag(lit(1L), lit(2L), lit(3L)))
    .match(current().eq(lit(2L)))   // ExprT<Boolean>; lit(2L) is ExprT<Long>
```

Type-system policy at a glance:

| Operation | Receiver constraint | Other side constraint | Result |
|---|---|---|---|
| `eq`, `ne`, `is`, `lt`, `le`, `gt`, `ge` | none (parametric) | none (parametric) | `ExprT<Boolean>` |
| `and`, `or` | none (loose) | `ExprT<Boolean>` | `ExprT<Boolean>` |
| `not()` | (assumed Boolean) | — | `ExprT<Boolean>` |
| `add`, `sub`, `mul`, `div` | `ExprT<T>` | `ExprT<T>` (same `T`) | `ExprT<T>` |
| `field(String)` / `arrow` / `at` / `unwind` / `current` / `var` | — | — | `ExprT<Object>` |
| `field(String, Class<T>)` (and overloads) | — | — | `ExprT<T>` |
| `match(...)` (on `PipelineBuilderT`) | — | `ExprT<Boolean>` | next stage |

## Side-by-side comparison

Five queries, three ways. (All come from the conformance tests.)

### 1. Simplest match

```mql
from <<1, 2, 3>> | match $ == 2
```

```java
// Bare AST
new Stage.MatchStage(
    new Stage.FromStageSimple(new Expr.BagConstructor(List.of(
        new Expr.ValueLit(new Value.VInt(1)),
        new Expr.ValueLit(new Value.VInt(2)),
        new Expr.ValueLit(new Value.VInt(3))))),
    new Expr.BinaryOp(BinaryOpType.EQ,
        new Expr.CurrentValue(),
        new Expr.ValueLit(new Value.VInt(2))))

// Untyped
from(bag(lit(1), lit(2), lit(3)))
    .match(current().eq(lit(2)))

// Typed
from(bag(lit(1L), lit(2L), lit(3L)))
    .match(current().eq(lit(2L)))
```

### 2. Format with arithmetic

```mql
from <<{a:1},{a:2}>> | format {doubled: a * 2}
```

```java
// Untyped
from(bag(doc(entry("a", lit(1))), doc(entry("a", lit(2)))))
    .format(doc(entry("doubled", field("a").mul(lit(2)))))

// Typed — strict arithmetic forces a Class<T> witness
from(bag(doc(entry("a", lit(1L))), doc(entry("a", lit(2L)))))
    .format(doc(entry("doubled", field("a", Long.class).mul(lit(2L)))))
```

### 3. Group with arrow + sum

```mql
from <<{a:1,b:2},{a:1,b:3},{a:2,b:4}>> | group (k=a) (s=sum($->b))
```

```java
// Untyped
from(bag(
    doc(entry("a", lit(1)), entry("b", lit(2))),
    doc(entry("a", lit(1)), entry("b", lit(3))),
    doc(entry("a", lit(2)), entry("b", lit(4)))))
    .group(
        List.of(assign("k", field("a"))),
        List.of(assign("s", sum(current().arrow("b")))))

// Typed — no annotations needed; sum is parametric and we don't .add anywhere
from(bag(
    doc(entry("a", lit(1L)), entry("b", lit(2L))),
    doc(entry("a", lit(1L)), entry("b", lit(3L))),
    doc(entry("a", lit(2L)), entry("b", lit(4L)))))
    .group(
        List.of(assign("k", field("a"))),
        List.of(assign("s", sum(current().arrow("b")))))
```

### 4. Let + variable

```mql
from let $x = 2 in $x + 3
```

```java
// Untyped
from(letIn(var("x").add(lit(3)), entry("x", lit(2))))

// Typed — var needs a Class<T> witness because add is strict
from(letIn(var("x", Long.class).add(lit(3L)), entry("x", lit(2L))))
```

### 5. Left-outer join

```mql
from c=<<{id:1},{id:2},{id:3}>>
  | join leftOuter o=<<{id:1,v:"x"},{id:3,v:"z"}>> (c.id == o.id)
```

```java
// Untyped
fromNested(entry("c", bag(
        doc(entry("id", lit(1))),
        doc(entry("id", lit(2))),
        doc(entry("id", lit(3))))))
    .join(
        JoinType.LEFT_OUTER,
        "o",
        bag(
            doc(entry("id", lit(1)), entry("v", lit("x"))),
            doc(entry("id", lit(3)), entry("v", lit("z")))),
        field("c").field("id").eq(field("o").field("id")))

// Typed — identical (eq is parametric; field chains stay ExprT<Object>; match
//         enforcement happens because join condition is typed ExprT<Boolean>).
fromNested(entry("c", bag(
        doc(entry("id", lit(1L))),
        doc(entry("id", lit(2L))),
        doc(entry("id", lit(3L))))))
    .join(
        JoinType.LEFT_OUTER,
        "o",
        bag(
            doc(entry("id", lit(1L)), entry("v", lit("x"))),
            doc(entry("id", lit(3L)), entry("v", lit("z")))),
        field("c").field("id").eq(field("o").field("id")))
```

## Trade-offs summary

|                                  | Bare AST | Untyped facade | Typed facade |
|----------------------------------|----------|----------------|--------------|
| Lines per query (median)         | ~10×     | 1×             | 1×           |
| Reads like MQLv2 source          | ✗        | ✓              | ✓ (mostly)   |
| Compile-time checks              | none     | none           | `match`, arithmetic; loose elsewhere |
| `Class<T>` witnesses required    | n/a      | none           | for arithmetic with `field`/`var`/`current` of unknown type |
| Mistakes surface at              | server   | server         | compile (some) + server (rest) |
| Encourages explicit typing       | no       | no             | yes (forces commit on arithmetic) |
| Builder syntax noise             | high     | low            | low           |
| Mixed-type comparisons accepted  | yes      | yes            | yes (parametric `eq`) |

The typed facade's payoff is concentrated in three places: `match()` (must be Boolean),
arithmetic (must agree on `T`), and function calls (typed results like `count()→Long`,
`avg()→Double`). Everywhere else, the typing is essentially loose — `eq`/`ne`/etc. are
parametric on the other side, so `field("status").eq(lit("paid"))` compiles without
annotation. The visible cost in user code is mostly `Class<T>` witnesses on the
arithmetic-side `field`/`var`/`current` calls.

## Running the tests

A mongod with the experimental `mqlv2` command must be reachable. The standard driver
test fixture (`org.mongodb.test.uri`) is honored; default is `mongodb://localhost:27017`.

```bash
./gradlew :driver-mqlv2:test                          # all driver-mqlv2 tests
./gradlew :driver-mqlv2:test --tests "*Conformance*"  # AST conformance only
./gradlew :driver-mqlv2:test --tests "*FacadeTest*"   # both facades
./gradlew :driver-mqlv2:check                         # tests + checkstyle + spotbugs
```

Test count: **57** across four files —
- `SerializerTest` (6, no mongod)
- `Mqlv2ConformanceTest` (17, bare AST against server)
- `UntypedFacadeTest` (17, facade AST + server)
- `TypedFacadeTest` (17, typed facade AST + server)

Every conformance test asserts both **AST equivalence with the bare form** (`equals` on
the underlying record graphs) and **`BsonDocument` result equality against the live
server**, so the three test files exercise the same 17 queries by three different
construction styles and prove they all produce identical results.
