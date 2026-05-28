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
4. Three **builder facades** on top of the AST — one untyped, one typed via phantom type
   parameters, one subtyped via a sealed-interface hierarchy. Same AST underneath; different
   ergonomics on top.

## Architecture

The user-side pieces (AST, the things that implement `Mqlv2Source`, and the shared
`Serializer`) all live in the `driver-mqlv2` module. The driver entry point
`MongoDatabase.mqlv2(...)` lives in `driver-sync`; it accepts any `Mqlv2Source` and
hands the rendered text to a wire-level `Mqlv2Operation` in `driver-core`.

```
  ┌──────────────────────────────────────────────────────────────────┐
  │ driver-mqlv2                                                     │
  │                                                                  │
  │   Stage AST   (com.mongodb.mqlv2.ast.*)                          │
  │     sealed Stage / Expr / Value / FieldPathTree / SortSpec / ... │
  │     built either by hand or via one of the facade builders below │
  │                                                                  │
  │   Mqlv2Source  (interface, in driver-core)                       │
  │     { String toMqlv2(); }                                        │
  │     implemented by four things in driver-mqlv2:                  │
  │       · facade.untyped.PipelineBuilder                           │
  │       · facade.typed.PipelineBuilderT                            │
  │       · facade.subtyped.PipelineBuilderS                         │
  │       · com.mongodb.mqlv2.Pipeline   (bare-AST wrapper)          │
  │     each one's toMqlv2() calls Serializer.serialize(stage)       │
  │                                                                  │
  │   Serializer  (com.mongodb.mqlv2.Serializer)                     │
  │     utility: Stage → MQLv2 surface text                          │
  └──────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼  db.mqlv2(Mqlv2Source, Class<T>)
                                 │
            ┌──────────────────────────────────────┐
            │ Mqlv2Operation  (driver-core)        │
            │   wire command: { mqlv2: "<text>",   │
            │                   $db: "<dbName>" }  │
            └──────────────────────────────────────┘
                                 │
                                 ▼  wire
                              ┌──────┐
                              │mongod│
                              └──────┘
```

All three facades produce the same AST shapes — the conformance tests assert
`facade.stage().equals(bareAst)` for every query. Anything that compiles via a facade
serializes to text the server accepts.

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

**Why MQLv2 expressions map cleanly to a builder API.** In MQLv1, nested aggregation
expressions are written outermost-operator-first — `{$reduce: {input: {$map: {input: {$filter: …}}}}}` — so the base operand is buried at the innermost level. A Java fluent API
naturally inverts this: `arr.filter(…).map(…).reduce(…)` puts the base first. Every
translation between shell syntax and the Java API required mentally reversing the nesting.

MQLv2 expressions are postfix, like its pipeline stages. `filter`, `map`, and `reduce` do
not exist in MQLv2 yet, but if they were added following the same idiom as `any` —
`sequence any (pred)` — the surface syntax would read:

```
arr filter ($ > 0) map ($ * 2) reduce (0, $ + acc)
```

The Java facade would then read in exactly the same order: `arr.filter(…).map(…).reduce(…)`.
The surface query and the builder API are structurally isomorphic; no mental inversion required.

### Bare AST (no facade)

Construct AST records directly. Verbose, unambiguous, and the baseline against which the
facades are measured.

```java
// orders: { customerId: Long, total: Double, status: String }
new Stage.MatchStage(
    new Stage.FromStageSimple(new Expr.VarRef("orders")),
    new Expr.BinaryOp(BinaryOpType.EQ,
        new Expr.FieldAccess(new Expr.CurrentValue(), "status"),
        new Expr.ValueLit(new Value.VString("shipped"))))
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

from(var("orders")).match(field("status").eq(lit("shipped")))
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

from(var("orders")).match(field("status").eq(lit("shipped")))
// eq is parametric — no Class<T> witness needed for the string comparison
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

### Subtyped facade (`com.mongodb.mqlv2.facade.subtyped.*`)

Sealed-interface hierarchy. Each operation lives on the subtype it applies to, so the
type system rejects things like multiplying two strings or comparing a date to a long.

```
ExprT  (root, sealed)
├── NumExprT  ── add/sub/mul/div
│   └── IntExprT  ── same arithmetic; stays Int when both sides are Int
├── BoolExprT  ── and/or/not
├── StrExprT  ── regexMatch
├── DateExprT  ── year/month/dayOfMonth/dayOfYear/dayOfWeek/hour/minute/second/millisecond
├── DocExprT  ── per-type field accessors (intField, strField, ...)
└── ArrExprT<E>  ── elementAt/unwind/any
```

A single package-private record `ExprImpl` implements every interface — same approach as
`MqlExpression` in `com.mongodb.client.model.mql`. Factories return the narrowest applicable
interface, so `mul` is reachable only on `NumExprT`/`IntExprT`, `regexMatch` only on
`StrExprT`, the nine date-component methods only on `DateExprT`. The arrow operator (`arrow`
plus typed variants `intArrow`/`numArrow`/.../`arrArrow`) sits on the base `ExprT` because
in MQLv2 arrow is legal on any expression.

**Three tiers per accessor.** Each of field/var/current has an untyped form
(`field("x")` → `ExprT`), a numeric form (`numField("x")` → `NumExprT`), and an integer
form (`intField("x")` → `IntExprT`). Pick the one that matches what you know about the
field.

**Compile-time rejections** — the two issues from the phantom facade that motivated this:

```java
// Phantom — both compile silently
field("a", String.class).mul(lit(""));      // String × String → ExprT<String>
field("a", Long.class).mul(litDate(1000));  // Long × Long-as-date → ExprT<Long>

// Subtyped — both refuse to compile
strField("a").mul(strLit(""));               // ✗ mul is not on StrExprT
intField("a").mul(dateLit(1000));            // ✗ mul wants NumExprT; DateExprT is not a NumExprT
```

**Escape hatch.** A base `ExprT` can be re-typed via the pure-cast refiners `asNum()` /
`asInt()` / `asStr()` / `asBool()` / `asDate()` / `asDoc()` / `asArr()`. They do not change
the AST; they rewrap the same `Expr` in a different interface type.

**Integer overflow.** `IntExprT.add/sub/mul/div(IntExprT)` and `sum`/`avg` over `IntExprT`
all return `IntExprT` statically, but on int64 overflow the MongoDB runtime widens to
`double` or `Decimal128`. The static type tracks the input, not a guarantee about the
output. Same trade-off as `MqlInteger` in `com.mongodb.client.model.mql.*`. `min`/`max`/
`count` and the date-component extractors don't widen — their `IntExprT` returns are exact.

## Side-by-side comparison

Five queries, four ways. (All come from the conformance tests.)

Collection schemas used below:
- `orders`:    `{ customerId: Long, total: Double, status: String }`
- `products`:  `{ name: String, price: Double, category: String }`
- `employees`: `{ name: String, salary: Long, department: { name: String } }`
- `customers`: `{ id: Long, name: String }`

### 1. Match on a collection

```mql
from $orders | match status == "shipped"
```

```java
// Bare AST
new Stage.MatchStage(
    new Stage.FromStageSimple(new Expr.VarRef("orders")),
    new Expr.BinaryOp(BinaryOpType.EQ,
        new Expr.FieldAccess(new Expr.CurrentValue(), "status"),
        new Expr.ValueLit(new Value.VString("shipped"))))

// Untyped
from(var("orders"))
    .match(field("status").eq(lit("shipped")))

// Typed — eq is parametric; no Class<T> witness needed
from(var("orders"))
    .match(field("status").eq(lit("shipped")))

// Subtyped
from(var("orders"))
    .match(strField("status").eq(strLit("shipped")))
```

### 2. Format with arithmetic

```mql
from $products | format {name: name, discounted: price * 0.9}
```

```java
// Bare AST
new Stage.FormatStage(
    new Stage.FromStageSimple(new Expr.VarRef("products")),
    new Expr.DocumentConstructor(List.of(
        Map.entry(new Expr.ValueLit(new Value.VString("name")),
                  new Expr.FieldAccess(new Expr.CurrentValue(), "name")),
        Map.entry(new Expr.ValueLit(new Value.VString("discounted")),
                  new Expr.BinaryOp(BinaryOpType.MUL,
                      new Expr.FieldAccess(new Expr.CurrentValue(), "price"),
                      new Expr.ValueLit(new Value.VDouble(0.9)))))))

// Untyped
from(var("products"))
    .format(doc(
        entry("name",       field("name")),
        entry("discounted", field("price").mul(lit(0.9)))))

// Typed — arithmetic requires a Class<T> witness on field()
from(var("products"))
    .format(doc(
        entry("name",       field("name")),
        entry("discounted", field("price", Double.class).mul(lit(0.9)))))

// Subtyped — numField returns NumExprT; mul is in scope without a witness
from(var("products"))
    .format(doc(
        entry("name",       field("name")),
        entry("discounted", numField("price").mul(numLit(0.9)))))
```

### 3. Group with aggregation

```mql
from $orders | group (customerId=customerId) (orderCount=count($*), totalSpent=sum($->total))
```

```java
// Untyped
from(var("orders"))
    .group(
        List.of(assign("customerId", field("customerId"))),
        List.of(assign("orderCount", count()),
                assign("totalSpent", sum(current().arrow("total")))))

// Typed — sum and count are parametric; no annotations needed
from(var("orders"))
    .group(
        List.of(assign("customerId", field("customerId"))),
        List.of(assign("orderCount", count()),
                assign("totalSpent", sum(current().arrow("total")))))

// Subtyped — numArrow returns NumExprT; sum accepts NumExprT
from(var("orders"))
    .group(
        List.of(assign("customerId", field("customerId"))),
        List.of(assign("orderCount", count()),
                assign("totalSpent", sum(current().numArrow("total")))))
```

### 4. Multi-stage with dot traversal

```mql
from $employees | match salary > 80000 | format {name, dept: department.name}
```

```java
// Untyped
from(var("employees"))
    .match(field("salary").gt(lit(80000L)))
    .format(doc(
        entry("name", field("name")),
        entry("dept", field("department").field("name"))))

// Typed — gt is parametric; field returns ExprT<Object>; no witnesses needed
from(var("employees"))
    .match(field("salary").gt(lit(80000L)))
    .format(doc(
        entry("name", field("name")),
        entry("dept", field("department").field("name"))))

// Subtyped — intField for salary; docField then strField to type the traversal
from(var("employees"))
    .match(intField("salary").gt(intLit(80000L)))
    .format(doc(
        entry("name", field("name")),
        entry("dept", docField("department").strField("name"))))
```

### 5. Left-outer join

```mql
from c=$customers
  | join leftOuter o=$orders (c.id == o.customerId)
  | format {customer: c.name, total: o.total}
```

```java
// Untyped
from(entry("c", var("customers")))
    .join(JoinType.LEFT_OUTER, "o", var("orders"),
        field("c").field("id").eq(field("o").field("customerId")))
    .format(doc(
        entry("customer", field("c").field("name")),
        entry("total",    field("o").field("total"))))

// Typed — eq is parametric; field chains return ExprT<Object>
from(entry("c", var("customers")))
    .join(JoinType.LEFT_OUTER, "o", var("orders"),
        field("c").field("id").eq(field("o").field("customerId")))  
    .format(doc(
        entry("customer", field("c").field("name")),
        entry("total",    field("o").field("total"))))

// Subtyped — typed field accessors on DocExprT recover the leaf type for the predicate
from(entry("c", var("customers")))
    .join(JoinType.LEFT_OUTER, "o", var("orders"),
        docField("c").intField("id").eq(docField("o").intField("customerId")))
    .format(doc(
        entry("customer", field("c").field("name")),
        entry("total",    field("o").field("total"))))
```

## Trade-offs summary

|                                  | Bare AST | Untyped facade | Typed facade | Subtyped facade |
|----------------------------------|----------|----------------|--------------|-----------------|
| Lines per query (median)         | ~2.5× (up to 7× for nested expressions like `let`) | 1× | 1× | 1× |
| Reads like MQLv2 source          | ✗        | ✓              | ✓ (mostly)   | ✓ (mostly)      |
| Compile-time checks              | none     | none           | `match`, arithmetic; loose elsewhere | `match`, arithmetic, string/date ops; method-name gating |
| `Class<T>` witnesses required    | n/a      | none           | for arithmetic with `field`/`var`/`current` of unknown type | none (use typed factory: `intField`, `intVar`, `intCurrent`) |
| Mistakes surface at              | server   | server         | compile (some) + server (rest) | compile (more) + server (rest) |
| Encourages explicit typing       | no       | no             | yes (forces commit on arithmetic) | yes (typed factory names carry the type) |
| Builder syntax noise             | high     | low            | low          | low             |
| Mixed-type comparisons accepted  | yes      | yes            | yes (parametric `eq`) | yes (parametric `eq`) |
| Arithmetic on strings caught?    | no       | no             | no           | **yes**         |
| Date confused with Long caught?  | no       | no             | no (dates wrap as `ExprT<Long>`) | **yes** (`DateExprT` is not `NumExprT`) |

The typed facade's payoff is concentrated in three places: `match()` (must be Boolean),
arithmetic (must agree on `T`), and function calls (typed results like `count()→Long`,
`avg()→Double`). Everywhere else, the typing is essentially loose — `eq`/`ne`/etc. are
parametric on the other side, so `field("status").eq(lit("paid"))` compiles without
annotation. The visible cost in user code is mostly `Class<T>` witnesses on the
arithmetic-side `field`/`var`/`current` calls.

The subtyped facade gates operations by subtype: `StrExprT` has no `mul`, `DateExprT` is
not a subtype of `NumExprT`, so confusing dates with numbers or strings with numbers
fails to compile. The cost is a wider set of factory names (`intLit`, `strLit`, `dateLit`,
`intField`, `dateField`, `intCurrent`, `intVar`); the two bugs the phantom facade silently
admitted become compile errors.

## Running the tests

A mongod with the experimental `mqlv2` command must be reachable. The standard driver
test fixture (`org.mongodb.test.uri`) is honored; default is `mongodb://localhost:27017`.

```bash
./gradlew :driver-mqlv2:test                          # all driver-mqlv2 tests
./gradlew :driver-mqlv2:test --tests "*Conformance*"  # AST conformance only
./gradlew :driver-mqlv2:test --tests "*FacadeTest*"   # all three facades
./gradlew :driver-mqlv2:check                         # tests + checkstyle + spotbugs
```

Test count: **80** across five files —
- `SerializerTest` (6, no mongod)
- `Mqlv2ConformanceTest` (17, bare AST against server)
- `UntypedFacadeTest` (19, facade AST + server)
- `TypedFacadeTest` (19, typed facade AST + server)
- `SubtypedFacadeTest` (19, subtyped facade AST + server; includes date extractor and comparison tests)

Every conformance test asserts both **AST equivalence with the bare form** (`equals` on
the underlying record graphs) and **`BsonDocument` result equality against the live
server**, so the four test files exercise queries by four different construction styles
and prove they all produce identical results.
