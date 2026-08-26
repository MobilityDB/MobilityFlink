# Architecture — MobilityFlink SQL PoC

This document describes the architecture of the PoC that makes MobilityFlink
usable directly from SQL (syntax identical to MobilityDB), without writing
any Java.

Intended audience: contributors picking up, extending, or maintaining this
code. For SQL usage from an end-user perspective, see `README.md`.

---

## 1. Goal and general principle

MobilityFlink exposes MobilityDB types (`FloatSpan`, `TBox`, `STBox`,
`TInt`/`TFloat`, ...) through JMEOS, a Java binding of the native MEOS
library (C, via JNR-FFI). This PoC adds a SQL layer on top of that, so these
types and their operations become usable as native Flink SQL types/functions
— `SELECT floatspan_lower(f1) FROM ...` instead of Java code.

Every ported MobilityDB type consistently follows the same three-layer
pattern:

```
JMEOS Java type (native, via JNR-FFI)
        │
        ▼
Flink "types" layer   →  makes the type serializable/transportable by Flink
        │
        ▼
Flink "udf" layer      →  exposes MobilityDB functions as SQL UDFs
```

This layering architecture is **mandatory** because it
guarantees that a type knows how to serialize/copy itself correctly (types
layer) before anything is done with it in SQL (udf layer).

---

## 2. Code layout

```
sql/
├── types/
│   ├── floatspan/     ← FloatSpanSerializer, FloatSpanSerializerSnapshot, FloatSpanTypeInfo
│   ├── stbox/          ← STBoxSerializer, STBoxSerializerSnapshot, STBoxTypeInfo
│   ├── tbox/            ← TBoxSerializer, TBoxSerializerSnapshot, TBoxTypeInfo
│   └── tnumber/         ← TNumberSerializerSnapshot (abstract),
│                           TIntSerializer/TypeInfo/Snapshot,
│                           TFloatSerializer/TypeInfo/Snapshot
├── udf/
│   ├── floatspan/      ← FloatSpanContains, FloatSpanOverlaps, FloatSpanLower,
│   │                       FloatSpanUpper, FloatSpanWidth, FloatSpanDistance,
│   │                       FloatSpanExtent (aggregate)
│   ├── stbox/            ← STBoxHasXY, STBoxHasT, STBoxContains, STBoxOverlaps,
│   │                        STBoxExpandSpace, STBoxExtent (aggregate), ...
│   ├── tbox/               ← TBoxHasX, TBoxHasT, TBoxToFloatSpan, TBoxContains,
│   │                          TBoxOverlaps, TBoxUnion, TBoxIntersection,
│   │                          TBoxExtent (aggregate), ...
│   └── tnumber/            ← TNumberAdd, TNumberSub, TFloatTAvg (aggregate),
│                              TNumberTypeInferenceSupport (helper), ...
├── MobilityFlinkSQL.java    ← central registration point for all UDFs
├── FloatSpanSQLTest.java    ← runnable demo, one class per type
├── STBoxSQLTest.java
├── TBoxSQLTest.java
└── TNumberSQLTest.java
```

Every type has a mirrored folder under `types/` and under `udf/`. This is
the pattern to reproduce for any new type added to the PoC.

---

## 3. `types` layer — making a JMEOS type usable by Flink

### 3.1 The three mandatory classes

For each type (`X` = `FloatSpan`, `TBox`, `STBox`, `TInt`, `TFloat`, ...):

| Class | Role |
|---|---|
| `XSerializer` | `extends TypeSerializer<X>` — serializes/copies/deserializes the object |
| `XSerializerSnapshot` | `extends SimpleTypeSerializerSnapshot<X>` — versions the serializer for Flink state recovery |
| `XTypeInfo` | `extends TypeInformation<X>` — describes the type for the DataStream API (`fromCollection`, etc.) |

### 3.2 Text round-trip: the serialization mechanism

Every serializer in this PoC follows the same principle: **the only point
where a JMEOS object crosses the Java native boundary is a string**, never
a raw native pointer.

```java
serialize(value, target)   →  target.writeUTF(value.<text_method>())
deserialize(source)        →  new X(source.readUTF())
copy(from)                 →  new X(from.<text_method>())   // never copy the pointer
```

**Central pitfall: the text method's name is not uniform across types.**
Some JMEOS classes override `toString()` (e.g. `FloatSpan`), others expose a
dedicated method instead (`to_string()`, `as_wkt()`) and leave `toString()`
inheriting the default `Object` behavior (`ClassName@hashcode`). **Never
assume** a class overrides `toString()` — verify first with an isolated
micro-test (see `PITFALLS.md`, "Text round-trip" section).

### 3.3 `XSerializerSnapshot` — uniform pattern

Every `*SerializerSnapshot` class in this PoC uses
`SimpleTypeSerializerSnapshot`, never a hand-rolled implementation of
`TypeSerializerSnapshot`:

```java
public class XSerializerSnapshot extends SimpleTypeSerializerSnapshot<X> {
    public XSerializerSnapshot() {
        super(() -> XSerializer.INSTANCE);
    }
}
```

This is a deliberate choice (see `PITFALLS.md`): a naive manual
implementation of `resolveSchemaCompatibility()` tends to always return
`compatibleAsIs()` without actually comparing the old and new serializer —
a silent bug that `SimpleTypeSerializerSnapshot` avoids natively.

### 3.4 The `tnumber` case: a generic hierarchy

`TInt` and `TFloat` are two interfaces under `TNumber`, each with several
concrete sub-forms (`Inst`/`Seq`/`SeqSet`). To avoid duplicating
serialization logic between the two, the `types/tnumber` layer introduces a
generic base class:

```java
public abstract class TNumberSerializerSnapshot<T extends TNumber>
        extends SimpleTypeSerializerSnapshot<T> {
    protected TNumberSerializerSnapshot(Supplier<? extends TypeSerializer<T>> serializerSupplier) {
        super(serializerSupplier);
    }
}
```

`TIntSerializerSnapshot`/`TFloatSerializerSnapshot` extend this base and
only provide a constructor pointing to their own `XSerializer.INSTANCE`.
**This generic level is the only factoring applied within `types/`** — it
does not extend to `FloatSpan`, `TBox`, `STBox`, which have no concrete
sub-forms to manage and deliberately remain independent from one another.

---

## 4. `udf` layer — exposing MobilityDB functions as SQL

### 4.1 Principle: one UDF class = one operation, not one per type combination

Each UDF class encapsulates **a single logical SQL function**
(`floatspan_contains`, `tAdd`, `stbox_expand`, ...) with as many `eval()`
overloads as there are argument type combinations to cover:

```java
public class FloatSpanContains extends ScalarFunction {
    public Boolean eval(FloatSpan s, FloatSpan other) { ... }
    public Boolean eval(FloatSpan s, Float value) { ... }

    @Override
    public TypeInference getTypeInference(DataTypeFactory f) { ... }
}
```

We **never** create one UDF class per (type, operation) pair — that would
double the class count for `tint`/`tfloat` for no reason.

### 4.2 `getTypeInference()` — two rules to remember

1. **As soon as `getTypeInference()` is overridden**, Flink requires the
   explicit strategies needed for that function kind:
    - `ScalarFunction`: `inputTypeStrategy` + `outputTypeStrategy`.
    - `AggregateFunction`: `inputTypeStrategy` + `outputTypeStrategy`, and
      **`accumulatorTypeStrategy` if and only if** the accumulator contains a
      non-standard field Flink cannot describe by reflection. If the
      accumulator only has standard types (`String`, `Long`,
      `List<String>`...), **do not override `getTypeInference()` at all** —
      let Flink perform automatic extraction.
2. **A `transient` field in an accumulator is invisible to Flink's
   automatic extraction.** If such a field still needs to be described, use
   `@DataTypeHint("RAW")` instead of `transient` — see `PITFALLS.md`.

### 4.3 `TNumberTypeInferenceSupport` — factoring out type inference

For `tnumber`, building the `TypeInference` (repeated RAW types, `tint`/
`tfloat` × scalar/tnumber combinations) is factored into a dedicated static
helper:

```java
sql.udf.tnumber.TNumberTypeInferenceSupport
    .TINT_TYPE / .TFLOAT_TYPE          // pre-built RAW DataTypes
    .binaryNumberTNumber(...)          // helpers for recurring patterns
    .unarySameType(...)
```

Design rule to follow when extending this PoC:

- **Only factor out `TypeInference` construction** (metadata), never
  execution logic (`eval()`). Flink resolves `eval()` methods via
  reflection over the concrete argument classes — a generic Java hierarchy
  on the execution side is fragile at runtime.
- **A UDF class must never depend on another UDF class** for its
  `TypeInference` (e.g. `STBoxContains` must not call a method hosted
  inside `STBoxHasXY`). Any logic shared between UDFs belongs in a
  dedicated helper (`XTypeInferenceSupport`), never in a concrete UDF
  class.

### 4.4 The `AggregateFunction` case — the tightest constraint in this PoC

Aggregates (`FloatSpanExtent`, `TBoxExtent`, `STBoxExtent`, `TFloatTAvg`)
are the trickiest part of the whole architecture:

**Absolute rule: a native JMEOS pointer (`jnr.ffi.Pointer`) must never be a
direct field of an `Accumulator`.**

Flink routes the accumulator through its state mechanism (Kryo
serialization by default, even in local execution, as soon as `getValue()`
is called between two `accumulate()` calls). A JNR-FFI `Pointer` is not an
opaque serializable type — Kryo attempts to serialize its full internal
structure (memory segments, JNR runtime) via reflection, which either
produces a JDK module error or — worse — reconstructs an object that no
longer points to a valid native memory address, leading to a native
`SIGSEGV` (a full, non-catchable JVM crash).

**Mandatory pattern for any future MEOS aggregate:**

```java
public class XExtent extends AggregateFunction<X, XExtent.Accumulator> {

    public static class Accumulator {
        public List<String> values = new ArrayList<>();  // fully serializable
    }

    public void accumulate(Accumulator acc, X value) {
        if (value == null) return;
        acc.values.add(value.<text_method>());            // never store a Pointer
    }

    @Override
    public X getValue(Accumulator acc) {
        if (acc.values.isEmpty()) return null;
        Pointer state = null;                              // native pointer LOCAL to this call
        for (String v : acc.values) {
            state = functions.X_transfn(state, new X(v).get_inner());
        }
        Pointer result = functions.X_finalfn(state);        // or a fold over a binary operator
        return result == null ? null : new X(result);
    }
}
```

The state Flink accumulates is always a collection of text representations;
the native MEOS state is **fully rebuilt on every call to `getValue()`**,
never kept between two calls. This is a deliberate performance/robustness
trade-off for this PoC (see `PITFALLS.md`).

### 4.5 `extent` on "box" types: no dedicated function

`FloatSpan`, `TBox`, `STBox` have **no** working native `*_extent_transfn`
function exposed in this JMEOS binding (`tspatial_extent_transfn` does
exist but is reserved for mobile spatiotemporal objects — `TPoint` — not
for boxes). For these types, `extent` is implemented as a **fold over the
union operator** (`union_stbox_stbox`, `union_tbox_tbox`, `span_union`
depending on the type), which is mathematically exact: the union of two
boxes *is*, by definition, the smallest box enclosing both.

A consequence that must be documented at every call site: union-based
aggregation requires **homogeneous dimensionality** across the whole
aggregated column (e.g. all `stbox` values with both X and T, or none) —
mixing boxes of different dimensionalities makes the union fail with an
explicit native error.

`tavg` (time-weighted temporal average) is the only aggregate in this PoC
with a genuine dedicated `transfn`/`finalfn` pair
(`tnumber_tavg_transfn` / `tnumber_tavg_finalfn`), distinct from
`tsum_transfn` — do not confuse the two despite the similar naming.

---

## 5. `MobilityFlinkSQL.java` — single registration point

All UDFs are registered here, under their MobilityDB SQL name (not their
Java class name):

```java
tEnv.createTemporaryFunction("floatspan_contains", FloatSpanContains.class);
tEnv.createTemporaryFunction("tbox_extent",         TBoxExtent.class);
```

Any new UDF must be registered here to become usable from SQL — an
unregistered UDF class compiles fine but stays invisible to `sqlQuery()`.

---

## 6. `*SQLTest.java` — runnable demos, one per type

Each type has its own standalone `main()` class (`FloatSpanSQLTest`,
`TBoxSQLTest`, `STBoxSQLTest`, `TNumberSQLTest`) that:

1. builds a small in-memory dataset via the JMEOS constructor
   (`new FloatSpan("...")`, `new TFloatSeq("...")`, ...);
2. exposes it as a temporary view via `XTypeInfo.INSTANCE` +
   `DataTypes.RAW(X.class, XSerializer.INSTANCE)`;
3. runs SQL queries covering accessors, binary operators, transformations,
   and aggregates.

These classes play the role a Jupyter notebook could have played, without
the associated risk: if a query triggers a native `SIGSEGV`, you re-run a
`main()`, not a kernel that loses its entire session state (see the
decision at the end of `PITFALLS.md`).

---

## 7. Extending this PoC to a new type or a new function

**New type:**
1. Create `sql/types/<type>/` with the three classes (§3.1). First verify,
   with an isolated micro-test, which JMEOS method produces the correct
   text for the round-trip (§3.2).
2. Create `sql/udf/<type>/` with a minimal representative subset: one
   accessor, one binary operator, possibly one aggregate — not the entire
   MobilityDB catalog at once.
3. Register the UDFs in `MobilityFlinkSQL.java`.
4. Add `<Type>SQLTest.java`.

**New function on an existing type:**
1. Verify with `javap -classpath jar/JMEOS.jar functions.functions | grep -i <keyword>`
   that the expected native function actually exists under that exact name
   — never assume a "logical" name is the right one (see `PITFALLS.md`).
2. Write the UDF class following the §4.1/§4.2 pattern.
3. If the operation is an aggregate, strictly apply the §4.4 pattern.
4. Register it in `MobilityFlinkSQL.java`.

For details on the pitfalls encountered and their rationale, see
`PITFALLS.md`.