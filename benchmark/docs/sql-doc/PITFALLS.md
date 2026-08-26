# Pitfalls — MobilityFlink SQL PoC

This document catalogues the pitfalls discovered while building this PoC,
written as general, reusable rules rather than a debugging journal. Each
section states the rule first, then the reasoning/evidence behind it.

Read this before writing a new `types/` or `udf/` class — most of what
follows will save you a debugging cycle you'd otherwise repeat.

---

## 1. Serialization layer (`types/`)

### 1.1 Never assume a JMEOS class overrides `toString()`

**Rule:** before writing a `Serializer`, check with an isolated test which
method actually returns the correct text representation of the object.
Don't assume `toString()` — some JMEOS classes override it (e.g.
`FloatSpan`), others don't and expose a dedicated method instead
(`to_string()`, `as_wkt()`), leaving `toString()` to silently fall back to
`Object`'s default (`ClassName@hashcode`).

**Evidence:** `STBoxSerializer.serialize()` used `value.toString()`, which
produced `types.boxes.STBox@4c398c80` instead of a real WKT-like string.
This corrupted the round-trip silently until deserialization failed with
`Invalid input syntax for type double` — a downstream, misleading error far
from the actual bug.

**Verification pattern:**
```java
STBox b = new STBox("STBOX X(((0.0,0.0),(10.0,10.0)))");
System.out.println("toString(): " + b.toString());     // prints STBox@hash if broken
System.out.println("to_string(): " + b.to_string());    // check the real method
System.out.println(new STBox(b.to_string()));            // confirm round-trip works
```

Apply this check to every new type before trusting its `Serializer`,
including ones that appear to work — verify with a real round-trip test,
not just a successful compile.

### 1.2 `TypeSerializerSnapshot`: use `SimpleTypeSerializerSnapshot`, not a manual implementation

**Rule:** for a stateless, singleton serializer with no internal
configuration (which is the case for every type in this PoC), extend
`SimpleTypeSerializerSnapshot<T>` rather than implementing
`TypeSerializerSnapshot<T>` by hand.

**Evidence:** a hand-written implementation is easy to get subtly wrong.
The first version written for this PoC read the persisted class name in
`readSnapshot()` and then discarded it immediately, while
`resolveSchemaCompatibility()` unconditionally returned `compatibleAsIs()`
regardless of what `oldSnapshot` actually contained — meaning the
compatibility check never checked anything. `SimpleTypeSerializerSnapshot`
performs this comparison correctly out of the box.

### 1.3 Generic type variance breaks naive class hierarchies for snapshots

**Rule:** if you want a shared base class for a family of related types
(e.g. `TNumberSerializerSnapshot<T extends TNumber>` shared by `TInt` and
`TFloat`), the base class must be **generic and abstract**. A concrete class
fixed on one type parameter (`extends SimpleTypeSerializerSnapshot<TNumber>`)
cannot be subclassed as `extends TNumberSerializerSnapshot<TFloat>` — Java
generics are invariant, so `TypeSerializerSnapshot<TFloat>` is not a
subtype of `TypeSerializerSnapshot<TNumber>` even though `TFloat extends
TNumber`.

**Correct shape:**
```java
public abstract class TNumberSerializerSnapshot<T extends TNumber>
        extends SimpleTypeSerializerSnapshot<T> {
    protected TNumberSerializerSnapshot(Supplier<? extends TypeSerializer<T>> serializerSupplier) {
        super(serializerSupplier);
    }
}
```
Concrete subclasses (`TFloatSerializerSnapshot`, `TIntSerializerSnapshot`)
must still expose a public no-arg constructor — Flink instantiates snapshot
classes by reflection when restoring persisted state.

---

## 2. Discovering the right native function

### 2.1 A "logical" function name is not proof it's the right one — verify with `javap`

**Rule:** before wiring a JMEOS/MEOS function into a UDF, confirm it
actually exists in the loaded native library, under the exact name you
expect:
```bash
javap -classpath jar/JMEOS.jar functions.functions | grep -i <keyword>
```

**Evidence, repeated across this PoC:**
- `expand_stbox(STBox, STBox)` compiled and ran without error, but its
  implementation silently ignored both parameters and returned a plain
  copy of `this` — a stub, not a real expansion. The actual working method
  was `expand_numerical(Number)`, found only by exploring `STBox`'s method
  list directly.
- `TNumber.mul(Object other)` is a single dispatcher method (not three
  overloads as one might assume from its javadoc). Calling it with a
  `Float` argument threw `UnsatisfiedLinkError: unknown` inside
  `mult_tfloat_float` — the underlying native symbol simply wasn't present
  in the loaded `libmeos.so`, most likely due to a version mismatch between
  the JMEOS binding and the installed native library.
- `functions.tspatial_extent_transfn` exists and compiles fine for `TBox`
  and `STBox`, but fails at runtime with `"The stbox must have X
  dimension"` / `"The value must be a spatiotemporal value"` — this
  function is designed for mobile spatiotemporal objects (`TGeomPoint`/
  `TGeogPoint`), not for boxes, despite its generic-sounding name.
- `tfloat_tsum_transfn` (sum) was mistakenly used instead of
  `tnumber_tavg_transfn` (average) as the transition function for a `tavg`
  aggregate. Both compiled and ran without a Java-level exception, but
  produced a corrupted internal skiplist that crashed the JVM with
  `SIGSEGV` inside `tsequence_tavg_finalfn` — a native memory-safety
  failure, not a catchable exception.

**Takeaway:** an error message that mentions a *different* type than the
one you're working with (e.g. `"stbox must have X dimension"` while
debugging `TBox`) is a strong signal that the wrong `_transfn`/native
function was called — not that your object is malformed.

### 2.2 Always verify a suspicious native call with an isolated test, outside Flink

**Rule:** when a native call misbehaves inside a Flink pipeline, reproduce
it in a standalone `main()` first, calling JMEOS/MEOS functions directly.
This separates "is the native call itself wrong" from "is something about
Flink's execution model interfering" — two very different classes of bugs
that produce similar-looking symptoms.

**Example (function pairing check):**
```java
Pointer state = null;
state = functions.tnumber_tavg_transfn(state, new TFloatSeq("[1.5@2020-06-01, 3.0@2020-06-02]").getNumberInner());
Pointer result = functions.tnumber_tavg_finalfn(state);
System.out.println(new TFloatSeq(result).as_wkt(6));
```
If this crashes, the bug is in the native call itself. If it doesn't, the
bug is downstream, in how Flink handles the resulting state (see §4).

### 2.3 A binary operator can double as an aggregate's transition function when the operation is associative/idempotent by nature

**Rule:** `extent(column)` and `union(a, b)` coincide exactly for box-like
types, because "the smallest box containing A and B" and "the union of A
and B" are the same object by definition for a box. This is *not* true in
general — do not generalize this substitution to aggregates that aren't
naturally reducible to folding a binary operator (e.g. `tavg`, which needs
real intermediate state — sum and count — not just a pairwise reduction).

**Evidence:** no `*_extent_transfn` function works for `FloatSpan`, `TBox`,
or `STBox` in this JMEOS binding (see §2.1). Folding `union_*_*` over the
accumulated values instead produces mathematically correct results for
these types specifically, because they're boxes.

### 2.4 `union_*_*` on boxes: `strict` flag and dimensionality constraints

**Rule:** `union_stbox_stbox`/`union_tbox_tbox` take a third `boolean`
parameter, most likely `strict`. Use `false` for aggregation purposes:
`true` rejects disjoint boxes with `"Result of box union would not be
contiguous"`, which is not the behavior you want when computing a running
`extent` over a stream where values may not overlap.

**Rule:** union-based aggregation requires **homogeneous dimensionality**
across every value being combined — mixing, say, an `STBOX XT` (space +
time) with an `STBOX X` (space only) fails with `"The arguments must be of
the same dimensionality"`. This is a real, permanent constraint of the
implementation, not just a test-data issue — document it wherever a
box-type `extent` aggregate is exposed, and filter/validate accordingly
upstream (e.g. `WHERE stbox_has_xy(f1) = true AND stbox_has_t(f1) = true`).

---

## 3. UDF type inference (`getTypeInference()`)

### 3.1 Overriding `getTypeInference()` requires *all* the strategies that function kind needs — or none at all

**Rule:** `TypeInference.newBuilder()` has no implicit fallback. As soon as
you override `getTypeInference()`:
- A `ScalarFunction` needs `inputTypeStrategy` + `outputTypeStrategy`.
- An `AggregateFunction` needs `inputTypeStrategy` + `outputTypeStrategy`,
  **and** `accumulatorTypeStrategy` if and only if the accumulator has a
  field Flink's default reflection-based extraction cannot describe.

**Evidence:** omitting `accumulatorTypeStrategy` while still overriding
`getTypeInference()` on an `AggregateFunction` produces:
```
ValidationException: Aggregating functions must provide exactly one state type strategy.
```
Conversely, if the accumulator only contains standard types (`String`,
`Long`, `List<String>`, ...), the simplest and most robust fix is to **not
override `getTypeInference()` at all** and let Flink's automatic extraction
handle input, output, and accumulator types by reflection.

### 3.2 A `transient` field is invisible to Flink's automatic type extraction

**Rule:** don't mark an accumulator field `transient` expecting Flink to
just skip it — it disappears entirely from what the extractor sees,
including from the count of "known fields."

**Evidence:**
```
ValidationException: Class 'sql.udf.tnumber.TFloatTAvg$Accumulator' has no fields.
```
This happened because the accumulator's only field (a `Pointer`) was
marked `transient` — Flink's reflection-based extractor ignores `transient`
fields the same way standard Java serialization does, leaving nothing to
describe. If a non-standard field genuinely needs to be part of the
accumulator's described type, use `@DataTypeHint("RAW")` instead — this
tells Flink to treat the field as an opaque type it shouldn't try to
introspect structurally, rather than hiding it from extraction altogether.

### 3.3 The declared output type must match what `eval()`/`getValue()` actually returns

**Rule:** double-check that `outputTypeStrategy` describes the same Java
type your method returns — Flink does not cross-validate this at compile
time.

**Evidence:** an early version of `STBoxExpandSpace` had `eval()` return
`STBox`, while `outputTypeStrategy` declared `DataTypes.RAW(FloatSpan.class,
...)` (a leftover from copy-pasting another class). This compiles without
error and only fails at runtime, typically as a `ClassCastException` or
silent data corruption inside the RAW type's byte buffer — a hard bug to
trace back to its source.

### 3.4 A UDF class must not depend on another UDF class for shared logic

**Rule:** factor `TypeInference`-building logic shared across multiple UDFs
into a dedicated static helper class (`XTypeInferenceSupport`) — never host
it as a "borrowed" static method inside one of the concrete UDF classes
that happens to need it too (e.g. `STBoxContains` calling a method that
physically lives inside `STBoxHasXY`). This creates an accidental coupling
between two functions that are conceptually unrelated, and makes future
renames or removals silently break unrelated UDFs.

---

## 4. Aggregate functions and native pointers

### 4.1 A native pointer must never be a direct field of an `AggregateFunction.Accumulator`

**This is the single most important rule discovered in this PoC.**

**Rule:** never store a `jnr.ffi.Pointer` (or any native handle) directly
in an `Accumulator`. Flink routes accumulators through its state mechanism
— Kryo serialization by default — as soon as `getValue()` is called between
two `accumulate()` calls, which happens routinely even in unwindowed
streaming `GROUP BY`, and always happens under checkpointing.

**Evidence, in order of discovery:**

1. **Round-tripping the native state through WKT text between calls**
   corrupts it. An early version converted the accumulated pointer to WKT
   after every `accumulate()` and reparsed it on the next call — this
   crashed the JVM with `SIGSEGV` inside `skiplist_splice`, because the
   intermediate aggregation state (a MEOS `SkipList`) has no valid text
   representation; only the *final* result does.

2. **Marking the field `transient`** to avoid this hides it from Flink's
   type extractor entirely (see §3.2), producing a `ValidationException`
   before the code even runs.

3. **Annotating the field `@DataTypeHint("RAW")`** satisfies the type
   extractor, but doesn't solve the underlying problem — it only defers it.
   An isolated Kryo round-trip test confirmed this conclusively:
   ```java
   Kryo kryo = new Kryo();
   kryo.setRegistrationRequired(false);
   ByteArrayOutputStream bos = new ByteArrayOutputStream();
   try (Output output = new Output(bos)) { kryo.writeObject(output, acc); }
   ```
   This failed with:
   ```
   KryoException: java.lang.reflect.InaccessibleObjectException:
   Unable to make field private final java.lang.String java.nio.ByteOrder.name accessible
   Serialization trace:
   byteOrder (jnr.ffi.provider.jffi.NativeRuntime)
   runtime (jnr.ffi.provider.jffi.DirectMemoryIO)
   statePtr (sql.udf.tnumber.TFloatTAvg$Accumulator)
   ```
   Kryo attempts to reflectively serialize `Pointer`'s *entire* internal
   object graph — down into JNR-FFI's runtime internals and even JDK-module
   internals — rather than treating it as an opaque byte blob. Even working
   around the JDK module error would not fix the deeper issue: a native
   memory address has no meaningful "copy by value" semantics across a
   serialize/deserialize cycle.

**Mandatory pattern going forward:**
```java
public static class Accumulator {
    public List<String> values = new ArrayList<>();  // fully Flink-serializable
}

public void accumulate(Accumulator acc, X value) {
    if (value == null) return;
    acc.values.add(value.<text_method>());             // never store a Pointer
}

@Override
public X getValue(Accumulator acc) {
    if (acc.values.isEmpty()) return null;
    Pointer state = null;                                // local to this call only
    for (String v : acc.values) {
        state = functions.X_transfn(state, new X(v).get_inner());
    }
    Pointer result = functions.X_finalfn(state);
    return result == null ? null : new X(result);
}
```
The native pointer only ever exists as a local variable inside
`getValue()`, fully rebuilt from scratch on every call, never persisted
between calls and never exposed to Flink's state backend.

**Trade-off to document:** this recomputes the native aggregate from
scratch on every emission (O(n) in the number of accumulated elements
rather than incremental), in exchange for actual correctness across
checkpoints and multi-call sequences. Acceptable for this PoC; worth
revisiting with a smarter caching strategy if this code moves toward
production.

### 4.2 Streaming re-invocation of `getValue()` can also break a native finalizer, independent of the pointer/Kryo issue

**Rule:** in a continuous streaming aggregate (unwindowed `GROUP BY`),
`getValue()` is called after every new accumulated row, not just once at
the end. If a native `finalfn` is only meant to be called once, on a
"final" state — a common assumption for a PostgreSQL-style aggregate — this
mismatch can itself be a source of crashes.

**Note:** in this PoC, this hypothesis was tested and *ruled out* for
`tnumber_tavg_finalfn` specifically — an isolated Java test calling
`transfn`/`finalfn` repeatedly on a growing state, without ever crossing
Flink's serialization boundary, ran without crashing. The actual root cause
turned out to be §4.1 (the Kryo round-trip), not repeated `finalfn` calls
on a live state. Still worth checking as a *separate* hypothesis for future
aggregates, since the two failure modes produce similar-looking `SIGSEGV`
symptoms and must be isolated independently before concluding which one is
at fault.

---

## 5. General debugging methodology used throughout this PoC

1. **Reproduce native calls outside Flink first.** A standalone `main()`
   calling JMEOS methods directly separates "the native call itself is
   wrong" from "something about Flink's execution model is interfering."
2. **Don't trust that a function compiles means it's correct.** Several
   bugs in this PoC (`expand_stbox`, `tsum_transfn` vs `tavg_transfn`,
   `tspatial_extent_transfn`) compiled and even partially ran without any
   Java-level exception.
3. **A crash that mentions the wrong type name, or crashes deep in native
   code, is a strong signal you're calling the wrong native function** —
   not that your object is malformed. Cross-check with `javap` before
   assuming the object itself is at fault.
4. **`SIGSEGV` means "stop and isolate," not "add a try/catch."** A native
   segfault kills the whole JVM and cannot be caught from Java. When one
   occurs, drop back to an isolated test immediately rather than iterating
   inside the full Flink pipeline.
5. **This crash risk is also why this PoC ships runnable `*SQLTest.java`
   demos instead of a Jupyter notebook.** A `SIGSEGV` kills a notebook
   kernel and loses all prior cell state; a crashed `main()` is just
   re-run.