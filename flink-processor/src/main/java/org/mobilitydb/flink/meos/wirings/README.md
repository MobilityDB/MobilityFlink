# DataStream wirings for the generated MEOS facades

This package supplies thin, generic Flink-DataStream wrappers around
the generated `org.mobilitydb.flink.meos.MeosOps*` facades, organized
per **streaming tier** (per
`tools/codegen/meos-ops-manifest.json` + `tools/codegen/meos-ops-free-manifest.json`):

| Tier | Wiring class(es) here | Status in this package |
|---|---|---|
| `stateless` | [`MeosStatelessMap`](MeosStatelessMap.java) (generic `MapFunction`) · [`MeosStatelessFilter`](MeosStatelessFilter.java) (generic `FilterFunction`) | ✅ shipped |
| `bounded-state` | [`MeosBoundedStateMap`](MeosBoundedStateMap.java) (generic `KeyedProcessFunction` with `ValueState<byte[]>` per key — state crosses the operator boundary as MEOS-WKB/WKT bytes so checkpoints/rescaling/savepoints are safe; raw `Pointer` never leaves the JVM-local operator instance) | ✅ shipped |
| `windowed` | `MeosWindowedAggregate` (generic `ProcessWindowFunction`) | next follow-up |
| `cross-stream` | `MeosCrossStreamJoin` (generic `KeyedCoProcessFunction` or interval-join) | next follow-up |
| `io-meta` | covered transitively by the stateless wirings (no state, no window) | n/a |
| `sequence-only` | inherently non-streamable — no wiring | n/a |

The wirings are **generic**: each takes a serializable lambda
forwarding to whichever generated `MeosOps*.f(...)` method the adopter
needs. No per-method boilerplate, no per-method registration —
adopters wire the entire ~800-method `stateless` slice through
`MeosStatelessMap` / `MeosStatelessFilter` without touching this
package.

## Why DataStream rather than Table API

The repo's existing pipeline (`berlinmod/`, `aisdata/`) is
DataStream-API only. Sticking to DataStream avoids adding the
~50 MB `flink-table-planner` runtime dependency to the build matrix.
A Table-API-shaped sibling
(`MeosOpsTableCatalogRegistrar` / `MeosScalarUDF` / `MeosAggregateFunction`)
is a clean follow-up if/when the repo adopts Table API for other
reasons.

## How a generated MEOS call becomes a Flink operator

The pattern is the same across all four tiers:

```java
// 1. Pick the generated MeosOps method
//    (Javadoc tier marker tells you which wiring to use)
boolean overlap = MeosOpsTBox.overlaps_tbox_tbox(boxA, boxB);  // tier = stateless

// 2. Wrap with the matching wiring
MeosStatelessFilter<TboxPair> filter = MeosStatelessFilter.fromIntPredicate(
    pair -> MeosOpsTBox.overlaps_tbox_tbox(pair.a, pair.b));

// 3. Apply to the DataStream
DataStream<TboxPair> overlapping = stream.filter(filter);
```

`MEOS_AVAILABLE` is probed once per JVM by `MeosOpsRuntime`'s static
initializer (shared across all `MeosOps*` and `MeosOpsFree*`
facades). When unavailable, every generated method throws
`UnsupportedOperationException` with a clear message — the wiring
layer doesn't have to handle that itself.

## End-to-end runnable demo

[`demo/MeosWiringsDemoJob.java`](demo/MeosWiringsDemoJob.java) walks
through a 3-stage DataStream pipeline using two of the generated
facades wired through `MeosStatelessMap` + `MeosStatelessFilter`:

1. Parse a stream of TBox WKT strings via
   `MeosOpsFreeCore.tbox_in` (io-meta, no state).
2. Filter to those overlapping a fixed query box via
   `MeosOpsTBox.overlaps_tbox_tbox` (stateless predicate).
3. Serialize each survivor to hex-WKB via
   `MeosOpsTBox.tbox_as_hexwkb` (io-meta, no state).

Run with:

```bash
mvn -q exec:java \
    -Dexec.mainClass=org.mobilitydb.flink.meos.wirings.demo.MeosWiringsDemoJob \
    -Dmobilityflink.meos.enabled=true
```

Output (expected): two `overlapping-tbox-hex` lines (the two input
boxes that overlap the query box), one disjoint box dropped, one
`MeosWirings stateless tier demo` job completion line.

## Coexistence with `berlinmod.MEOSBridge`

`MEOSBridge.java` is the BerlinMOD-specific, hand-written bridge for
the 9-query streaming-form parity matrix — high-level and
query-shaped. The wirings here are low-level and catalog-shaped —
applicable to any of the ~800 stateless or 800 bounded-state
generated facade methods, not just the BerlinMOD-9 subset. Both
share the same `MEOS_AVAILABLE` discipline (`MeosOpsRuntime`).
