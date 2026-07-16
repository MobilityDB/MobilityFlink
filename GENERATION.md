# MobilityFlink generation — the canonical per-binding generator policy

This document is the contract for how MobilityFlink is generated, under the ecosystem-wide
per-binding generator policy.

## The policy (ecosystem-wide)

Every MobilityDB language/surface binding is a **pure projection of the MEOS-API catalog**,
and **each binding owns its own generator, in its own repo**, in a canonical layout. The
single source of truth is the **catalog** (`MEOS-API/output/meos-idl.json`, generated from
the MEOS C headers). A binding is an independent, plug-and-play module that owns its
generation.

Each binding repo satisfies the same invariants: in-repo generator; catalog/jar input from
a specific MobilityDB commit; thin language projection; full automation toward a
zero-hand-written surface (generate-then-retire; the last green-CI version is the
equivalence probe).

## MobilityFlink scope: generated MEOS facades over the JMEOS surface

MobilityFlink is a **consumer** binding: it binds the **JMEOS jar** (the JVM FFI projection
of the catalog), not MEOS-API directly. Its generator **`tools/codegen_facades.py`** reads
the bundled JMEOS raw-FFI surface (intersected with the streaming-relevance baseline) and
emits the `org.mobilitydb.meos.MeosOps*` 1:1 forwarder facades the Flink processor consumes.
The facades are a *consumer* projection (they live here, not in JMEOS, so the JMEOS FFI line
and the facade line do not diverge).

## The streaming-relevance baseline (generator input)

`tools/codegen_facades.py` emits a facade only for functions in the
**streaming-relevant** tiers, read from `tools/baseline/streaming-relevance-baseline.json`.
That baseline is itself **generated and reproducible** — it is not a hand-maintained
list. It is produced by **`tools/classify_streaming_relevance.py`**, a deterministic
classifier: the tier of a function is decided purely by its name, its object-model role,
and its number of temporal parameters (zero per-function judgement), so the same MEOS
catalog always yields the same baseline.

The full input chain, from upstream MobilityDB master:

```
MobilityDB @ master
  → provision-meos (MEOS-API/run.py over the MEOS headers)  → meos-idl.json  (the catalog)
  → tools/classify_streaming_relevance.py                   → tools/baseline/streaming-relevance-baseline.json
  → tools/codegen_facades.py  (jar-surface ∩ baseline)      → org.mobilitydb.meos.MeosOps* facades
```

CI derives the baseline from the master catalog with `tools/regen_baseline.sh
<path-to-meos-idl.json>` before the build; the baseline is gitignored, not committed.
Because the classifier is deterministic, the same catalog regenerates it byte-for-byte, so
a local refresh is: build the catalog from master with MEOS-API, then run the same script.

## Generate-then-retire — the green-CI version is the probe

Hand-written facades/glue are replaced by the generated forwarders **family by family,
never wipe-first**: regenerate, build green, **prove generated ⊇ hand** against the **last
green-CI version** (the test suite + the streaming benchmark), then retire the hand path.

The `MeosOps*` facades are emitted at build time and are **not committed**: Maven
`generate-sources` runs `tools/codegen_facades.py` into `target/generated-facades`, and
`build-helper` adds it as a source root. Only the generator and its `tools/` classifier are
tracked; the catalog, the baseline and the JMEOS jar are all derived in CI, and the sole
hand-written class under `org.mobilitydb.meos` is `MeosSetSetJoin`.

## Surface match

The JMEOS jar is built from **JMEOS `main`** in CI against the master-derived catalog and
installed as `org.jmeos:meos:1.0`; the generator reads its raw-FFI surface. The `libmeos.so`
the smoke tests load is built from the **same** master catalog, so the jar surface, the
facades and the native library all track master together — surface-match by construction,
with no committed jar or pinned commit to drift.
