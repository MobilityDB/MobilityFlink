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

## Generate-then-retire — the green-CI version is the probe

Hand-written facades/glue are replaced by the generated forwarders **family by family,
never wipe-first**: regenerate, build green, **prove generated ⊇ hand** against the **last
green-CI version** (the test suite + the streaming benchmark), then retire the hand path.

The `MeosOps*` facades are emitted at build time and are **not committed**: Maven
`generate-sources` runs `tools/codegen_facades.py` into `target/generated-facades`, and
`build-helper` adds it as a source root. Only the generator, its `tools/baseline/`, and the
bundled jar are tracked; the sole hand-written class under `org.mobilitydb.meos` is
`MeosSetSetJoin`.

## Surface match

The bundled JMEOS jar (`flink-processor/jar/JMEOS.jar`) is committed and consumed
system-scoped; the generator reads its raw-FFI surface. The `libmeos.so` it links must be
built from the **same MobilityDB commit** the JMEOS surface was generated against —
surface-match, else runtime symbol faults. That commit is the *catalog/surface* input for
the whole binding.
