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
of the catalog), not MEOS-API directly. Its generator is the shared
**`tools/codegen_jvm.py --engine flink`**, the single generator vendored identically by every
JVM binding (MobilitySpark, MobilityFlink, MobilityKafka); the `flink` and `kafka` engines
emit the `org.mobilitydb.meos.MeosOps*` 1:1 forwarder facades the streaming processors
consume. The facades are a *consumer* projection (they live here, not in JMEOS, so the JMEOS
FFI line and the facade line do not diverge).

## Full surface, grouped by the catalog object model

`codegen_jvm.py --engine flink` emits a facade for **every** function on the bundled JMEOS
`functions.GeneratedFunctions` surface, grouped by the MEOS-API catalog object model: one
`MeosOps<Class>` per object-model class plus one `MeosOpsFree<Header>` per source header for
the free functions, with a shared `MeosOpsRuntime` that probes libmeos once per JVM. Each
forwarder carries a runtime guard: functions whose catalog return type is sequence-typed
(build a whole `TSequence`/`SeqSet`, inherently non-streamable) throw
`UnsupportedOperationException`; all others forward to `GeneratedFunctions` behind the
`MEOS_AVAILABLE` probe. The class/role/header are read straight from the catalog's
`objectModel`, and the sequence check from `returnType.canonical` — no separate classifier.

The full input chain, from upstream MobilityDB master:

```
MobilityDB @ master
  → provision-meos (MEOS-API/run.py over the MEOS headers)  → meos-idl.json  (the catalog)
  → tools/codegen_jvm.py --engine flink  (full jar surface) → org.mobilitydb.meos.MeosOps* facades
```

CI stages the master-derived catalog to `tools/meos-idl.json` before the build; the catalog
is gitignored, not committed.

## Generate-then-retire — the green-CI version is the probe

Hand-written facades/glue are replaced by the generated forwarders **family by family,
never wipe-first**: regenerate, build green, **prove generated ⊇ hand** against the **last
green-CI version** (the test suite + the streaming benchmark), then retire the hand path.

The `MeosOps*` facades are emitted at build time and are **not committed**: Maven
`generate-sources` runs `tools/codegen_jvm.py --engine flink` into `target/generated-facades`,
and `build-helper` adds it as a source root. Only the generator is tracked; the catalog and
the JMEOS jar are all derived in CI, and the sole hand-written class under
`org.mobilitydb.meos` is `MeosSetSetJoin`.

## Surface match

The JMEOS jar is built from **JMEOS `main`** in CI against the master-derived catalog and
installed as `org.jmeos:meos:1.0`; the generator reads its raw-FFI surface. The `libmeos.so`
the smoke tests load is built from the **same** master catalog, so the jar surface, the
facades and the native library all track master together — surface-match by construction,
with no committed jar or pinned commit to drift.

## Regenerating by hand

CI performs the steps below via `provision-meos`. To run them yourself you need a JDK, Maven,
CMake and the MEOS build dependencies.

**1. Derive libmeos and the catalog from MobilityDB master.** Both come from one commit; see
`MEOS-API/GENERATION.md` for the two commands:

```bash
MDB=~/src/MobilityDB                     # checkout at the commit you are deriving from
MEOSAPI=~/src/MEOS-API
cmake -S "$MDB" -B "$MDB/build" -DCMAKE_BUILD_TYPE=Release -DMEOS=ON -DALL=ON
cmake --build "$MDB/build" -j"$(nproc)"
cmake --install "$MDB/build" --prefix "$MDB/.prefix"
cd "$MEOSAPI" && MDB_SRC_ROOT="$MDB" python3 run.py "$MDB/.prefix/include"
```

**2. Build the JMEOS jar against that catalog and install it into the local Maven
repository** under the coordinates this build resolves — `org.jmeos:meos:1.0`:

```bash
cd ~/src/JMEOS                            # JMEOS main
CATALOG="$MEOSAPI/output/meos-idl.json" LIBMEOS="$MDB/.prefix/lib/libmeos.so" \
  tools/regen-from-catalog.sh
mvn install:install-file -Dfile=jar/JMEOS.jar \
  -DgroupId=org.jmeos -DartifactId=meos -Dversion=1.0 -Dpackaging=jar
```

**3. Stage the catalog and build.** `tools/meos-idl.json` is derived, not committed:

```bash
cd ~/src/MobilityFlink
cp "$MEOSAPI/output/meos-idl.json" tools/meos-idl.json
cd flink-processor
mvn -Dmeos.lib.dir="$MDB/.prefix/lib" -Dmeos.enabled=true clean test
```

`generate-sources` runs `tools/codegen_jvm.py --engine flink --catalog ../tools/meos-idl.json
--jar <the installed jar> --out target/generated-facades`, so the `MeosOps*` facades are
regenerated by the build itself. `meos.lib.dir` is where the smoke tests look for
`libmeos.so`, and `meos.enabled` turns them on.
