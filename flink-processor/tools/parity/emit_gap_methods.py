#!/usr/bin/env python3
"""Emit forwarding facade methods for MEOS public-surface functions that the
generated MeosOps* facade does not yet expose.

Each method forwards to its `functions.GeneratedFunctions` export using the
exact JMEOS signature (captured via `javap`), so the output compiles by
construction.  Family and nature (scalar / sequence-constructor / tiling) are
recorded in the Javadoc; the wiring tier is governed by the wirings layer, not
by the presence of the forwarder.

Run from flink-processor/:
    javap -classpath jar/JMEOS.jar -p functions.GeneratedFunctions > /tmp/gen_sigs.txt
    python3 tools/parity/emit_gap_methods.py /tmp/gen_sigs.txt
"""
import re, os, sys, glob

HERE = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
INC = os.environ.get("MEOS_INCLUDE", "/home/esteban/src/MobilityDB/meos/include")
PUBLIC_HEADERS = ["meos.h", "meos_geo.h", "meos_cbuffer.h", "meos_npoint.h", "meos_pose.h", "meos_rgeo.h"]
FACADE = os.path.join(HERE, "src/main/java/org/mobilitydb/flink/meos")
OUT = os.path.join(FACADE, "MeosOpsParityGaps.java")
# Forwarder facades are themselves derived; the "already covered" baseline is the
# tier-aware generated OO classes only, so the generator reproduces idempotently.
DERIVED = {"MeosOpsParityGaps.java", "MeosOpsSqlSurface.java"}

_DECL = re.compile(r'^\s*extern\s+.+?\b([a-z][A-Za-z0-9_]*)\s*\(', re.M)
_PUBSTATIC = re.compile(r'public static [A-Za-z0-9_.<>\[\]]+ ([a-z0-9_]+)\(')
_SIG = re.compile(r'public static (\S+) (\w+)\(([^)]*)\);')


def family(name, fam):
    return fam.get(name, "meos.h")


def nature(name):
    if name.endswith("_make") or "_from_base_" in name or name.endswith("make_coords"):
        return "whole-sequence constructor — not a per-event op"
    if name.endswith("_tiles"):
        return "multidimensional tiling (windowed)"
    return "scalar / stateless"


def main():
    sigfile = sys.argv[1] if len(sys.argv) > 1 else "/tmp/parity/gen_sigs.txt"
    fam, pub = {}, set()
    for h in PUBLIC_HEADERS:
        for n in set(_DECL.findall(open(os.path.join(INC, h)).read())):
            pub.add(n); fam.setdefault(n, h)
    facade = set()
    for f in glob.glob(os.path.join(FACADE, "MeosOps*.java")):
        if os.path.basename(f) in DERIVED:
            continue
        facade |= set(_PUBSTATIC.findall(open(f).read()))
    # all JMEOS signatures, grouped by name
    sigs = {}
    for line in open(sigfile):
        m = _SIG.search(line)
        if m:
            ret, name, args = m.group(1), m.group(2), m.group(3).strip()
            sigs.setdefault(name, []).append((ret, args))
    missing = sorted(n for n in (pub & set(sigs)) - facade)

    L = ["package org.mobilitydb.flink.meos;", "",
         "/**", " * Forwarding facade methods for MEOS public-surface functions not emitted",
         " * by the tier-aware code generator. Each method delegates to its JMEOS",
         " * {@code functions.GeneratedFunctions} export under the shared",
         " * {@link MeosOpsRuntime#MEOS_AVAILABLE} guard.",
         " */",
         "public final class MeosOpsParityGaps {", "",
         "    private MeosOpsParityGaps() { /* utility */ }", ""]
    count = 0
    for name in missing:
        for ret, args in sigs[name]:
            params = [a.strip() for a in args.split(",")] if args else []
            decl = ", ".join(f"{t} arg{i}" for i, t in enumerate(params))
            call = ", ".join(f"arg{i}" for i in range(len(params)))
            ret_kw = "" if ret == "void" else "return "
            L += [f"    /** MEOS {{@code {name}}} — {family(name, fam)} · {nature(name)}. */",
                  f"    public static {ret} {name}({decl}) {{",
                  f"        if (!MeosOpsRuntime.MEOS_AVAILABLE)",
                  f'            throw new UnsupportedOperationException("{name} requires libmeos'
                  f' — set -Dmobilityflink.meos.enabled=true");',
                  f"        {ret_kw}functions.GeneratedFunctions.{name}({call});",
                  "    }", ""]
            count += 1
    L.append("}")
    open(OUT, "w").write("\n".join(L) + "\n")
    print(f"missing public-surface functions: {len(missing)}")
    print(f"forwarding methods emitted:       {count}")
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
