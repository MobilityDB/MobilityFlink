#!/usr/bin/env python3
"""v3 generator: scale from TBox+STBox (87 fns) to all streamable tiers
across ALL object-model classes on Flink (target ~1,705 fns).

Strategy:
- One Java class per MEOS object-model class (e.g. MeosOpsTFloat,
  MeosOpsTBox, MeosOpsSet, ...) — keeps each file small and navigable.
- Functions appearing in multiple OO classes get emitted in each class
  they're a member of (catalog of the OO model; mirrors PyMEOS multi-class
  pattern).
- Same JMEOS-signature-forwarding pattern as v2.
- Same tier-aware Javadoc + emit rules.
"""

# Canonical MobilityDB PostgreSQL-License header, emitted on every generated file.
MEOS_LICENSE_BANNER = (
    '/*****************************************************************************\n'
    ' *\n'
    ' * This MobilityDB code is provided under The PostgreSQL License.\n'
    ' * Copyright (c) 2020-2026, Université libre de Bruxelles and MobilityDB\n'
    ' * contributors\n'
    ' *\n'
    ' * Permission to use, copy, modify, and distribute this software and its\n'
    ' * documentation for any purpose, without fee, and without a written\n'
    ' * agreement is hereby granted, provided that the above copyright notice and\n'
    ' * this paragraph and the following two paragraphs appear in all copies.\n'
    ' *\n'
    ' * IN NO EVENT SHALL UNIVERSITE LIBRE DE BRUXELLES BE LIABLE TO ANY PARTY FOR\n'
    ' * DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING\n'
    ' * LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION,\n'
    ' * EVEN IF UNIVERSITE LIBRE DE BRUXELLES HAS BEEN ADVISED OF THE POSSIBILITY\n'
    ' * OF SUCH DAMAGE.\n'
    ' *\n'
    ' * UNIVERSITE LIBRE DE BRUXELLES SPECIFICALLY DISCLAIMS ANY WARRANTIES,\n'
    ' * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY\n'
    ' * AND FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON\n'
    ' * AN "AS IS" BASIS, AND UNIVERSITE LIBRE DE BRUXELLES HAS NO OBLIGATIONS TO\n'
    ' * PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.\n'
    ' *\n'
    ' *****************************************************************************/'
)
import json, re, sys
from pathlib import Path
from collections import Counter, defaultdict

SIG_PATH = Path("/home/esteban/src/_flink_codegen/jmeos_pr19_signatures.txt")
BL_PATH  = Path("/home/esteban/src/_streaming_relevance/out/streaming-relevance-baseline.json")
CAT_PATH = Path("/home/esteban/src/_streaming_relevance/meos-api/output/meos-idl.json")
OUT_ROOT = Path("/home/esteban/src/_flink_codegen/generated")
OUT_PKG  = "org.mobilitydb.flink.meos"
OUT_DIR  = OUT_ROOT / "src/main/java" / OUT_PKG.replace('.', '/')
OUT_DIR.mkdir(parents=True, exist_ok=True)

SIG_RE = re.compile(r'^\s*public\s+static\s+(?P<ret>[\w\.<>\[\]]+)\s+(?P<name>\w+)\((?P<args>[^)]*)\)')
jmeos = {}
with open(SIG_PATH) as f:
    for line in f:
        m = SIG_RE.match(line.rstrip(';\n'))
        if m:
            ret = m.group('ret')
            name = m.group('name')
            raw = m.group('args').strip()
            arg_types = [a.strip() for a in raw.split(',')] if raw else []
            jmeos[name] = {'ret': ret, 'arg_types': arg_types}
print(f'JMEOS methods parsed: {len(jmeos)}')

with open(BL_PATH) as f:  bl  = json.load(f)
with open(CAT_PATH) as f: cat = json.load(f)
sig_by_name = {fn['name']: fn for fn in cat['functions']}

# Scope: streamable + io-meta tiers across ALL OO classes (skip sequence-only
# and ambiguous for now — those need design decisions surfaced separately).
EMIT_TIERS = {'stateless', 'bounded-state', 'windowed', 'cross-stream', 'io-meta'}
target_rows = [r for r in bl['functions']
               if r['tier'] in EMIT_TIERS
               and r.get('class')      # must be classified into the OO model
               and r['tier'] != 'internal']

# Inner-join with JMEOS
emit_rows = []
not_in_jmeos = []
for r in target_rows:
    if r['name'] in jmeos:
        sig = jmeos[r['name']]
        emit_rows.append({**r,
                          'java_ret': sig['ret'],
                          'java_params': [(t, f'arg{i}') for i, t in enumerate(sig['arg_types'])]})
    else:
        not_in_jmeos.append(r['name'])

print(f'baseline target: {len(target_rows)} fns')
print(f'JMEOS inner-join: {len(emit_rows)} emit / {len(not_in_jmeos)} absent from JMEOS')

# Group emit_rows by OO class
by_class = defaultdict(list)
for r in emit_rows:
    for c in r['class']:
        by_class[c].append(r)

print(f'classes to emit: {len(by_class)}')

TIER_DOC = {
    'stateless':     'Pure per-event; safe in any Flink scalar position.',
    'bounded-state': 'Per-event with bounded per-key state (MEOS handle).',
    'windowed':      'Requires window operator — caller wraps in AggregateFunction.',
    'cross-stream':  'Pairwise across streams — caller wraps in a join.',
    'sequence-only': 'Inherently non-streamable; honest marker.',
    'io-meta':       'I/O / catalog / lifecycle helper.',
}

# ----- Per-class file emission ---------------------------------------------
def short_type(t):
    if t.startswith('java.lang.'):
        return t[len('java.lang.'):]
    return t.split('.')[-1] if '.' in t else t

def java_class_name(oo_class):
    """Map OO class name → Java class name."""
    return f'MeosOps{oo_class}'

def emit_class(oo_class, rows):
    """Emit one Java class collecting all methods of `oo_class`."""
    imports = set(['functions.GeneratedFunctions'])
    for r in rows:
        for t in [r['java_ret']] + [a[0] for a in r['java_params']]:
            if '.' in t and not t.startswith('java.lang.'):
                imports.add(t.replace('[]', ''))
    imports = sorted(i for i in imports if '.' in i)

    cls = java_class_name(oo_class)
    tier_cnt = Counter(r['tier'] for r in rows)
    L = [
        MEOS_LICENSE_BANNER, '',
        f'package {OUT_PKG};',
        '',
        f'/* AUTO-GENERATED by codegen_flink_v3.py — do not edit by hand.',
        f' * MEOS object-model class: {oo_class}',
        f' * Methods emitted: {len(rows)} (' + ' · '.join(f'{t}={c}' for t, c in tier_cnt.most_common()) + ')',
        f' * Source: JMEOS jar (PR #15 / regen-against-meos-1.4) ∩ streaming-relevance baseline v4.',
        ' */',
        '',
    ]
    for i in imports:
        L.append(f'import {i};')
    L += [
        '',
        f'public final class {cls} {{',
        '',
        '    public static final boolean MEOS_AVAILABLE = MeosOpsRuntime.MEOS_AVAILABLE;',
        '',
        f'    private {cls}() {{ /* utility */ }}',
        '',
    ]

    # Sort: by tier (stateless first → most-used) then by name
    TIER_ORDER = {'stateless':0, 'bounded-state':1, 'windowed':2, 'cross-stream':3, 'io-meta':4, 'sequence-only':5}
    for r in sorted(rows, key=lambda r: (TIER_ORDER.get(r['tier'], 99), r['name'])):
        fname = r['name']
        args = ', '.join(f'{short_type(t)} {n}' for (t, n) in r['java_params'])
        call_args = ', '.join(n for (t, n) in r['java_params'])
        ret_short = short_type(r['java_ret'])
        L += [
            '    /**',
            f'     * MEOS {{@code {fname}}} — tier <b>{r["tier"]}</b>.',
            f'     * <p>{TIER_DOC.get(r["tier"], "")}</p>',
        ]
        if r.get('role'):
            L.append(f'     * <p>Object-model role: {{@code {r["role"]}}}.</p>')
        L.append(f'     * <p>Classification: {r["reason"]}</p>')
        L.append('     */')
        if r['tier'] == 'sequence-only':
            L += [
                f'    public static {ret_short} {fname}({args}) {{',
                f'        throw new UnsupportedOperationException(',
                f'            "{fname} is sequence-only — not supported in a streaming context");',
                '    }',
                '',
            ]
        else:
            ret_stmt = '' if ret_short == 'void' else 'return '
            L += [
                f'    public static {ret_short} {fname}({args}) {{',
                '        if (!MEOS_AVAILABLE) {',
                f'            throw new UnsupportedOperationException(',
                f'                "{fname} requires libmeos — set -Dmobilityflink.meos.enabled=true");',
                '        }',
                f'        {ret_stmt}GeneratedFunctions.{fname}({call_args});',
                '    }',
                '',
            ]
    L.append('}')
    return '\n'.join(L) + '\n'

# Shared runtime helper (the MEOS_AVAILABLE static-init lives once)
runtime_src = f'''{MEOS_LICENSE_BANNER}

package {OUT_PKG};

import functions.GeneratedFunctions;

/* AUTO-GENERATED by codegen_flink_v3.py — do not edit by hand.
 * Shared runtime helper: owns the single MEOS_AVAILABLE static-init across
 * all generated MeosOps* facades, so libmeos is probed exactly once per
 * JVM rather than 82 times. */
final class MeosOpsRuntime {{

    static final boolean MEOS_AVAILABLE;

    static {{
        boolean enabled = Boolean.parseBoolean(
                System.getProperty("mobilityflink.meos.enabled", "true"));
        boolean ok = false;
        if (enabled) {{
            try {{
                GeneratedFunctions.meos_initialize();
                ok = true;
            }} catch (Throwable t) {{
                ok = false;
            }}
        }}
        MEOS_AVAILABLE = ok;
    }}

    private MeosOpsRuntime() {{ /* utility */ }}
}}
'''
(OUT_DIR / 'MeosOpsRuntime.java').write_text(runtime_src)

# Wipe stale class files from v2 run
for f in OUT_DIR.glob('MeosBoxOps.java'):
    f.unlink()
for f in OUT_DIR.glob('MeosOps*.java'):
    if f.name not in ('MeosOpsRuntime.java',):
        f.unlink()

# Write one file per OO class
emitted_files = []
for oo_class, rows in sorted(by_class.items()):
    src = emit_class(oo_class, rows)
    fname = f'{java_class_name(oo_class)}.java'
    (OUT_DIR / fname).write_text(src)
    emitted_files.append((fname, len(rows)))

print(f'emitted {len(emitted_files)} Java classes')
print('top by method count:')
for fn, c in sorted(emitted_files, key=lambda x: -x[1])[:10]:
    print(f'  {fn:35s}: {c}')

# Manifest
manifest = {
    'generator': 'codegen_flink_v3.py',
    'package': OUT_PKG,
    'jmeos_method_total': len(jmeos),
    'baseline_target_count': len(target_rows),
    'emitted_methods': len(emit_rows),
    'emitted_files': len(emitted_files),
    'absent_from_jmeos': len(not_in_jmeos),
    'absent_from_jmeos_sample': not_in_jmeos[:20],
    'tier_breakdown': dict(Counter(r['tier'] for r in emit_rows)),
    'classes_emitted': sorted(by_class.keys()),
    'methods_per_class': {oo: len(rs) for oo, rs in sorted(by_class.items())},
}
(OUT_ROOT / 'meos-ops-manifest.json').write_text(json.dumps(manifest, indent=2))
print(f'wrote {OUT_ROOT / "meos-ops-manifest.json"}')
print()
print('tier breakdown of emitted (method-level, may multi-count if fn in 2 OO classes):')
for t, c in Counter(r['tier'] for r in emit_rows).most_common():
    print(f'  {t:18s}: {c}')
