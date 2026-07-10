#!/usr/bin/env python3
"""Classify the MEOS public API into streaming tiers — the reproducible
producer of ``tools/baseline/streaming-relevance-baseline.json``.

The baseline is the generator input consumed by ``codegen_facades.py``
(which emits a facade only for the streaming-relevant tiers). This script
is deterministic: the tier of a function is decided purely by its name,
its object-model role, and the number of temporal parameters — there is
zero per-function judgement, so the same MEOS catalog always yields the
same baseline.

Usage:
    classify_streaming_relevance.py <meos-idl.json> [-o <out.json>]
                                    [--source-ref <label>]

``<meos-idl.json>`` is the MEOS-API catalog produced by running
``MEOS-API/run.py`` over the MEOS headers at the commit recorded in
``tools/meos-source-commit.txt`` (see GENERATION.md for the full chain).
The output defaults to stdout. ``--source-ref`` records portable
provenance in the artifact instead of an absolute local path.
"""
import argparse
import json
import re
import sys
from collections import Counter, defaultdict
from os.path import basename

TIERS = ['stateless', 'bounded-state', 'windowed', 'cross-stream',
         'sequence-only', 'io-meta', 'internal', 'ambiguous']

PUBLIC_HEADERS = {
    'meos.h', 'meos_geo.h', 'meos_cbuffer.h', 'meos_npoint.h',
    'meos_pose.h', 'meos_rgeo.h', 'meos_transform.h', 'meos_catalog.h',
}

TEMPORAL_TYPES = (
    'Temporal', 'TInstant', 'TSequence', 'TSequenceSet',
    'TInt', 'TFloat', 'TBool', 'TText',
    'TGeo', 'TGeoPoint', 'TGeompoint', 'TGeogpoint',
    'TGeometry', 'TGeography',
    'TCbuffer', 'TNpoint', 'TPose', 'TRGeometry',
)


def is_temporal(t):
    s = t.get('canonical', t.get('c', ''))
    return any(re.search(rf'\b{tt}\b', s) for tt in TEMPORAL_TYPES)


def is_sequence_typed(t):
    s = t.get('canonical', t.get('c', ''))
    return 'TSequence' in s or 'SeqSet' in s


def classify(fn, fn_role):
    name = fn['name']
    header = fn['file']
    if header not in PUBLIC_HEADERS:
        return ('internal', 'high', f'header {header} not public')

    role = fn_role.get(name)
    ret = fn['returnType']
    params = fn['params']
    n_temporal_params = sum(1 for p in params if is_temporal(p))
    ret_is_temporal = is_temporal(ret)
    ret_is_seq = is_sequence_typed(ret)

    # --- IO / parsing / serialization ---
    if role == 'output' or re.search(
        r'_(in|out|recv|send|as_text|as_hexwkb|as_wkb|as_mfjson|as_ewkt|as_geojson|from_(wkb|hexwkb|text|mfjson|ewkt|geojson|wkt))$', name):
        return ('io-meta', 'high', 'IO/serialization')
    if 'mfjson' in name or 'wkb' in name or 'ewkt' in name or '_to_string' in name:
        return ('io-meta', 'high', 'name has IO token')
    # MEOS lifecycle / errno / array container helpers / rtree / ensure_
    if re.match(r'^(meos_|ensure_|rtree_|skiplist_|meostype_|temptype_)', name):
        return ('io-meta', 'high', 'MEOS infra / catalog / utility')

    # --- Type conversions ---
    if role == 'conversion':
        return ('stateless', 'high', 'role=conversion')
    if re.search(r'_to_(timestamptz|date|float|int|bigint|text|bool|tstzspan|span|stbox|tbox|tgeompoint|tgeogpoint|geo|geom|geog|set)$', name):
        return ('stateless', 'high', 'name pattern is _to_<type>')

    # --- Accessor ---
    if role == 'accessor':
        return ('bounded-state', 'high', 'role=accessor')

    # --- Restriction (at/minus/delete) ---
    if role == 'restriction':
        return ('bounded-state', 'high', 'role=restriction')
    if re.match(r'^(minus|at|delete)_', name) or '_at_' in name or '_minus_' in name or '_delete_' in name:
        return ('bounded-state', 'high', 'restriction name pattern')

    # --- Aggregate (windowed by definition) ---
    if role == 'aggregate':
        return ('windowed', 'high', 'role=aggregate')

    # --- Ever / Always (e* / a* AND ever_/always_) ---
    if name.startswith('ever_') or name.startswith('always_'):
        if n_temporal_params >= 2:
            return ('cross-stream', 'high', 'ever/always over 2 temporals')
        return ('windowed', 'high', 'ever/always over 1 temporal')
    if re.match(r'^[ea](intersects|contains|disjoint|touches|within|covers|crosses|overlaps|dwithin)_', name):
        if n_temporal_params >= 2:
            return ('cross-stream', 'high', 'ever/always spatial-rel on 2 temporals')
        return ('bounded-state', 'high', 'ever/always spatial-rel on 1 temporal')

    # --- Predicate (other) ---
    if role == 'predicate':
        if n_temporal_params >= 2:
            return ('cross-stream', 'high', 'predicate on 2 temporals')
        return ('bounded-state', 'high', 'predicate on 1 temporal')

    # --- Constructor ---
    if role == 'constructor':
        if ret_is_seq:
            return ('sequence-only', 'high', 'constructor of TSequence/TSeqSet')
        return ('stateless', 'high', 'constructor of instant/scalar')

    # === Non-OO public functions, name-pattern fallbacks ===

    # Set / span / spanset / tbox / stbox algebra (union, intersection,
    # contains, overlaps, adjacent, position) - pure box/set algebra
    if re.match(
        r'^(union|intersection|difference|minus|contains|contained|overlaps|adjacent|'
        r'left|right|overleft|overright|below|above|overbelow|overabove|front|back|'
        r'overfront|overback|before|after|overbefore|overafter)_'
        r'(set|span|spanset|tbox|stbox|date|timestamptz|int|bigint|float|text|bool|geo|geom|geog)',
        name):
        return ('stateless', 'high', 'set/span/box algebra (pure)')

    # Numeric / text / bool ops on temporals (per-instant)
    if re.match(
        r'^t(int|float|number|text|bool)_(add|sub|mul|div|mod|abs|sqrt|round|ceil|floor|sign|neg|'
        r'pow|exp|ln|log10|sin|cos|tan|degrees|radians|upper|lower|concat|and|or|not|xor|reverse|length)$',
        name):
        return ('stateless', 'high', 'temporal numeric/text/bool op')

    # Temporal compare (teq, tne, tlt, tle, tgt, tge) - per-instant
    if re.match(r'^t(eq|ne|lt|le|gt|ge)_', name):
        return ('stateless', 'high', 'temporal comparison (per-instant)')

    # Per-value comparison / hash on base / set / span / tbox
    if re.match(
        r'^(temporal|tfloat|tint|tbool|ttext|set|span|spanset|tbox|stbox|cbuffer|npoint|pose|'
        r'date|geo|geom|geog|float|int|bigint|text|bool|timestamptz)_(eq|ne|cmp|lt|le|gt|ge|hash)',
        name):
        return ('stateless', 'high', 'scalar comparison/hash')

    # Sequence-derived metrics (windowed)
    if re.search(
        r'_(length|cumulative_length|speed|integral|derivative|twavg|twmin|twmax|twsum|stops|'
        r'min_dist|max_dist|nearest_approach|nearestapproach|granger)', name):
        return ('windowed', 'high', 'sequence-derived metric')

    # Trajectory / timespan / valueset / *_n / num_* — derived from full sequence
    if re.search(r'_(trajectory|timespan|periods|periodset|valueset|num_(instants|sequences|values|timestamps))$', name):
        return ('windowed', 'medium', 'derived from full sequence')

    # Distance / NAD / NAI / shortestline
    if re.match(r'^(nad|nai|shortestline|tdistance|distance)_', name):
        if n_temporal_params >= 2:
            return ('cross-stream', 'high', 'distance on 2 temporals')
        return ('bounded-state', 'high', 'distance op')

    # Constructor-like make / from_base / from_mfjson on instants
    if re.search(r'(_make|_from_base|_make_array)$', name) or re.match(r'^geo_make', name) or re.match(r'^geom_make', name):
        if ret_is_seq:
            return ('sequence-only', 'medium', 'make-of-sequence')
        return ('stateless', 'medium', 'make/from_base of instant/scalar')

    # Shift / scale / round / fix / canonicalize / expand
    if re.search(r'_(shift|scale|shift_scale|round|fix|canonicalize|expand|expandspace|expandtime|setbbox|setprecision)$', name) or \
       re.search(r'_(shift|scale|shift_scale|round|fix|canonicalize|expand|expandspace|expandtime)_', name):
        return ('stateless', 'high', 'transform/normalize (pure)')

    # Geometry transformers (geom_to_*, geog_to_*, geo_*)
    if re.match(r'^(geo|geom|geog|pose|cbuffer|npoint|stbox|tbox|set|span|spanset|float|int|bigint|text|bool|date|timestamptz)_', name):
        # Default for base-type funcs without a more specific rule
        return ('stateless', 'medium', 'base-type fn, default pure')

    # Utility: copy / free / hash / compress / decompress
    if re.search(r'_(copy|free|hash|compress|decompress|fix|valid|set_|setbbox)$', name):
        return ('stateless', 'medium', 'utility')

    # Topology "same_" predicate on box/temporal
    if re.match(r'^same_', name):
        if n_temporal_params >= 2:
            return ('cross-stream', 'high', 'same predicate on 2 temporals')
        return ('stateless', 'high', 'same predicate (pure box equality)')

    # Temporal spatial relation lifted (returns a tbool)
    if re.match(r'^t(intersects|contains|covers|disjoint|dwithin|touches|crosses|overlaps|within|equals)_', name):
        if n_temporal_params >= 2:
            return ('cross-stream', 'high', 'temporal spatial-rel lift on 2 temporals')
        return ('bounded-state', 'high', 'temporal spatial-rel lift on 1 temporal')

    # textcat / text-concat operator on temporals
    if re.match(r'^textcat_', name):
        return ('stateless', 'high', 'text concatenation (per-instant)')

    # Temporal arithmetic operators (different name shapes: add_<X>_<Y>, sub_*, mul_*, div_*)
    if re.match(r'^(add|sub|mul|div|mod)_(t(int|float|number)|int|float)', name):
        return ('stateless', 'high', 'arithmetic op on temporal number (per-instant)')

    # Position / topology relations — type-suffix-agnostic, signature-driven.
    # Catches adjacent_numspan_tnumber, contained_tnumber_tbox, left_X_Y, etc.
    if re.match(r'^(adjacent|contained|contains|overlaps|same|left|right|overleft|overright|'
                r'before|after|overbefore|overafter|below|above|overbelow|overabove|'
                r'front|back|overfront|overback)_', name):
        if n_temporal_params >= 2:
            return ('cross-stream', 'high', 'topology/position rel on 2 temporals')
        if n_temporal_params == 1:
            return ('bounded-state', 'high', 'topology/position rel on 1 temporal + scalar')
        return ('stateless', 'high', 'topology/position rel on 2 scalars (box/span algebra)')

    # Scalar arithmetic (add_date_int, add_timestamptz_interval, sub_*, mul_*)
    if re.match(r'^(add|sub|mul|div|mod)_(date|timestamptz|int|bigint|float|interval)', name):
        return ('stateless', 'high', 'scalar arithmetic')

    # int32_cmp / int64_cmp / other scalar comparators with width suffix
    if re.match(r'^(int32|int64|uint32|uint64|float32|float64)_(eq|ne|cmp|lt|le|gt|ge|hash)', name):
        return ('stateless', 'high', 'scalar comparison with width suffix')

    # cstring2text / text2cstring / pg_* casts / cstring_to_text
    if re.match(r'^(cstring|text)2(cstring|text)|^cstring_to_|^text_to_cstring|^pg_', name):
        return ('io-meta', 'high', 'string/CString conversion (low-level)')

    # Last-resort: anything that returns a Datum / void / int errno-style without a temporal param = io-meta
    # (numeric utility wrappers, MEOS internals exposed for codegen, error handling)
    if n_temporal_params == 0 and not ret_is_temporal:
        rc = ret.get('canonical', '') or ret.get('c', '')
        if rc in ('int', 'bool', 'void', 'char *', 'const char *', 'Datum'):
            return ('stateless', 'low', 'scalar in/out (default-stateless catch-all)')

    return ('ambiguous', 'low', f'no rule (role={role}, hdr={header}, ntemp={n_temporal_params})')


def build(cat, source_ref):
    fn_role, fn_class = {}, {}
    for cls_name, cls in cat['objectModel']['classes'].items():
        for m in cls.get('methods', []):
            fn = m['function']
            fn_role[fn] = m.get('role')
            fn_class.setdefault(fn, []).append(cls_name)

    results = []
    for fn in cat['functions']:
        tier, conf, reason = classify(fn, fn_role)
        results.append({
            'name': fn['name'], 'file': fn['file'],
            'role': fn_role.get(fn['name']),
            'class': fn_class.get(fn['name'], []),
            'tier': tier, 'confidence': conf, 'reason': reason,
            'return_c': fn['returnType'].get('canonical', fn['returnType'].get('c')),
            'n_params': len(fn['params']),
        })

    total = len(results)
    public_rows = [r for r in results if r['tier'] != 'internal']
    tot = Counter(r['tier'] for r in results)
    pub_tot = Counter(r['tier'] for r in public_rows)
    by_h = defaultdict(Counter)
    for r in public_rows:
        by_h[r['file']][r['tier']] += 1
    streaming_relevant = sum(pub_tot[t] for t in ['stateless', 'bounded-state', 'windowed', 'cross-stream'])

    return {
        'schema_version': '0.1.0-draft',
        'source_catalog': source_ref,
        'source_catalog_function_count': total,
        'classifier': 'v4',
        'vocabulary': TIERS,
        'public_headers': sorted(PUBLIC_HEADERS),
        'rollup': {
            'total': dict(tot),
            'public_only': dict(pub_tot),
            'public_by_header': {h: dict(c) for h, c in by_h.items()},
            'headline_public': {
                'streaming_relevant': streaming_relevant,
                'sequence_only': pub_tot['sequence-only'],
                'io_meta': pub_tot['io-meta'],
                'ambiguous': pub_tot['ambiguous'],
                'public_total': len(public_rows),
            },
        },
        'functions': results,
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('catalog', help='meos-idl.json produced by MEOS-API/run.py')
    ap.add_argument('-o', '--out', default='-',
                    help='output baseline path (default: stdout)')
    ap.add_argument('--source-ref', default=None,
                    help='portable provenance recorded in the artifact '
                         '(default: the catalog basename)')
    a = ap.parse_args()

    with open(a.catalog) as f:
        cat = json.load(f)
    artifact = build(cat, a.source_ref or basename(a.catalog))

    text = json.dumps(artifact, indent=2) + '\n'
    if a.out == '-':
        sys.stdout.write(text)
    else:
        with open(a.out, 'w') as f:
            f.write(text)
        pub = artifact['rollup']['headline_public']
        sys.stderr.write(
            f"wrote {a.out}: {artifact['source_catalog_function_count']} fns, "
            f"{pub['streaming_relevant']} streaming-relevant, "
            f"{pub['ambiguous']} ambiguous\n")


if __name__ == '__main__':
    main()
