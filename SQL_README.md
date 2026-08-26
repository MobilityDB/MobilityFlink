# MobilityFlink SQL PoC

Use MobilityDB-style spatiotemporal types and functions directly from
**Flink SQL** — no Java required. If you already know MobilityDB SQL, the
syntax here is deliberately the same.

This PoC currently supports four type families:

| Type | Description |
|---|---|
| `floatspan` | A range of float values, e.g. `[1.0, 3.5]` |
| `tbox` | A box over a numeric value range + a time range |
| `stbox` | A box over space (X, Y, optionally Z) + a time range |
| `tint` / `tfloat` | A temporal number: a value that changes over time |

---

## Running the demos

Each type has its own runnable example with sample data and SQL queries:

```
FloatSpanSQLTest.java   → floatspan examples
TBoxSQLTest.java        → tbox examples
STBoxSQLTest.java       → stbox examples
TNumberSQLTest.java     → tint / tfloat examples
```

Run any of them as a normal Java `main()` (or via the provided Docker
image). Each prints the result of several SQL queries to the console.

---

## `floatspan`

A range of float values, with inclusive/exclusive bounds — e.g. `[1.0,
3.5]` (both bounds included) or `(2.0, 6.0)` (both excluded).

```sql
SELECT
    floatspan_lower(f1)                          AS lower,
    floatspan_upper(f1)                          AS upper,
    floatspan_width(f1)                          AS width,
    floatspan_contains(f1, CAST(2.5 AS FLOAT))   AS contains_2_5
FROM spans
```

Available functions:

| Function | Description |
|---|---|
| `floatspan_lower(span)` | Lower bound |
| `floatspan_upper(span)` | Upper bound |
| `floatspan_width(span)` | Width (upper − lower) |
| `floatspan_contains(span, span \| float)` | Does the span contain the other span/value? |
| `floatspan_overlaps(span, span)` | Do the two spans overlap? |
| `floatspan_distance(span, span)` | Distance between two spans |
| `floatspan_extent(span)` *(aggregate)* | Smallest span containing all input spans |

```sql
SELECT floatspan_out(floatspan_extent(f1)) AS extent
FROM spans
```

---

## `tbox`

A box combining a numeric value range and a time range — e.g.
`TBOXFLOAT XT([0, 10),[2020-06-01, 2020-06-05])`. A `tbox` can have only a
value dimension, only a time dimension, or both.

```sql
SELECT
    tbox_has_x(f1)                                    AS has_x,
    tbox_has_t(f1)                                    AS has_t,
    floatspan_out(tbox_to_floatspan(f1))               AS float_span,
    tbox_overlaps(f1, tbox('TBOXFLOAT XT([8, 15),[2020-06-04, 2020-06-08])'))
                                                        AS overlaps_query
FROM tboxes
```

Available functions:

| Function | Description |
|---|---|
| `tbox(text)` | Build a `tbox` from its text representation |
| `tbox_has_x(box)` | Does the box have a value dimension? |
| `tbox_has_t(box)` | Does the box have a time dimension? |
| `tbox_to_floatspan(box)` | Extract the value dimension as a `floatspan` |
| `tbox_contains(box, box)` | Does the box contain the other box? |
| `tbox_contained(box, box)` | Is the box contained in the other box? |
| `tbox_overlaps(box, box)` | Do the two boxes overlap? |
| `tbox_same(box, box)` | Are the two boxes equal? |
| `tbox_adjacent(box, box)` | Are the two boxes adjacent? |
| `tbox_left` / `tbox_overleft` / `tbox_right` / `tbox_overright` | Relative value-axis position |
| `tbox_before` / `tbox_overbefore` / `tbox_after` / `tbox_overafter` | Relative time-axis position |
| `tbox_union(box, box)` | Union of two boxes |
| `tbox_intersection(box, box)` | Intersection of two boxes |
| `tbox_extent(box)` *(aggregate)* | Smallest box containing all input boxes |

**Note on `tbox_extent`:** every row aggregated together must have the
**same dimensionality** — either all boxes have both a value and a time
dimension, or none do. Mixing them will fail. Filter accordingly:

```sql
SELECT floatspan_out(tbox_to_floatspan(tbox_extent(f1))) AS extent
FROM tboxes
WHERE tbox_has_x(f1) = true AND tbox_has_t(f1) = true
```

---

## `stbox`

A box combining space (X, Y, optionally Z) and a time range — e.g.
`STBOX XT(((1.5,2.5),(3.3,4.4)),[2020-06-01,2020-06-05])`. Like `tbox`, an
`stbox` can be space-only, time-only, or both.

```sql
SELECT
    stbox_has_xy(f1)   AS has_xy,
    stbox_has_t(f1)    AS has_t,
    stbox_xmin(f1)     AS x_min,
    stbox_ymin(f1)     AS y_min
FROM stboxes
WHERE stbox_has_xy(f1) = true
```

Available functions:

| Function | Description |
|---|---|
| `stbox(text)` | Build an `stbox` from its text representation |
| `stbox_has_xy(box)` | Does the box have a spatial dimension? |
| `stbox_has_t(box)` | Does the box have a time dimension? |
| `stbox_xmin(box)` / `stbox_ymin(box)` | Minimum X / Y coordinate |
| `stbox_contains(box, box)` | Does the box contain the other box? |
| `stbox_overlaps(box, box)` | Do the two boxes overlap? |
| `stbox_expand(box, distance)` | Expand the box by a numeric distance in every spatial direction |
| `stbox_get_space(box)` | Extract the spatial dimension only (drop time) |
| `stbox_out(box)` | Text representation of a box |
| `stbox_extent(box)` *(aggregate)* | Smallest box containing all input boxes |

```sql
SELECT stbox_out(stbox_expand(f1, CAST(1.0 AS FLOAT))) AS expanded
FROM stboxes
```

**Note on `stbox_extent`:** same dimensionality constraint as `tbox_extent`
above — all aggregated boxes must share the same combination of X/Y/T
dimensions.

---

## `tint` / `tfloat`

A temporal number: a value (integer or float) that changes over time,
built from a sequence of timestamped values — e.g.
`[1.5@2020-06-01, 3.0@2020-06-02, 2.5@2020-06-03]`.

```sql
SELECT
    tfloat_out(derivative(f1))          AS derivative,
    tfloat_out(tfloat_round(f1, 1))     AS rounded_1dec,
    tfloat_out(deltaValue(f1))          AS delta_value
FROM tfloats
```

Arithmetic — a temporal number can be combined with a plain number or with
another temporal number:

```sql
SELECT
    tfloat_out(tAdd(f1, 2.0))       AS plus_2,
    tfloat_out(tSub(f1, 1.5))       AS minus_1_5
FROM tfloats
```

```sql
SELECT tfloat_out(tAdd(a.f1, b.f1)) AS sum
FROM tfloats a JOIN tfloats b ON a.f0 = b.f0
```

Time-weighted average, as an aggregate over multiple rows:

```sql
SELECT tfloat_tavg(tfloat_out(f1)) AS tavg
FROM tfloats
```

Available functions:

| Function | Description |
|---|---|
| `tfloat_out(t)` / `tint_out(t)` | Text representation |
| `tAdd(t, number)` / `tAdd(t, t)` | Addition |
| `tSub(t, number)` / `tSub(t, t)` | Subtraction |
| `deltaValue(t)` | Value-to-value difference over time |
| `derivative(t)` | Rate of change over time (`tfloat` only) |
| `tfloat_round(t, decimals)` | Round all values to N decimal places (`tfloat` only) |
| `tfloat_tavg(...)` *(aggregate)* | Time-weighted average across multiple rows |

**Known limitation — `tMul` (multiplication) is not available.**
Multiplying a temporal number by a scalar or by another temporal number
currently fails due to a native library issue unrelated to this PoC's SQL
layer (a missing/mismatched native function in the underlying MEOS
library). `tAdd`, `tSub`, `deltaValue`, `derivative`, and `tfloat_round`
are unaffected and fully supported.

**Note on `tfloat_tavg`:** this aggregate rebuilds its result from all
accumulated input each time it is emitted. In a continuous/unbounded
streaming query with frequent updates, this recomputation happens on every
new row; it is best suited to bounded or batch-style queries.

---

## General notes

- All function names match MobilityDB's SQL naming as closely as possible.
  If you know MobilityDB, most of this should look immediately familiar.
- Types shown above with `_out` functions (`floatspan_out`, `tbox_out`,
  `stbox_out`, `tfloat_out`, `tint_out`) need to be converted to text
  explicitly to be printed/displayed — the underlying value itself is not
  plain text.
- Casts on numeric literals (e.g. `CAST(2.5 AS FLOAT)`) are sometimes
  required — SQL numeric literals default to a decimal type that some
  functions don't accept directly.

For implementation details, internal architecture, and a catalogue of
known pitfalls (useful if you're extending this PoC rather than just using
it), see `ARCHITECTURE.md` and `PITFALLS.md` in `benchmark/docs/sql-doc`.