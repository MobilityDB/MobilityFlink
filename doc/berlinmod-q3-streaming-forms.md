# BerlinMOD-Q3 streaming forms

This document defines what **BerlinMOD-Q3** means in each of the three
streaming forms the parity contract specifies for the MobilityFlink /
MobilityKafka / MobilityNebula trio (see the planned-tier section of the
[ecosystem profile](https://github.com/MobilityDB/.github)).

## The batch query

> *Which vehicles were within distance `d` of point `P` at time `T`?*

Parameters: a point `P = (lon, lat)`, a radius `d` in metres, and a time `T`.
Returns: the set of `vehicle_id`s whose trajectory passed within `d` of `P` at `T`.

The batch reference implementation lives in
[MobilityDB-BerlinMOD](https://github.com/MobilityDB/MobilityDB-BerlinMOD) and
runs against the three SQL surfaces (MobilityDB / MobilityDuck / MobilitySpark)
with byte-identical results — the batch oracle for the snapshot streaming form
below.

## The three streaming forms

### 1. Continuous form

> *"At every moment, which vehicles are currently within `d` of `P`?"*

For each incoming GPS event `(vehicle_id, t, lon, lat)`:

- Evaluate the radius predicate `distance((lon, lat), P) ≤ d`.
- Emit `(vehicle_id, t, near)` per event.

No window; output updates per event. Watermark-independent.

Use case: real-time geofence alerting where each event matters.

Implemented by [`Q3ContinuousFunction`](../flink-processor/src/main/java/berlinmod/Q3ContinuousFunction.java).

### 2. Windowed form

> *"Per N-second tumbling window, how many distinct vehicles were
> within `d` of `P` at any time during the window?"*

Tumbling event-time window of size `W` (default `W = 10s`). For each window:

- Collect all events whose timestamp falls in the window.
- Compute the distinct set `{vehicle_id : ∃ event in window with distance ≤ d}`.
- Emit `(window_start, window_end, distinct_count)`.

Use case: time-bucketed dashboards, near-real-time aggregates.

Implemented by [`Q3WindowedFunction`](../flink-processor/src/main/java/berlinmod/Q3WindowedFunction.java).

### 3. Snapshot form — **the parity oracle**

> *"At time `T`, which vehicles are within `d` of `P`?"*

Watermark-driven. Per vehicle, maintain `lastKnownPosition` state. At each
snapshot tick (event-time timer at multiples of `snapshotTickMillis`,
default `5000 ms`):

- For each vehicle's most recent `(lon, lat)`, evaluate the radius predicate.
- Emit `(T, vehicle_id)` for every vehicle satisfying the predicate at `T`.

As the watermark advances to `T = max(event_times)`, the streaming snapshot
output **equals the batch BerlinMOD-Q3 result** on the same scale-factor
corpus. This is the parity property the contract enforces:

```
streaming-Q3-snapshot(T) ≡ batch-BerlinMOD-Q3 on data up to T
                          (same SF, same P, same d)
```

Use case: lambda-architecture style verification — streaming pipeline's
output must converge to the batch reference.

Implemented by [`Q3SnapshotFunction`](../flink-processor/src/main/java/berlinmod/Q3SnapshotFunction.java).

## Default parameters

The `BerlinMODQ3Main` entry point uses:

| Parameter | Value | Source |
|---|---|---|
| `P` (lon, lat) | (4.3517, 50.8503) — Brussels city centre | Default centre for the BerlinMOD-Brussels corpus |
| `d` (radius) | 5 000 m | Within-city-centre scale |
| `W` (window size) | 10 s | Same as the AIS example for consistency |
| Snapshot tick | 5 s | Half the window for finer parity-oracle granularity |
| Topic | `berlinmod` | Single shared topic across the three forms |

## Predicate implementation

The within-distance predicate evaluates through the MEOS `edwithin_tgeo_geo`
operator — the same call used by `MobilityNebula/Queries/Query1.yaml`. The
vehicle position is built as a `tgeogpoint` instant and tested against the
query geography in metres on the WGS84 spheroid. All spatial predicates route
through [`MEOSBridge`](../flink-processor/src/main/java/berlinmod/MEOSBridge.java),
which holds no spatial mathematics of its own: it constructs the MEOS inputs
(temporal instants and geographies) and delegates the computation to libmeos.

## Companion producer

The BerlinMOD CSV → Kafka producer lives at
[`kafka-producer/python-producer-berlinmod.py`](../kafka-producer/python-producer-berlinmod.py).
Generate a BerlinMOD CSV at scale factor SF with the upstream generator
(`meos/examples/data/generate_berlinmod_trips.sql` in MobilityDB), name the
columns `(t, vehicle_id, lon, lat)`, and the producer streams it to the
`berlinmod` topic.
