# BerlinMOD streaming-matrix throughput

Throughput of the BerlinMOD-9 × 3-form streaming matrix (9 queries ×
{continuous, windowed, snapshot} = 27 cells) on the Flink local mini-cluster
over the real BerlinMOD instants corpus. The spatial predicates evaluate through
MEOS: within-distance through `edwithin_tgeo_geo`, region containment through
`eintersects_tgeo_geo`, and distances through `geog_distance` (see
[`MEOSBridge`](../src/main/java/berlinmod/MEOSBridge.java)).

## Method

The corpus is the BerlinMOD `berlinmod_instants.csv` produced by the BerlinMOD
generator — 216 075 instants, 5 vehicles, over ~11 days. Instants are stored in
EPSG:3857 and reprojected to EPSG:4326 through MEOS `geo_transform` at load (see
[`BerlinMODCorpus`](../src/main/java/berlinmod/BerlinMODCorpus.java)); the
per-query parameters (point `P` = corpus centroid, region box, road segment,
points of interest, target vehicle ids) and the window/tick granularity are
derived from the corpus so each spatial cell is selective and the matrix
produces a comparable number of windows. Each cell runs as its own Flink job
terminated by a counting sink; throughput is input events ÷ wall-clock and
`output rows` is the sink cardinality. Parallelism 1, Flink 1.16, Java 21,
16-core x86-64 Linux; libmeos built `-DMEOS=ON -DCBUFFER=ON -DNPOINT=ON
-DPOSE=ON -DRGEO=ON`.

Run from `flink-processor/`:

```
LD_LIBRARY_PATH=<libmeos-dir> java \
  --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.util=ALL-UNNAMED \
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
  --add-opens=java.base/java.io=ALL-UNNAMED \
  --add-opens=java.base/java.time=ALL-UNNAMED \
  -cp target/classes:jar/JMEOS.jar:<deps> \
  berlinmod.BerlinMODBenchmark --csv <berlinmod_instants.csv>
```

## Results — real BerlinMOD instants (216 075 events)

| Cell | Events in | Output rows | Wall (ms) | Throughput (ev/s) |
|---|---:|---:|---:|---:|
| Q1-continuous | 216075 | 5 | 2508 | 86,154 |
| Q1-windowed | 216075 | 86 | 1294 | 166,982 |
| Q1-snapshot | 216075 | 274 | 1056 | 204,616 |
| Q2-continuous | 216075 | 61170 | 1074 | 201,187 |
| Q2-windowed | 216075 | 50 | 1027 | 210,394 |
| Q2-snapshot | 216075 | 71 | 985 | 219,365 |
| Q3-continuous | 216075 | 216075 | 2928 | 73,796 |
| Q3-windowed | 216075 | 86 | 2507 | 86,189 |
| Q3-snapshot | 216075 | 0 | 926 | 233,342 |
| Q4-continuous | 216075 | 62 | 3254 | 66,403 |
| Q4-windowed | 216075 | 98 | 3234 | 66,814 |
| Q4-snapshot | 216075 | 1944 | 3223 | 67,042 |
| Q5-continuous | 216075 | 73063 | 9161 | 23,586 |
| Q5-windowed | 216075 | 6 | 954 | 226,494 |
| Q5-snapshot | 216075 | 0 | 915 | 236,148 |
| Q6-continuous | 216075 | 216075 | 2382 | 90,712 |
| Q6-windowed | 216075 | 203 | 2637 | 81,940 |
| Q6-snapshot | 216075 | 274 | 2214 | 97,595 |
| Q7-continuous | 216075 | 5 | 3973 | 54,386 |
| Q7-windowed | 216075 | 53 | 5004 | 43,180 |
| Q7-snapshot | 216075 | 288 | 3931 | 54,967 |
| Q8-continuous | 216075 | 216075 | 2883 | 74,948 |
| Q8-windowed | 216075 | 86 | 2864 | 75,445 |
| Q8-snapshot | 216075 | 126 | 928 | 232,839 |
| Q9-continuous | 216075 | 107870 | 1858 | 116,294 |
| Q9-windowed | 216075 | 22 | 924 | 233,847 |
| Q9-snapshot | 216075 | 95 | 992 | 217,818 |

## Parity — streaming continuous form ≡ batch MEOS predicate

The continuous form emits `predicate(event)` for every event, so it is checked
event-for-event against a batch pass over the same corpus through the same
`MEOSBridge` call ([`BerlinMODParity`](../src/main/java/berlinmod/BerlinMODParity.java)).
Both spatial-membership queries match exactly.

| Query | Events | Streaming-true | Batch-true | Mismatches | Parity |
|---|---:|---:|---:|---:|---|
| Q3 (within `d` of `P`) | 216075 | 56086 | 56086 | 0 | exact |
| Q8 (within `d` of segment) | 216075 | 118498 | 118498 | 0 | exact |

## Characteristics

Q5-continuous enumerates every meeting pair across all vehicles on each event
(O(V²) per event, keyed to a single subtask); it is the lowest throughput of the
matrix. The snapshot form is a sampled form — it evaluates each vehicle's
last-known position at tick instants — so a within-`P` snapshot can be empty when
no vehicle is within `d` of `P` at a tick boundary even though the continuous
form reports near-`P` events between boundaries.
