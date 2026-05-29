# BerlinMOD streaming-matrix throughput

Throughput of the BerlinMOD-9 × 3-form streaming matrix (9 queries ×
{continuous, windowed, snapshot} = 27 cells) on the Flink local mini-cluster.
The spatial predicates evaluate through MEOS: within-distance through
`edwithin_tgeo_geo`, region containment through `eintersects_tgeo_geo`, and
distances through `geog_distance` (see
[`MEOSBridge`](../src/main/java/berlinmod/MEOSBridge.java)).

## Method

Each cell runs as its own Flink job over a shared synthetic BerlinMOD corpus of
50 vehicles × 600 events = 30 000 events spread over 600 s of event time, with
vehicles distributed on a disc around Brussels centre and drifting per event.
The job is terminated by a counting sink; the harness
([`BerlinMODBenchmark`](../src/main/java/berlinmod/BerlinMODBenchmark.java))
measures wall-clock around `env.execute()` and reports throughput as events ÷
wall-clock and the sink's output cardinality. Parallelism is 1. Wall-clock
includes the per-job mini-cluster startup (~0.7–0.8 s), so the figures are
end-to-end job throughput rather than steady-state per-operator rate; the
steady-state row below amortises that startup over a larger corpus.

Environment: Flink 1.16.0, Java 21, 16-core x86-64 Linux; libmeos built with
`-DMEOS=ON -DCBUFFER=ON -DNPOINT=ON -DPOSE=ON -DRGEO=ON`.

Run from `flink-processor/`:

```
LD_LIBRARY_PATH=<libmeos-dir> java \
  --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.util=ALL-UNNAMED \
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
  --add-opens=java.base/java.io=ALL-UNNAMED \
  --add-opens=java.base/java.time=ALL-UNNAMED \
  -cp target/classes:jar/JMEOS.jar:<deps> \
  berlinmod.BerlinMODBenchmark [numVehicles] [eventsPerVehicle] [cellFilter]
```

## Results — 30 000 events

| Cell | Events in | Output rows | Wall (ms) | Throughput (ev/s) |
|---|---:|---:|---:|---:|
| Q1-continuous | 30000 | 50 | 2233 | 13,435 |
| Q1-windowed | 30000 | 60 | 1065 | 28,169 |
| Q1-snapshot | 30000 | 6000 | 895 | 33,520 |
| Q2-continuous | 30000 | 600 | 798 | 37,594 |
| Q2-windowed | 30000 | 60 | 752 | 39,894 |
| Q2-snapshot | 30000 | 120 | 773 | 38,810 |
| Q3-continuous | 30000 | 30000 | 1577 | 19,023 |
| Q3-windowed | 30000 | 60 | 1271 | 23,603 |
| Q3-snapshot | 30000 | 3120 | 864 | 34,722 |
| Q4-continuous | 30000 | 8 | 1101 | 27,248 |
| Q4-windowed | 30000 | 480 | 1131 | 26,525 |
| Q4-snapshot | 30000 | 960 | 1073 | 27,959 |
| Q5-continuous | 30000 | 7171498 | 74391 | 403 |
| Q5-windowed | 30000 | 14359 | 915 | 32,787 |
| Q5-snapshot | 30000 | 29640 | 1079 | 27,804 |
| Q6-continuous | 30000 | 30000 | 1023 | 29,326 |
| Q6-windowed | 30000 | 3000 | 959 | 31,283 |
| Q6-snapshot | 30000 | 6000 | 929 | 32,293 |
| Q7-continuous | 30000 | 15 | 1272 | 23,585 |
| Q7-windowed | 30000 | 766 | 1304 | 23,006 |
| Q7-snapshot | 30000 | 1790 | 1469 | 20,422 |
| Q8-continuous | 30000 | 30000 | 1043 | 28,763 |
| Q8-windowed | 30000 | 60 | 1016 | 29,528 |
| Q8-snapshot | 30000 | 3720 | 826 | 36,320 |
| Q9-continuous | 30000 | 1199 | 769 | 39,012 |
| Q9-windowed | 30000 | 60 | 791 | 37,927 |
| Q9-snapshot | 30000 | 120 | 706 | 42,493 |

## Steady-state per-event predicate

Q3-continuous applies one MEOS `edwithin_tgeo_geo` per event (the canonical
within-distance operator). Over a 200 000-event corpus, with the per-job startup
amortised:

| Cell | Events in | Output rows | Wall (ms) | Throughput (ev/s) |
|---|---:|---:|---:|---:|
| Q3-continuous | 200000 | 200000 | 4435 | 45,096 |

## Characteristics

Q5-continuous enumerates every meeting pair across all vehicles on each event
(O(V²) per event, keyed to a single subtask), producing 7 171 498 rows from
30 000 events — the lowest throughput in the matrix, inherent to the all-pairs
meeting query rather than to the predicate path.
