/*****************************************************************************
 *
 * This MobilityDB code is provided under The PostgreSQL License.
 * Copyright (c) 2020-2026, Université libre de Bruxelles and MobilityDB
 * contributors
 *
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for any purpose, without fee, and without a written
 * agreement is hereby granted, provided that the above copyright notice and
 * this paragraph and the following two paragraphs appear in all copies.
 *
 * IN NO EVENT SHALL UNIVERSITE LIBRE DE BRUXELLES BE LIABLE TO ANY PARTY FOR
 * DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
 * LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION,
 * EVEN IF UNIVERSITE LIBRE DE BRUXELLES HAS BEEN ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 *
 * UNIVERSITE LIBRE DE BRUXELLES SPECIFICALLY DISCLAIMS ANY WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
 * AND FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON
 * AN "AS IS" BASIS, AND UNIVERSITE LIBRE DE BRUXELLES HAS NO OBLIGATIONS TO
 * PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 *
 *****************************************************************************/

package org.mobilitydb.flink.meos.wirings.demo;

import jnr.ffi.Pointer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.mobilitydb.meos.MeosOpsFreeCore;
import org.mobilitydb.meos.MeosOpsTBox;
import org.mobilitydb.flink.meos.wirings.MeosBoundedStateMap;
import org.mobilitydb.flink.meos.wirings.MeosCrossStreamJoin;
import org.mobilitydb.flink.meos.wirings.MeosStatelessFilter;
import org.mobilitydb.flink.meos.wirings.MeosWindowedAggregate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;

/**
 * Capstone end-to-end demo composing ALL FOUR tier wirings in a single
 * Flink DataStream pipeline.
 *
 * <p>Proves the wirings compose into a realistic pipeline shape, not
 * just work in isolation. Each tier-wiring class drives one stage of
 * the pipeline:
 *
 * <pre>{@code
 *  Stream A (vehicles)                Stream B (queries)
 *       │                                  │
 *  ① MeosStatelessFilter                   │
 *      (keep events in regions of interest)│
 *       │                                  │
 *  ② MeosBoundedStateMap                   │
 *      (per-vehicle running tbox union)    │
 *       │                                  │
 *  ③ MeosWindowedAggregate                 │
 *      (30s tumbling per-vehicle aggregate)│
 *       │                                  │
 *  └─────────────┐                  ┌──────┘
 *                ↓                  ↓
 *  ④ MeosCrossStreamJoin
 *       (interval-join: vehicle aggregates vs region queries
 *        within ±1m time bound, match by region key)
 *                ↓
 *           output
 * }</pre>
 *
 * <p>The pipeline answers: "for each region, which vehicles had an
 * aggregate trajectory (running union) overlapping the region's
 * query bbox during the latest 30-second window?"
 *
 * <p>Tier per stage:
 * <ol>
 *   <li><b>Stateless filter</b> — drop events outside any region of
 *       interest (per-event predicate, no state).</li>
 *   <li><b>Bounded-state map</b> — per-vehicle running tbox union
 *       (MEOS handle persisted across events as byte[] state).</li>
 *   <li><b>Windowed aggregate</b> — per-vehicle 30s tumbling tbox
 *       (window-close-only aggregation, no handle persistence across
 *       windows).</li>
 *   <li><b>Cross-stream join</b> — interval-join vehicle aggregates
 *       against region queries (pre-keyed by region, ±1m bound).</li>
 * </ol>
 *
 * <p>Run with:
 *
 * <pre>{@code
 * mvn -q exec:java \
 *     -Dexec.mainClass=org.mobilitydb.flink.meos.wirings.demo.MeosAllTiersCapstoneDemo \
 *     -Dmeos.enabled=true
 * }</pre>
 */
public final class MeosAllTiersCapstoneDemo {

    private static final Logger LOG = LoggerFactory.getLogger(MeosAllTiersCapstoneDemo.class);

    /** Region IDs we care about — the stateless filter drops events outside this set. */
    private static final java.util.Set<Integer> REGIONS_OF_INTEREST =
            new java.util.HashSet<>(Arrays.asList(1, 2));

    /** Vehicle event stream — (vehicleId, regionId, eventTboxWKT, eventTimeMs). */
    private static final Tuple4<Integer, Integer, String, Long>[] VEHICLE_EVENTS = new Tuple4[]{
            // window 1: [0s, 30s)
            Tuple4.of(10, 1, "TBOX XT([0,2],[2026-01-01,2026-01-01 00:00:15])",   ts("00:00:00")),
            Tuple4.of(10, 1, "TBOX XT([1,3],[2026-01-01 00:00:15,2026-01-01 00:00:25])", ts("00:00:15")),
            Tuple4.of(20, 2, "TBOX XT([10,12],[2026-01-01,2026-01-01 00:00:15])", ts("00:00:05")),
            Tuple4.of(99, 9, "TBOX XT([90,92],[2026-01-01,2026-01-01 00:00:15])", ts("00:00:08")), // region 9 — dropped by stage 1
            Tuple4.of(20, 2, "TBOX XT([11,13],[2026-01-01 00:00:15,2026-01-01 00:00:25])", ts("00:00:20")),
            // window 2: [30s, 60s)
            Tuple4.of(10, 1, "TBOX XT([0,4],[2026-01-01 00:00:30,2026-01-01 00:00:45])", ts("00:00:30")),
            Tuple4.of(20, 2, "TBOX XT([10,15],[2026-01-01 00:00:30,2026-01-01 00:00:45])", ts("00:00:35")),
    };

    /** Region query stream — (regionId, queryTboxWKT, eventTimeMs). */
    private static final Tuple2<Integer, String>[] REGION_QUERIES = new Tuple2[]{
            Tuple2.of(1, "TBOX XT([1,3],[2026-01-01 00:00:10,2026-01-01 00:00:25])"),
            Tuple2.of(2, "TBOX XT([11,13],[2026-01-01 00:00:10,2026-01-01 00:00:25])"),
            Tuple2.of(1, "TBOX XT([2,4],[2026-01-01 00:00:35,2026-01-01 00:00:50])"),
            Tuple2.of(2, "TBOX XT([12,14],[2026-01-01 00:00:35,2026-01-01 00:00:50])"),
    };

    private static long ts(String hms) {
        String[] parts = hms.split(":");
        long secs = Integer.parseInt(parts[0]) * 3600L
                  + Integer.parseInt(parts[1]) * 60L
                  + Integer.parseInt(parts[2]);
        return 1767225600000L + secs * 1000L;
    }

    public static void main(String[] args) throws Exception {
        if (!MeosOpsTBox.MEOS_AVAILABLE) {
            LOG.error("MEOS not available — the demo requires libmeos.");
            System.exit(1);
        }

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // ── Stream A: vehicle events ────────────────────────────────────────
        DataStream<Tuple4<Integer, Integer, String, Long>> rawEvents =
                env.fromCollection(Arrays.asList(VEHICLE_EVENTS))
                   .assignTimestampsAndWatermarks(
                           WatermarkStrategy
                                   .<Tuple4<Integer, Integer, String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                                   .withTimestampAssigner((e, t) -> e.f3));

        // ── ① STATELESS FILTER ── keep only events in regions of interest ──
        DataStream<Tuple4<Integer, Integer, String, Long>> inRegion =
                rawEvents.filter(new MeosStatelessFilter<Tuple4<Integer, Integer, String, Long>>(
                        evt -> REGIONS_OF_INTEREST.contains(evt.f1)));

        // ── ② BOUNDED-STATE MAP ── per-vehicle running tbox union ──────────
        // State holds the MEOS-WKT text of the per-vehicle running union;
        // emit (vehicleId, regionId, runningUnionWKT, eventTimeMs) per event.
        DataStream<Tuple4<Integer, Integer, String, Long>> runningUnion = inRegion
                .keyBy(t -> t.f0)   // key by vehicleId
                .process(new MeosBoundedStateMap<Integer, Tuple4<Integer, Integer, String, Long>, Tuple4<Integer, Integer, String, Long>>(
                        ptr -> MeosOpsTBox.tbox_out(ptr, 6).getBytes(StandardCharsets.UTF_8),
                        bytes -> MeosOpsTBox.tbox_in(new String(bytes, StandardCharsets.UTF_8)),
                        (prior, evt) -> {
                            Pointer eventTbox = MeosOpsTBox.tbox_in(evt.f2);
                            Pointer newUnion = (prior == null)
                                    ? eventTbox
                                    : MeosOpsFreeCore.union_tbox_tbox(prior, eventTbox, /*strict=*/false);
                            Tuple4<Integer, Integer, String, Long> output =
                                    Tuple4.of(evt.f0, evt.f1, MeosOpsTBox.tbox_out(newUnion, 6), evt.f3);
                            return new MeosBoundedStateMap.MeosStep<>(newUnion, output);
                        }))
                .returns(org.apache.flink.api.common.typeinfo.TypeInformation.of(
                        new org.apache.flink.api.common.typeinfo.TypeHint<Tuple4<Integer, Integer, String, Long>>() {}));

        // ── ③ WINDOWED AGGREGATE ── per-vehicle 30s tumbling tbox union ─────
        // Within each 30s window: take the FINAL running-union value per
        // vehicle as the per-window summary.
        DataStream<Tuple4<Integer, Integer, String, Long>> windowed = runningUnion
                .keyBy(t -> t.f0)   // key by vehicleId
                .window(TumblingEventTimeWindows.of(Time.seconds(30)))
                .process(new MeosWindowedAggregate<
                        Integer,
                        Tuple4<Integer, Integer, String, Long>,
                        Tuple4<Integer, Integer, String, Long>,
                        TimeWindow
                        >((window, events, ctx) -> {
                            // Emit the LAST event in the window (the running union at window close).
                            Tuple4<Integer, Integer, String, Long> last = null;
                            for (Tuple4<Integer, Integer, String, Long> e : events) {
                                last = e;
                            }
                            return last;
                        }))
                .returns(org.apache.flink.api.common.typeinfo.TypeInformation.of(
                        new org.apache.flink.api.common.typeinfo.TypeHint<Tuple4<Integer, Integer, String, Long>>() {}));

        // ── Stream B: region queries (keyed by regionId for the join) ───────
        DataStream<Tuple2<Integer, String>> queryStream =
                env.fromCollection(Arrays.asList(REGION_QUERIES))
                   .assignTimestampsAndWatermarks(
                           WatermarkStrategy
                                   .<Tuple2<Integer, String>>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                                   .withTimestampAssigner((e, t) -> ts("00:00:20")));  // single query-time

        // ── ④ CROSS-STREAM JOIN ── vehicle aggregates × region queries ──────
        // Pre-key both by regionId; interval-join within ±1m time bound.
        // Per matched pair, emit (regionId, vehicleId, aggUnionWKT, queryWKT, vehicleTs).
        KeyedStream<Tuple4<Integer, Integer, String, Long>, Integer> vehiclesKeyed =
                windowed.keyBy(t -> t.f1);   // key by regionId
        KeyedStream<Tuple2<Integer, String>, Integer> queriesKeyed =
                queryStream.keyBy(q -> q.f0);

        DataStream<Tuple5<Integer, Integer, String, String, Long>> overlaps =
                vehiclesKeyed.intervalJoin(queriesKeyed)
                             .between(Time.minutes(-1), Time.minutes(1))
                             .process(new MeosCrossStreamJoin<
                                     Tuple4<Integer, Integer, String, Long>,
                                     Tuple2<Integer, String>,
                                     Tuple5<Integer, Integer, String, String, Long>
                                     >((vehAgg, query, ctx) -> {
                                         Pointer aggTbox   = MeosOpsTBox.tbox_in(vehAgg.f2);
                                         Pointer queryTbox = MeosOpsTBox.tbox_in(query.f1);
                                         if (MeosOpsFreeCore.overlaps_tbox_tbox(aggTbox, queryTbox)) {
                                             return Tuple5.of(vehAgg.f1, vehAgg.f0, vehAgg.f2, query.f1, vehAgg.f3);
                                         }
                                         return null;
                                     }))
                             .returns(org.apache.flink.api.common.typeinfo.TypeInformation.of(
                                     new org.apache.flink.api.common.typeinfo.TypeHint<Tuple5<Integer, Integer, String, String, Long>>() {}));

        overlaps.print("capstone-output");

        env.execute("MeosWirings capstone (all 4 tiers composed)");
    }
}
