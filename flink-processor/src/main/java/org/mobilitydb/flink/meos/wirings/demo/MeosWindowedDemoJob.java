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
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.mobilitydb.meos.MeosOpsFreeCore;
import org.mobilitydb.meos.MeosOpsTBox;
import org.mobilitydb.flink.meos.wirings.MeosWindowedAggregate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;

/**
 * End-to-end runnable demo of the {@code windowed} tier wiring.
 *
 * <p>Pipeline:
 * <ol>
 *   <li>Stream of {@code (vehicleId, tboxWKT, eventTimeMs)} for 2
 *       vehicles, 4 events each.</li>
 *   <li>{@code assignTimestampsAndWatermarks} so event-time windows
 *       fire on a bounded-out-of-orderness schedule.</li>
 *   <li>{@code keyBy(vehicleId)} → 30-second tumbling event-time
 *       window.</li>
 *   <li>Per-window {@link MeosWindowedAggregate}: union all in-window
 *       event tboxes into a single per-window aggregate tbox via
 *       repeated {@code MeosOpsFreeCore.union_tbox_tbox}, emit
 *       {@code (vehicleId, windowStart, eventCount, aggregateTboxWKT)}.</li>
 * </ol>
 *
 * <p>What the demo proves:
 * <ul>
 *   <li><b>Window-close timing</b> — events outside the window are
 *       excluded; events within are aggregated together.</li>
 *   <li><b>Per-key isolation</b> — vehicle 1's window aggregate does
 *       not include vehicle 2's events, and vice versa.</li>
 *   <li><b>Stateless aggregation</b> — unlike {@code bounded-state},
 *       no MEOS handle persists across window boundaries; each window
 *       builds its aggregate from scratch from the iterable.</li>
 * </ul>
 *
 * <p>Run with:
 *
 * <pre>{@code
 * mvn -q exec:java \
 *     -Dexec.mainClass=org.mobilitydb.flink.meos.wirings.demo.MeosWindowedDemoJob \
 *     -Dmeos.enabled=true
 * }</pre>
 *
 * <p>Expected output: 4 lines (2 windows × 2 vehicles), each showing
 * the aggregate tbox spanning that window's events for that vehicle.
 */
public final class MeosWindowedDemoJob {

    private static final Logger LOG = LoggerFactory.getLogger(MeosWindowedDemoJob.class);

    /** 8 events across 2 vehicles, two 30s windows each. */
    private static final Tuple3<Integer, String, Long>[] EVENTS = new Tuple3[]{
            // window 1: [0s, 30s)
            Tuple3.of(1, "TBOX XT([0,2],[2026-01-01,2026-01-01 00:00:10])",   ts("00:00:00")),
            Tuple3.of(2, "TBOX XT([10,12],[2026-01-01,2026-01-01 00:00:10])", ts("00:00:05")),
            Tuple3.of(1, "TBOX XT([3,5],[2026-01-01 00:00:10,2026-01-01 00:00:20])", ts("00:00:10")),
            Tuple3.of(2, "TBOX XT([13,15],[2026-01-01 00:00:10,2026-01-01 00:00:20])", ts("00:00:15")),
            // window 2: [30s, 60s)
            Tuple3.of(1, "TBOX XT([1,4],[2026-01-01 00:00:30,2026-01-01 00:00:40])", ts("00:00:30")),
            Tuple3.of(2, "TBOX XT([11,14],[2026-01-01 00:00:30,2026-01-01 00:00:40])", ts("00:00:35")),
            Tuple3.of(1, "TBOX XT([2,3],[2026-01-01 00:00:40,2026-01-01 00:00:50])", ts("00:00:40")),
            Tuple3.of(2, "TBOX XT([12,13],[2026-01-01 00:00:40,2026-01-01 00:00:50])", ts("00:00:45")),
    };

    /** Convert "HH:MM:SS" relative to 2026-01-01T00:00:00 into epoch milliseconds. */
    private static long ts(String hms) {
        String[] parts = hms.split(":");
        long secs = Integer.parseInt(parts[0]) * 3600L
                  + Integer.parseInt(parts[1]) * 60L
                  + Integer.parseInt(parts[2]);
        return 1767225600000L + secs * 1000L;   // 2026-01-01T00:00:00 UTC in ms
    }

    public static void main(String[] args) throws Exception {
        if (!MeosOpsTBox.MEOS_AVAILABLE) {
            LOG.error("MEOS not available — the demo requires libmeos.");
            System.exit(1);
        }

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Tuple3<Integer, String, Long>> events =
                env.fromCollection(Arrays.asList(EVENTS))
                   .assignTimestampsAndWatermarks(
                           WatermarkStrategy
                                   .<Tuple3<Integer, String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                                   .withTimestampAssigner((e, ts) -> e.f2));

        DataStream<Tuple4<Integer, Long, Integer, String>> aggregates = events
                .keyBy(t -> t.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(30)))
                .process(new MeosWindowedAggregate<
                        Integer,                                        // K
                        Tuple3<Integer, String, Long>,                  // IN
                        Tuple4<Integer, Long, Integer, String>,         // OUT
                        TimeWindow                                      // W
                        >((window, inWindowEvents, ctx) -> {
                            Pointer agg = null;
                            int count = 0;
                            for (Tuple3<Integer, String, Long> evt : inWindowEvents) {
                                Pointer evtTbox = MeosOpsTBox.tbox_in(evt.f1);
                                agg = (agg == null)
                                        ? evtTbox
                                        : MeosOpsFreeCore.union_tbox_tbox(agg, evtTbox, /*strict=*/false);
                                count++;
                            }
                            String aggWkt = (agg == null) ? "(empty)" : MeosOpsTBox.tbox_out(agg, 6);
                            return Tuple4.of(ctx.getCurrentKey(), window.getStart(), count, aggWkt);
                        }))
                .returns(org.apache.flink.api.common.typeinfo.TypeInformation.of(
                        new org.apache.flink.api.common.typeinfo.TypeHint<Tuple4<Integer, Long, Integer, String>>() {}));

        aggregates.print("window-aggregate");

        env.execute("MeosWirings windowed tier demo");
    }
}
