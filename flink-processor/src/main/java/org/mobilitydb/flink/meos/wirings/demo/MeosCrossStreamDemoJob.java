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
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.mobilitydb.meos.MeosOpsFreeCore;
import org.mobilitydb.meos.MeosOpsTBox;
import org.mobilitydb.flink.meos.wirings.MeosCrossStreamJoin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;

/**
 * End-to-end runnable demo of the {@code cross-stream} tier wiring.
 *
 * <p>Pipeline:
 * <ol>
 *   <li>Two parallel streams, each carrying {@code (regionId,
 *       vehicleId, tboxWKT, eventTimeMs)}, sharing the {@code regionId}
 *       key so cross-stream pairing is per-region.</li>
 *   <li>{@code keyBy(regionId)} on both, then
 *       {@code .intervalJoin().between(-1m, +1m)} so each event in
 *       stream A is matched with events in stream B within ±1 minute
 *       in the same region.</li>
 *   <li>{@link MeosCrossStreamJoin}: for each matched pair, test
 *       whether the two tboxes overlap via
 *       {@code MeosOpsFreeCore.overlaps_tbox_tbox}; if yes, emit
 *       {@code (regionId, vehAId, vehBId, leftTs, rightTs)}.</li>
 * </ol>
 *
 * <p>What the demo proves:
 * <ul>
 *   <li><b>Interval-join semantics</b> — only pairs within the time
 *       bound are matched; outside-window events are skipped.</li>
 *   <li><b>Per-key isolation</b> — events in region 1 don't match
 *       events in region 2, even if their timestamps overlap.</li>
 *   <li><b>Pairwise MEOS call</b> — the wiring lambda receives both
 *       matched events; the adopter calls any cross-stream MeosOps
 *       method on the pair (here {@code overlaps_tbox_tbox}, which
 *       is technically stateless on box pairs but the join-pairing
 *       is what makes it cross-stream).</li>
 * </ul>
 *
 * <p>Run with:
 *
 * <pre>{@code
 * mvn -q exec:java \
 *     -Dexec.mainClass=org.mobilitydb.flink.meos.wirings.demo.MeosCrossStreamDemoJob \
 *     -Dmeos.enabled=true
 * }</pre>
 */
public final class MeosCrossStreamDemoJob {

    private static final Logger LOG = LoggerFactory.getLogger(MeosCrossStreamDemoJob.class);

    /** Stream A — vehicle events, 3 per region across 2 regions. */
    private static final Tuple4<Integer, Integer, String, Long>[] EVENTS_A = new Tuple4[]{
            Tuple4.of(1, 10, "TBOX XT([0,5],[2026-01-01,2026-01-01 00:00:30])",   ts("00:00:00")),
            Tuple4.of(2, 20, "TBOX XT([100,105],[2026-01-01,2026-01-01 00:00:30])", ts("00:00:05")),
            Tuple4.of(1, 11, "TBOX XT([10,15],[2026-01-01 00:00:30,2026-01-01 00:01:00])", ts("00:00:30")),
    };

    /** Stream B — different vehicles, 3 per region across 2 regions. */
    private static final Tuple4<Integer, Integer, String, Long>[] EVENTS_B = new Tuple4[]{
            Tuple4.of(1, 30, "TBOX XT([3,8],[2026-01-01,2026-01-01 00:00:30])",   ts("00:00:10")), // overlaps with A:(1,10)
            Tuple4.of(2, 40, "TBOX XT([200,205],[2026-01-01,2026-01-01 00:00:30])", ts("00:00:15")), // disjoint from A:(2,20)
            Tuple4.of(1, 31, "TBOX XT([12,17],[2026-01-01 00:00:30,2026-01-01 00:01:00])", ts("00:00:40")), // overlaps with A:(1,11)
    };

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

        DataStream<Tuple4<Integer, Integer, String, Long>> a =
                env.fromCollection(Arrays.asList(EVENTS_A))
                   .assignTimestampsAndWatermarks(
                           WatermarkStrategy
                                   .<Tuple4<Integer, Integer, String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                                   .withTimestampAssigner((e, ts) -> e.f3));
        DataStream<Tuple4<Integer, Integer, String, Long>> b =
                env.fromCollection(Arrays.asList(EVENTS_B))
                   .assignTimestampsAndWatermarks(
                           WatermarkStrategy
                                   .<Tuple4<Integer, Integer, String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                                   .withTimestampAssigner((e, ts) -> e.f3));

        KeyedStream<Tuple4<Integer, Integer, String, Long>, Integer> aKeyed = a.keyBy(t -> t.f0);
        KeyedStream<Tuple4<Integer, Integer, String, Long>, Integer> bKeyed = b.keyBy(t -> t.f0);

        // Interval-join: pair events in A with events in B within ±1 minute, same region key.
        DataStream<Tuple5<Integer, Integer, Integer, Long, Long>> overlaps =
                aKeyed.intervalJoin(bKeyed)
                      .between(Time.minutes(-1), Time.minutes(1))
                      .process(new MeosCrossStreamJoin<
                              Tuple4<Integer, Integer, String, Long>,  // L
                              Tuple4<Integer, Integer, String, Long>,  // R
                              Tuple5<Integer, Integer, Integer, Long, Long>  // OUT: (region, vehA, vehB, lts, rts)
                              >((left, right, ctx) -> {
                                  Pointer leftTbox  = MeosOpsTBox.tbox_in(left.f2);
                                  Pointer rightTbox = MeosOpsTBox.tbox_in(right.f2);
                                  if (MeosOpsFreeCore.overlaps_tbox_tbox(leftTbox, rightTbox)) {
                                      return Tuple5.of(left.f0, left.f1, right.f1,
                                                       ctx.getLeftTimestamp(), ctx.getRightTimestamp());
                                  }
                                  return null;
                              }))
                      .returns(org.apache.flink.api.common.typeinfo.TypeInformation.of(
                              new org.apache.flink.api.common.typeinfo.TypeHint<Tuple5<Integer, Integer, Integer, Long, Long>>() {}));

        overlaps.print("cross-stream-overlap");

        env.execute("MeosWirings cross-stream tier demo");
    }
}
