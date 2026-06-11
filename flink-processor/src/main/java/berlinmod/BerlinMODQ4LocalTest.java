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

package berlinmod;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Local end-to-end test driver for the BerlinMOD-Q4 three streaming forms.
 *
 * <p>Region R = bounding box {@code (4.30, 50.84, 4.36, 50.86)} — a rectangle
 * around the canonical sample area. The synthetic corpus is designed to produce
 * <i>multiple</i> outside → inside transitions so the entry-detection logic
 * is exercised non-trivially:
 *
 * <ul>
 *   <li><b>Vehicle 100</b> sits inside R for all 7 events (no transitions).</li>
 *   <li><b>Vehicle 200</b> oscillates: outside at t=1, inside at t=3 (entry),
 *       outside at t=5, inside at t=7 (entry), outside at t=9, inside at
 *       t=11 (entry), outside at t=13 → <b>three entries</b>.</li>
 *   <li><b>Vehicle 300</b> stays in Forest (outside R) for all 7 events.</li>
 * </ul>
 *
 * <p>Expected output:
 * <ul>
 *   <li><b>Q4-continuous</b>: 3 entries (vehicle 2's three outside → inside transitions)</li>
 *   <li><b>Q4-windowed</b>: per the intra-window scoping convention — window
 *       [0, 10 s) contains vehicle 1's first-seen-inside event AND vehicle 2's two entries
 *       (t=3, t=7); window [10, 20 s) contains vehicle 1's first-event-in-window
 *       AND vehicle 2's third entry (t=11). 5 emissions total.</li>
 *   <li><b>Q4-snapshot</b>: cumulative entries up to each tick. Tick 5: 1
 *       (vehicle 2 t=3). Tick 10: 2 (vehicle 2 t=3, t=7). Tick 15: 3 (vehicle 2 t=3, t=7,
 *       t=11). vehicle 1 contributes 0 (always inside, no transition). vehicle 3
 *       contributes 0. 6 emissions total (1+2+3).</li>
 * </ul>
 */
public class BerlinMODQ4LocalTest {

    private static final Logger LOG = LoggerFactory.getLogger(BerlinMODQ4LocalTest.class);

    // Region R — Brussels centre rectangle
    private static final double XMIN = 4.40;
    private static final double YMIN = 50.74;
    private static final double XMAX = 4.47;
    private static final double YMAX = 50.86;

    private static final long WINDOW_SIZE_SECONDS = 10L;
    private static final long SNAPSHOT_TICK_MILLIS = 5_000L;
    private static final long T0 = 1_735_711_200_000L;

    public static void main(String[] args) throws Exception {
        LOG.info("BerlinMODQ4LocalTest starting; R=({},{},{},{}) window={}s tick={}ms",
                XMIN, YMIN, XMAX, YMAX, WINDOW_SIZE_SECONDS, SNAPSHOT_TICK_MILLIS);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        List<BerlinMODTrip> events = BerlinMODCorpus.loadSample();
        DataStreamSource<BerlinMODTrip> raw = env.fromCollection(events);
        DataStream<BerlinMODTrip> trips = raw.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<BerlinMODTrip>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner((e, t) -> e.getTimestamp()));

        DataStream<Tuple2<Integer, Long>> cont = trips
                .keyBy(BerlinMODTrip::getVehicleId)
                .process(new Q4ContinuousFunction(XMIN, YMIN, XMAX, YMAX));
        cont.print("Q4-continuous");

        DataStream<Tuple4<Long, Long, Integer, Long>> win = trips
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(WINDOW_SIZE_SECONDS)))
                .process(new Q4WindowedFunction(XMIN, YMIN, XMAX, YMAX));
        win.print("Q4-windowed");

        DataStream<Tuple3<Long, Integer, Long>> snap = trips
                .keyBy(BerlinMODTrip::getVehicleId)
                .process(new Q4SnapshotFunction(XMIN, YMIN, XMAX, YMAX, SNAPSHOT_TICK_MILLIS));
        snap.print("Q4-snapshot");

        env.execute("BerlinMODQ4LocalTest");
        LOG.info("BerlinMODQ4LocalTest done");
    }


}
