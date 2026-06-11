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
 * Local end-to-end test driver for the BerlinMOD-Q8 three streaming forms.
 *
 * <p>Same stationary-vehicle corpus as the other Qs. Road segment runs
 * from (4.30, 50.83) to (4.36, 50.87) — a diagonal across the Brussels-
 * centre region. With a {@code d = 5 km} proximity threshold:
 *
 * <ul>
 *   <li><b>vehicle 1</b> at (canonical vehicle 1) — ~1.1 km from segment → <b>near</b></li>
 *   <li><b>vehicle 2</b> at (canonical vehicle 2) — ~0.5 km from segment → <b>near</b></li>
 *   <li><b>vehicle 3</b> at (canonical vehicle 3) — ~13 km from segment → <b>not near</b></li>
 * </ul>
 *
 * <p>Expected output shape:
 * <ul>
 *   <li><b>Q8-continuous</b>: 21 events (14 near=true for vehicle 1/vehicle 2, 7 near=false for vehicle 3)</li>
 *   <li><b>Q8-windowed</b>: 2 windows, each with {@code distinctCount=2} (vehicles 100 and 200)</li>
 *   <li><b>Q8-snapshot</b>: 3 ticks × 2 near vehicles = 6 emissions</li>
 * </ul>
 *
 * <p>Same shape as Q3 with a segment-distance predicate substituted for the
 * point-radius one — the algebraic pattern parity intentional.
 */
public class BerlinMODQ8LocalTest {

    private static final Logger LOG = LoggerFactory.getLogger(BerlinMODQ8LocalTest.class);

    // Road segment endpoints
    private static final double S1_LON = 4.3321, S1_LAT = 50.7696;  // vehicle 2
    private static final double S2_LON = 4.3063, S2_LAT = 50.8825;  // vehicle 4
    private static final double RADIUS_METRES = 5_000.0;
    private static final long WINDOW_SIZE_SECONDS = 10L;
    private static final long SNAPSHOT_TICK_MILLIS = 5_000L;
    private static final long T0 = 1_735_711_200_000L;

    public static void main(String[] args) throws Exception {
        LOG.info("BerlinMODQ8LocalTest starting; segment=({},{}) → ({},{}) d={}m",
                S1_LON, S1_LAT, S2_LON, S2_LAT, RADIUS_METRES);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        List<BerlinMODTrip> events = BerlinMODCorpus.loadSample();
        DataStreamSource<BerlinMODTrip> raw = env.fromCollection(events);
        DataStream<BerlinMODTrip> trips = raw.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<BerlinMODTrip>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner((e, t) -> e.getTimestamp()));

        DataStream<Tuple3<Integer, Long, Boolean>> cont = trips
                .process(new Q8ContinuousFunction(S1_LON, S1_LAT, S2_LON, S2_LAT, RADIUS_METRES));
        cont.print("Q8-continuous");

        DataStream<Tuple3<Long, Long, Long>> win = trips
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(WINDOW_SIZE_SECONDS)))
                .process(new Q8WindowedFunction(S1_LON, S1_LAT, S2_LON, S2_LAT, RADIUS_METRES));
        win.print("Q8-windowed");

        DataStream<Tuple2<Long, Integer>> snap = trips
                .keyBy(BerlinMODTrip::getVehicleId)
                .process(new Q8SnapshotFunction(S1_LON, S1_LAT, S2_LON, S2_LAT, RADIUS_METRES, SNAPSHOT_TICK_MILLIS));
        snap.print("Q8-snapshot");

        env.execute("BerlinMODQ8LocalTest");
        LOG.info("BerlinMODQ8LocalTest done");
    }


}
