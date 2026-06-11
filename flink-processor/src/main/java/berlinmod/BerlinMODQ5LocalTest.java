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
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
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
 * Local end-to-end test driver for the BerlinMOD-Q5 three streaming forms.
 *
 * <p>Same stationary-vehicle corpus as Q1/Q2/Q3/Q9. Reference point P =
 * the canonical sample area (canonical vehicle 1); {@code dP = 5 km} (vehicle near P);
 * {@code dMeet = 5 km} (pair-meeting threshold).
 *
 * <p>Pairs:
 * <ul>
 *   <li><b>(100, 200)</b> — both near P; distance 4.1 km ≤ dMeet → <b>MEET</b></li>
 *   <li>(100, 300) — vehicle 3 not near P → don't qualify</li>
 *   <li>(200, 300) — vehicle 3 not near P → don't qualify</li>
 * </ul>
 *
 * <p>Expected output (only the (100, 200) pair qualifies):
 * <ul>
 *   <li><b>Q5-continuous</b>: pair (100, 200) emits on every event from t=1
 *       onward (the first t=0 events of vehicle 1 and vehicle 3 happen before vehicle 2 is
 *       known, so no pair exists yet). 21 - 2 = 19 emissions.</li>
 *   <li><b>Q5-windowed</b>: each of the two 10-second windows contains
 *       events for vehicle 1 and vehicle 2 — both qualify, the pair meets. 2 emissions.</li>
 *   <li><b>Q5-snapshot</b>: 3 ticks × 1 meeting pair = 3 emissions.</li>
 * </ul>
 */
public class BerlinMODQ5LocalTest {

    private static final Logger LOG = LoggerFactory.getLogger(BerlinMODQ5LocalTest.class);

    private static final double P_LON = 4.3822;   // midpoint of vehicles 1 and 2
    private static final double P_LAT = 50.7683;
    private static final double D_P_METRES = 5_000.0;
    private static final double D_MEET_METRES = 8_000.0;
    private static final long WINDOW_SIZE_SECONDS = 10L;
    private static final long SNAPSHOT_TICK_MILLIS = 5_000L;
    private static final long T0 = 1_735_711_200_000L;

    public static void main(String[] args) throws Exception {
        LOG.info("BerlinMODQ5LocalTest starting; P=({}, {}) dP={}m dMeet={}m",
                P_LON, P_LAT, D_P_METRES, D_MEET_METRES);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        List<BerlinMODTrip> events = BerlinMODCorpus.loadSample();
        DataStreamSource<BerlinMODTrip> raw = env.fromCollection(events);
        DataStream<BerlinMODTrip> trips = raw.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<BerlinMODTrip>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner((e, t) -> e.getTimestamp()));

        DataStream<Tuple4<Integer, Integer, Long, Double>> cont = trips
                .keyBy(t -> 0)
                .process(new Q5ContinuousFunction(P_LON, P_LAT, D_P_METRES, D_MEET_METRES));
        cont.print("Q5-continuous");

        DataStream<Tuple5<Long, Long, Integer, Integer, Double>> win = trips
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(WINDOW_SIZE_SECONDS)))
                .process(new Q5WindowedFunction(P_LON, P_LAT, D_P_METRES, D_MEET_METRES));
        win.print("Q5-windowed");

        DataStream<Tuple4<Long, Integer, Integer, Double>> snap = trips
                .keyBy(t -> 0)
                .process(new Q5SnapshotFunction(P_LON, P_LAT, D_P_METRES, D_MEET_METRES, SNAPSHOT_TICK_MILLIS));
        snap.print("Q5-snapshot");

        env.execute("BerlinMODQ5LocalTest");
        LOG.info("BerlinMODQ5LocalTest done");
    }


}
