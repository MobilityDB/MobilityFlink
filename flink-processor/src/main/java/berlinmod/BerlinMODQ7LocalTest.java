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
import org.apache.flink.api.java.tuple.Tuple3;
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
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;

/**
 * Local end-to-end test driver for the BerlinMOD-Q7 three streaming forms.
 *
 * <p>Same stationary-vehicle corpus as Q1/Q2/Q3/Q5/Q9. POI list:
 * <ul>
 *   <li>POI 1 = the canonical sample area (canonical vehicle 1), radius 2000 m</li>
 *   <li>POI 2 = Anderlecht (canonical vehicle 2), radius 1000 m</li>
 *   <li>POI 3 = south of Brussels (canonical vehicle 3), radius 2000 m</li>
 * </ul>
 *
 * <p>Per (vehicle, POI) match-up:
 * <ul>
 *   <li>vehicle 1 is inside POI 1 (0 m), outside POI 2 (~4.1 km) and POI 3 (~13 km)</li>
 *   <li>vehicle 2 is inside POI 2 (0 m), outside POI 1 and POI 3</li>
 *   <li>vehicle 3 is inside POI 3 (~1.3 km), outside POI 1 and POI 2</li>
 * </ul>
 *
 * <p>Expected output:
 * <ul>
 *   <li><b>Q7-continuous</b>: 3 emissions — first-passages on each vehicle's
 *       very first event (vehicle 1 t=0 → POI 1; vehicle 2 t=1 → POI 2; vehicle 3 t=0 →
 *       POI 3)</li>
 *   <li><b>Q7-windowed</b>: per-window intra-window first-passages —
 *       window [0, 10 s) sees all 3 first-passages; window [10, 20 s) sees
 *       all 3 again (intra-window scoping has no cross-window memory). 6 lines.</li>
 *   <li><b>Q7-snapshot</b>: 3 ticks × 3 cumulative (vehicle, POI) first-passages = 9 lines</li>
 * </ul>
 */
public class BerlinMODQ7LocalTest {

    private static final Logger LOG = LoggerFactory.getLogger(BerlinMODQ7LocalTest.class);

    private static final long WINDOW_SIZE_SECONDS = 10L;
    private static final long SNAPSHOT_TICK_MILLIS = 5_000L;
    private static final long T0 = 1_735_711_200_000L;

    private static final List<PointOfInterest> POIS = Arrays.asList(
            new PointOfInterest(1, 4.3321, 50.7696, 2_000.0),   // near vehicle 2
            new PointOfInterest(2, 4.4571, 50.8515, 2_000.0),   // near vehicle 3
            new PointOfInterest(3, 4.4252, 50.9190, 2_000.0));  // near vehicle 5

    public static void main(String[] args) throws Exception {
        LOG.info("BerlinMODQ7LocalTest starting; #POIs={} window={}s tick={}ms",
                POIS.size(), WINDOW_SIZE_SECONDS, SNAPSHOT_TICK_MILLIS);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        List<BerlinMODTrip> events = BerlinMODCorpus.loadSample();
        DataStreamSource<BerlinMODTrip> raw = env.fromCollection(events);
        DataStream<BerlinMODTrip> trips = raw.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<BerlinMODTrip>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner((e, t) -> e.getTimestamp()));

        DataStream<Tuple3<Integer, Integer, Long>> cont = trips
                .keyBy(BerlinMODTrip::getVehicleId)
                .process(new Q7ContinuousFunction(POIS));
        cont.print("Q7-continuous");

        DataStream<Tuple5<Long, Long, Integer, Integer, Long>> win = trips
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(WINDOW_SIZE_SECONDS)))
                .process(new Q7WindowedFunction(POIS));
        win.print("Q7-windowed");

        DataStream<Tuple4<Long, Integer, Integer, Long>> snap = trips
                .keyBy(BerlinMODTrip::getVehicleId)
                .process(new Q7SnapshotFunction(POIS, SNAPSHOT_TICK_MILLIS));
        snap.print("Q7-snapshot");

        env.execute("BerlinMODQ7LocalTest");
        LOG.info("BerlinMODQ7LocalTest done");
    }


}
