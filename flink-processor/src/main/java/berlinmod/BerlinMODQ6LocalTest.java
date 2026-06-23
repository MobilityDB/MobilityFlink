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
 * Local end-to-end test driver for the BerlinMOD-Q6 three streaming forms.
 *
 * <p>Unlike Q1/Q2/Q3 which use a stationary-vehicles corpus, Q6 needs vehicles
 * that actually move so the cumulative-distance arithmetic produces non-zero
 * output. The synthetic corpus here drifts each vehicle by a fixed bearing
 * per event:
 *
 * <ul>
 *   <li><b>Vehicle 100</b> drifts east ~100 m per 2 s event (0.001423° lon at lat 50.85)</li>
 *   <li><b>Vehicle 200</b> drifts south ~50 m per 2 s event (0.000450° lat)</li>
 *   <li><b>Vehicle 300</b> drifts west ~200 m per 2 s event (0.002846° lon)</li>
 * </ul>
 *
 * <p>With 7 events per vehicle (6 inter-event steps), per-vehicle totals are
 * approximately:
 *
 * <ul>
 *   <li>vehicle 1: 6 × 100 m = 600 m</li>
 *   <li>vehicle 2: 6 × 50 m = 300 m</li>
 *   <li>vehicle 3: 6 × 200 m = 1200 m</li>
 * </ul>
 *
 * <p>Expected output:
 *
 * <ul>
 *   <li><b>Q6-continuous</b>: 21 lines, cumulative metres rising monotonically per vehicle</li>
 *   <li><b>Q6-windowed</b>: 6 windowed emissions (2 windows × 3 vehicles)</li>
 *   <li><b>Q6-snapshot</b>: 9 emissions (3 ticks × 3 vehicles, all-source-closed)</li>
 * </ul>
 */
public class BerlinMODQ6LocalTest {

    private static final Logger LOG = LoggerFactory.getLogger(BerlinMODQ6LocalTest.class);

    private static final long WINDOW_SIZE_SECONDS = 10L;
    private static final long SNAPSHOT_TICK_MILLIS = 5_000L;
    private static final long T0 = 1_735_711_200_000L;

    // Drift per event step
    private static final double V100_DLON = 100.0 / (111_000.0 * Math.cos(Math.toRadians(50.85)));
    private static final double V200_DLAT = -50.0 / 111_000.0;
    private static final double V300_DLON = -200.0 / (111_000.0 * Math.cos(Math.toRadians(50.85)));

    public static void main(String[] args) throws Exception {
        LOG.info("BerlinMODQ6LocalTest starting; window={}s tick={}ms",
                WINDOW_SIZE_SECONDS, SNAPSHOT_TICK_MILLIS);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        List<BerlinMODTrip> events = BerlinMODCorpus.loadSample();
        DataStreamSource<BerlinMODTrip> raw = env.fromCollection(events);
        DataStream<BerlinMODTrip> trips = raw.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<BerlinMODTrip>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner((e, t) -> e.getTimestamp()));

        DataStream<Tuple3<Integer, Long, Double>> cont = trips
                .keyBy(BerlinMODTrip::getVehicleId)
                .process(new Q6ContinuousFunction());
        cont.print("Q6-continuous");

        DataStream<Tuple4<Long, Long, Integer, Double>> win = trips
                .keyBy(BerlinMODTrip::getVehicleId)
                .window(TumblingEventTimeWindows.of(Time.seconds(WINDOW_SIZE_SECONDS)))
                .process(new Q6WindowedFunction());
        win.print("Q6-windowed");

        DataStream<Tuple3<Long, Integer, Double>> snap = trips
                .keyBy(BerlinMODTrip::getVehicleId)
                .process(new Q6SnapshotFunction(SNAPSHOT_TICK_MILLIS));
        snap.print("Q6-snapshot");

        env.execute("BerlinMODQ6LocalTest");
        LOG.info("BerlinMODQ6LocalTest done");
    }

}
