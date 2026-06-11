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
 * Local end-to-end test driver for the BerlinMOD-Q9 three streaming forms.
 *
 * <p>Same stationary-vehicle synthetic corpus as Q1/Q2/Q3 (3 vehicles, 21
 * events). Queried pair X = 100 (the canonical sample area), Y = 200 (Anderlecht);
 * their actual distance is ~4.1 km — the expected output for every emission.
 *
 * <p>Expected output:
 * <ul>
 *   <li><b>Q9-continuous</b>: 13 lines — emitted whenever either X or Y has
 *       a new event AND the other has been seen at least once. vehicle 1 fires
 *       first at t=0; vehicle 2's first event at t=1 produces the first paired
 *       emission; subsequent 12 events (alternating) each produce one
 *       emission.</li>
 *   <li><b>Q9-windowed</b>: 2 windows — both contain X and Y events, each
 *       emits the X-Y distance using last seen-in-window positions.</li>
 *   <li><b>Q9-snapshot</b>: 3 ticks × 1 emission each = 3 lines.</li>
 * </ul>
 */
public class BerlinMODQ9LocalTest {

    private static final Logger LOG = LoggerFactory.getLogger(BerlinMODQ9LocalTest.class);

    private static final int X_VEHICLE_ID = 1;
    private static final int Y_VEHICLE_ID = 2;
    private static final long WINDOW_SIZE_SECONDS = 10L;
    private static final long SNAPSHOT_TICK_MILLIS = 5_000L;
    private static final long T0 = 1_735_711_200_000L;

    public static void main(String[] args) throws Exception {
        LOG.info("BerlinMODQ9LocalTest starting; X={} Y={} window={}s tick={}ms",
                X_VEHICLE_ID, Y_VEHICLE_ID, WINDOW_SIZE_SECONDS, SNAPSHOT_TICK_MILLIS);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        List<BerlinMODTrip> events = BerlinMODCorpus.loadSample();
        DataStreamSource<BerlinMODTrip> raw = env.fromCollection(events);
        DataStream<BerlinMODTrip> trips = raw.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<BerlinMODTrip>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner((e, t) -> e.getTimestamp()));

        // Pre-filter to {X, Y} and key by a constant so the shared X+Y state
        // lives in a single subtask.
        DataStream<BerlinMODTrip> xy = trips
                .filter(t -> t.getVehicleId() == X_VEHICLE_ID || t.getVehicleId() == Y_VEHICLE_ID);

        DataStream<Tuple2<Long, Double>> cont = xy
                .keyBy(t -> 0)
                .process(new Q9ContinuousFunction(X_VEHICLE_ID, Y_VEHICLE_ID));
        cont.print("Q9-continuous");

        DataStream<Tuple3<Long, Long, Double>> win = xy
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(WINDOW_SIZE_SECONDS)))
                .process(new Q9WindowedFunction(X_VEHICLE_ID, Y_VEHICLE_ID));
        win.print("Q9-windowed");

        DataStream<Tuple2<Long, Double>> snap = xy
                .keyBy(t -> 0)
                .process(new Q9SnapshotFunction(X_VEHICLE_ID, Y_VEHICLE_ID, SNAPSHOT_TICK_MILLIS));
        snap.print("Q9-snapshot");

        env.execute("BerlinMODQ9LocalTest");
        LOG.info("BerlinMODQ9LocalTest done");
    }


}
