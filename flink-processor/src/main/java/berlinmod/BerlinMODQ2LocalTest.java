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
 * Local end-to-end test driver for the BerlinMOD-Q2 three streaming forms.
 *
 * <p>Same structural shape as {@link BerlinMODQ3LocalTest} but exercises Q2 ("where is vehicle X
 * at time T?") with {@code X = 200} (the Anderlecht vehicle). Same 3-vehicle /
 * 21-event synthetic corpus.
 *
 * <p>Expected output shape (with {@code X = 200}):
 * <ul>
 *   <li><b>Q2-continuous</b>: 7 events (the 7 vehicle-200 events; vehicles 100 and 300 filtered out)</li>
 *   <li><b>Q2-windowed</b>: 2 windows of size 10 s, each emitting the last vehicle-200 position seen in the window</li>
 *   <li><b>Q2-snapshot</b>: 3 ticks × 1 emission each = 3 lines (vehicle 200's last-known position at each 5 s tick)</li>
 * </ul>
 *
 * <p>Run after {@code mvn package} with:
 * <pre>
 *   java -cp target/flink-kafka2postgres-1.0-SNAPSHOT.jar berlinmod.BerlinMODQ2LocalTest
 * </pre>
 */
public class BerlinMODQ2LocalTest {

    private static final Logger LOG = LoggerFactory.getLogger(BerlinMODQ2LocalTest.class);

    private static final int TARGET_VEHICLE_ID = 200;
    private static final long WINDOW_SIZE_SECONDS = 10L;
    private static final long SNAPSHOT_TICK_MILLIS = 5_000L;
    private static final long T0 = 1_735_711_200_000L; // 2025-01-01 06:00:00 UTC

    public static void main(String[] args) throws Exception {
        LOG.info("BerlinMODQ2LocalTest starting; X={} window={}s tick={}ms",
                TARGET_VEHICLE_ID, WINDOW_SIZE_SECONDS, SNAPSHOT_TICK_MILLIS);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // deterministic output ordering

        List<BerlinMODTrip> events = buildEvents();
        DataStreamSource<BerlinMODTrip> raw = env.fromCollection(events);
        DataStream<BerlinMODTrip> trips = raw.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<BerlinMODTrip>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner((e, t) -> e.getTimestamp()));

        DataStream<BerlinMODTrip> cont = trips
                .process(new Q2ContinuousFunction(TARGET_VEHICLE_ID));
        cont.map(t -> String.format("v=%d t=%d (%.4f,%.4f)",
                t.getVehicleId(), t.getTimestamp(), t.getLon(), t.getLat()))
            .print("Q2-continuous");

        DataStream<Tuple5<Long, Long, Integer, Double, Double>> win = trips
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(WINDOW_SIZE_SECONDS)))
                .process(new Q2WindowedFunction(TARGET_VEHICLE_ID));
        win.print("Q2-windowed");

        DataStream<Tuple4<Long, Double, Double, Long>> snap = trips
                .keyBy(BerlinMODTrip::getVehicleId)
                .process(new Q2SnapshotFunction(TARGET_VEHICLE_ID, SNAPSHOT_TICK_MILLIS));
        snap.print("Q2-snapshot");

        env.execute("BerlinMODQ2LocalTest");
        LOG.info("BerlinMODQ2LocalTest done");
    }

    private static List<BerlinMODTrip> buildEvents() {
        List<BerlinMODTrip> events = new ArrayList<>();
        // Same synthetic corpus as Q3LocalTest, so any user can run both and
        // see them work over identical inputs.
        for (int i = 0; i <= 12; i += 2) {
            events.add(make(100, T0 + i * 1000L, 4.3517, 50.8503));
        }
        for (int i = 1; i <= 13; i += 2) {
            events.add(make(200, T0 + i * 1000L, 4.3060, 50.8270));
        }
        for (int i = 0; i <= 12; i += 2) {
            events.add(make(300, T0 + i * 1000L, 4.2000, 50.7500));
        }
        return events;
    }

    private static BerlinMODTrip make(int vid, long t, double lon, double lat) {
        BerlinMODTrip trip = new BerlinMODTrip();
        trip.setVehicleId(vid);
        trip.setTimestamp(t);
        trip.setLon(lon);
        trip.setLat(lat);
        return trip;
    }
}
