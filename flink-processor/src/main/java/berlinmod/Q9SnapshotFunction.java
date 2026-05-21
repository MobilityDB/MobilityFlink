package berlinmod;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * BerlinMOD-Q9 — <b>snapshot form</b>.
 *
 * <p><i>"At time T, what is the distance between vehicles X and Y (using
 * their most-recent-known positions on or before T)?"</i>
 *
 * <p>This is the parity-oracle form: streaming output at watermark T must
 * equal the batch BerlinMOD-Q9 result on the same data up to T.
 *
 * <p>Shared single-key state matches {@link Q9ContinuousFunction}. On each
 * X or Y event, update the corresponding pair of slots and register an
 * event-time timer at the next snapshot tick. On timer fire at T, emit
 * {@code (T, distanceMetres)} if both X and Y have been seen by T.
 */
public class Q9SnapshotFunction
        extends KeyedProcessFunction<Integer, BerlinMODTrip, Tuple2<Long, Double>> {

    private final int xVehicleId;
    private final int yVehicleId;
    private final long snapshotTickMillis;
    private transient ValueState<Tuple4<Double, Double, Double, Double>> xy;

    public Q9SnapshotFunction(int xVehicleId, int yVehicleId, long snapshotTickMillis) {
        this.xVehicleId = xVehicleId;
        this.yVehicleId = yVehicleId;
        this.snapshotTickMillis = snapshotTickMillis;
    }

    @Override
    public void open(Configuration parameters) {
        TypeInformation<Tuple4<Double, Double, Double, Double>> tInfo =
                TypeInformation.of(new TypeHint<Tuple4<Double, Double, Double, Double>>() {});
        xy = getRuntimeContext().getState(new ValueStateDescriptor<>("q9SnapXy", tInfo));
    }

    @Override
    public void processElement(
            BerlinMODTrip trip,
            Context ctx,
            Collector<Tuple2<Long, Double>> out) throws Exception {
        Tuple4<Double, Double, Double, Double> s = xy.value();
        if (s == null) {
            s = new Tuple4<>(Double.NaN, Double.NaN, Double.NaN, Double.NaN);
        }
        if (trip.getVehicleId() == xVehicleId) {
            s = new Tuple4<>(trip.getLon(), trip.getLat(), s.f2, s.f3);
        } else if (trip.getVehicleId() == yVehicleId) {
            s = new Tuple4<>(s.f0, s.f1, trip.getLon(), trip.getLat());
        } else {
            return;
        }
        xy.update(s);
        long nextTick = ((trip.getTimestamp() / snapshotTickMillis) + 1) * snapshotTickMillis;
        ctx.timerService().registerEventTimeTimer(nextTick);
    }

    @Override
    public void onTimer(
            long timestamp,
            OnTimerContext ctx,
            Collector<Tuple2<Long, Double>> out) throws Exception {
        Tuple4<Double, Double, Double, Double> s = xy.value();
        if (s != null && !Double.isNaN(s.f0) && !Double.isNaN(s.f2)) {
            double d = MEOSBridge.distanceMetres(s.f0, s.f1, s.f2, s.f3);
            out.collect(new Tuple2<>(timestamp, d));
        }
    }
}
