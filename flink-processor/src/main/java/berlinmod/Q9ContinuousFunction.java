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
 * BerlinMOD-Q9 — <b>continuous form</b>.
 *
 * <p><i>"What is the current distance between vehicles X and Y?"</i>
 *
 * <p>Driven by events from either X or Y. State holds the last-known position
 * of each as {@code Tuple4(xLon, xLat, yLon, yLat)} (with sentinel
 * {@code Double.NaN} for unseen). On each event, update the corresponding
 * pair of slots; if both are known, emit {@code (eventTime, distanceMetres)}.
 *
 * <p>Caller is expected to filter the stream to {@code vehicleId ∈ {X, Y}}
 * and key by a constant so the single shared state lives in one subtask.
 * (Two-vehicle Q9 is single-task by construction; a generalised "all-pairs"
 * variant would be a different operator.)
 */
public class Q9ContinuousFunction
        extends KeyedProcessFunction<Integer, BerlinMODTrip, Tuple2<Long, Double>> {

    private final int xVehicleId;
    private final int yVehicleId;
    private transient ValueState<Tuple4<Double, Double, Double, Double>> xy;

    public Q9ContinuousFunction(int xVehicleId, int yVehicleId) {
        this.xVehicleId = xVehicleId;
        this.yVehicleId = yVehicleId;
    }

    @Override
    public void open(Configuration parameters) {
        TypeInformation<Tuple4<Double, Double, Double, Double>> tInfo =
                TypeInformation.of(new TypeHint<Tuple4<Double, Double, Double, Double>>() {});
        xy = getRuntimeContext().getState(new ValueStateDescriptor<>("q9xy", tInfo));
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
        if (!Double.isNaN(s.f0) && !Double.isNaN(s.f2)) {
            double d = Haversine.distanceMetres(s.f0, s.f1, s.f2, s.f3);
            out.collect(new Tuple2<>(trip.getTimestamp(), d));
        }
    }
}
