package berlinmod;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * BerlinMOD-Q6 — <b>continuous form</b>.
 *
 * <p><i>"What is each vehicle's cumulative distance travelled so far?"</i>
 *
 * <p>Keyed by vehicleId. For each event, computes the great-circle distance
 * from the previous-known position (or 0 if first event), adds it to the
 * cumulative total, and emits {@code (vehicleId, t, cumulativeMetres)}.
 *
 * <p>Predicate today: pure-Java great-circle distance (see MEOS geog_dwithin/geog_distance).
 * Same MEOS-side analogue as Q3 — a future JMEOS bridge would replace the
 * Java accumulator with a MEOS {@code length} call over the per-vehicle
 * trajectory.
 */
public class Q6ContinuousFunction
        extends KeyedProcessFunction<Integer, BerlinMODTrip, Tuple3<Integer, Long, Double>> {

    private transient ValueState<Tuple2<Double, Double>> lastPos; // lon, lat
    private transient ValueState<Double> totalDist;

    @Override
    public void open(Configuration parameters) {
        TypeInformation<Tuple2<Double, Double>> posType =
                TypeInformation.of(new TypeHint<Tuple2<Double, Double>>() {});
        lastPos = getRuntimeContext().getState(
                new ValueStateDescriptor<>("q6LastPos", posType));
        totalDist = getRuntimeContext().getState(
                new ValueStateDescriptor<>("q6TotalDist", Double.class));
    }

    @Override
    public void processElement(
            BerlinMODTrip trip,
            Context ctx,
            Collector<Tuple3<Integer, Long, Double>> out) throws Exception {
        Tuple2<Double, Double> prev = lastPos.value();
        Double total = totalDist.value();
        if (total == null) {
            total = 0.0;
        }
        if (prev != null) {
            total += MEOSBridge.distanceMetres(prev.f0, prev.f1, trip.getLon(), trip.getLat());
        }
        lastPos.update(new Tuple2<>(trip.getLon(), trip.getLat()));
        totalDist.update(total);
        out.collect(new Tuple3<>(trip.getVehicleId(), trip.getTimestamp(), total));
    }
}
