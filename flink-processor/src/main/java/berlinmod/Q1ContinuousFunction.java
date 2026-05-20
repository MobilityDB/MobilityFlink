package berlinmod;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * BerlinMOD-Q1 — <b>continuous form</b>.
 *
 * <p><i>"Which vehicles have appeared in the stream?"</i>
 *
 * <p>Emits {@code (vehicleId, firstSeenTimestamp)} the first time each vehicle
 * is seen; subsequent events for the same vehicle are deduplicated via keyed
 * state. Keyed by vehicleId.
 */
public class Q1ContinuousFunction
        extends KeyedProcessFunction<Integer, BerlinMODTrip, Tuple2<Integer, Long>> {

    private transient ValueState<Boolean> seen;

    @Override
    public void open(Configuration parameters) {
        seen = getRuntimeContext().getState(
                new ValueStateDescriptor<>("q1SeenVehicle", Boolean.class));
    }

    @Override
    public void processElement(
            BerlinMODTrip trip,
            Context ctx,
            Collector<Tuple2<Integer, Long>> out) throws Exception {
        Boolean s = seen.value();
        if (s == null || !s) {
            out.collect(new Tuple2<>(trip.getVehicleId(), trip.getTimestamp()));
            seen.update(true);
        }
    }
}
