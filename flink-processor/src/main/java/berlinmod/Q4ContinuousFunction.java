package berlinmod;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * BerlinMOD-Q4 — <b>continuous form</b>.
 *
 * <p><i>"Which vehicles entered region R (transition outside → inside),
 * and when?"</i>
 *
 * <p>Keyed by vehicleId. Per-vehicle state tracks the last seen
 * inside-or-outside flag for R. On each event, computes the current
 * inside-or-outside, and if the transition is outside→inside, emits
 * {@code (vehicleId, entryTime)}.
 *
 * <p>Predicate: pure-Java axis-aligned point-in-box. The rectangular region
 * is degenerate as a geographic predicate (no projection needed); a generic
 * polygon-R variant would route through {@link MEOSBridge} for MEOS
 * {@code eintersects_tgeo_geo}.
 */
public class Q4ContinuousFunction
        extends KeyedProcessFunction<Integer, BerlinMODTrip, Tuple2<Integer, Long>> {

    private final double xmin, ymin, xmax, ymax;
    private transient ValueState<Boolean> wasInside;

    public Q4ContinuousFunction(double xmin, double ymin, double xmax, double ymax) {
        this.xmin = xmin;
        this.ymin = ymin;
        this.xmax = xmax;
        this.ymax = ymax;
    }

    @Override
    public void open(Configuration parameters) {
        wasInside = getRuntimeContext().getState(
                new ValueStateDescriptor<>("q4WasInside", Boolean.class));
    }

    @Override
    public void processElement(
            BerlinMODTrip trip,
            Context ctx,
            Collector<Tuple2<Integer, Long>> out) throws Exception {
        boolean isInside = inBox(trip.getLon(), trip.getLat());
        Boolean prev = wasInside.value();
        boolean prevInside = prev != null && prev;
        if (isInside && !prevInside) {
            out.collect(new Tuple2<>(trip.getVehicleId(), trip.getTimestamp()));
        }
        wasInside.update(isInside);
    }

    private boolean inBox(double lon, double lat) {
        return lon >= xmin && lon <= xmax && lat >= ymin && lat <= ymax;
    }
}
