package berlinmod;

import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BerlinMOD-Q2 — <b>windowed form</b>.
 *
 * <p><i>"Per N-second tumbling window, what is vehicle X's most recent
 * position seen within the window?"</i>
 *
 * <p>For each window, filter to events matching {@code targetVehicleId}, keep
 * the event with the largest timestamp, and emit
 * {@code (windowStart, windowEnd, vehicleId, lon, lat)}. If the vehicle had
 * no events in the window, emit nothing.
 */
public class Q2WindowedFunction
        extends ProcessAllWindowFunction<BerlinMODTrip, Tuple5<Long, Long, Integer, Double, Double>, TimeWindow> {

    private static final Logger LOG = LoggerFactory.getLogger(Q2WindowedFunction.class);

    private final int targetVehicleId;

    public Q2WindowedFunction(int targetVehicleId) {
        this.targetVehicleId = targetVehicleId;
    }

    @Override
    public void process(
            Context ctx,
            Iterable<BerlinMODTrip> elements,
            Collector<Tuple5<Long, Long, Integer, Double, Double>> out) {
        BerlinMODTrip latest = null;
        for (BerlinMODTrip trip : elements) {
            if (trip.getVehicleId() != targetVehicleId) {
                continue;
            }
            if (latest == null || trip.getTimestamp() > latest.getTimestamp()) {
                latest = trip;
            }
        }
        if (latest != null) {
            out.collect(new Tuple5<>(
                    ctx.window().getStart(),
                    ctx.window().getEnd(),
                    latest.getVehicleId(),
                    latest.getLon(),
                    latest.getLat()));
            LOG.info("Q2-windowed: [{}, {}) vehicle={} last=({}, {})",
                    ctx.window().getStart(), ctx.window().getEnd(),
                    latest.getVehicleId(), latest.getLon(), latest.getLat());
        }
    }
}
