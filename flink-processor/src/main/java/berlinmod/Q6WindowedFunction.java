package berlinmod;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * BerlinMOD-Q6 — <b>windowed form</b>.
 *
 * <p><i>"Per N-second tumbling window, per vehicle, how far did the vehicle
 * travel during the window?"</i>
 *
 * <p>Keyed by vehicleId, tumbling event-time window. Within each window,
 * sort events by timestamp and accumulate great-circle distances between
 * consecutive points. Emit {@code (windowStart, windowEnd, vehicleId,
 * distanceMetres)}.
 */
public class Q6WindowedFunction
        extends ProcessWindowFunction<BerlinMODTrip, Tuple4<Long, Long, Integer, Double>, Integer, TimeWindow> {

    @Override
    public void process(
            Integer vehicleId,
            Context ctx,
            Iterable<BerlinMODTrip> elements,
            Collector<Tuple4<Long, Long, Integer, Double>> out) {
        List<BerlinMODTrip> sorted = new ArrayList<>();
        for (BerlinMODTrip trip : elements) {
            sorted.add(trip);
        }
        sorted.sort((a, b) -> Long.compare(a.getTimestamp(), b.getTimestamp()));
        double total = 0.0;
        for (int i = 1; i < sorted.size(); i++) {
            BerlinMODTrip prev = sorted.get(i - 1);
            BerlinMODTrip curr = sorted.get(i);
            total += Haversine.distanceMetres(prev.getLon(), prev.getLat(),
                                              curr.getLon(), curr.getLat());
        }
        out.collect(new Tuple4<>(ctx.window().getStart(), ctx.window().getEnd(), vehicleId, total));
    }
}
