package berlinmod;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Set;

/**
 * BerlinMOD-Q1 — <b>windowed form</b>.
 *
 * <p><i>"Per N-second tumbling window, how many distinct vehicles appeared
 * in the window?"</i>
 *
 * <p>Emits {@code (windowStart, windowEnd, distinctCount)} per window.
 */
public class Q1WindowedFunction
        extends ProcessAllWindowFunction<BerlinMODTrip, Tuple3<Long, Long, Long>, TimeWindow> {

    @Override
    public void process(
            Context ctx,
            Iterable<BerlinMODTrip> elements,
            Collector<Tuple3<Long, Long, Long>> out) {
        Set<Integer> distinct = new HashSet<>();
        for (BerlinMODTrip trip : elements) {
            distinct.add(trip.getVehicleId());
        }
        out.collect(new Tuple3<>(ctx.window().getStart(), ctx.window().getEnd(), (long) distinct.size()));
    }
}
