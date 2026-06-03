package berlinmod;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * BerlinMOD-Q4 — <b>snapshot form</b>.
 *
 * <p><i>"At time T, what is the list of (vehicleId, entryTime) pairs for all
 * vehicles that entered region R at or before T?"</i>
 *
 * <p>This is the parity-oracle form: streaming output at watermark T must
 * equal the batch BerlinMOD-Q4 result on the same data up to T.
 *
 * <p>Keyed by vehicleId. Per-vehicle state: a {@code wasInside} flag plus a
 * {@code ListState<Long>} of recorded entry times. On each event, detect
 * outside → inside transitions and append entry time. Register an event-time
 * timer at the next snapshot tick. On timer fire at T, emit one
 * {@code (T, vehicleId, entryTime)} per recorded entry.
 */
public class Q4SnapshotFunction
        extends KeyedProcessFunction<Integer, BerlinMODTrip, Tuple3<Long, Integer, Long>> {

    private final double xmin, ymin, xmax, ymax;
    private final long snapshotTickMillis;
    private transient ValueState<Boolean> wasInside;
    private transient ListState<Long> entries;

    public Q4SnapshotFunction(double xmin, double ymin, double xmax, double ymax, long snapshotTickMillis) {
        this.xmin = xmin;
        this.ymin = ymin;
        this.xmax = xmax;
        this.ymax = ymax;
        this.snapshotTickMillis = snapshotTickMillis;
    }

    @Override
    public void open(Configuration parameters) {
        wasInside = getRuntimeContext().getState(
                new ValueStateDescriptor<>("q4SnapWasInside", Boolean.class));
        entries = getRuntimeContext().getListState(
                new ListStateDescriptor<>("q4SnapEntries", Long.class));
    }

    @Override
    public void processElement(
            BerlinMODTrip trip,
            Context ctx,
            Collector<Tuple3<Long, Integer, Long>> out) throws Exception {
        boolean curr = inBox(trip.getLon(), trip.getLat());
        Boolean prev = wasInside.value();
        boolean prevInside = prev != null && prev;
        if (curr && !prevInside) {
            entries.add(trip.getTimestamp());
        }
        wasInside.update(curr);
        long nextTick = ((trip.getTimestamp() / snapshotTickMillis) + 1) * snapshotTickMillis;
        ctx.timerService().registerEventTimeTimer(nextTick);
    }

    @Override
    public void onTimer(
            long timestamp,
            OnTimerContext ctx,
            Collector<Tuple3<Long, Integer, Long>> out) throws Exception {
        for (Long entry : entries.get()) {
            if (entry <= timestamp) {
                out.collect(new Tuple3<>(timestamp, ctx.getCurrentKey(), entry));
            }
        }
    }

    private boolean inBox(double lon, double lat) {
        return lon >= xmin && lon <= xmax && lat >= ymin && lat <= ymax;
    }
}
