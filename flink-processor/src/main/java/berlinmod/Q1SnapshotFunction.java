package berlinmod;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * BerlinMOD-Q1 — <b>snapshot form</b>.
 *
 * <p><i>"At time T, which vehicles have appeared in the stream up to T?"</i>
 *
 * <p>Keyed by vehicleId. On each event, mark the vehicle as seen and register
 * an event-time timer at the next snapshot tick. When the timer fires at time
 * T, emit {@code (T, vehicleId)} for each vehicle that has been seen by T.
 *
 * <p>This is the parity-oracle form: at watermark T, the streaming output is
 * the set of vehicleIds whose first event occurred at or before T, which
 * equals the batch BerlinMOD-Q1 result on data up to T.
 */
public class Q1SnapshotFunction
        extends KeyedProcessFunction<Integer, BerlinMODTrip, Tuple2<Long, Integer>> {

    private final long snapshotTickMillis;
    private transient ValueState<Boolean> seen;

    public Q1SnapshotFunction(long snapshotTickMillis) {
        this.snapshotTickMillis = snapshotTickMillis;
    }

    @Override
    public void open(Configuration parameters) {
        seen = getRuntimeContext().getState(
                new ValueStateDescriptor<>("q1SnapshotSeen", Boolean.class));
    }

    @Override
    public void processElement(
            BerlinMODTrip trip,
            Context ctx,
            Collector<Tuple2<Long, Integer>> out) throws Exception {
        seen.update(true);
        long nextTick = ((trip.getTimestamp() / snapshotTickMillis) + 1) * snapshotTickMillis;
        ctx.timerService().registerEventTimeTimer(nextTick);
    }

    @Override
    public void onTimer(
            long timestamp,
            OnTimerContext ctx,
            Collector<Tuple2<Long, Integer>> out) throws Exception {
        Boolean s = seen.value();
        if (Boolean.TRUE.equals(s)) {
            out.collect(new Tuple2<>(timestamp, ctx.getCurrentKey()));
        }
    }
}
