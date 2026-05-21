package org.mobilitydb.flink.meos.wirings.demo;

import jnr.ffi.Pointer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.mobilitydb.flink.meos.MeosOpsFreeCore;
import org.mobilitydb.flink.meos.MeosOpsTBox;
import org.mobilitydb.flink.meos.wirings.MeosBoundedStateMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * End-to-end runnable demo of the {@code bounded-state} tier wiring.
 *
 * <p>Pipeline:
 * <ol>
 *   <li>Stream of {@code (vehicleId, eventTboxWKT)} for 2 vehicles, 3
 *       events each.</li>
 *   <li>{@code keyBy(vehicleId)} so per-vehicle state isolates.</li>
 *   <li>Per-vehicle running tbox union via
 *       {@link MeosBoundedStateMap}: state holds the MEOS-WKT text of
 *       the current union; on each event, deserialize → call
 *       {@code MeosOpsFreeCore.union_tbox_tbox} → re-serialize.</li>
 *   <li>Emit {@code (vehicleId, runningUnionTboxWKT)} per event.</li>
 * </ol>
 *
 * <p>What the demo proves:
 * <ul>
 *   <li><b>Checkpoint-safe state</b> — state crosses the operator
 *       boundary as {@code byte[]} (MEOS-WKT here, MEOS-WKB in
 *       production); no raw native pointers in checkpoints.</li>
 *   <li><b>Per-key isolation</b> — vehicle 1's running union does not
 *       leak into vehicle 2's, and vice versa.</li>
 *   <li><b>First-event correctness</b> — the wiring handles
 *       {@code prior == null} on the first event for each key by
 *       skipping deserialize and seeding state with the first event's
 *       tbox.</li>
 * </ul>
 *
 * <p>Run with:
 *
 * <pre>{@code
 * mvn -q exec:java \
 *     -Dexec.mainClass=org.mobilitydb.flink.meos.wirings.demo.MeosBoundedStateDemoJob \
 *     -Dmobilityflink.meos.enabled=true
 * }</pre>
 *
 * <p>Expected output: 6 lines (3 per vehicle), each showing the growing
 * union tbox after that event.
 */
public final class MeosBoundedStateDemoJob {

    private static final Logger LOG = LoggerFactory.getLogger(MeosBoundedStateDemoJob.class);

    /** 6 events across 2 vehicles — the running union grows monotonically per key. */
    private static final Tuple2<Integer, String>[] EVENTS = new Tuple2[]{
            Tuple2.of(1, "TBOX XT([0,2],[2026-01-01,2026-01-01 01:00])"),
            Tuple2.of(2, "TBOX XT([10,12],[2026-01-01,2026-01-01 01:00])"),
            Tuple2.of(1, "TBOX XT([3,5],[2026-01-01 01:00,2026-01-01 02:00])"),
            Tuple2.of(2, "TBOX XT([13,15],[2026-01-01 01:00,2026-01-01 02:00])"),
            Tuple2.of(1, "TBOX XT([1,4],[2026-01-01 02:00,2026-01-01 03:00])"),
            Tuple2.of(2, "TBOX XT([11,14],[2026-01-01 02:00,2026-01-01 03:00])"),
    };

    public static void main(String[] args) throws Exception {
        if (!MeosOpsTBox.MEOS_AVAILABLE) {
            LOG.error("MEOS not available — the demo requires libmeos.");
            System.exit(1);
        }

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Tuple2<Integer, String>> events =
                env.fromCollection(Arrays.asList(EVENTS));

        KeyedStream<Tuple2<Integer, String>, Integer> keyed =
                events.keyBy(t -> t.f0);

        // Wire the per-vehicle running union via MeosBoundedStateMap.
        // State is the MEOS-WKT text of the current union (byte[] form);
        // each event deserializes, unions with the event tbox, re-serializes.
        DataStream<Tuple2<Integer, String>> runningUnion = keyed.process(
                new MeosBoundedStateMap<Integer, Tuple2<Integer, String>, Tuple2<Integer, String>>(
                        /* serialize:   Pointer → byte[] */
                        ptr -> MeosOpsTBox.tbox_out(ptr, 6).getBytes(StandardCharsets.UTF_8),
                        /* deserialize: byte[] → Pointer */
                        bytes -> MeosOpsTBox.tbox_in(new String(bytes, StandardCharsets.UTF_8)),
                        /* step: (prior union, this event) → (new union, output) */
                        (prior, evt) -> {
                            Pointer eventTbox = MeosOpsTBox.tbox_in(evt.f1);
                            // First event for a key: prior is null — seed with the event's tbox.
                            // Subsequent events: union prior with the new event's tbox.
                            Pointer newUnion = (prior == null)
                                    ? eventTbox
                                    : MeosOpsFreeCore.union_tbox_tbox(prior, eventTbox, /*strict=*/0);
                            Tuple2<Integer, String> output =
                                    Tuple2.of(evt.f0, MeosOpsTBox.tbox_out(newUnion, 6));
                            return new MeosBoundedStateMap.MeosStep<>(newUnion, output);
                        }))
                .returns(org.apache.flink.api.common.typeinfo.TypeInformation.of(
                        new org.apache.flink.api.common.typeinfo.TypeHint<Tuple2<Integer, String>>() {}));

        runningUnion.print("running-union");

        env.execute("MeosWirings bounded-state tier demo");
    }
}
