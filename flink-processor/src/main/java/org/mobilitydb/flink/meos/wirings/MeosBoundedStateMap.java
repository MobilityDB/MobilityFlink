/*****************************************************************************
 *
 * This MobilityDB code is provided under The PostgreSQL License.
 * Copyright (c) 2020-2026, Université libre de Bruxelles and MobilityDB
 * contributors
 *
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for any purpose, without fee, and without a written
 * agreement is hereby granted, provided that the above copyright notice and
 * this paragraph and the following two paragraphs appear in all copies.
 *
 * IN NO EVENT SHALL UNIVERSITE LIBRE DE BRUXELLES BE LIABLE TO ANY PARTY FOR
 * DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
 * LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION,
 * EVEN IF UNIVERSITE LIBRE DE BRUXELLES HAS BEEN ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 *
 * UNIVERSITE LIBRE DE BRUXELLES SPECIFICALLY DISCLAIMS ANY WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
 * AND FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON
 * AN "AS IS" BASIS, AND UNIVERSITE LIBRE DE BRUXELLES HAS NO OBLIGATIONS TO
 * PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 *
 *****************************************************************************/

package org.mobilitydb.flink.meos.wirings;

import jnr.ffi.Pointer;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.Serializable;

/**
 * DataStream wiring for the {@code bounded-state} streaming tier of
 * the generated {@code org.mobilitydb.flink.meos.MeosOps*} facades.
 *
 * <p>The {@code bounded-state} tier is "per-event with bounded per-key
 * state, the state IS a MEOS handle". The canonical example is a
 * per-key accumulator that keeps the running MEOS value alive across
 * events (e.g. per-vehicle running trajectory, per-key tbox union).
 *
 * <p><b>Why state lives as bytes, not as a {@code Pointer}.</b> A
 * {@code jnr.ffi.Pointer} is a raw native-memory address. It is not
 * portable across JVM restarts; Flink would not be able to checkpoint
 * or replay state. The wiring stores the state as a {@code byte[]}
 * (typically the MEOS-WKB serialization of the temporal value), with
 * adopter-supplied serialize / deserialize / step lambdas mediating
 * the round-trip through MEOS:
 *
 * <pre>{@code
 *  byte[] state               -- the per-key serialized MEOS value
 *      ↓ deserialize (MEOS-WKB → Pointer)
 *  Pointer prev               -- the in-flight MEOS handle
 *      ↓ step(prev, event) → (newPointer, output)
 *  Pointer next, OUT out      -- the new in-flight handle + per-event output
 *      ↓ serialize (Pointer → MEOS-WKB)
 *  byte[] newState            -- the per-key serialized new MEOS value
 * }</pre>
 *
 * <p>This serde discipline is the same one MobilityDuck's persistent
 * state machines use; it survives Flink savepoint / checkpoint /
 * rescaling correctly because state crossing the operator boundary is
 * always the MEOS-WKB bytes — never a raw pointer.
 *
 * <p><b>Typical usage</b> — per-vehicle running tbox union via
 * {@code MeosOpsFreeCore.union_tbox_tbox} (stateless on its own;
 * stateful when applied as a running fold):
 *
 * <pre>{@code
 * DataStream<VehicleTbox> in = ...;          // (vehicleId, tbox)
 * DataStream<RunningTbox> out = in
 *     .keyBy(VehicleTbox::vehicleId)
 *     .process(new MeosBoundedStateMap<Integer, VehicleTbox, RunningTbox>(
 *         /* serialize *(/   ptr -> MeosOpsTBox.tbox_as_wkb(ptr, (byte) 4).array(),
 *         /* deserialize *(/ bytes -> MeosOpsTBox.tbox_from_wkb(Pointer.wrap(...), bytes.length),
 *         /* step *(/        (prev, evt) -> {
 *             Pointer eventTbox = evt.toMeosTbox();
 *             Pointer merged = (prev == null) ? eventTbox
 *                                              : MeosOpsFreeCore.union_tbox_tbox(prev, eventTbox);
 *             RunningTbox result = new RunningTbox(evt.vehicleId(), MeosOpsTBox.tbox_as_hexwkb(merged, (byte) 4, null));
 *             return new MeosStep<>(merged, result);
 *         }));
 * }</pre>
 *
 * <p>The first event for a key sees {@code prev == null} (no prior
 * state); the wiring handles that case by skipping the
 * {@code deserialize} call. On subsequent events, the state is
 * re-hydrated, mutated, re-serialized.
 *
 * <p><b>Coverage</b>: bounded-state is the second-largest tier in the
 * v4 baseline (797 of 2,097 emitted methods — 513 OO-classified + 284
 * free-fn). Any of them can be wrapped through this single class —
 * adopters provide the three lambdas, the wiring handles all of the
 * Flink state plumbing.
 *
 * <p><b>State serializer</b>: this implementation uses Flink's built-in
 * {@code byte[]} primitive-array serializer (no custom Kryo / Avro / Pojo
 * registration needed). The state size per key is bounded by the
 * MEOS-WKB size of the running value — sub-KB for typical
 * accumulator scenarios.
 *
 * @param <K>   the key type ({@code keyBy} extractor return type)
 * @param <IN>  the input event type
 * @param <OUT> the output type emitted per event
 */
public final class MeosBoundedStateMap<K, IN, OUT>
        extends KeyedProcessFunction<K, IN, OUT> {

    /** Serializable Pointer → bytes serializer (typically MEOS-WKB). */
    @FunctionalInterface
    public interface PointerSerialize extends Serializable {
        byte[] toBytes(Pointer pointer) throws Exception;
    }

    /** Serializable bytes → Pointer deserializer (typically MEOS-WKB). */
    @FunctionalInterface
    public interface PointerDeserialize extends Serializable {
        Pointer fromBytes(byte[] bytes) throws Exception;
    }

    /** Per-event step: (prior MEOS handle, event) → (new handle, output). */
    @FunctionalInterface
    public interface MeosStepFn<IN, OUT> extends Serializable {
        MeosStep<OUT> apply(Pointer prior, IN event) throws Exception;
    }

    /** Tuple returned by the step lambda. */
    public static final class MeosStep<OUT> implements Serializable {
        private static final long serialVersionUID = 1L;
        public final Pointer newState;
        public final OUT output;
        public MeosStep(Pointer newState, OUT output) {
            this.newState = newState;
            this.output = output;
        }
    }

    private final PointerSerialize serialize;
    private final PointerDeserialize deserialize;
    private final MeosStepFn<IN, OUT> step;

    private transient ValueState<byte[]> handleState;

    public MeosBoundedStateMap(PointerSerialize serialize,
                               PointerDeserialize deserialize,
                               MeosStepFn<IN, OUT> step) {
        this.serialize = serialize;
        this.deserialize = deserialize;
        this.step = step;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        MeosWiringRuntime.ensureInitializedOnThread();
        ValueStateDescriptor<byte[]> descriptor = new ValueStateDescriptor<>(
                "meos-bounded-state",
                PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO);
        handleState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(IN event,
                                KeyedProcessFunction<K, IN, OUT>.Context ctx,
                                Collector<OUT> out) throws Exception {
        byte[] priorBytes = handleState.value();
        Pointer prior = (priorBytes == null) ? null : deserialize.fromBytes(priorBytes);

        MeosStep<OUT> stepResult = step.apply(prior, event);

        byte[] newBytes = serialize.toBytes(stepResult.newState);
        handleState.update(newBytes);

        if (stepResult.output != null) {
            out.collect(stepResult.output);
        }
    }
}
