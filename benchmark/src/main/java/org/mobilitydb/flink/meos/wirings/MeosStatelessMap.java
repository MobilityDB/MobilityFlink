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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.OpenContext;

import java.io.Serializable;

/**
 * DataStream wiring for the {@code stateless} streaming tier of the
 * generated {@code org.mobilitydb.meos.MeosOps*} facades.
 *
 * <p>Wraps any {@code MeosOps*.f(...)} call whose streaming tier is
 * {@code stateless} (per {@code tools/codegen/meos-ops-manifest.json}
 * or {@code meos-ops-free-manifest.json}) as a Flink
 * {@link MapFunction}. No per-key state is allocated; each event is
 * mapped independently. The wrapped call:
 *
 * <ul>
 *   <li>does no MEOS-handle state across events (per the
 *       {@code stateless} tier contract);</li>
 *   <li>does not touch the time domain (no window required);</li>
 *   <li>may delegate to MEOS via the bundled JMEOS jar when
 *       {@code MeosOpsRuntime.MEOS_AVAILABLE} — otherwise the
 *       generated facade throws {@code UnsupportedOperationException}.</li>
 * </ul>
 *
 * <p><b>Typical usage</b>: register a stateless MEOS predicate / arithmetic
 * call as a per-event map step in a DataStream pipeline. Example with
 * the generated {@code MeosOpsTBox.overlaps_tbox_tbox} (tier =
 * {@code stateless}, per the codegen manifest):
 *
 * <pre>{@code
 * DataStream<TbiePair> in = ...;            // (tboxA, tboxB)
 * DataStream<Boolean> overlap = in.map(
 *     new MeosStatelessMap<>(
 *         pair -> MeosOpsTBox.overlaps_tbox_tbox(pair.a, pair.b)));
 * }</pre>
 *
 * <p><b>Tier coverage</b>: as of the codegen state on the parent PR,
 * 804 of the 2,097 generated methods are {@code stateless} (92 OO-
 * classified + 712 free-fn). Any of those can be wrapped through this
 * single class without per-method boilerplate.
 *
 * <p><b>Coexistence with {@code berlinmod.MEOSBridge}</b>: this is the
 * <i>low-level catalog-shaped</i> wiring; {@code MEOSBridge} stays as
 * the <i>high-level query-shaped</i> wiring for the BerlinMOD-9 suite.
 * Both share the same {@code MeosOpsRuntime.MEOS_AVAILABLE} discipline.
 *
 * @param <IN>  the input record type
 * @param <OUT> the output type returned by the wrapped MEOS call
 */
public final class MeosStatelessMap<IN, OUT> extends RichMapFunction<IN, OUT> {

    /**
     * Serializable per-event MEOS call. Implementations forward to a
     * generated {@code MeosOps*.f(...)} static method, returning the
     * Java type that the generated facade exposes.
     */
    @FunctionalInterface
    public interface MeosCall<IN, OUT> extends Serializable {
        OUT apply(IN event) throws Exception;
    }

    private final MeosCall<IN, OUT> call;

    /**
     * @param call serializable lambda forwarding to a stateless
     *             generated MEOS facade method. The lambda must be
     *             serializable (Java 8+ lambdas implementing a
     *             {@link Serializable} functional interface are).
     */
    public MeosStatelessMap(MeosCall<IN, OUT> call) {
        this.call = call;
    }

    @Override
    public void open(OpenContext parameters) throws Exception {
        super.open(parameters);
        // No per-key state in the stateless tier; the only per-operator
        // concern is MEOS' per-thread session, initialized on this task thread.
        MeosWiringRuntime.ensureInitializedOnThread();
    }

    @Override
    public OUT map(IN event) throws Exception {
        // When chained to a legacy source, records are processed on the source's
        // emitter thread rather than the thread open() ran on; the ThreadLocal
        // guard makes this a cheap no-op after the first call per thread.
        MeosWiringRuntime.ensureInitializedOnThread();
        return call.apply(event);
    }
}
