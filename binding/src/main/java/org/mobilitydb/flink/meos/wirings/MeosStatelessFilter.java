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

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.OpenContext;

import java.io.Serializable;

/**
 * DataStream wiring for the {@code stateless} streaming tier of the
 * generated {@code org.mobilitydb.meos.MeosOps*} facades — the
 * predicate-shaped sibling of {@link MeosStatelessMap}.
 *
 * <p>Wraps any {@code MeosOps*.f(...)} call that returns {@code boolean}
 * (or {@code int} interpreted as a 0/1 flag, common in JMEOS' int-coded
 * predicates) and whose streaming tier is {@code stateless} (per
 * {@code tools/codegen/meos-ops-manifest.json}) as a Flink
 * {@link FilterFunction}. No per-key state; each event filtered
 * independently.
 *
 * <p><b>Typical usage</b>: scalar-predicate filter against the
 * generated {@code MeosOpsTBox.overlaps_tbox_tbox} (tier =
 * {@code stateless}):
 *
 * <pre>{@code
 * DataStream<TbiePair> in = ...;
 * DataStream<TbiePair> overlapping = in.filter(
 *     new MeosStatelessFilter<>(
 *         pair -> MeosOpsTBox.overlaps_tbox_tbox(pair.a, pair.b)));
 * }</pre>
 *
 * <p>For int-coded predicates (JMEOS returns {@code int} for some MEOS
 * predicates rather than {@code boolean}), use
 * {@link #fromIntPredicate}:
 *
 * <pre>{@code
 * DataStream<StboxPair> in = ...;
 * DataStream<StboxPair> adj = in.filter(
 *     MeosStatelessFilter.fromIntPredicate(
 *         pair -> MeosOpsFreeGeo.adjacent_stbox_stbox(pair.a, pair.b)));
 * }</pre>
 *
 * @param <IN> the record type being filtered
 */
public final class MeosStatelessFilter<IN> extends RichFilterFunction<IN> {

    /** Serializable boolean-returning per-event MEOS predicate. */
    @FunctionalInterface
    public interface MeosPredicate<IN> extends Serializable {
        boolean test(IN event) throws Exception;
    }

    /** Serializable int-returning per-event MEOS predicate (0/1 flag). */
    @FunctionalInterface
    public interface MeosIntPredicate<IN> extends Serializable {
        int test(IN event) throws Exception;
    }

    private final MeosPredicate<IN> predicate;

    public MeosStatelessFilter(MeosPredicate<IN> predicate) {
        this.predicate = predicate;
    }

    /**
     * Adapt an {@code int}-returning generated MEOS predicate (treating
     * non-zero as {@code true}) into a Flink {@code FilterFunction}.
     */
    public static <IN> MeosStatelessFilter<IN> fromIntPredicate(MeosIntPredicate<IN> p) {
        return new MeosStatelessFilter<>(event -> p.test(event) != 0);
    }

    @Override
    public void open(OpenContext parameters) throws Exception {
        super.open(parameters);
        MeosWiringRuntime.ensureInitializedOnThread();
    }

    @Override
    public boolean filter(IN event) throws Exception {
        // When chained to a legacy source, records are processed on the source's
        // emitter thread rather than the thread open() ran on; the ThreadLocal
        // guard makes this a cheap no-op after the first call per thread.
        MeosWiringRuntime.ensureInitializedOnThread();
        return predicate.test(event);
    }
}
