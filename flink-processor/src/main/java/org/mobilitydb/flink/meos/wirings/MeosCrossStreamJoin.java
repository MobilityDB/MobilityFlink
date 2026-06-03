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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;

import java.io.Serializable;

/**
 * DataStream wiring for the {@code cross-stream} streaming tier of
 * the generated {@code org.mobilitydb.meos.MeosOps*} facades.
 *
 * <p>The {@code cross-stream} tier is "pairwise across two streams,
 * pre-keyed by the same K, time-bounded match window". Canonical
 * examples are spatial-relations between two trajectories
 * ({@code edwithin_tgeo_tgeo}, {@code eintersects_tgeo_tgeo}) and
 * distance functions on two temporals
 * ({@code nad_tgeo_tgeo}, {@code mindistance_tgeo_tgeo}).
 *
 * <p>Wraps any cross-stream MeosOps call as a Flink
 * {@link ProcessJoinFunction} (the operator backing
 * {@code KeyedStream.intervalJoin(other)}). The wiring receives one
 * left event and one right event per match, both already paired by
 * Flink's interval-join machinery, and the adopter's lambda computes
 * the pairwise output via the matching MeosOps call.
 *
 * <p><b>Typical usage</b> — per-vehicle-pair "did they come within
 * 100m of each other in the last 5 minutes?" via
 * {@code MeosOpsTGeo.edwithin_tgeo_tgeo} (tier = {@code cross-stream}):
 *
 * <pre>{@code
 * KeyedStream<VehiclePosition, Integer> a = streamA.keyBy(VehiclePosition::regionId);
 * KeyedStream<VehiclePosition, Integer> b = streamB.keyBy(VehiclePosition::regionId);
 *
 * DataStream<MeetingEvent> meetings = a
 *     .intervalJoin(b)
 *         .between(Time.minutes(-5), Time.minutes(5))
 *         .process(new MeosCrossStreamJoin<VehiclePosition, VehiclePosition, MeetingEvent>(
 *             (left, right, ctx) -> {
 *                 Pointer leftT  = left.toTGeoPointer();
 *                 Pointer rightT = right.toTGeoPointer();
 *                 if (MeosOpsTGeo.edwithin_tgeo_tgeo(leftT, rightT, 100.0) != 0) {
 *                     return new MeetingEvent(left.id(), right.id(), ctx.getLeftTimestamp());
 *                 }
 *                 return null;  // no output for non-matches
 *             }));
 * }</pre>
 *
 * <p>The interval-join is keyed (both streams must be pre-keyed by
 * the same K, and only events sharing a key are considered for
 * pairing). The match window is time-bounded
 * ({@code .between(lowerBound, upperBound)}) and event-time aware —
 * watermarks drive when matches are emitted.
 *
 * <p><b>Slim adopter signature</b> — same {@code ContextLike}-style
 * pattern as {@link MeosWindowedAggregate}: the lambda receives the
 * matched left + right events and a slim context exposing the
 * left/right timestamps (the bits a MEOS cross-stream call typically
 * needs), keeping the wiring lambda free of Flink internals.
 *
 * <p><b>Coverage</b>: 140 of the 2,097 emitted methods (~7%) qualify
 * as {@code cross-stream} per the v4 baseline — all of them wrappable
 * through this single class. With this PR, every streamable tier in
 * the baseline has a generic wiring class; 1,957 of 2,097 (93%) of
 * the generated MeosOps* methods are wirable through 4 classes
 * without per-method registration.
 *
 * @param <L>   the left-stream event type
 * @param <R>   the right-stream event type
 * @param <OUT> the per-match output type
 */
public final class MeosCrossStreamJoin<L, R, OUT>
        extends ProcessJoinFunction<L, R, OUT> {

    /** Serializable per-match MEOS pairwise call. */
    @FunctionalInterface
    public interface JoinFn<L, R, OUT> extends Serializable {
        OUT join(L left, R right, ContextLike ctx) throws Exception;
    }

    /**
     * Slimmer alternative to Flink's {@code ProcessJoinFunction.Context}
     * — exposes only the bits a MEOS pairwise call typically needs.
     */
    public interface ContextLike {
        long getLeftTimestamp();
        long getRightTimestamp();
    }

    private final JoinFn<L, R, OUT> joinFn;

    public MeosCrossStreamJoin(JoinFn<L, R, OUT> joinFn) {
        this.joinFn = joinFn;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        MeosWiringRuntime.ensureInitializedOnThread();
    }

    @Override
    public void processElement(L left, R right,
                                ProcessJoinFunction<L, R, OUT>.Context context,
                                Collector<OUT> out) throws Exception {
        ContextLike ctx = new ContextLike() {
            @Override public long getLeftTimestamp() { return context.getLeftTimestamp(); }
            @Override public long getRightTimestamp() { return context.getRightTimestamp(); }
        };
        OUT result = joinFn.join(left, right, ctx);
        if (result != null) {
            out.collect(result);
        }
    }
}
