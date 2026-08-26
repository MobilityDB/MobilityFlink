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

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.io.Serializable;

/**
 * DataStream wiring for the {@code windowed} streaming tier of the
 * generated {@code org.mobilitydb.meos.MeosOps*} facades.
 *
 * <p>The {@code windowed} tier is "output cardinality changes; needs a
 * window". The canonical examples are
 * {@code temporal_length(tgeo)} (one length per trajectory window),
 * {@code temporal_twavg(tnumber)} (one time-weighted average per
 * window), and the per-class {@code _trajectory} / {@code _time} /
 * {@code _timespan} accessors that reduce a full sequence to a single
 * derived value.
 *
 * <p>Wraps any windowed MeosOps call as a Flink
 * {@link ProcessWindowFunction}: per-window, the adopter receives the
 * full iterable of events in the window, applies whatever MEOS
 * sequence-derived operation is appropriate, and emits a single
 * per-window output. The wiring handles the
 * {@code ProcessWindowFunction} boilerplate (context, collector) so
 * adopters write a single serializable lambda.
 *
 * <p><b>State considerations</b>: unlike
 * {@link MeosBoundedStateMap}, the {@code windowed} tier does not
 * keep MEOS handles across event boundaries — each window's MEOS
 * value is built fresh from the iterable on window close (or
 * watermark trigger), used to compute the output, and discarded. The
 * iterable's events are Flink-side data; MEOS handles are short-lived
 * per-window.
 *
 * <p><b>Typical usage</b> — per-vehicle per-tumbling-window
 * trajectory length via {@code MeosOpsTemporal.temporal_length} (tier
 * = {@code windowed}):
 *
 * <pre>{@code
 * DataStream<VehiclePoint> events = ...;     // (vehicleId, lon, lat, timestamp)
 * DataStream<VehicleLength> lengths = events
 *     .assignTimestampsAndWatermarks(...)
 *     .keyBy(VehiclePoint::vehicleId)
 *     .window(TumblingEventTimeWindows.of(Duration.ofMinutes(10)))
 *     .process(new MeosWindowedAggregate<Integer, VehiclePoint, VehicleLength, TimeWindow>(
 *         (window, events, ctx) -> {
 *             Pointer trajectory = buildTrajectoryFromPoints(events);  // adopter helper
 *             double length = MeosOpsTemporal.temporal_length(trajectory);
 *             return new VehicleLength(ctx.getCurrentKey(), window.getStart(), length);
 *         }));
 * }</pre>
 *
 * <p>The window-close path is event-time-aware: when Flink determines
 * the window is complete (via watermark), it invokes the lambda once
 * with the full iterable, the window metadata, and a context giving
 * access to the key. The adopter returns a single output value.
 *
 * <p><b>Coverage</b>: 161 of the 2,097 emitted methods (~8%) qualify
 * as {@code windowed} per the v4 baseline — all of them wrappable
 * through this single class.
 *
 * @param <K>   the key type
 * @param <IN>  the input event type within the window
 * @param <OUT> the per-window output type
 * @param <W>   the window type ({@code TimeWindow}, {@code GlobalWindow}, etc.)
 */
public final class MeosWindowedAggregate<K, IN, OUT, W extends Window>
        extends ProcessWindowFunction<IN, OUT, K, W> {

    /**
     * Serializable per-window MEOS aggregate. The lambda receives the
     * window metadata, the full iterable of in-window events, and a
     * context (for key access). It returns a single per-window output
     * value, or {@code null} to emit nothing.
     */
    @FunctionalInterface
    public interface WindowFn<K, IN, OUT, W extends Window> extends Serializable {
        OUT aggregate(W window, Iterable<IN> events, ContextLike<K> ctx) throws Exception;
    }

    /**
     * Slimmer alternative to Flink's {@code ProcessWindowFunction.Context}
     * — exposes only the bits a MEOS aggregate typically needs (key +
     * current processing time + current watermark). Keeps the wiring
     * lambda free of Flink internals.
     */
    public interface ContextLike<K> {
        K getCurrentKey();
        long getCurrentProcessingTime();
        long getCurrentWatermark();
    }

    private final WindowFn<K, IN, OUT, W> windowFn;

    public MeosWindowedAggregate(WindowFn<K, IN, OUT, W> windowFn) {
        this.windowFn = windowFn;
    }

    @Override
    public void open(OpenContext parameters) throws Exception {
        super.open(parameters);
        MeosWiringRuntime.ensureInitializedOnThread();
    }

    @Override
    public void process(K key,
                         ProcessWindowFunction<IN, OUT, K, W>.Context context,
                         Iterable<IN> elements,
                         Collector<OUT> out) throws Exception {
        ContextLike<K> ctx = new ContextLike<K>() {
            @Override public K getCurrentKey() { return key; }
            @Override public long getCurrentProcessingTime() { return context.currentProcessingTime(); }
            @Override public long getCurrentWatermark() { return context.currentWatermark(); }
        };
        OUT result = windowFn.aggregate(context.window(), elements, ctx);
        if (result != null) {
            out.collect(result);
        }
    }
}
