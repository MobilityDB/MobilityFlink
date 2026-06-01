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

package org.mobilitydb.flink.meos.wirings.demo;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.mobilitydb.flink.meos.MeosOpsFreeCore;
import org.mobilitydb.flink.meos.MeosOpsTBox;
import org.mobilitydb.flink.meos.wirings.MeosStatelessFilter;
import org.mobilitydb.flink.meos.wirings.MeosStatelessMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * End-to-end runnable demo showing how the generated
 * {@code org.mobilitydb.flink.meos.MeosOps*} facades wire into a Flink
 * {@code DataStream} pipeline through the
 * {@code org.mobilitydb.flink.meos.wirings} helpers.
 *
 * <p>The demo:
 * <ol>
 *   <li>Builds a small in-memory stream of TBox-WKT strings.</li>
 *   <li>Parses each into a JMEOS {@code Pointer} via
 *       {@code MeosOpsTBox.tbox_in} (tier = {@code io-meta}).</li>
 *   <li>Filters to those that overlap with a fixed query TBox via
 *       {@code MeosOpsTBox.overlaps_tbox_tbox} wrapped as a
 *       {@link MeosStatelessFilter} (tier = {@code stateless}).</li>
 *   <li>Maps each surviving TBox to its serialized WKB hex via
 *       {@code MeosOpsTBox.tbox_as_hexwkb} wrapped as a
 *       {@link MeosStatelessMap} (tier = {@code io-meta} but no per-key
 *       state, so the {@code stateless} wiring works for it too).</li>
 * </ol>
 *
 * <p>Run with:
 *
 * <pre>{@code
 * mvn -q exec:java \
 *     -Dexec.mainClass=org.mobilitydb.flink.meos.wirings.demo.MeosWiringsDemoJob \
 *     -Dmobilityflink.meos.enabled=true   # require libmeos loadable
 * }</pre>
 *
 * <p>If libmeos is not loadable on the runtime (or
 * {@code -Dmobilityflink.meos.enabled=false}), every wrapped MeosOps
 * call throws {@code UnsupportedOperationException} with a clear
 * message — the demo prints the throw shape and exits non-zero.
 */
public final class MeosWiringsDemoJob {

    private static final Logger LOG = LoggerFactory.getLogger(MeosWiringsDemoJob.class);

    /** A small box covering (xmin=0, ymin=0, xmax=10, ymax=10). */
    private static final String QUERY_TBOX_WKT = "TBOX XT([0,10],[2026-01-01,2026-01-02])";

    /** Three input boxes — two overlap the query box, one doesn't. */
    private static final String[] INPUT_TBOX_WKTS = {
            "TBOX XT([5,15],[2026-01-01,2026-01-02])",   // overlaps
            "TBOX XT([20,30],[2026-01-01,2026-01-02])",  // disjoint
            "TBOX XT([3,8],[2026-01-01,2026-01-02])",    // overlaps
    };

    public static void main(String[] args) throws Exception {
        // Probe MEOS availability (the static initializer in MeosOpsRuntime
        // fires the first time any MeosOps class is touched).
        if (!MeosOpsTBox.MEOS_AVAILABLE) {
            LOG.error("MEOS not available — the demo requires libmeos. "
                    + "Set -Dmobilityflink.meos.enabled=true and ensure libmeos is loadable.");
            System.exit(1);
        }

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> tboxWkts = env.fromCollection(Arrays.asList(INPUT_TBOX_WKTS));

        // The records crossing operator boundaries are serialized MEOS values
        // (WKT text) — never raw native pointers, which are process-local and
        // not serializable across Flink tasks. Each operator parses to a
        // transient MEOS handle, calls MEOS, and re-serializes.

        // Stage 1: parse each WKT and re-serialize via tbox_out (stateless io-meta).
        DataStream<String> normalized = tboxWkts.map(
                new MeosStatelessMap<String, String>(
                        wkt -> MeosOpsTBox.tbox_out(MeosOpsTBox.tbox_in(wkt), 6)))
                .returns(Types.STRING);

        // Stage 2: filter to those overlapping the query box (stateless).
        // The query box is the constant WKT operand, parsed inside the predicate;
        // overlaps_tbox_tbox lives on MeosOpsFreeCore (free fn, not OO-classified).
        DataStream<String> overlapping = normalized.filter(
                new MeosStatelessFilter<String>(
                        wkt -> MeosOpsFreeCore.overlaps_tbox_tbox(
                                MeosOpsTBox.tbox_in(wkt),
                                MeosOpsTBox.tbox_in(QUERY_TBOX_WKT))));

        overlapping.print("overlapping-tbox");

        env.execute("MeosWirings stateless tier demo");
    }
}
