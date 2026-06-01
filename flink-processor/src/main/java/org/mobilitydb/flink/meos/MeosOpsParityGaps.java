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

package org.mobilitydb.flink.meos;

/**
 * Forwarding facade methods for MEOS public-surface functions not emitted
 * by the tier-aware code generator. Each method delegates to its JMEOS
 * {@code functions.GeneratedFunctions} export under the shared
 * {@link MeosOpsRuntime#MEOS_AVAILABLE} guard.
 */
public final class MeosOpsParityGaps {

    private MeosOpsParityGaps() { /* utility */ }

    /** MEOS {@code acovers_geo_tgeo} — meos_geo.h · scalar / stateless. */
    public static int acovers_geo_tgeo(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("acovers_geo_tgeo requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.acovers_geo_tgeo(arg0, arg1);
    }

    /** MEOS {@code acovers_tcbuffer_tcbuffer} — meos_cbuffer.h · scalar / stateless. */
    public static int acovers_tcbuffer_tcbuffer(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("acovers_tcbuffer_tcbuffer requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.acovers_tcbuffer_tcbuffer(arg0, arg1);
    }

    /** MEOS {@code acovers_tgeo_geo} — meos_geo.h · scalar / stateless. */
    public static int acovers_tgeo_geo(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("acovers_tgeo_geo requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.acovers_tgeo_geo(arg0, arg1);
    }

    /** MEOS {@code bearing_tpoint_point} — meos_geo.h · scalar / stateless. */
    public static jnr.ffi.Pointer bearing_tpoint_point(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1, boolean arg2) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("bearing_tpoint_point requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.bearing_tpoint_point(arg0, arg1, arg2);
    }

    /** MEOS {@code bearing_tpoint_tpoint} — meos_geo.h · scalar / stateless. */
    public static jnr.ffi.Pointer bearing_tpoint_tpoint(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("bearing_tpoint_tpoint requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.bearing_tpoint_tpoint(arg0, arg1);
    }

    /** MEOS {@code bigintspan_to_floatspan} — meos.h · scalar / stateless. */
    public static jnr.ffi.Pointer bigintspan_to_floatspan(jnr.ffi.Pointer arg0) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("bigintspan_to_floatspan requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.bigintspan_to_floatspan(arg0);
    }

    /** MEOS {@code bigintspan_to_intspan} — meos.h · scalar / stateless. */
    public static jnr.ffi.Pointer bigintspan_to_intspan(jnr.ffi.Pointer arg0) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("bigintspan_to_intspan requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.bigintspan_to_intspan(arg0);
    }

    /** MEOS {@code floatspan_to_bigintspan} — meos.h · scalar / stateless. */
    public static jnr.ffi.Pointer floatspan_to_bigintspan(jnr.ffi.Pointer arg0) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("floatspan_to_bigintspan requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.floatspan_to_bigintspan(arg0);
    }

    /** MEOS {@code geogpoint_make2d} — meos_geo.h · scalar / stateless. */
    public static jnr.ffi.Pointer geogpoint_make2d(int arg0, double arg1, double arg2) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("geogpoint_make2d requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.geogpoint_make2d(arg0, arg1, arg2);
    }

    /** MEOS {@code geogpoint_make3dz} — meos_geo.h · scalar / stateless. */
    public static jnr.ffi.Pointer geogpoint_make3dz(int arg0, double arg1, double arg2, double arg3) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("geogpoint_make3dz requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.geogpoint_make3dz(arg0, arg1, arg2, arg3);
    }

    /** MEOS {@code geomeas_to_tpoint} — meos_geo.h · scalar / stateless. */
    public static jnr.ffi.Pointer geomeas_to_tpoint(jnr.ffi.Pointer arg0) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("geomeas_to_tpoint requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.geomeas_to_tpoint(arg0);
    }

    /** MEOS {@code geompoint_make2d} — meos_geo.h · scalar / stateless. */
    public static jnr.ffi.Pointer geompoint_make2d(int arg0, double arg1, double arg2) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("geompoint_make2d requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.geompoint_make2d(arg0, arg1, arg2);
    }

    /** MEOS {@code geompoint_make3dz} — meos_geo.h · scalar / stateless. */
    public static jnr.ffi.Pointer geompoint_make3dz(int arg0, double arg1, double arg2, double arg3) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("geompoint_make3dz requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.geompoint_make3dz(arg0, arg1, arg2, arg3);
    }

    /** MEOS {@code geompoint_to_npoint} — meos_npoint.h · scalar / stateless. */
    public static jnr.ffi.Pointer geompoint_to_npoint(jnr.ffi.Pointer arg0) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("geompoint_to_npoint requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.geompoint_to_npoint(arg0);
    }

    /** MEOS {@code intersection_cbuffer_set} — meos_cbuffer.h · scalar / stateless. */
    public static jnr.ffi.Pointer intersection_cbuffer_set(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("intersection_cbuffer_set requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.intersection_cbuffer_set(arg0, arg1);
    }

    /** MEOS {@code intersection_npoint_set} — meos_npoint.h · scalar / stateless. */
    public static jnr.ffi.Pointer intersection_npoint_set(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("intersection_npoint_set requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.intersection_npoint_set(arg0, arg1);
    }

    /** MEOS {@code intersection_pose_set} — meos_pose.h · scalar / stateless. */
    public static jnr.ffi.Pointer intersection_pose_set(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("intersection_pose_set requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.intersection_pose_set(arg0, arg1);
    }

    /** MEOS {@code intspan_to_bigintspan} — meos.h · scalar / stateless. */
    public static jnr.ffi.Pointer intspan_to_bigintspan(jnr.ffi.Pointer arg0) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("intspan_to_bigintspan requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.intspan_to_bigintspan(arg0);
    }

    /** MEOS {@code line_interpolate_point} — meos_geo.h · scalar / stateless. */
    public static jnr.ffi.Pointer line_interpolate_point(jnr.ffi.Pointer arg0, double arg1, boolean arg2) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("line_interpolate_point requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.line_interpolate_point(arg0, arg1, arg2);
    }

    /** MEOS {@code line_locate_point} — meos_geo.h · scalar / stateless. */
    public static double line_locate_point(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("line_locate_point requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.line_locate_point(arg0, arg1);
    }

    /** MEOS {@code line_point_n} — meos_geo.h · scalar / stateless. */
    public static jnr.ffi.Pointer line_point_n(jnr.ffi.Pointer arg0, int arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("line_point_n requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.line_point_n(arg0, arg1);
    }

    /** MEOS {@code line_substring} — meos_geo.h · scalar / stateless. */
    public static jnr.ffi.Pointer line_substring(jnr.ffi.Pointer arg0, double arg1, double arg2) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("line_substring requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.line_substring(arg0, arg1, arg2);
    }

    /** MEOS {@code meos_initialize_noexit_error_handler} — meos.h · scalar / stateless. */
    public static void meos_initialize_noexit_error_handler() {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("meos_initialize_noexit_error_handler requires libmeos — set -Dmobilityflink.meos.enabled=true");
        functions.GeneratedFunctions.meos_initialize_noexit_error_handler();
    }

    /** MEOS {@code mindistance_tgeo_tgeo} — meos_geo.h · scalar / stateless. */
    public static double mindistance_tgeo_tgeo(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1, double arg2) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("mindistance_tgeo_tgeo requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.mindistance_tgeo_tgeo(arg0, arg1, arg2);
    }

    /** MEOS {@code mul_float_tfloat} — meos.h · scalar / stateless. */
    public static jnr.ffi.Pointer mul_float_tfloat(double arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("mul_float_tfloat requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.mul_float_tfloat(arg0, arg1);
    }

    /** MEOS {@code mul_int_tint} — meos.h · scalar / stateless. */
    public static jnr.ffi.Pointer mul_int_tint(int arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("mul_int_tint requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.mul_int_tint(arg0, arg1);
    }

    /** MEOS {@code mul_tfloat_float} — meos.h · scalar / stateless. */
    public static jnr.ffi.Pointer mul_tfloat_float(jnr.ffi.Pointer arg0, double arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("mul_tfloat_float requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.mul_tfloat_float(arg0, arg1);
    }

    /** MEOS {@code mul_tint_int} — meos.h · scalar / stateless. */
    public static jnr.ffi.Pointer mul_tint_int(jnr.ffi.Pointer arg0, int arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("mul_tint_int requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.mul_tint_int(arg0, arg1);
    }

    /** MEOS {@code mul_tnumber_tnumber} — meos.h · scalar / stateless. */
    public static jnr.ffi.Pointer mul_tnumber_tnumber(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("mul_tnumber_tnumber requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.mul_tnumber_tnumber(arg0, arg1);
    }

    /** MEOS {@code nsegment_end_position} — meos_npoint.h · scalar / stateless. */
    public static double nsegment_end_position(jnr.ffi.Pointer arg0) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("nsegment_end_position requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.nsegment_end_position(arg0);
    }

    /** MEOS {@code nsegment_start_position} — meos_npoint.h · scalar / stateless. */
    public static double nsegment_start_position(jnr.ffi.Pointer arg0) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("nsegment_start_position requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.nsegment_start_position(arg0);
    }

    /** MEOS {@code route_geom} — meos_npoint.h · scalar / stateless. */
    public static jnr.ffi.Pointer route_geom(long arg0) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("route_geom requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.route_geom(arg0);
    }

    /** MEOS {@code spatialset_set_srid} — meos_geo.h · scalar / stateless. */
    public static jnr.ffi.Pointer spatialset_set_srid(jnr.ffi.Pointer arg0, int arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("spatialset_set_srid requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.spatialset_set_srid(arg0, arg1);
    }

    /** MEOS {@code spatialset_transform} — meos_geo.h · scalar / stateless. */
    public static jnr.ffi.Pointer spatialset_transform(jnr.ffi.Pointer arg0, int arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("spatialset_transform requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.spatialset_transform(arg0, arg1);
    }

    /** MEOS {@code spatialset_transform_pipeline} — meos_geo.h · scalar / stateless. */
    public static jnr.ffi.Pointer spatialset_transform_pipeline(jnr.ffi.Pointer arg0, java.lang.String arg1, int arg2, boolean arg3) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("spatialset_transform_pipeline requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.spatialset_transform_pipeline(arg0, arg1, arg2, arg3);
    }

    /** MEOS {@code tand_bool_tbool} — meos.h · scalar / stateless. */
    public static jnr.ffi.Pointer tand_bool_tbool(boolean arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("tand_bool_tbool requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.tand_bool_tbool(arg0, arg1);
    }

    /** MEOS {@code tand_tbool_bool} — meos.h · scalar / stateless. */
    public static jnr.ffi.Pointer tand_tbool_bool(jnr.ffi.Pointer arg0, boolean arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("tand_tbool_bool requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.tand_tbool_bool(arg0, arg1);
    }

    /** MEOS {@code tand_tbool_tbool} — meos.h · scalar / stateless. */
    public static jnr.ffi.Pointer tand_tbool_tbool(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("tand_tbool_tbool requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.tand_tbool_tbool(arg0, arg1);
    }

    /** MEOS {@code tboolseq_from_base_tstzset} — meos.h · whole-sequence constructor — not a per-event op. */
    public static jnr.ffi.Pointer tboolseq_from_base_tstzset(boolean arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("tboolseq_from_base_tstzset requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.tboolseq_from_base_tstzset(arg0, arg1);
    }

    /** MEOS {@code tboolseq_from_base_tstzspan} — meos.h · whole-sequence constructor — not a per-event op. */
    public static jnr.ffi.Pointer tboolseq_from_base_tstzspan(boolean arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("tboolseq_from_base_tstzspan requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.tboolseq_from_base_tstzspan(arg0, arg1);
    }

    /** MEOS {@code tboolseqset_from_base_tstzspanset} — meos.h · whole-sequence constructor — not a per-event op. */
    public static jnr.ffi.Pointer tboolseqset_from_base_tstzspanset(boolean arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("tboolseqset_from_base_tstzspanset requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.tboolseqset_from_base_tstzspanset(arg0, arg1);
    }

    /** MEOS {@code tbox_to_bigintspan} — meos.h · scalar / stateless. */
    public static jnr.ffi.Pointer tbox_to_bigintspan(jnr.ffi.Pointer arg0) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("tbox_to_bigintspan requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.tbox_to_bigintspan(arg0);
    }

    /** MEOS {@code tfloatbox_time_tiles} — meos.h · multidimensional tiling (windowed). */
    public static jnr.ffi.Pointer tfloatbox_time_tiles(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1, java.time.OffsetDateTime arg2, jnr.ffi.Pointer arg3) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("tfloatbox_time_tiles requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.tfloatbox_time_tiles(arg0, arg1, arg2, arg3);
    }

    /** MEOS {@code tfloatbox_value_tiles} — meos.h · multidimensional tiling (windowed). */
    public static jnr.ffi.Pointer tfloatbox_value_tiles(jnr.ffi.Pointer arg0, double arg1, double arg2, jnr.ffi.Pointer arg3) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("tfloatbox_value_tiles requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.tfloatbox_value_tiles(arg0, arg1, arg2, arg3);
    }

    /** MEOS {@code tfloatbox_value_time_tiles} — meos.h · multidimensional tiling (windowed). */
    public static jnr.ffi.Pointer tfloatbox_value_time_tiles(jnr.ffi.Pointer arg0, double arg1, jnr.ffi.Pointer arg2, double arg3, java.time.OffsetDateTime arg4, jnr.ffi.Pointer arg5) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("tfloatbox_value_time_tiles requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.tfloatbox_value_time_tiles(arg0, arg1, arg2, arg3, arg4, arg5);
    }

    /** MEOS {@code tfloatseq_from_base_tstzset} — meos.h · whole-sequence constructor — not a per-event op. */
    public static jnr.ffi.Pointer tfloatseq_from_base_tstzset(double arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("tfloatseq_from_base_tstzset requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.tfloatseq_from_base_tstzset(arg0, arg1);
    }

    /** MEOS {@code tfloatseq_from_base_tstzspan} — meos.h · whole-sequence constructor — not a per-event op. */
    public static jnr.ffi.Pointer tfloatseq_from_base_tstzspan(double arg0, jnr.ffi.Pointer arg1, int arg2) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("tfloatseq_from_base_tstzspan requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.tfloatseq_from_base_tstzspan(arg0, arg1, arg2);
    }

    /** MEOS {@code tfloatseqset_from_base_tstzspanset} — meos.h · whole-sequence constructor — not a per-event op. */
    public static jnr.ffi.Pointer tfloatseqset_from_base_tstzspanset(double arg0, jnr.ffi.Pointer arg1, int arg2) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("tfloatseqset_from_base_tstzspanset requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.tfloatseqset_from_base_tstzspanset(arg0, arg1, arg2);
    }

    /** MEOS {@code tgeoarr_tgeoarr_mindist} — meos_geo.h · scalar / stateless. */
    public static double tgeoarr_tgeoarr_mindist(jnr.ffi.Pointer arg0, int arg1, jnr.ffi.Pointer arg2, int arg3) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("tgeoarr_tgeoarr_mindist requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.tgeoarr_tgeoarr_mindist(arg0, arg1, arg2, arg3);
    }

    /** MEOS {@code tgeoseq_from_base_tstzset} — meos_geo.h · whole-sequence constructor — not a per-event op. */
    public static jnr.ffi.Pointer tgeoseq_from_base_tstzset(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("tgeoseq_from_base_tstzset requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.tgeoseq_from_base_tstzset(arg0, arg1);
    }

    /** MEOS {@code tgeoseq_from_base_tstzspan} — meos_geo.h · whole-sequence constructor — not a per-event op. */
    public static jnr.ffi.Pointer tgeoseq_from_base_tstzspan(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1, int arg2) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("tgeoseq_from_base_tstzspan requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.tgeoseq_from_base_tstzspan(arg0, arg1, arg2);
    }

    /** MEOS {@code tgeoseqset_from_base_tstzspanset} — meos_geo.h · whole-sequence constructor — not a per-event op. */
    public static jnr.ffi.Pointer tgeoseqset_from_base_tstzspanset(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1, int arg2) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("tgeoseqset_from_base_tstzspanset requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.tgeoseqset_from_base_tstzspanset(arg0, arg1, arg2);
    }

    /** MEOS {@code tintbox_time_tiles} — meos.h · multidimensional tiling (windowed). */
    public static jnr.ffi.Pointer tintbox_time_tiles(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1, java.time.OffsetDateTime arg2, jnr.ffi.Pointer arg3) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("tintbox_time_tiles requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.tintbox_time_tiles(arg0, arg1, arg2, arg3);
    }

    /** MEOS {@code tintbox_value_tiles} — meos.h · multidimensional tiling (windowed). */
    public static jnr.ffi.Pointer tintbox_value_tiles(jnr.ffi.Pointer arg0, int arg1, int arg2, jnr.ffi.Pointer arg3) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("tintbox_value_tiles requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.tintbox_value_tiles(arg0, arg1, arg2, arg3);
    }

    /** MEOS {@code tintbox_value_time_tiles} — meos.h · multidimensional tiling (windowed). */
    public static jnr.ffi.Pointer tintbox_value_time_tiles(jnr.ffi.Pointer arg0, int arg1, jnr.ffi.Pointer arg2, int arg3, java.time.OffsetDateTime arg4, jnr.ffi.Pointer arg5) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("tintbox_value_time_tiles requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.tintbox_value_time_tiles(arg0, arg1, arg2, arg3, arg4, arg5);
    }

    /** MEOS {@code tintseq_from_base_tstzset} — meos.h · whole-sequence constructor — not a per-event op. */
    public static jnr.ffi.Pointer tintseq_from_base_tstzset(int arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("tintseq_from_base_tstzset requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.tintseq_from_base_tstzset(arg0, arg1);
    }

    /** MEOS {@code tintseq_from_base_tstzspan} — meos.h · whole-sequence constructor — not a per-event op. */
    public static jnr.ffi.Pointer tintseq_from_base_tstzspan(int arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("tintseq_from_base_tstzspan requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.tintseq_from_base_tstzspan(arg0, arg1);
    }

    /** MEOS {@code tintseqset_from_base_tstzspanset} — meos.h · whole-sequence constructor — not a per-event op. */
    public static jnr.ffi.Pointer tintseqset_from_base_tstzspanset(int arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("tintseqset_from_base_tstzspanset requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.tintseqset_from_base_tstzspanset(arg0, arg1);
    }

    /** MEOS {@code tnot_tbool} — meos.h · scalar / stateless. */
    public static jnr.ffi.Pointer tnot_tbool(jnr.ffi.Pointer arg0) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("tnot_tbool requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.tnot_tbool(arg0);
    }

    /** MEOS {@code tor_bool_tbool} — meos.h · scalar / stateless. */
    public static jnr.ffi.Pointer tor_bool_tbool(boolean arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("tor_bool_tbool requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.tor_bool_tbool(arg0, arg1);
    }

    /** MEOS {@code tor_tbool_bool} — meos.h · scalar / stateless. */
    public static jnr.ffi.Pointer tor_tbool_bool(jnr.ffi.Pointer arg0, boolean arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("tor_tbool_bool requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.tor_tbool_bool(arg0, arg1);
    }

    /** MEOS {@code tor_tbool_tbool} — meos.h · scalar / stateless. */
    public static jnr.ffi.Pointer tor_tbool_tbool(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("tor_tbool_tbool requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.tor_tbool_tbool(arg0, arg1);
    }

    /** MEOS {@code tpointseq_from_base_tstzset} — meos_geo.h · whole-sequence constructor — not a per-event op. */
    public static jnr.ffi.Pointer tpointseq_from_base_tstzset(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("tpointseq_from_base_tstzset requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.tpointseq_from_base_tstzset(arg0, arg1);
    }

    /** MEOS {@code tpointseq_from_base_tstzspan} — meos_geo.h · whole-sequence constructor — not a per-event op. */
    public static jnr.ffi.Pointer tpointseq_from_base_tstzspan(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1, int arg2) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("tpointseq_from_base_tstzspan requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.tpointseq_from_base_tstzspan(arg0, arg1, arg2);
    }

    /** MEOS {@code tpointseq_make_coords} — meos_geo.h · whole-sequence constructor — not a per-event op. */
    public static jnr.ffi.Pointer tpointseq_make_coords(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1, jnr.ffi.Pointer arg2, jnr.ffi.Pointer arg3, int arg4, int arg5, boolean arg6, boolean arg7, boolean arg8, int arg9, boolean arg10) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("tpointseq_make_coords requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.tpointseq_make_coords(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10);
    }

    /** MEOS {@code tpointseqset_from_base_tstzspanset} — meos_geo.h · whole-sequence constructor — not a per-event op. */
    public static jnr.ffi.Pointer tpointseqset_from_base_tstzspanset(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1, int arg2) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("tpointseqset_from_base_tstzspanset requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.tpointseqset_from_base_tstzspanset(arg0, arg1, arg2);
    }

    /** MEOS {@code tsequence_make} — meos.h · whole-sequence constructor — not a per-event op. */
    public static jnr.ffi.Pointer tsequence_make(jnr.ffi.Pointer arg0, int arg1, boolean arg2, boolean arg3, int arg4, boolean arg5) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("tsequence_make requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.tsequence_make(arg0, arg1, arg2, arg3, arg4, arg5);
    }

    /** MEOS {@code tsequenceset_make} — meos.h · whole-sequence constructor — not a per-event op. */
    public static jnr.ffi.Pointer tsequenceset_make(jnr.ffi.Pointer arg0, int arg1, boolean arg2) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("tsequenceset_make requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.tsequenceset_make(arg0, arg1, arg2);
    }

    /** MEOS {@code ttextseq_from_base_tstzset} — meos.h · whole-sequence constructor — not a per-event op. */
    public static jnr.ffi.Pointer ttextseq_from_base_tstzset(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("ttextseq_from_base_tstzset requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.ttextseq_from_base_tstzset(arg0, arg1);
    }

    /** MEOS {@code ttextseq_from_base_tstzspan} — meos.h · whole-sequence constructor — not a per-event op. */
    public static jnr.ffi.Pointer ttextseq_from_base_tstzspan(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("ttextseq_from_base_tstzspan requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.ttextseq_from_base_tstzspan(arg0, arg1);
    }

    /** MEOS {@code ttextseqset_from_base_tstzspanset} — meos.h · whole-sequence constructor — not a per-event op. */
    public static jnr.ffi.Pointer ttextseqset_from_base_tstzspanset(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("ttextseqset_from_base_tstzspanset requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.ttextseqset_from_base_tstzspanset(arg0, arg1);
    }

    /** MEOS {@code union_cbuffer_set} — meos_cbuffer.h · scalar / stateless. */
    public static jnr.ffi.Pointer union_cbuffer_set(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("union_cbuffer_set requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.union_cbuffer_set(arg0, arg1);
    }

    /** MEOS {@code union_npoint_set} — meos_npoint.h · scalar / stateless. */
    public static jnr.ffi.Pointer union_npoint_set(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("union_npoint_set requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.union_npoint_set(arg0, arg1);
    }

    /** MEOS {@code union_pose_set} — meos_pose.h · scalar / stateless. */
    public static jnr.ffi.Pointer union_pose_set(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("union_pose_set requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.union_pose_set(arg0, arg1);
    }

}
