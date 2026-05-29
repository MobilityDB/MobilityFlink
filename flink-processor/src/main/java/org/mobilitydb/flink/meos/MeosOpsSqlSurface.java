package org.mobilitydb.flink.meos;

/**
 * Forwarding facade methods for MEOS functions that the MobilityDB SQL layer
 * exposes as user functions but whose implementation lives in the internal headers
 * ({@code meos_internal*.h}). JMEOS binds them; they are exposed here so the facade
 * matches the SQL user surface as well as the public MEOS API. Each method delegates
 * to its {@code functions.GeneratedFunctions} export under the
 * {@link MeosOpsRuntime#MEOS_AVAILABLE} guard.
 */
public final class MeosOpsSqlSurface {

    private MeosOpsSqlSurface() { /* utility */ }

    /** MEOS {@code adjacent_span_value} — SQL-surface function (meos_internal). */
    public static boolean adjacent_span_value(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("adjacent_span_value requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.adjacent_span_value(arg0, arg1);
    }

    /** MEOS {@code adjacent_spanset_value} — SQL-surface function (meos_internal). */
    public static boolean adjacent_spanset_value(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("adjacent_spanset_value requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.adjacent_spanset_value(arg0, arg1);
    }

    /** MEOS {@code adjacent_value_spanset} — SQL-surface function (meos_internal). */
    public static boolean adjacent_value_spanset(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("adjacent_value_spanset requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.adjacent_value_spanset(arg0, arg1);
    }

    /** MEOS {@code always_eq_base_temporal} — SQL-surface function (meos_internal). */
    public static int always_eq_base_temporal(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("always_eq_base_temporal requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.always_eq_base_temporal(arg0, arg1);
    }

    /** MEOS {@code always_eq_temporal_base} — SQL-surface function (meos_internal). */
    public static int always_eq_temporal_base(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("always_eq_temporal_base requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.always_eq_temporal_base(arg0, arg1);
    }

    /** MEOS {@code always_ge_base_temporal} — SQL-surface function (meos_internal). */
    public static int always_ge_base_temporal(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("always_ge_base_temporal requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.always_ge_base_temporal(arg0, arg1);
    }

    /** MEOS {@code always_ge_temporal_base} — SQL-surface function (meos_internal). */
    public static int always_ge_temporal_base(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("always_ge_temporal_base requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.always_ge_temporal_base(arg0, arg1);
    }

    /** MEOS {@code always_gt_base_temporal} — SQL-surface function (meos_internal). */
    public static int always_gt_base_temporal(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("always_gt_base_temporal requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.always_gt_base_temporal(arg0, arg1);
    }

    /** MEOS {@code always_gt_temporal_base} — SQL-surface function (meos_internal). */
    public static int always_gt_temporal_base(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("always_gt_temporal_base requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.always_gt_temporal_base(arg0, arg1);
    }

    /** MEOS {@code always_le_base_temporal} — SQL-surface function (meos_internal). */
    public static int always_le_base_temporal(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("always_le_base_temporal requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.always_le_base_temporal(arg0, arg1);
    }

    /** MEOS {@code always_le_temporal_base} — SQL-surface function (meos_internal). */
    public static int always_le_temporal_base(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("always_le_temporal_base requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.always_le_temporal_base(arg0, arg1);
    }

    /** MEOS {@code always_lt_base_temporal} — SQL-surface function (meos_internal). */
    public static int always_lt_base_temporal(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("always_lt_base_temporal requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.always_lt_base_temporal(arg0, arg1);
    }

    /** MEOS {@code always_lt_temporal_base} — SQL-surface function (meos_internal). */
    public static int always_lt_temporal_base(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("always_lt_temporal_base requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.always_lt_temporal_base(arg0, arg1);
    }

    /** MEOS {@code always_ne_base_temporal} — SQL-surface function (meos_internal). */
    public static int always_ne_base_temporal(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("always_ne_base_temporal requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.always_ne_base_temporal(arg0, arg1);
    }

    /** MEOS {@code always_ne_temporal_base} — SQL-surface function (meos_internal). */
    public static int always_ne_temporal_base(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("always_ne_temporal_base requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.always_ne_temporal_base(arg0, arg1);
    }

    /** MEOS {@code contained_value_set} — SQL-surface function (meos_internal). */
    public static boolean contained_value_set(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("contained_value_set requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.contained_value_set(arg0, arg1);
    }

    /** MEOS {@code contained_value_span} — SQL-surface function (meos_internal). */
    public static boolean contained_value_span(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("contained_value_span requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.contained_value_span(arg0, arg1);
    }

    /** MEOS {@code contained_value_spanset} — SQL-surface function (meos_internal). */
    public static boolean contained_value_spanset(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("contained_value_spanset requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.contained_value_spanset(arg0, arg1);
    }

    /** MEOS {@code contains_set_value} — SQL-surface function (meos_internal). */
    public static boolean contains_set_value(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("contains_set_value requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.contains_set_value(arg0, arg1);
    }

    /** MEOS {@code contains_span_value} — SQL-surface function (meos_internal). */
    public static boolean contains_span_value(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("contains_span_value requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.contains_span_value(arg0, arg1);
    }

    /** MEOS {@code contains_spanset_value} — SQL-surface function (meos_internal). */
    public static boolean contains_spanset_value(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("contains_spanset_value requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.contains_spanset_value(arg0, arg1);
    }

    /** MEOS {@code distance_set_set} — SQL-surface function (meos_internal). */
    public static int distance_set_set(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("distance_set_set requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.distance_set_set(arg0, arg1);
    }

    /** MEOS {@code distance_set_value} — SQL-surface function (meos_internal). */
    public static int distance_set_value(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("distance_set_value requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.distance_set_value(arg0, arg1);
    }

    /** MEOS {@code distance_span_span} — SQL-surface function (meos_internal). */
    public static int distance_span_span(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("distance_span_span requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.distance_span_span(arg0, arg1);
    }

    /** MEOS {@code distance_span_value} — SQL-surface function (meos_internal). */
    public static int distance_span_value(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("distance_span_value requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.distance_span_value(arg0, arg1);
    }

    /** MEOS {@code distance_spanset_span} — SQL-surface function (meos_internal). */
    public static int distance_spanset_span(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("distance_spanset_span requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.distance_spanset_span(arg0, arg1);
    }

    /** MEOS {@code distance_spanset_spanset} — SQL-surface function (meos_internal). */
    public static int distance_spanset_spanset(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("distance_spanset_spanset requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.distance_spanset_spanset(arg0, arg1);
    }

    /** MEOS {@code distance_spanset_value} — SQL-surface function (meos_internal). */
    public static int distance_spanset_value(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("distance_spanset_value requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.distance_spanset_value(arg0, arg1);
    }

    /** MEOS {@code distance_value_value} — SQL-surface function (meos_internal). */
    public static int distance_value_value(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1, int arg2) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("distance_value_value requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.distance_value_value(arg0, arg1, arg2);
    }

    /** MEOS {@code ever_eq_base_temporal} — SQL-surface function (meos_internal). */
    public static int ever_eq_base_temporal(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("ever_eq_base_temporal requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.ever_eq_base_temporal(arg0, arg1);
    }

    /** MEOS {@code ever_eq_temporal_base} — SQL-surface function (meos_internal). */
    public static int ever_eq_temporal_base(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("ever_eq_temporal_base requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.ever_eq_temporal_base(arg0, arg1);
    }

    /** MEOS {@code ever_ge_base_temporal} — SQL-surface function (meos_internal). */
    public static int ever_ge_base_temporal(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("ever_ge_base_temporal requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.ever_ge_base_temporal(arg0, arg1);
    }

    /** MEOS {@code ever_ge_temporal_base} — SQL-surface function (meos_internal). */
    public static int ever_ge_temporal_base(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("ever_ge_temporal_base requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.ever_ge_temporal_base(arg0, arg1);
    }

    /** MEOS {@code ever_gt_base_temporal} — SQL-surface function (meos_internal). */
    public static int ever_gt_base_temporal(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("ever_gt_base_temporal requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.ever_gt_base_temporal(arg0, arg1);
    }

    /** MEOS {@code ever_gt_temporal_base} — SQL-surface function (meos_internal). */
    public static int ever_gt_temporal_base(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("ever_gt_temporal_base requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.ever_gt_temporal_base(arg0, arg1);
    }

    /** MEOS {@code ever_le_base_temporal} — SQL-surface function (meos_internal). */
    public static int ever_le_base_temporal(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("ever_le_base_temporal requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.ever_le_base_temporal(arg0, arg1);
    }

    /** MEOS {@code ever_le_temporal_base} — SQL-surface function (meos_internal). */
    public static int ever_le_temporal_base(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("ever_le_temporal_base requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.ever_le_temporal_base(arg0, arg1);
    }

    /** MEOS {@code ever_lt_base_temporal} — SQL-surface function (meos_internal). */
    public static int ever_lt_base_temporal(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("ever_lt_base_temporal requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.ever_lt_base_temporal(arg0, arg1);
    }

    /** MEOS {@code ever_lt_temporal_base} — SQL-surface function (meos_internal). */
    public static int ever_lt_temporal_base(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("ever_lt_temporal_base requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.ever_lt_temporal_base(arg0, arg1);
    }

    /** MEOS {@code ever_ne_base_temporal} — SQL-surface function (meos_internal). */
    public static int ever_ne_base_temporal(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("ever_ne_base_temporal requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.ever_ne_base_temporal(arg0, arg1);
    }

    /** MEOS {@code ever_ne_temporal_base} — SQL-surface function (meos_internal). */
    public static int ever_ne_temporal_base(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("ever_ne_temporal_base requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.ever_ne_temporal_base(arg0, arg1);
    }

    /** MEOS {@code intersection_set_value} — SQL-surface function (meos_internal). */
    public static jnr.ffi.Pointer intersection_set_value(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("intersection_set_value requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.intersection_set_value(arg0, arg1);
    }

    /** MEOS {@code intersection_span_value} — SQL-surface function (meos_internal). */
    public static jnr.ffi.Pointer intersection_span_value(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("intersection_span_value requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.intersection_span_value(arg0, arg1);
    }

    /** MEOS {@code intersection_spanset_value} — SQL-surface function (meos_internal). */
    public static jnr.ffi.Pointer intersection_spanset_value(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("intersection_spanset_value requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.intersection_spanset_value(arg0, arg1);
    }

    /** MEOS {@code intersection_value_set} — SQL-surface function (meos_internal). */
    public static jnr.ffi.Pointer intersection_value_set(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("intersection_value_set requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.intersection_value_set(arg0, arg1);
    }

    /** MEOS {@code intersection_value_span} — SQL-surface function (meos_internal). */
    public static jnr.ffi.Pointer intersection_value_span(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("intersection_value_span requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.intersection_value_span(arg0, arg1);
    }

    /** MEOS {@code intersection_value_spanset} — SQL-surface function (meos_internal). */
    public static jnr.ffi.Pointer intersection_value_spanset(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("intersection_value_spanset requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.intersection_value_spanset(arg0, arg1);
    }

    /** MEOS {@code left_set_value} — SQL-surface function (meos_internal). */
    public static boolean left_set_value(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("left_set_value requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.left_set_value(arg0, arg1);
    }

    /** MEOS {@code left_span_value} — SQL-surface function (meos_internal). */
    public static boolean left_span_value(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("left_span_value requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.left_span_value(arg0, arg1);
    }

    /** MEOS {@code left_spanset_value} — SQL-surface function (meos_internal). */
    public static boolean left_spanset_value(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("left_spanset_value requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.left_spanset_value(arg0, arg1);
    }

    /** MEOS {@code left_value_set} — SQL-surface function (meos_internal). */
    public static boolean left_value_set(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("left_value_set requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.left_value_set(arg0, arg1);
    }

    /** MEOS {@code left_value_span} — SQL-surface function (meos_internal). */
    public static boolean left_value_span(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("left_value_span requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.left_value_span(arg0, arg1);
    }

    /** MEOS {@code left_value_spanset} — SQL-surface function (meos_internal). */
    public static boolean left_value_spanset(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("left_value_spanset requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.left_value_spanset(arg0, arg1);
    }

    /** MEOS {@code minus_set_value} — SQL-surface function (meos_internal). */
    public static jnr.ffi.Pointer minus_set_value(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("minus_set_value requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.minus_set_value(arg0, arg1);
    }

    /** MEOS {@code minus_span_value} — SQL-surface function (meos_internal). */
    public static jnr.ffi.Pointer minus_span_value(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("minus_span_value requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.minus_span_value(arg0, arg1);
    }

    /** MEOS {@code minus_spanset_value} — SQL-surface function (meos_internal). */
    public static jnr.ffi.Pointer minus_spanset_value(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("minus_spanset_value requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.minus_spanset_value(arg0, arg1);
    }

    /** MEOS {@code minus_value_set} — SQL-surface function (meos_internal). */
    public static jnr.ffi.Pointer minus_value_set(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("minus_value_set requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.minus_value_set(arg0, arg1);
    }

    /** MEOS {@code minus_value_span} — SQL-surface function (meos_internal). */
    public static jnr.ffi.Pointer minus_value_span(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("minus_value_span requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.minus_value_span(arg0, arg1);
    }

    /** MEOS {@code minus_value_spanset} — SQL-surface function (meos_internal). */
    public static jnr.ffi.Pointer minus_value_spanset(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("minus_value_spanset requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.minus_value_spanset(arg0, arg1);
    }

    /** MEOS {@code nad_tbox_tbox} — SQL-surface function (meos_internal). */
    public static double nad_tbox_tbox(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("nad_tbox_tbox requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.nad_tbox_tbox(arg0, arg1);
    }

    /** MEOS {@code nad_tnumber_number} — SQL-surface function (meos_internal). */
    public static double nad_tnumber_number(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("nad_tnumber_number requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.nad_tnumber_number(arg0, arg1);
    }

    /** MEOS {@code nad_tnumber_tbox} — SQL-surface function (meos_internal). */
    public static double nad_tnumber_tbox(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("nad_tnumber_tbox requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.nad_tnumber_tbox(arg0, arg1);
    }

    /** MEOS {@code nad_tnumber_tnumber} — SQL-surface function (meos_internal). */
    public static double nad_tnumber_tnumber(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("nad_tnumber_tnumber requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.nad_tnumber_tnumber(arg0, arg1);
    }

    /** MEOS {@code number_timestamptz_to_tbox} — SQL-surface function (meos_internal). */
    public static jnr.ffi.Pointer number_timestamptz_to_tbox(jnr.ffi.Pointer arg0, int arg1, java.time.OffsetDateTime arg2) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("number_timestamptz_to_tbox requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.number_timestamptz_to_tbox(arg0, arg1, arg2);
    }

    /** MEOS {@code number_tstzspan_to_tbox} — SQL-surface function (meos_internal). */
    public static jnr.ffi.Pointer number_tstzspan_to_tbox(jnr.ffi.Pointer arg0, int arg1, jnr.ffi.Pointer arg2) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("number_tstzspan_to_tbox requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.number_tstzspan_to_tbox(arg0, arg1, arg2);
    }

    /** MEOS {@code numset_shift_scale} — SQL-surface function (meos_internal). */
    public static jnr.ffi.Pointer numset_shift_scale(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1, jnr.ffi.Pointer arg2, boolean arg3, boolean arg4) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("numset_shift_scale requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.numset_shift_scale(arg0, arg1, arg2, arg3, arg4);
    }

    /** MEOS {@code numspan_expand} — SQL-surface function (meos_internal). */
    public static jnr.ffi.Pointer numspan_expand(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("numspan_expand requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.numspan_expand(arg0, arg1);
    }

    /** MEOS {@code numspan_shift_scale} — SQL-surface function (meos_internal). */
    public static jnr.ffi.Pointer numspan_shift_scale(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1, jnr.ffi.Pointer arg2, boolean arg3, boolean arg4) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("numspan_shift_scale requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.numspan_shift_scale(arg0, arg1, arg2, arg3, arg4);
    }

    /** MEOS {@code numspan_width} — SQL-surface function (meos_internal). */
    public static int numspan_width(jnr.ffi.Pointer arg0) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("numspan_width requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.numspan_width(arg0);
    }

    /** MEOS {@code numspanset_shift_scale} — SQL-surface function (meos_internal). */
    public static jnr.ffi.Pointer numspanset_shift_scale(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1, jnr.ffi.Pointer arg2, boolean arg3, boolean arg4) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("numspanset_shift_scale requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.numspanset_shift_scale(arg0, arg1, arg2, arg3, arg4);
    }

    /** MEOS {@code numspanset_width} — SQL-surface function (meos_internal). */
    public static int numspanset_width(jnr.ffi.Pointer arg0, boolean arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("numspanset_width requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.numspanset_width(arg0, arg1);
    }

    /** MEOS {@code overleft_set_value} — SQL-surface function (meos_internal). */
    public static boolean overleft_set_value(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("overleft_set_value requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.overleft_set_value(arg0, arg1);
    }

    /** MEOS {@code overleft_span_value} — SQL-surface function (meos_internal). */
    public static boolean overleft_span_value(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("overleft_span_value requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.overleft_span_value(arg0, arg1);
    }

    /** MEOS {@code overleft_spanset_value} — SQL-surface function (meos_internal). */
    public static boolean overleft_spanset_value(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("overleft_spanset_value requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.overleft_spanset_value(arg0, arg1);
    }

    /** MEOS {@code overleft_value_set} — SQL-surface function (meos_internal). */
    public static boolean overleft_value_set(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("overleft_value_set requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.overleft_value_set(arg0, arg1);
    }

    /** MEOS {@code overleft_value_span} — SQL-surface function (meos_internal). */
    public static boolean overleft_value_span(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("overleft_value_span requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.overleft_value_span(arg0, arg1);
    }

    /** MEOS {@code overleft_value_spanset} — SQL-surface function (meos_internal). */
    public static boolean overleft_value_spanset(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("overleft_value_spanset requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.overleft_value_spanset(arg0, arg1);
    }

    /** MEOS {@code overright_set_value} — SQL-surface function (meos_internal). */
    public static boolean overright_set_value(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("overright_set_value requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.overright_set_value(arg0, arg1);
    }

    /** MEOS {@code overright_span_value} — SQL-surface function (meos_internal). */
    public static boolean overright_span_value(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("overright_span_value requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.overright_span_value(arg0, arg1);
    }

    /** MEOS {@code overright_spanset_value} — SQL-surface function (meos_internal). */
    public static boolean overright_spanset_value(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("overright_spanset_value requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.overright_spanset_value(arg0, arg1);
    }

    /** MEOS {@code overright_value_set} — SQL-surface function (meos_internal). */
    public static boolean overright_value_set(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("overright_value_set requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.overright_value_set(arg0, arg1);
    }

    /** MEOS {@code overright_value_span} — SQL-surface function (meos_internal). */
    public static boolean overright_value_span(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("overright_value_span requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.overright_value_span(arg0, arg1);
    }

    /** MEOS {@code overright_value_spanset} — SQL-surface function (meos_internal). */
    public static boolean overright_value_spanset(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("overright_value_spanset requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.overright_value_spanset(arg0, arg1);
    }

    /** MEOS {@code right_set_value} — SQL-surface function (meos_internal). */
    public static boolean right_set_value(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("right_set_value requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.right_set_value(arg0, arg1);
    }

    /** MEOS {@code right_span_value} — SQL-surface function (meos_internal). */
    public static boolean right_span_value(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("right_span_value requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.right_span_value(arg0, arg1);
    }

    /** MEOS {@code right_spanset_value} — SQL-surface function (meos_internal). */
    public static boolean right_spanset_value(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("right_spanset_value requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.right_spanset_value(arg0, arg1);
    }

    /** MEOS {@code right_value_set} — SQL-surface function (meos_internal). */
    public static boolean right_value_set(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("right_value_set requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.right_value_set(arg0, arg1);
    }

    /** MEOS {@code right_value_span} — SQL-surface function (meos_internal). */
    public static boolean right_value_span(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("right_value_span requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.right_value_span(arg0, arg1);
    }

    /** MEOS {@code right_value_spanset} — SQL-surface function (meos_internal). */
    public static boolean right_value_spanset(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("right_value_spanset requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.right_value_spanset(arg0, arg1);
    }

    /** MEOS {@code set_end_value} — SQL-surface function (meos_internal). */
    public static int set_end_value(jnr.ffi.Pointer arg0) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("set_end_value requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.set_end_value(arg0);
    }

    /** MEOS {@code set_mem_size} — SQL-surface function (meos_internal). */
    public static int set_mem_size(jnr.ffi.Pointer arg0) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("set_mem_size requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.set_mem_size(arg0);
    }

    /** MEOS {@code set_start_value} — SQL-surface function (meos_internal). */
    public static int set_start_value(jnr.ffi.Pointer arg0) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("set_start_value requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.set_start_value(arg0);
    }

    /** MEOS {@code set_value_n} — SQL-surface function (meos_internal). */
    public static jnr.ffi.Pointer set_value_n(jnr.ffi.Pointer arg0, int arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("set_value_n requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.set_value_n(arg0, arg1);
    }

    /** MEOS {@code set_values} — SQL-surface function (meos_internal). */
    public static jnr.ffi.Pointer set_values(jnr.ffi.Pointer arg0) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("set_values requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.set_values(arg0);
    }

    /** MEOS {@code span_bins} — SQL-surface function (meos_internal). */
    public static jnr.ffi.Pointer span_bins(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1, jnr.ffi.Pointer arg2, jnr.ffi.Pointer arg3) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("span_bins requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.span_bins(arg0, arg1, arg2, arg3);
    }

    /** MEOS {@code spanset_bins} — SQL-surface function (meos_internal). */
    public static jnr.ffi.Pointer spanset_bins(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1, jnr.ffi.Pointer arg2, jnr.ffi.Pointer arg3) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("spanset_bins requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.spanset_bins(arg0, arg1, arg2, arg3);
    }

    /** MEOS {@code spanset_lower} — SQL-surface function (meos_internal). */
    public static int spanset_lower(jnr.ffi.Pointer arg0) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("spanset_lower requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.spanset_lower(arg0);
    }

    /** MEOS {@code spanset_mem_size} — SQL-surface function (meos_internal). */
    public static int spanset_mem_size(jnr.ffi.Pointer arg0) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("spanset_mem_size requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.spanset_mem_size(arg0);
    }

    /** MEOS {@code spanset_upper} — SQL-surface function (meos_internal). */
    public static int spanset_upper(jnr.ffi.Pointer arg0) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("spanset_upper requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.spanset_upper(arg0);
    }

    /** MEOS {@code tbox_expand_value} — SQL-surface function (meos_internal). */
    public static jnr.ffi.Pointer tbox_expand_value(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1, int arg2) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("tbox_expand_value requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.tbox_expand_value(arg0, arg1, arg2);
    }

    /** MEOS {@code tbox_get_value_time_tile} — SQL-surface function (meos_internal). */
    public static jnr.ffi.Pointer tbox_get_value_time_tile(jnr.ffi.Pointer arg0, java.time.OffsetDateTime arg1, jnr.ffi.Pointer arg2, jnr.ffi.Pointer arg3, jnr.ffi.Pointer arg4, java.time.OffsetDateTime arg5, int arg6, int arg7) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("tbox_get_value_time_tile requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.tbox_get_value_time_tile(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7);
    }

    /** MEOS {@code tbox_shift_scale_value} — SQL-surface function (meos_internal). */
    public static jnr.ffi.Pointer tbox_shift_scale_value(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1, jnr.ffi.Pointer arg2, boolean arg3, boolean arg4) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("tbox_shift_scale_value requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.tbox_shift_scale_value(arg0, arg1, arg2, arg3, arg4);
    }

    /** MEOS {@code tdistance_tnumber_number} — SQL-surface function (meos_internal). */
    public static jnr.ffi.Pointer tdistance_tnumber_number(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("tdistance_tnumber_number requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.tdistance_tnumber_number(arg0, arg1);
    }

    /** MEOS {@code temporal_end_value} — SQL-surface function (meos_internal). */
    public static int temporal_end_value(jnr.ffi.Pointer arg0) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("temporal_end_value requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.temporal_end_value(arg0);
    }

    /** MEOS {@code temporal_from_mfjson} — SQL-surface function (meos_internal). */
    public static jnr.ffi.Pointer temporal_from_mfjson(java.lang.String arg0, int arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("temporal_from_mfjson requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.temporal_from_mfjson(arg0, arg1);
    }

    /** MEOS {@code temporal_max_value} — SQL-surface function (meos_internal). */
    public static int temporal_max_value(jnr.ffi.Pointer arg0) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("temporal_max_value requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.temporal_max_value(arg0);
    }

    /** MEOS {@code temporal_mem_size} — SQL-surface function (meos_internal). */
    public static long temporal_mem_size(jnr.ffi.Pointer arg0) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("temporal_mem_size requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.temporal_mem_size(arg0);
    }

    /** MEOS {@code temporal_min_value} — SQL-surface function (meos_internal). */
    public static int temporal_min_value(jnr.ffi.Pointer arg0) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("temporal_min_value requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.temporal_min_value(arg0);
    }

    /** MEOS {@code temporal_start_value} — SQL-surface function (meos_internal). */
    public static int temporal_start_value(jnr.ffi.Pointer arg0) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("temporal_start_value requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.temporal_start_value(arg0);
    }

    /** MEOS {@code temporal_value_at_timestamptz} — SQL-surface function (meos_internal). */
    public static jnr.ffi.Pointer temporal_value_at_timestamptz(jnr.ffi.Pointer arg0, java.time.OffsetDateTime arg1, boolean arg2) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("temporal_value_at_timestamptz requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.temporal_value_at_timestamptz(arg0, arg1, arg2);
    }

    /** MEOS {@code temporal_value_n} — SQL-surface function (meos_internal). */
    public static jnr.ffi.Pointer temporal_value_n(jnr.ffi.Pointer arg0, int arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("temporal_value_n requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.temporal_value_n(arg0, arg1);
    }

    /** MEOS {@code tinstant_value} — SQL-surface function (meos_internal). */
    public static int tinstant_value(jnr.ffi.Pointer arg0) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("tinstant_value requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.tinstant_value(arg0);
    }

    /** MEOS {@code tnumber_shift_scale_value} — SQL-surface function (meos_internal). */
    public static jnr.ffi.Pointer tnumber_shift_scale_value(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1, jnr.ffi.Pointer arg2, boolean arg3, boolean arg4) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("tnumber_shift_scale_value requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.tnumber_shift_scale_value(arg0, arg1, arg2, arg3, arg4);
    }

    /** MEOS {@code tnumber_value_bins} — SQL-surface function (meos_internal). */
    public static jnr.ffi.Pointer tnumber_value_bins(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1, jnr.ffi.Pointer arg2, jnr.ffi.Pointer arg3) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("tnumber_value_bins requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.tnumber_value_bins(arg0, arg1, arg2, arg3);
    }

    /** MEOS {@code tnumber_value_split} — SQL-surface function (meos_internal). */
    public static jnr.ffi.Pointer tnumber_value_split(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1, jnr.ffi.Pointer arg2, jnr.ffi.Pointer arg3, jnr.ffi.Pointer arg4) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("tnumber_value_split requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.tnumber_value_split(arg0, arg1, arg2, arg3, arg4);
    }

    /** MEOS {@code tnumber_value_time_boxes} — SQL-surface function (meos_internal). */
    public static jnr.ffi.Pointer tnumber_value_time_boxes(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1, jnr.ffi.Pointer arg2, jnr.ffi.Pointer arg3, java.time.OffsetDateTime arg4, jnr.ffi.Pointer arg5) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("tnumber_value_time_boxes requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.tnumber_value_time_boxes(arg0, arg1, arg2, arg3, arg4, arg5);
    }

    /** MEOS {@code tnumber_value_time_split} — SQL-surface function (meos_internal). */
    public static jnr.ffi.Pointer tnumber_value_time_split(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1, jnr.ffi.Pointer arg2, jnr.ffi.Pointer arg3, java.time.OffsetDateTime arg4, jnr.ffi.Pointer arg5, jnr.ffi.Pointer arg6, jnr.ffi.Pointer arg7) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("tnumber_value_time_split requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.tnumber_value_time_split(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7);
    }

    /** MEOS {@code tsequence_from_base_tstzset} — SQL-surface function (meos_internal). */
    public static jnr.ffi.Pointer tsequence_from_base_tstzset(jnr.ffi.Pointer arg0, int arg1, jnr.ffi.Pointer arg2) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("tsequence_from_base_tstzset requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.tsequence_from_base_tstzset(arg0, arg1, arg2);
    }

    /** MEOS {@code tsequence_from_base_tstzspan} — SQL-surface function (meos_internal). */
    public static jnr.ffi.Pointer tsequence_from_base_tstzspan(jnr.ffi.Pointer arg0, int arg1, jnr.ffi.Pointer arg2, int arg3) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("tsequence_from_base_tstzspan requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.tsequence_from_base_tstzspan(arg0, arg1, arg2, arg3);
    }

    /** MEOS {@code tsequenceset_from_base_tstzspanset} — SQL-surface function (meos_internal). */
    public static jnr.ffi.Pointer tsequenceset_from_base_tstzspanset(jnr.ffi.Pointer arg0, int arg1, jnr.ffi.Pointer arg2, int arg3) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("tsequenceset_from_base_tstzspanset requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.tsequenceset_from_base_tstzspanset(arg0, arg1, arg2, arg3);
    }

    /** MEOS {@code union_set_value} — SQL-surface function (meos_internal). */
    public static jnr.ffi.Pointer union_set_value(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("union_set_value requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.union_set_value(arg0, arg1);
    }

    /** MEOS {@code union_span_value} — SQL-surface function (meos_internal). */
    public static jnr.ffi.Pointer union_span_value(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("union_span_value requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.union_span_value(arg0, arg1);
    }

    /** MEOS {@code union_spanset_value} — SQL-surface function (meos_internal). */
    public static jnr.ffi.Pointer union_spanset_value(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("union_spanset_value requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.union_spanset_value(arg0, arg1);
    }

    /** MEOS {@code union_value_set} — SQL-surface function (meos_internal). */
    public static jnr.ffi.Pointer union_value_set(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("union_value_set requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.union_value_set(arg0, arg1);
    }

    /** MEOS {@code union_value_span} — SQL-surface function (meos_internal). */
    public static jnr.ffi.Pointer union_value_span(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("union_value_span requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.union_value_span(arg0, arg1);
    }

    /** MEOS {@code union_value_spanset} — SQL-surface function (meos_internal). */
    public static jnr.ffi.Pointer union_value_spanset(jnr.ffi.Pointer arg0, jnr.ffi.Pointer arg1) {
        if (!MeosOpsRuntime.MEOS_AVAILABLE)
            throw new UnsupportedOperationException("union_value_spanset requires libmeos — set -Dmobilityflink.meos.enabled=true");
        return functions.GeneratedFunctions.union_value_spanset(arg0, arg1);
    }

}
