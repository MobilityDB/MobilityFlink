package org.mobilitydb.flink.meos.wirings;

import functions.GeneratedFunctions;

/**
 * Per-thread MEOS initialization for the {@code org.mobilitydb.flink.meos.wirings}
 * operators.
 *
 * <p>MEOS keeps its timezone / session state per OS thread. Each Flink
 * subtask runs on its own task thread, so every wiring operator must
 * initialize MEOS on that thread from its {@code open()} — the JVM-wide
 * probe in {@code MeosOpsRuntime} only covers the thread that first
 * touches a facade class (typically the job's main thread), not the task
 * threads where the operators actually run.
 *
 * <p>{@link #ensureInitializedOnThread()} is idempotent per thread (guarded
 * by a {@link ThreadLocal}), so it is safe to call from every operator's
 * {@code open()} even when operators are chained onto the same thread. It
 * installs a no-op error handler so a MEOS-side error surfaces as a thrown
 * exception rather than terminating the JVM.
 */
public final class MeosWiringRuntime {

    private static final ThreadLocal<Boolean> INITIALIZED =
            ThreadLocal.withInitial(() -> Boolean.FALSE);

    private MeosWiringRuntime() { /* utility */ }

    /** Initialize MEOS on the calling thread exactly once. */
    public static void ensureInitializedOnThread() {
        if (!INITIALIZED.get()) {
            GeneratedFunctions.meos_initialize_error_handler((level, code, message) -> { });
            GeneratedFunctions.meos_initialize();
            INITIALIZED.set(Boolean.TRUE);
        }
    }
}
