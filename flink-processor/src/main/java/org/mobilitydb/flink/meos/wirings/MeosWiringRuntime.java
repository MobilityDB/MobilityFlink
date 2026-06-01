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
