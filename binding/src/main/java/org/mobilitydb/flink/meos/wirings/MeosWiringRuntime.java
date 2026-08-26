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
import functions.error_handler_fn;

/**
 * MEOS initialization for the {@code org.mobilitydb.flink.meos.wirings} operators.
 *
 * <p>MEOS setup has two lifetimes: process-global state (the allocator and the
 * error handler) is installed once per JVM, while thread-local state (the
 * timezone and collation caches; the PROJ, GEOS and GSL contexts) belongs to
 * each task thread. Each Flink subtask runs on its own task thread, so every
 * wiring operator calls {@link #ensureInitializedOnThread()} from its
 * {@code open()} to install the thread-local part on that thread.
 *
 * <p>{@link #ensureInitializedOnThread()} is idempotent per thread (guarded by a
 * {@link ThreadLocal}), so it is safe to call from every operator's
 * {@code open()} even when operators are chained onto the same thread. The
 * process-global no-exit error handler makes a MEOS-side error surface as a
 * thrown exception rather than terminating the JVM.
 */
public final class MeosWiringRuntime {

    /**
     * No-exit MEOS error handler. MEOS's default handler calls exit(EXIT_FAILURE)
     * on an ERROR, which would tear the JVM down if a MEOS error fired inside a
     * task thread; this handler returns instead, and the error still surfaces
     * because MEOS sets meos_errno. Held as a static field so JNR keeps the
     * native callback alive for the process lifetime.
     */
    private static final error_handler_fn NOEXIT_ERROR_HANDLER =
            (level, code, message) -> { /* do not exit the JVM */ };

    /**
     * Process-global MEOS setup, installed exactly once per JVM: the allocator
     * and the error handler are process-global, not thread-local. The holder's
     * class initializer runs under the JVM class-initialization lock. meos_initialize()
     * runs first — it installs MEOS's exiting default handler — and the no-exit
     * handler then replaces it, so a thread reaching its per-thread setup always
     * sees the no-exit handler and the exiting default is never observable.
     */
    private static final class ProcessInit {
        static {
            GeneratedFunctions.meos_initialize();
            GeneratedFunctions.meos_initialize_error_handler(NOEXIT_ERROR_HANDLER);
        }
        /** Invoking this forces the class initializer above to run once. */
        static void ensure() { /* side effect: class initialization */ }
    }

    /**
     * Per-thread MEOS setup, run once per task thread: only the thread-local
     * caches. The timezone and collation caches are thread-local and set
     * explicitly per thread; the PROJ, GEOS and GSL contexts are thread-local too
     * and created lazily by MEOS on first use. Full meos_initialize() is NOT run
     * per thread — it re-installs the exiting default error handler that every
     * other thread relies on being the no-exit one.
     */
    private static final ThreadLocal<Boolean> INITIALIZED = ThreadLocal.withInitial(() -> {
        ProcessInit.ensure();
        GeneratedFunctions.meos_initialize_timezone("UTC");
        GeneratedFunctions.meos_initialize_collation();
        return Boolean.TRUE;
    });

    private MeosWiringRuntime() { /* utility */ }

    /** Initialize MEOS on the calling thread exactly once. */
    public static void ensureInitializedOnThread() {
        INITIALIZED.get();
    }
}
