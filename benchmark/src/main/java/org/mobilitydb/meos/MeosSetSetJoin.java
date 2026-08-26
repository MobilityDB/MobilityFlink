package org.mobilitydb.meos;

import functions.GeneratedFunctions;
import java.lang.ref.Reference;
import java.lang.reflect.Field;
import jnr.ffi.Memory;
import jnr.ffi.Pointer;
import jnr.ffi.Runtime;
import sun.misc.Unsafe;

/**
 * NxN cross-stream set-set join operator over two arrays of temporal geometries.
 *
 * <p>Each method marshals two Java {@code Pointer[]} (one per stream window) into native
 * pointer arrays and calls the MEOS NxN array spatial-relationship functions, returning the
 * matching {@code [i, j]} index pairs (and, for tDwithin, the per-pair intersection periods
 * as hex-WKB span sets):
 * <ul>
 *   <li>{@link GeneratedFunctions#edwithin_tgeoarr_tgeoarr} — ever-dwithin pairs;</li>
 *   <li>{@link GeneratedFunctions#adisjoint_tgeoarr_tgeoarr} — always-disjoint pairs;</li>
 *   <li>{@link GeneratedFunctions#tdwithin_tgeoarr_tgeoarr} — temporal-dwithin pairs + periods.</li>
 * </ul>
 *
 * <p>The backing C functions take {@code (Temporal **arr1, int n1, Temporal **arr2, int n2,
 * [double dist,] int *count [, SpanSet ***periods])} and return a flat {@code int *} of
 * {@code 2*count} indices (caller frees). Native result buffers are freed via {@code Unsafe}.
 */
public final class MeosSetSetJoin {
    private static final Unsafe UNSAFE;

    private MeosSetSetJoin() {
    }

    public static int[][] eDwithinPairs(Pointer[] a, Pointer[] b, double dist) {
        if (a == null || b == null || a.length == 0 || b.length == 0) {
            return new int[0][];
        }
        Runtime rt = Runtime.getSystemRuntime();
        Pointer arr1 = marshal(a, rt);
        Pointer arr2 = marshal(b, rt);
        Pointer countPtr = Memory.allocateDirect(rt, 4);
        Pointer res = GeneratedFunctions.edwithin_tgeoarr_tgeoarr(arr1, a.length, arr2, b.length, dist, countPtr);
        Reference.reachabilityFence(arr1);
        Reference.reachabilityFence(arr2);
        return readPairsAndFree(res, countPtr.getInt(0L));
    }

    public static int[][] aDisjointPairs(Pointer[] a, Pointer[] b) {
        if (a == null || b == null || a.length == 0 || b.length == 0) {
            return new int[0][];
        }
        Runtime rt = Runtime.getSystemRuntime();
        Pointer arr1 = marshal(a, rt);
        Pointer arr2 = marshal(b, rt);
        Pointer countPtr = Memory.allocateDirect(rt, 4);
        Pointer res = GeneratedFunctions.adisjoint_tgeoarr_tgeoarr(arr1, a.length, arr2, b.length, countPtr);
        Reference.reachabilityFence(arr1);
        Reference.reachabilityFence(arr2);
        return readPairsAndFree(res, countPtr.getInt(0L));
    }

    public static TDwithin tDwithinPairs(Pointer[] a, Pointer[] b, double dist) {
        if (a == null || b == null || a.length == 0 || b.length == 0) {
            return new TDwithin(new int[0][], new String[0]);
        }
        Runtime rt = Runtime.getSystemRuntime();
        Pointer arr1 = marshal(a, rt);
        Pointer arr2 = marshal(b, rt);
        Pointer countPtr = Memory.allocateDirect(rt, 4);
        Pointer periodsPtr = Memory.allocateDirect(rt, 8);
        Pointer res = GeneratedFunctions.tdwithin_tgeoarr_tgeoarr(arr1, a.length, arr2, b.length, dist, countPtr, periodsPtr);
        Reference.reachabilityFence(arr1);
        Reference.reachabilityFence(arr2);
        int cnt = countPtr.getInt(0L);
        if (res == null || cnt == 0) {
            return new TDwithin(new int[0][], new String[0]);
        }
        Pointer ssArr = periodsPtr.getPointer(0L);
        int[][] pairs = new int[cnt][2];
        String[] periods = new String[cnt];
        for (int k = 0; k < cnt; ++k) {
            pairs[k][0] = res.getInt((long) (2 * k) * 4L);
            pairs[k][1] = res.getInt((long) (2 * k + 1) * 4L);
            Pointer ss = ssArr == null ? null : ssArr.getPointer((long) k * 8L);
            periods[k] = ss == null ? null : GeneratedFunctions.spanset_as_hexwkb(ss, (byte) 0);
            free(ss);
        }
        free(ssArr);
        free(res);
        return new TDwithin(pairs, periods);
    }

    private static Pointer marshal(Pointer[] xs, Runtime rt) {
        Pointer buf = Memory.allocateDirect(rt, xs.length * 8);
        for (int i = 0; i < xs.length; ++i) {
            buf.putPointer((long) i * 8L, xs[i]);
        }
        return buf;
    }

    private static int[][] readPairsAndFree(Pointer res, int cnt) {
        if (res == null || cnt == 0) {
            return new int[0][];
        }
        int[][] out = new int[cnt][2];
        for (int k = 0; k < cnt; ++k) {
            out[k][0] = res.getInt((long) (2 * k) * 4L);
            out[k][1] = res.getInt((long) (2 * k + 1) * 4L);
        }
        free(res);
        return out;
    }

    private static void free(Pointer p) {
        if (p != null) {
            UNSAFE.freeMemory(p.address());
        }
    }

    static {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            UNSAFE = (Unsafe) f.get(null);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /** Result of {@link #tDwithinPairs}: matching index pairs + each pair's intersection periods (hex-WKB). */
    public static final class TDwithin {
        public final int[][] pairs;
        public final String[] periodsHexwkb;

        TDwithin(int[][] pairs, String[] periodsHexwkb) {
            this.pairs = pairs;
            this.periodsHexwkb = periodsHexwkb;
        }
    }
}
