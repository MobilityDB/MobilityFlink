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

package org.mobilitydb.meos;

import functions.GeneratedFunctions;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import jnr.ffi.Memory;
import jnr.ffi.Pointer;
import jnr.ffi.Runtime;
import types.temporal.TInterpolation;

/**
 * Builds the temporal points of the benchmark events as MEOS values.
 *
 * <p>An event's time is its milliseconds from the Unix epoch as an {@link OffsetDateTime},
 * which JMEOS casts to a MEOS {@code TimestampTz} through {@code utils.TimestampTzConverter},
 * and its position is a point MEOS makes from the longitude and the latitude. No value passes
 * through text, so an instant keeps the millisecond of its event and the coordinates of its
 * position.
 */
public final class TemporalPoints {

    /** The SRID of a geography point: WGS84, the SRID {@code tgeogpoint_in} gives one. */
    private static final int WGS84 = 4326;

    private TemporalPoints() {
    }

    /** The instant in UTC of a time in milliseconds from the Unix epoch. */
    public static OffsetDateTime eventTime(long epochMillis) {
        return Instant.ofEpochMilli(epochMillis).atOffset(ZoneOffset.UTC);
    }

    /** The {@code tgeogpoint} instant at a longitude and a latitude in WGS84 at a time. */
    public static Pointer tgeogpointInst(double lon, double lat, long epochMillis) {
        return GeneratedFunctions.tpointinst_make(
                GeneratedFunctions.geogpoint_make2d(WGS84, lon, lat), eventTime(epochMillis));
    }

    /** The {@code tgeompoint} instant at a planar point with no SRID at a time. */
    public static Pointer tgeompointInst(double x, double y, long epochMillis) {
        return GeneratedFunctions.tpointinst_make(
                GeneratedFunctions.geompoint_make2d(0, x, y), eventTime(epochMillis));
    }

    /**
     * The sequence of instants in increasing time with an interpolation; MEOS refuses the
     * sequence when two of them share a time.
     */
    public static Pointer sequence(List<Pointer> instants, TInterpolation interp) {
        Runtime runtime = Runtime.getSystemRuntime();
        Pointer array = Memory.allocate(runtime, Math.multiplyExact(instants.size(), Long.BYTES));
        for (int i = 0; i < instants.size(); i++) {
            array.putPointer((long) i * Long.BYTES, instants.get(i));
        }
        return GeneratedFunctions.tsequence_make(
                array, instants.size(), true, true, interp.getValue(), true);
    }
}
