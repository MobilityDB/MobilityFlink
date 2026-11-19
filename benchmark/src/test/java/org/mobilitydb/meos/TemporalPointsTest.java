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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import functions.GeneratedFunctions;
import java.time.OffsetDateTime;
import java.util.List;
import jnr.ffi.Pointer;
import org.junit.jupiter.api.Test;
import types.temporal.TInterpolation;

/**
 * Checks that the temporal points of the benchmark events keep the time of their event, to
 * the millisecond, and the SRID the text input gave them. It needs libmeos on the library
 * path.
 */
class TemporalPointsTest {

    /** 2024-08-01 13:52:28.123 UTC, an event of the SNCB stream with its milliseconds. */
    private static final long EVENT_MS = 1_722_520_348_123L;

    @Test
    void anInstantKeepsTheMillisecondOfItsEvent() {
        Pointer inst = TemporalPoints.tgeogpointInst(4.3658, 50.6456, EVENT_MS);
        assertNotNull(inst);
        assertEquals(OffsetDateTime.parse("2024-08-01T13:52:28.123Z"),
                GeneratedFunctions.temporal_start_timestamptz(inst));
        assertEquals(4326, GeneratedFunctions.tspatial_srid(inst));
    }

    @Test
    void aGeometryInstantHasNoSrid() {
        Pointer inst = TemporalPoints.tgeompointInst(4.3658, 50.6456, EVENT_MS);
        assertNotNull(inst);
        assertEquals(TemporalPoints.eventTime(EVENT_MS),
                GeneratedFunctions.temporal_start_timestamptz(inst));
        assertEquals(0, GeneratedFunctions.tspatial_srid(inst));
    }

    /** Two events within one second are two instants of the sequence, not one time twice. */
    @Test
    void eventsWithinOneSecondMakeASequence() {
        Pointer seq = TemporalPoints.sequence(List.of(
                TemporalPoints.tgeogpointInst(4.3658, 50.6456, EVENT_MS),
                TemporalPoints.tgeogpointInst(4.3659, 50.6457, EVENT_MS + 500)),
                TInterpolation.DISCRETE);
        assertNotNull(seq);
        assertEquals(2, GeneratedFunctions.temporal_num_instants(seq));
        assertEquals(OffsetDateTime.parse("2024-08-01T13:52:28.623Z"),
                GeneratedFunctions.temporal_end_timestamptz(seq));
    }
}
