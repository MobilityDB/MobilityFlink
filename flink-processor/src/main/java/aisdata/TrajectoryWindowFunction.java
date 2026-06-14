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

package aisdata;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.configuration.Configuration; // Added import

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import functions.*;
import functions.GeneratedFunctions;
import types.boxes.*;
import types.basic.tpoint.tgeom.*;
import types.basic.tpoint.tgeom.TGeomPointSeq;

public class TrajectoryWindowFunction extends 
ProcessWindowFunction<Tuple4<Integer, Double, Double, Long>, TGeomPointSeq, Integer, TimeWindow> {

    private static final Logger logger = LoggerFactory.getLogger(TrajectoryWindowFunction.class);
    private transient error_handler_fn errorHandler; // Non-static and transient
    // private int count = 0; // count variable seems unused, can be removed if not needed

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        errorHandler = new error_handler(); // Initialize error handler here
        // JMEOS 1.4 split: no-arg meos_initialize() + separate tz + error-handler entry points
        GeneratedFunctions.meos_initialize();
        GeneratedFunctions.meos_initialize_timezone("UTC");
        logger.info("MEOS initialized in TrajectoryWindowFunction.open()");
    }

    // @Override
    // public void close() throws Exception {
    //     GeneratedFunctions.meos_finalize();
    //     logger.info("MEOS finalized in TrajectoryWindowFunction.close()");
    //     super.close();
    // }

    @Override
    public void process(Integer mmsiKey, Context context, 
                   Iterable<Tuple4<Integer, Double, Double, Long>> elements,
                   Collector<TGeomPointSeq> out) {

        List<Tuple4<Integer, Double, Double, Long>> sortedElements = new ArrayList<>();
        
        // First collect all elements from the iterable
        for (Tuple4<Integer, Double, Double, Long> tuple : elements) {
            sortedElements.add(tuple);
        }
        
        // Sort elements by timestamp (f3) to ensure increasing order for TGeomPointSeq
        sortedElements.sort((t1, t2) -> Long.compare(t1.f3, t2.f3));
        
        if (sortedElements.isEmpty()) {
            return; 
        }

        StringBuilder trajbuffer = new StringBuilder();
        boolean firstPoint = true;
        
        for (Tuple4<Integer, Double, Double, Long> tuple : sortedElements) {
            String t = convertMillisToTimestamp(tuple.f3);
            // Tuple: f0=mmsi, f1=lon, f2=lat, f3=timestamp
            String str_pointbuffer = String.format("POINT(%f %f)@%s", tuple.f1, tuple.f2, t);

            if (firstPoint) {
                trajbuffer.append("{" + str_pointbuffer);
                firstPoint = false;
            } else {
                trajbuffer.append("," + str_pointbuffer);
            }
        }
        trajbuffer.append("}");

        logger.info("MMSI={}, Window [{} - {}]: {} trajectory points", 
            mmsiKey,
            convertMillisToTimestamp(context.window().getStart()),
            convertMillisToTimestamp(context.window().getEnd()),
            sortedElements.size());
    
        logger.info("trajbuffer for MMSI {}: {}", mmsiKey, trajbuffer.toString());
        
        try {
            TGeomPointSeq trajectoryMEOS = new TGeomPointSeq(trajbuffer.toString());
            logger.info("trajectoryMEOS created for MMSI {} in window [{} - {}]", 
                mmsiKey,
                convertMillisToTimestamp(context.window().getStart()),
                convertMillisToTimestamp(context.window().getEnd()));
                
            out.collect(trajectoryMEOS);
        } catch (Exception e) {
            logger.error("Error creating or collecting TGeomPointSeq for MMSI {} with buffer [{}]: {}", 
                         mmsiKey, trajbuffer.toString(), e.getMessage(), e);
        }
    }

    private String convertMillisToTimestamp(long millis) {
        Instant instant = Instant.ofEpochMilli(millis);
        LocalDateTime dateTime = LocalDateTime.ofInstant(instant, ZoneId.of("UTC"));
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
        return dateTime.format(formatter);
    }
}