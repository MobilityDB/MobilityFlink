package aisdata;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.configuration.Configuration; // Added import

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import functions.*;
import types.boxes.*;
import types.basic.tpoint.tgeom.*;
import types.basic.tpoint.tgeom.TGeomPointSeq;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

public class TrajectoryWindowFunction extends 
ProcessWindowFunction<Tuple4<Integer, Double, Double, Long>, TGeomPointSeq, Integer, TimeWindow> {

    private static final Logger logger = LoggerFactory.getLogger(TrajectoryWindowFunction.class);
    private transient error_handler_fn errorHandler; // Non-static and transient
    // private int count = 0; // count variable seems unused, can be removed if not needed

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        errorHandler = new error_handler(); // Initialize error handler here
        functions.meos_initialize_timezone("UTC");
        functions.meos_initialize_error_handler(errorHandler);
        logger.info("MEOS initialized in TrajectoryWindowFunction.open()");
    }

    // @Override
    // public void close() throws Exception {
    //     functions.meos_finalize();
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

        List<Tuple4<Integer, Double, Double, Long>> dedupedElements = new ArrayList<>();
        long lastTs = Long.MIN_VALUE;
        for (Tuple4<Integer, Double, Double, Long> tuple : sortedElements) {
            if (tuple.f3 != lastTs) {
                dedupedElements.add(tuple);
                lastTs = tuple.f3;
            }
        }
        sortedElements = dedupedElements;

        if (sortedElements.isEmpty()) return;

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
        OffsetDateTime dateTime = instant.atOffset(ZoneOffset.UTC);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssxxx");
        return dateTime.format(formatter);
    }
}