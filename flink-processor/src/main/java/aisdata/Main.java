package aisdata;

import java.time.Duration;
import java.util.Properties;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import functions.*;
import types.boxes.*;
import types.basic.tpoint.tgeom.*;
import types.basic.tpoint.TPoint.*;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);
    static error_handler_fn errorHandler = new error_handler();
    private STBox stbx; 

    public static void main(String[] args) throws Exception {
        // Log Java library path to help with native library loading issues
        String javaLibraryPath = System.getProperty("java.library.path");
        logger.info("Java library path: {}", javaLibraryPath);

        // Initialize MEOS with proper error handling
        try {
            logger.info("Initializing MEOS library");
            functions.meos_initialize_timezone("UTC");
            functions.meos_initialize_error_handler(errorHandler);
            
            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);

            //STBox stbx = new STBox("SRID=4326;STBOX XT(((3.3615, 53.964367),(16.505853, 59.24544)),[2011-01-03 00:00:00,2011-01-03 00:00:21])");

            Properties properties = new Properties();
            properties.setProperty("bootstrap.servers", "kafka:29092");
            properties.setProperty("group.id", "flink_consumer");
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty("auto.offset.reset", "earliest");

            KafkaSource<AISData> kafkaSource = KafkaSource.<AISData>builder()
                .setBootstrapServers("kafka:29092")
                .setGroupId("flink_consumer")
                .setTopics("aisdata")
                .setStartingOffsets(OffsetsInitializer.earliest())
                //.setValueOnlyDeserializer(new SimpleStringSchema())
                .setValueOnlyDeserializer(new AISDataDeserializationSchema())
                .build();

            DataStream<AISData> rawStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");

            // Print stream from kafka
            //rawStream.map(new LogKafkaMessagesMapFunction());

            DataStream<AISData> source = rawStream
                //.map(new DeserializeAISDataMapFunction())
                .assignTimestampsAndWatermarks(
                    WatermarkStrategy.<AISData>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner(new AISDataTimestampAssigner())
                        .withIdleness(Duration.ofMinutes(1))
                );

            
            DataStream<TGeomPointSeq> trajectories = source
                .map(new AISDataToTuple4MapFunction())
                .keyBy(tuple -> tuple.f0) 
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                //.allowedLateness(Time.seconds(0))
                .process(new TrajectoryWindowFunction());

            //trajectories.print();
                
            env.execute("Process AIS Trajectories");
            logger.info("Done");
        } catch (Exception e) {
            logger.error("Error during execution: {}", e.getMessage(), e);
            throw e;
        } finally {
            // Always ensure MEOS is finalized
            try {
                logger.info("Finalizing MEOS library");
                functions.meos_finalize();
            } catch (Exception e) {
                logger.error("Error during MEOS finalization: {}", e.getMessage(), e);
            }
        }
    }

    // Static nested classes to avoid serialization issues
    public static class LogKafkaMessagesMapFunction implements MapFunction<String, String> {
        private static final Logger logger = LoggerFactory.getLogger(LogKafkaMessagesMapFunction.class);

        @Override
        public String map(String value) throws Exception {
            logger.info("Received message from Kafka: {}", value);
            return value;
        }
    }

    public static class DeserializeAISDataMapFunction implements MapFunction<String, AISData> {
        private static final Logger logger = LoggerFactory.getLogger(DeserializeAISDataMapFunction.class);

        @Override
        public AISData map(String value) throws Exception {
            //logger.info("Deserializing message: {}", value);
            return new AISDataDeserializationSchema().deserialize(value.getBytes());
        }
    }

    public static class AISDataTimestampAssigner implements SerializableTimestampAssigner<AISData> {
        private static final Logger logger = LoggerFactory.getLogger(AISDataTimestampAssigner.class);

        @Override
        public long extractTimestamp(AISData element, long recordTimestamp) {
            long timestamp = element.getTimestamp();
            //logger.info("Extracted timestamp: {}", timestamp);
            return timestamp;
        }
    }

    // Modified to include mmsi in the tuple (now Tuple4 instead of Tuple3)
    public static class AISDataToTuple4MapFunction implements MapFunction<AISData, Tuple4<Integer, Double, Double, Long>> {
        private static final Logger logger = LoggerFactory.getLogger(AISDataToTuple4MapFunction.class);

        @Override
        public Tuple4<Integer, Double, Double, Long> map(AISData value) throws Exception {
            // logger.info("Mapping AISData to Tuple4: MMSI={}, Long={}, Latitude={}, Timestamp={}", 
            //    value.getMmsi(), value.getLon(), value.getLat(), value.getTimestamp());
            return new Tuple4<>(value.getMmsi(), value.getLon(), value.getLat(), value.getTimestamp());
        }
    }



}