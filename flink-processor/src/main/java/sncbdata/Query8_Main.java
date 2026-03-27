package sncbdata;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.math3.filter.DefaultMeasurementModel;
import org.apache.commons.math3.filter.DefaultProcessModel;
import org.apache.commons.math3.filter.KalmanFilter;
import org.apache.commons.math3.filter.MeasurementModel;
import org.apache.commons.math3.filter.ProcessModel;
import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jnr.ffi.Pointer;
import functions.functions;
import functions.error_handler;

/**
 * Query 8 - Trajectory Denoising with Extended Kalman Filter (SNCB dataset)
 *
 * <p>Smooths raw GPS train trajectories using an Extended Kalman Filter
 * with a constant-velocity motion model.
 *
 * <p>Original MobilityNebula pseudocode:
 * <pre>
 *   Query::from(GPS)
 *     .window(TumblingWindow::of(EventTime(ts), Seconds(10)))          // Line 2
 *     .apply(temporal_ext_kalman_filter(                               // Line 3
 *         temporal_sequence(lon, lat, ts),
 *         gate=3.0, q=0.01, variance=1.0, dropOutliers=false));
 * </pre>
 *
 * <p><b>HOW TO RUN:</b> change Dockerfile entrypoint to {@code sncbdata.Query8_Main},
 * then {@code mvn clean package && docker compose up --build}.
 */
public class Query8_Main {

    private static final Logger logger = LoggerFactory.getLogger(Query8_Main.class);

    // EKF parameters
    private static final double  GATE          = 3.0;
    private static final double  Q             = 0.01;
    private static final double  VARIANCE      = 1.0;
    private static final boolean DROP_OUTLIERS = false;

    public static void main(String[] args) throws Exception {

        logger.info("Java library path: {}", System.getProperty("java.library.path"));

        try {
            functions.meos_initialize_timezone("UTC");
            functions.meos_initialize_error_handler(new error_handler());

            final StreamExecutionEnvironment env =
                    StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);

            KafkaSource<SNCBData> kafkaSource = KafkaSource.<SNCBData>builder()
                    .setBootstrapServers("kafka:29092")
                    .setGroupId("flink_consumer_q8")
                    .setTopics("sncbdata")
                    .setStartingOffsets(OffsetsInitializer.earliest())
                    .setValueOnlyDeserializer(new SNCBDataDeserializationSchema())
                    .build();

            WatermarkStrategy<SNCBData> watermarkStrategy =
                    WatermarkStrategy.<SNCBData>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                            .withTimestampAssigner((event, recordTs) -> event.getTimestamp())
                            .withIdleness(Duration.ofMinutes(1));

            DataStream<SNCBData> source = env.fromSource(kafkaSource, watermarkStrategy, "Kafka Source");

            source
                    .keyBy(SNCBData::getDeviceId)
                    .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                    .process(new EKFWindowFunction(GATE, Q, VARIANCE, DROP_OUTLIERS))
                    .print();

            env.execute("Query 8 - Trajectory Denoising with EKF (SNCB)");

        } catch (Exception e) {
            logger.error("Error during execution: {}", e.getMessage(), e);
            throw e;
        } finally {
            try { functions.meos_finalize(); }
            catch (Exception e) { logger.error("Error during MEOS finalization: {}", e.getMessage(), e); }
        }
    }

    public static class EKFWindowFunction
            extends ProcessWindowFunction<SNCBData, String, Integer, TimeWindow> {

        private static final Logger log = LoggerFactory.getLogger(EKFWindowFunction.class);
        private static final DateTimeFormatter TIMESTAMP_FMT =
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssxxx");
        private static final double EARTH_RADIUS = 6_371_000.0;

        private final double gate;
        private final double q;
        private final double variance;
        private final boolean dropOutliers;

        public EKFWindowFunction(double gate, double q, double variance, boolean dropOutliers) {
            this.gate = gate;
            this.q = q;
            this.variance = variance;
            this.dropOutliers = dropOutliers;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            functions.meos_initialize_timezone("UTC");
            functions.meos_initialize_error_handler(new error_handler());
        }

        @Override
        public void process(Integer deviceId, Context context,
                Iterable<SNCBData> elements, Collector<String> out) {

            String windowStart = millisToTimestamp(context.window().getStart());
            String windowEnd   = millisToTimestamp(context.window().getEnd());

            List<SNCBData> sorted = new ArrayList<>();
            for (SNCBData e : elements) sorted.add(e);
            if (sorted.size() < 2) return;
            sorted.sort((a, b) -> Long.compare(a.getTimestamp(), b.getTimestamp()));

            double originLat = sorted.get(0).getLat();
            double originLon = sorted.get(0).getLon();

            double[][] rawEnu = new double[sorted.size()][2];
            for (int i = 0; i < sorted.size(); i++)
                rawEnu[i] = latLonToEnu(sorted.get(i).getLat(), sorted.get(i).getLon(), originLat, originLon);

            double[][] smoothedEnu = applyEkf(rawEnu, sorted, q, variance, gate, dropOutliers);

            StringBuilder rawSeq      = new StringBuilder("{");
            StringBuilder smoothedSeq = new StringBuilder("{");

            for (int i = 0; i < sorted.size(); i++) {
                String ts = millisToTimestamp(sorted.get(i).getTimestamp());
                if (i > 0) { rawSeq.append(","); smoothedSeq.append(","); }
                rawSeq.append(String.format("POINT(%f %f)@%s",
                        sorted.get(i).getLon(), sorted.get(i).getLat(), ts));
                double[] sll = enuToLatLon(smoothedEnu[i][0], smoothedEnu[i][1], originLat, originLon);
                smoothedSeq.append(String.format("POINT(%f %f)@%s", sll[1], sll[0], ts));
            }
            rawSeq.append("}");
            smoothedSeq.append("}");

            Pointer rawTraj      = functions.tgeogpoint_in(rawSeq.toString());
            Pointer smoothedTraj = functions.tgeogpoint_in(smoothedSeq.toString());
            if (rawTraj == null || smoothedTraj == null) return;

            String result = String.format(
                    "[EKF][Q8] DeviceID=%-6d | points=%2d | gate=%.1f q=%.3f var=%.1f"
                            + " | window [%s - %s]%n"
                            + "         raw:      %s%n"
                            + "         smoothed: %s",
                    deviceId, sorted.size(), gate, q, variance, windowStart, windowEnd,
                    functions.tspatial_as_ewkt(rawTraj, 6),
                    functions.tspatial_as_ewkt(smoothedTraj, 6));

            log.info(result);
            out.collect(result);
        }

        private double[][] applyEkf(double[][] rawEnu, List<SNCBData> events,
                double q, double variance, double gate, boolean dropOutliers) {

            int n = rawEnu.length;
            double[][] smoothed = new double[n][2];

            RealMatrix H  = new Array2DRowRealMatrix(new double[][]{{1,0,0,0},{0,1,0,0}});
            RealMatrix R  = new Array2DRowRealMatrix(new double[][]{{variance,0},{0,variance}});
            RealMatrix Q4 = new Array2DRowRealMatrix(new double[][]{{q,0,0,0},{0,q,0,0},{0,0,q,0},{0,0,0,q}});

            RealVector x = new ArrayRealVector(new double[]{rawEnu[0][0], rawEnu[0][1], 0.0, 0.0});
            RealMatrix P = new Array2DRowRealMatrix(new double[][]{
                    {variance,0,0,0},{0,variance,0,0},{0,0,variance,0},{0,0,0,variance}});

            smoothed[0][0] = x.getEntry(0);
            smoothed[0][1] = x.getEntry(1);

            for (int i = 1; i < n; i++) {
                double dt = (events.get(i).getTimestamp() - events.get(i-1).getTimestamp()) / 1000.0;
                if (dt <= 0) dt = 1.0;

                // A: state transition matrix (4×4) — constant-velocity model.
                // Rebuilt each step because dt varies (irregular AIS/SNCB stream).
                RealMatrix A = new Array2DRowRealMatrix(new double[][]{
                        {1,0,dt,0},{0,1,0,dt},{0,0,1,0},{0,0,0,1}});

                ProcessModel     pm = new DefaultProcessModel(A, null, Q4, x, P);
                MeasurementModel mm = new DefaultMeasurementModel(H, R);
                KalmanFilter     kf = new KalmanFilter(pm, mm);

                kf.predict();

                double[] zArr  = {rawEnu[i][0], rawEnu[i][1]};
                double[] xPred = kf.getStateEstimation();
                double[] innov = {zArr[0]-xPred[0], zArr[1]-xPred[1]};

                RealMatrix PPred = new Array2DRowRealMatrix(kf.getErrorCovariance());
                RealMatrix S     = H.multiply(PPred).multiply(H.transpose()).add(R);
                RealMatrix Sinv  = invertSymmetric2x2(S);
                RealVector yVec  = new ArrayRealVector(innov);
                double mahal     = Math.sqrt(Sinv.preMultiply(yVec).dotProduct(yVec));

                if (mahal > gate && dropOutliers) {
                    smoothed[i][0] = xPred[0];
                    smoothed[i][1] = xPred[1];
                } else {
                    kf.correct(zArr);
                    double[] xCorr = kf.getStateEstimation();
                    smoothed[i][0] = xCorr[0];
                    smoothed[i][1] = xCorr[1];
                }
                x = new ArrayRealVector(kf.getStateEstimation());
                P = new Array2DRowRealMatrix(kf.getErrorCovariance());
            }
            return smoothed;
        }

        private RealMatrix invertSymmetric2x2(RealMatrix M) {
            double a=M.getEntry(0,0), b=M.getEntry(0,1), c=M.getEntry(1,0), d=M.getEntry(1,1);
            double det = a*d - b*c;
            if (Math.abs(det) < 1e-12) det = 1e-12;
            return new Array2DRowRealMatrix(new double[][]{{d/det,-b/det},{-c/det,a/det}});
        }

        private double[] latLonToEnu(double lat, double lon, double originLat, double originLon) {
            double dLat = Math.toRadians(lat - originLat);
            double dLon = Math.toRadians(lon - originLon);
            double latRad = Math.toRadians(originLat);
            return new double[]{dLon * EARTH_RADIUS * Math.cos(latRad), dLat * EARTH_RADIUS};
        }

        private double[] enuToLatLon(double east, double north, double originLat, double originLon) {
            double latRad = Math.toRadians(originLat);
            return new double[]{
                    originLat + Math.toDegrees(north / EARTH_RADIUS),
                    originLon + Math.toDegrees(east / (EARTH_RADIUS * Math.cos(latRad)))};
        }

        private String millisToTimestamp(long millis) {
            return Instant.ofEpochMilli(millis).atOffset(ZoneOffset.UTC).format(TIMESTAMP_FMT);
        }
    }
}
