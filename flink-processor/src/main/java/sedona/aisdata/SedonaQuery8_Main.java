package sedona.aisdata;

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
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.locationtech.jts.geom.CoordinateXYM;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.WKTWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import aisdata.AISData;
import aisdata.AISDataDeserializationSchema;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * SedonaQuery8_Main (AIS): Trajectory Denoising with Extended Kalman Filter
 *
 * MEOS (Query8_Main.java):
 *   - Line 2: TumblingEventTimeWindows(10s)
 *   - Line 3: EKF computed in Java (Apache Commons Math)
 *
 * Sedona/JTS:
 *   - Line 2: identical
 *   - Line 3: identical
 *
 * Why no Table API:
 *   Q8 aggregates N records per window to build a trajectory, same as Q3.
 */
public class SedonaQuery8_Main {

    private static final Logger logger = LoggerFactory.getLogger(SedonaQuery8_Main.class);

    private static final double  GATE          = 3.0;
    private static final double  Q             = 0.01;
    private static final double  VARIANCE      = 1.0;
    private static final boolean DROP_OUTLIERS = false;

    // KeySelector: extract mmsi from AISData for keyBy
    static class MmsiKeySelector implements KeySelector<AISData, Integer>, Serializable {
        @Override
        public Integer getKey(AISData e) { return e.getMmsi(); }
    }

    // EKF computation is pure Java (Apache Commons Math)
    // Coordinate system:
    //   EKF operates in ENU (East-North-Up) meters relative to the first point.
    //   latLonToEnu / enuToLatLon convert between lon/lat degrees and ENU meters.
    static class EKFWindowFunction
            extends ProcessWindowFunction<AISData, String, Integer, TimeWindow>
            implements Serializable {

        private static final Logger log = LoggerFactory.getLogger(EKFWindowFunction.class);
        private static final DateTimeFormatter FMT =
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssxxx");
        private static final double EARTH_RADIUS = 6_371_000.0;
        private static final GeometryFactory FACTORY = new GeometryFactory();

        private final double gate;
        private final double q;
        private final double variance;
        private final boolean dropOutliers;

        public EKFWindowFunction(double gate, double q, double variance, boolean dropOutliers) {
            this.gate         = gate;
            this.q            = q;
            this.variance     = variance;
            this.dropOutliers = dropOutliers;
        }

        @Override
        public void process(Integer mmsi, Context context,
                            Iterable<AISData> elements, Collector<String> out) {

            String wStart = millisToTs(context.window().getStart());
            String wEnd   = millisToTs(context.window().getEnd());

            // collect and sort by timestamp
            List<AISData> sorted = new ArrayList<>();
            for (AISData e : elements) sorted.add(e);
            if (sorted.size() < 2) return;
            sorted.sort((a, b) -> Long.compare(a.getTimestamp(), b.getTimestamp()));

            // convert lon/lat to ENU meters (origin = first point)
            double originLat = sorted.get(0).getLat();
            double originLon = sorted.get(0).getLon();

            double[][] rawEnu = new double[sorted.size()][2];
            for (int i = 0; i < sorted.size(); i++) {
                rawEnu[i] = latLonToEnu(
                        sorted.get(i).getLat(), sorted.get(i).getLon(),
                        originLat, originLon);
            }

            // apply EKF
            double[][] smoothedEnu = applyEkf(rawEnu, sorted);

            // build JTS LINESTRING M for raw and smoothed trajectories
            // CoordinateXYM(lon, lat, epoch_ms) = equivalent of "POINT(lon lat)@ts" in MEOS
            CoordinateXYM[] rawCoords      = new CoordinateXYM[sorted.size()];
            CoordinateXYM[] smoothedCoords = new CoordinateXYM[sorted.size()];

            for (int i = 0; i < sorted.size(); i++) {
                long ts = sorted.get(i).getTimestamp();
                rawCoords[i] = new CoordinateXYM(
                        sorted.get(i).getLon(), sorted.get(i).getLat(), ts);
                // enuToLatLon returns [lat, lon], swap to (lon, lat) for CoordinateXYM
                double[] sll = enuToLatLon(smoothedEnu[i][0], smoothedEnu[i][1], originLat, originLon);
                smoothedCoords[i] = new CoordinateXYM(sll[1], sll[0], ts);
            }

            WKTWriter writer = new WKTWriter(3);
            String rawWkt      = buildGeometry(writer, rawCoords);
            String smoothedWkt = buildGeometry(writer, smoothedCoords);

            String result = String.format(
                    "[EKF][Q8-Sedona-AIS] MMSI=%-12d | points=%2d"
                            + " | gate=%.1f q=%.3f var=%.1f"
                            + " | window [%s - %s]%n"
                            + "    raw:      %s%n"
                            + "    smoothed: %s",
                    mmsi, sorted.size(),
                    gate, q, variance,
                    wStart, wEnd,
                    rawWkt, smoothedWkt);

            log.info(result);
            out.collect(result);
        }

        private double[][] applyEkf(double[][] rawEnu, List<AISData> events) {
            int n = rawEnu.length;
            double[][] smoothed = new double[n][2];

            RealMatrix H  = new Array2DRowRealMatrix(new double[][]{{1,0,0,0},{0,1,0,0}});
            RealMatrix R  = new Array2DRowRealMatrix(new double[][]{{variance,0},{0,variance}});
            RealMatrix Q4 = new Array2DRowRealMatrix(new double[][]{
                    {q,0,0,0},{0,q,0,0},{0,0,q,0},{0,0,0,q}});

            RealVector x = new ArrayRealVector(
                    new double[]{rawEnu[0][0], rawEnu[0][1], 0.0, 0.0});
            RealMatrix P = new Array2DRowRealMatrix(new double[][]{
                    {variance,0,0,0},{0,variance,0,0},{0,0,variance,0},{0,0,0,variance}});

            smoothed[0][0] = x.getEntry(0);
            smoothed[0][1] = x.getEntry(1);

            for (int i = 1; i < n; i++) {
                double dt = (events.get(i).getTimestamp() - events.get(i-1).getTimestamp()) / 1000.0;
                if (dt <= 0) dt = 1.0;

                // A: state transition matrix (4x4), constant-velocity model.
                // Rebuilt each step because dt varies (irregular AIS stream).
                RealMatrix A = new Array2DRowRealMatrix(new double[][]{
                        {1,0,dt,0},{0,1,0,dt},{0,0,1,0},{0,0,0,1}});

                ProcessModel     pm = new DefaultProcessModel(A, null, Q4, x, P);
                MeasurementModel mm = new DefaultMeasurementModel(H, R);
                KalmanFilter     kf = new KalmanFilter(pm, mm);

                kf.predict();

                double[] zArr  = {rawEnu[i][0], rawEnu[i][1]};
                double[] xPred = kf.getStateEstimation();
                double[] innov = {zArr[0] - xPred[0], zArr[1] - xPred[1]};

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
            double a = M.getEntry(0,0), b = M.getEntry(0,1);
            double c = M.getEntry(1,0), d = M.getEntry(1,1);
            double det = a*d - b*c;
            if (Math.abs(det) < 1e-12) det = 1e-12;
            return new Array2DRowRealMatrix(
                    new double[][]{{d/det,-b/det},{-c/det,a/det}});
        }

        // ENU conversion helpers
        private double[] latLonToEnu(double lat, double lon, double originLat, double originLon) {
            double dLat   = Math.toRadians(lat - originLat);
            double dLon   = Math.toRadians(lon - originLon);
            double latRad = Math.toRadians(originLat);
            return new double[]{
                    dLon * EARTH_RADIUS * Math.cos(latRad),
                    dLat * EARTH_RADIUS};
        }

        // Returns [lat, lon]
        private double[] enuToLatLon(double east, double north, double originLat, double originLon) {
            double latRad = Math.toRadians(originLat);
            return new double[]{
                    originLat + Math.toDegrees(north / EARTH_RADIUS),
                    originLon + Math.toDegrees(east  / (EARTH_RADIUS * Math.cos(latRad)))};
        }

        // Build a JTS LINESTRING M from CoordinateXYM array.
        // Falls back to a Point if only 1 coordinate (JTS requires >= 2 for LineString).
        private String buildGeometry(WKTWriter writer, CoordinateXYM[] coords) {
            if (coords.length == 1) return writer.write(FACTORY.createPoint(coords[0]));
            LineString ls = FACTORY.createLineString(coords);
            return writer.write(ls);
        }

        private String millisToTs(long ms) {
            return Instant.ofEpochMilli(ms).atOffset(ZoneOffset.UTC).format(FMT);
        }
    }

    // =========================================================================
    // MAIN
    // =========================================================================
    public static void main(String[] args) throws Exception {
        logger.info("=== SedonaQuery8_Main (AIS), starting (no MEOS) ===");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().disableClosureCleaner();

        KafkaSource<AISData> kafkaSource = KafkaSource.<AISData>builder()
                .setBootstrapServers("kafka:29092")
                .setGroupId("flink_consumer_sedona_ais_q8")
                .setTopics("aisdata")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new AISDataDeserializationSchema())
                .build();

        WatermarkStrategy<AISData> watermarkStrategy =
                WatermarkStrategy.<AISData>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner((event, ts) -> event.getTimestamp())
                        .withIdleness(Duration.ofMinutes(1));

        DataStream<AISData> source = env.fromSource(
                kafkaSource, watermarkStrategy, "Kafka AIS Source");

        source
                .keyBy(new MmsiKeySelector())
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new EKFWindowFunction(GATE, Q, VARIANCE, DROP_OUTLIERS))
                .print();

        logger.info("Job running...");
        env.execute("SedonaQuery8-AIS, Trajectory Denoising with EKF (no MEOS)");
    }
}