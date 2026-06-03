package aisdata;

import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

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
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jnr.ffi.Pointer;
import functions.functions;
import functions.error_handler;

/**
 * Query 8 - Trajectory "Denoising" with Extended Kalman Filter (AIS dataset)
 *
 * <h2>Problem statement</h2>
 * <p>A GPS receiver never returns the exact position of a vessel. Every measurement contains
 * a random error of several metres caused by disturbances and internal clock imprecision.
 * The result is a "zigzag" trajectory around the true path, even for a ship sailing in a straight line.
 * This is <b>GPS noise</b>.
 *
 * <p>This query implements the MobilityNebula Query 8: it smooths raw GPS
 * trajectories using a <b>Kalman Filter</b> over 10-second tumbling event-time windows.
 *
 * <h2>Why convert to ENU before filtering?</h2>
 * <p>The Kalman Filter performs arithmetic operations (additions, distances, matrix products).
 * These operations require a <b>uniform metric space</b>. Latitude/longitude degrees are
 * <em>not</em> a uniform unit of distance:
 * <ul>
 *   <li>1° of longitude ≈ 111 km at the equator</li>
 *   <li>1° of longitude ≈ 70 km at Brussels (51°N)</li>
 *   <li>1° of longitude = 0 km at the North Pole</li>
 * </ul>
 * <p>A solution I found is <b>ENU (East-North-Up)</b>: the first GPS point
 * of each window is chosen as origin (0, 0). All subsequent positions are expressed in
 * <em>metres</em> relative to that origin (east axis, north axis). This frame is flat.
 * After filtering, ENU positions are converted back to WGS-84 degrees for MEOS output.
 *
 * <pre>
 *   GPS (lat/lon degrees)
 *       └─ latLonToEnu() ──→ ENU (metres) ──→ Kalman Filter ──→ ENU smoothed
 *                                                                    └─ enuToLatLon() ──→ lat/lon smoothed
 * </pre>
 *
 * <h2>Original MobilityNebula pseudocode</h2>
 * <pre>
 *   Query::from(GPS)
 *     .window(TumblingWindow::of(EventTime(ts), Seconds(10)))              // Line 2
 *     .apply(temporal_ext_kalman_filter(                                   // Line 3
 *         temporal_sequence(lon, lat, ts),
 *         gate=3.0, q=0.01, variance=1.0, dropOutliers=false));
 * </pre>
 *
 * <h2>MEOS availability note</h2>
 * <p>{@code temporal_ext_kalman_filter} is described in the article but is not yet exposed
 * in MEOS nor in the public JMEOS bindings. It is implemented here in pure Java using the standard
 * Kalman Filter formulation from Apache Commons Math3.
 *
 * <h2>EKF parameters</h2>
 * <ul>
 *   <li><b>gate = 3.0</b>: Mahalanobis distance threshold. A measurement whose normalised
 *       innovation exceeds 3 standard deviations is flagged as a potential GPS outlier.
 *       The 3-sigma rule means only ~0.3% of normal measurements are incorrectly flagged.</li>
 *   <li><b>q = 0.01</b>: Process noise variance. Controls how much the filter trusts the
 *       constant-velocity motion model. Small Q → strong trust in the model → strong
 *       smoothing. Large Q → less trust → trajectory stays closer to raw GPS.</li>
 *   <li><b>variance = 1.0</b>: Measurement noise variance R (m²). Encodes GPS accuracy.
 *       1.0 m² ≈ ±1 m standard deviation, typical for commercial maritime GPS.</li>
 *   <li><b>dropOutliers = false</b>: Even outlier measurements are included in the output.
 *       The filter attenuates them rather than removing them.</li>
 * </ul>
 *
 * <h2>How to run</h2>
 * <p>In the Dockerfile, change the entrypoint to {@code aisdata.Query8_Main}, then:
 * <pre>
 *   mvn clean package &amp;&amp; docker compose up --build
 * </pre>
 *
 * <h2>Troubleshooting</h2>
 * <ul>
 *   <li>MobilityDB {@code stable-1.3} or later may be required:
 *       {@code https://github.com/MobilityDB/MobilityDB.git -b stable-1.3}</li>
 *   <li>Use a JMEOS version compatible with your MobilityDB version:
 *       {@code --branch fix-tests-using-docker https://github.com/MobilityDB/JMEOS}</li>
 * </ul>
 */
public class Query8_Main {

    private static final Logger logger = LoggerFactory.getLogger(Query8_Main.class);

    // =========================================================================
    // EKF parameters
    // =========================================================================

    /**
     * Mahalanobis distance gate (σ threshold).
     *
     * <p>After each predict/correct cycle, the Mahalanobis distance of the innovation is
     * compared to this value. A distance > 3.0 means the GPS measurement is more than
     * 3 standard deviations away from what the model predicted: statistically very
     * unlikely for normal GPS noise, so the point is classified as an outlier.
     *
     * <p>With {@link #DROP_OUTLIERS} = false, outliers are corrected by the filter but
     * not discarded.
     */
    private static final double GATE = 3.0;

    /**
     * Process noise variance Q.
     *
     * <p>Q models how imperfect the model is.
     *
     * <p>Small Q (0.01) → the filter strongly trusts the motion model → strong smoothing.
     * Large Q → the filter defers more to GPS measurements → weak smoothing.
     */
    private static final double Q = 0.01;

    /**
     * Measurement noise variance R (metres²).
     *
     * <p>R appears on the diagonal of the 2×2 measurement noise covariance matrix:
     * It encodes how inaccurate the GPS sensor is.
     *
     * <p>Larger R → the filter trusts GPS less → more weight given to the motion model.
     * Smaller R → the filter trusts GPS more → smoothed trajectory stays close to raw GPS.
     */
    private static final double VARIANCE = 1.0;

    /**
     * Whether to discard outlier measurements.
     *
     * <p>When {@code false}: outlier measurements (Mahalanobis > gate)
     * are still used to correct the state. They are attenuated by the filter's gain K
     * but not ignored. This avoids masking genuine velocity changes.
     *
     * <p>When {@code true}: the state is kept at the predicted value for outlier steps,
     * effectively ignoring the GPS measurement entirely for that instant.
     */
    private static final boolean DROP_OUTLIERS = false;


    public static void main(String[] args) throws Exception {

        logger.info("Java library path: {}", System.getProperty("java.library.path"));

        try {
            logger.info("Initializing MEOS library");
            functions.meos_initialize_timezone("UTC");
            functions.meos_initialize_error_handler(new error_handler());

            final StreamExecutionEnvironment env =
                    StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);

            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class.getName());
            properties.setProperty("auto.offset.reset", "earliest");

            KafkaSource<AISData> kafkaSource = KafkaSource.<AISData>builder()
                    .setBootstrapServers("kafka:29092")
                    .setGroupId("flink_consumer_q8")
                    .setTopics("aisdata")
                    .setStartingOffsets(OffsetsInitializer.earliest())
                    .setValueOnlyDeserializer(new AISDataDeserializationSchema())
                    .build();

            DataStream<AISData> source = env
                    .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")
                    .assignTimestampsAndWatermarks(
                            WatermarkStrategy.<AISData>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                    .withTimestampAssigner((event, recordTs) -> event.getTimestamp())
                                    .withIdleness(Duration.ofMinutes(1)));

            // Flink pipeline implementing MobilityNebula Query 8:
            //
            //   keyBy(mmsi)              → one independent Kalman Filter per vessel.
            //                              Each vessel's GPS stream is smoothed separately.
            //   window(Tumbling 10s)     → collect 10 seconds of events.
            //   process(EKFWindowFn)     → 1. temporal_sequence : sort events by timestamp.
            //                              2. temporal_ext_kalman_filter : run EKF.
            //                              3. Emit raw + smoothed trajectories as EWKT.
            source
                    .keyBy(AISData::getMmsi)
                    .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                    .process(new EKFWindowFunction(GATE, Q, VARIANCE, DROP_OUTLIERS))
                    .print();

            env.execute("Query 8 - Trajectory Denoising with Extended Kalman Filter");
            logger.info("Done");

        } catch (Exception e) {
            logger.error("Error during execution: {}", e.getMessage(), e);
            throw e;
        } finally {
            try {
                functions.meos_finalize();
            } catch (Exception e) {
                logger.error("Error during MEOS finalization: {}", e.getMessage(), e);
            }
        }
    }

    // =========================================================================
    // EKF window function
    // =========================================================================

    /**
     * Flink {@link ProcessWindowFunction} for each (MMSI, window) pair.
     *
     * <h2>Core idea of what the Kalman Filter does</h2>
     * <p>The filter maintains two complementary sources of information:
     * <ol>
     *   <li><b>The motion model</b>: "I know how a vessel moves (constant velocity over dt
     *       seconds) and I can predict where it should be now."</li>
     *   <li><b>The GPS measurement</b>: "The sensor says the vessel is here."</li>
     * </ol>
     *
     * <h2>State vector: what the filter "knows"</h2>
     * <p>The filter tracks a 4-dimensional state at every instant:
     * <pre>
     *   x = [ east (m),  north (m),  vx (m/s),  vy (m/s) ]
     *          ↑           ↑           ↑            ↑
     *       position E  position N   speed E      speed N
     * </pre>
     * <p>Velocity is included because it enables the model to <em>predict</em> the next
     * position.
     *
     * <h2>EKF state model</h2>
     * <pre>
     *   State vector:  x = [east (m), north (m), vx (m/s), vy (m/s)]
     *
     *   State transition A for time step dt (seconds):
     *     A = | 1  0  dt   0 |    east_new   = east   + vx * dt
     *         | 0  1   0  dt |    north_new  = north  + vy * dt
     *         | 0  0   1   0 |    vx_new     = vx               (constant velocity)
     *         | 0  0   0   1 |    vy_new     = vy               (constant velocity)
     *
     *   Observation matrix H (GPS observes only position, not velocity):
     *     H = | 1  0  0  0 |    → extracts [east, north] from x
     *         | 0  1  0  0 |
     * </pre>
     */
    public static class EKFWindowFunction
            extends ProcessWindowFunction<AISData, String, Integer, TimeWindow> {

        private static final Logger log = LoggerFactory.getLogger(EKFWindowFunction.class);

        private final double gate;
        private final double q;
        private final double variance;
        private final boolean dropOutliers;

        private static final DateTimeFormatter TIMESTAMP_FMT =
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssxxx");

        /**
         * Earth radius used for the flat-earth ENU projection (metres).
         * Valid approximation for distances up to a few kilometres.
         */
        private static final double EARTH_RADIUS = 6_371_000.0;

        public EKFWindowFunction(double gate, double q, double variance, boolean dropOutliers) {
            this.gate         = gate;
            this.q            = q;
            this.variance     = variance;
            this.dropOutliers = dropOutliers;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            functions.meos_initialize_timezone("UTC");
            functions.meos_initialize_error_handler(new error_handler());
        }

        /**
         * Called once per (MMSI, 10-second window) pair.
         *
         * <ol>
         *   <li>{@code temporal_sequence}: collect and sort events by timestamp.</li>
         *   <li>{@code temporal_ext_kalman_filter}: project to ENU, run EKF, reproject.</li>
         *   <li>Build raw and smoothed {@code tgeogpoint} sequences via MEOS and emit EWKT.</li>
         * </ol>
         */
        @Override
        public void process(
                Integer mmsi,
                Context context,
                Iterable<AISData> elements,
                Collector<String> out) {

            String windowStart = millisToTimestamp(context.window().getStart());
            String windowEnd   = millisToTimestamp(context.window().getEnd());

            // Step 1: temporal_sequence
            // Collect all AIS events in this window and sort them chronologically.
            List<AISData> sorted = new ArrayList<>();
            for (AISData e : elements) sorted.add(e);
            if (sorted.size() < 2) return;   // It would be useless to smooth a trajectory of only one single point.
            sorted.sort((a, b) -> Long.compare(a.getTimestamp(), b.getTimestamp()));

            // ENU origin: the first GPS point in the window
            // All positions in this window will be expressed in metres relative to
            // this origin. At the end, results are converted back to lat/lon degrees.
            double originLat = sorted.get(0).getLat();
            double originLon = sorted.get(0).getLon();

            // Convert all GPS points from degrees to ENU metres
            double[][] rawEnu = new double[sorted.size()][2];
            for (int i = 0; i < sorted.size(); i++) {
                rawEnu[i] = latLonToEnu(sorted.get(i).getLat(), sorted.get(i).getLon(),
                        originLat, originLon);
            }

            // Step 2: temporal_ext_kalman_filter
            // Run the Kalman Filter over the ENU positions. Returns the same number
            // of positions but smoothed: GPS noise is attenuated, the trajectory is
            // more continuous and physically consistent with the motion model.
            double[][] smoothedEnu = applyEkf(rawEnu, sorted, q, variance, gate, dropOutliers);

            // Step 3: rebuild tgeogpoint sequences
            // Build two MEOS temporal geometry sequences:
            //   - rawSeq:      the original unfiltered GPS trajectory.
            //   - smoothedSeq: the Kalman-filtered trajectory.
            // Format: {POINT(lon lat)@timestamp, POINT(lon lat)@timestamp, ...}
            StringBuilder rawSeq      = new StringBuilder("{");
            StringBuilder smoothedSeq = new StringBuilder("{");

            for (int i = 0; i < sorted.size(); i++) {
                String ts = millisToTimestamp(sorted.get(i).getTimestamp());
                if (i > 0) { rawSeq.append(","); smoothedSeq.append(","); }

                // Raw point: original GPS coordinates (lon, lat order for MEOS WKT).
                rawSeq.append(String.format("POINT(%f %f)@%s",
                        sorted.get(i).getLon(), sorted.get(i).getLat(), ts));

                // Smoothed point: convert ENU result back to WGS-84 degrees.
                // enuToLatLon returns [lat, lon]; MEOS WKT requires (lon lat).
                double[] smoothedLatLon = enuToLatLon(
                        smoothedEnu[i][0], smoothedEnu[i][1], originLat, originLon);
                smoothedSeq.append(String.format("POINT(%f %f)@%s",
                        smoothedLatLon[1], smoothedLatLon[0], ts));
            }
            rawSeq.append("}");
            smoothedSeq.append("}");

            Pointer rawTraj      = functions.tgeogpoint_in(rawSeq.toString());
            Pointer smoothedTraj = functions.tgeogpoint_in(smoothedSeq.toString());
            if (rawTraj == null || smoothedTraj == null) return;

            String rawEwkt      = functions.tspatial_as_ewkt(rawTraj,      6);
            String smoothedEwkt = functions.tspatial_as_ewkt(smoothedTraj, 6);

            String result = String.format(
                    "[EKF][Q8] MMSI=%-12d | points=%2d | gate=%.1f q=%.3f var=%.1f"
                            + " | window [%s - %s]%n"
                            + "         raw:      %s%n"
                            + "         smoothed: %s",
                    mmsi, sorted.size(), gate, q, variance,
                    windowStart, windowEnd,
                    rawEwkt, smoothedEwkt);

            log.info(result);
            out.collect(result);
        }

        // =====================================================================
        // Core EKF implementation
        // =====================================================================

        /**
         * Runs the Kalman Filter over a sequence of 2D ENU positions.
         *
         * https://commons.apache.org/proper/commons-math/userguide/filter.html
         *
         * <p>Uses {@code org.apache.commons.math3.filter.KalmanFilter} from Apache Commons Math3.
         *
         * <p>For each new GPS measurement, the filter executes two steps:
         * <ol>
         *   <li><b>Predict</b>: "Where should the vessel be, based solely on the motion model?"
         *       <ul>
         *         <li>{@code x_pred}  (apply: new_pos = old_pos + velocity × dt)</li>
         *         <li>{@code P_pred}  (uncertainty grows: model is imperfect)</li>
         *       </ul>
         *   </li>
         *   <li>"The GPS just gave a new measurement: how do we update?"
         *       <ul>
         *         <li>Compute innovation: {@code innov} (GPS minus prediction)</li>
         *         <li>Compute Kalman Gain: {@code K}
         *             (how much to trust the GPS vs. the model)</li>
         *         <li>Correct state: {@code x_corr}</li>
         *         <li>Reduce uncertainty: {@code P_corr</li>
         *       </ul>
         *   </li>
         * </ol>
         *
         * <h3>Time-varying filter (dt changes at each step)</h3>
         * <p>AIS messages are not strictly periodic. The matrix A depends on {@code dt}
         * (the time elapsed since the previous measurement), so A is rebuilt at each iteration.
         * This makes the filter time-varying since we're using an irregular stream.
         *
         * <h3>Outlier detection via Mahalanobis distance</h3>
         * <p>Before calling {@code correct()}, the normalised innovation distance is computed:
         * {@code mahal = √(innovᵀ × S⁻¹ × innov)}. This is measured in standard deviations
         * (σ). If {@code mahal > gate} and {@code dropOutliers=true}, the GPS measurement is
         * rejected and the predicted state is used instead. But here, dropOutliers is False so we keep and smooth them
         *
         * @param rawEnu       raw ENU positions [n × 2] (east, north) in metres
         * @param events       corresponding AISData events (used for dt computation)
         * @param q            process noise scalar (Q = q × I₄)
         * @param variance     measurement noise scalar (R = variance × I₂)
         * @param gate         Mahalanobis distance threshold for outlier detection
         * @param dropOutliers if true, outlier measurements revert the state to prediction only
         * @return smoothed ENU positions [n × 2]
         */
        private double[][] applyEkf(
                double[][] rawEnu,
                List<AISData> events,
                double q, double variance, double gate, boolean dropOutliers) {

            int n = rawEnu.length;
            double[][] smoothed = new double[n][2];

            // Fixed matrices: defined once, reused at every step

            // H is the observation matrix (2×4)
            // The GPS sensor observes only position (east, north), not velocity (vx, vy).
            // Row 1: [1, 0, 0, 0] → east  = 1×east + 0×north + 0×vx + 0×vy
            // Row 2: [0, 1, 0, 0] → north = 0×east + 1×north + 0×vx + 0×vy
            // In short: H × x = [east, north]  (extracts position from the full state).
            RealMatrix H = new Array2DRowRealMatrix(new double[][]{
                    {1, 0, 0, 0},
                    {0, 1, 0, 0}
            });

            // R: measurement noise covariance (2×2): how imprecise the GPS sensor is.
            // Diagonal entries: variance of GPS error on east and north axes (in m²).
            //   variance = 1.0 m² → GPS standard deviation ≈ ±1 m.
            // Off-diagonal zeros: east and north GPS errors are assumed independent.
            // A larger R → the filter trusts GPS less → more smoothing.
            RealMatrix R = new Array2DRowRealMatrix(new double[][]{
                    {variance, 0},
                    {0,        variance}
            });

            // Q: process noise covariance (4×4): how imperfect the physics model is.
            // Diagonal q on all 4 components: position and velocity can both deviate
            // from the constant-velocity prediction (vessels accelerate/turn).
            // Off-diagonal zeros: deviations on each component are assumed independent.
            // q = 0.01 → the filter strongly trusts the motion model → strong smoothing.
            // A larger q → the filter trusts GPS more → less smoothing.
            RealMatrix Q4 = new Array2DRowRealMatrix(new double[][]{
                    {q, 0, 0, 0},
                    {0, q, 0, 0},
                    {0, 0, q, 0},
                    {0, 0, 0, q}
            });

            // Initial state x: the filter's first knowledge of the vessel
            // Components: [east=0, north=0, vx=0, vy=0]
            //   - Position: the first GPS point is the ENU origin → (0, 0).
            //   - Velocity: unknown at start → initialised to 0.
            RealVector x = new ArrayRealVector(
                    new double[]{rawEnu[0][0], rawEnu[0][1], 0.0, 0.0});

            // Initial state covariance P: the filter's uncertainty
            // P is a 4×4 matrix. Its diagonal entries are the variances of each
            // state component; off-diagonals encode correlations (initially 0).
            //   P[0,0] = variance m² uncertainty on east position
            //   P[1,1] = variance m² uncertainty on north position
            //   P[2,2] = variance uncertainty on vx
            //   P[3,3] = variance uncertainty on vy
            // P evolves dynamically: it GROWS after each predict step (we become
            // less certain), and SHRINKS after each correct step (a GPS measurement
            // brings new information).
            RealMatrix P = new Array2DRowRealMatrix(new double[][]{
                    {variance, 0,        0,        0       },
                    {0,        variance, 0,        0       },
                    {0,        0,        variance, 0       },
                    {0,        0,        0,        variance}
            });

            // The first point has no predecessor: nothing to predict or correct.
            // Copy it directly as the starting position of the smoothed trajectory.
            smoothed[0][0] = x.getEntry(0);   // east
            smoothed[0][1] = x.getEntry(1);   // north

            // Main loop: one iteration per GPS measurement
            for (int i = 1; i < n; i++) {

                // dt: elapsed time since the previous measurement (seconds)
                // dt is used by the model to predict how far the vessel moved.
                // AIS timestamps are in milliseconds → divide by 1000.
                double dt = (events.get(i).getTimestamp() - events.get(i - 1).getTimestamp())
                        / 1000.0;
                if (dt <= 0) dt = 1.0;

                // A: state transition matrix (4×4): the physics of motion
                //   east_new  = east  + vx × dt     (row 1: [1, 0, dt,  0])
                //   north_new = north + vy × dt     (row 2: [0, 1,  0, dt])
                //   vx_new    = vx                  (row 3: [0, 0,  1,  0] - constant velocity)
                //   vy_new    = vy                  (row 4: [0, 0,  0,  1] - constant velocity)
                // A is rebuilt at every step because dt varies (irregular AIS stream).
                // For a perfectly periodic stream, A could be built once outside the loop.
                RealMatrix A = new Array2DRowRealMatrix(new double[][]{
                        {1, 0, dt,  0},
                        {0, 1,  0, dt},
                        {0, 0,  1,  0},
                        {0, 0,  0,  1}
                });

                // Instantiate the Kalman Filter for this step.
                // ProcessModel takes: A (motion model), null (no external control input B, Q (model noise),
                //   x (current state), P (current uncertainty).
                // MeasurementModel takes: H (what the GPS observes), R (GPS noise).
                // The filter is re-created every iteration because A changes with dt.
                ProcessModel     pm = new DefaultProcessModel(A, null, Q4, x, P);
                MeasurementModel mm = new DefaultMeasurementModel(H, R);
                KalmanFilter     kf = new KalmanFilter(pm, mm);

                // PREDICT step
                // "Without any new measurement, where should the vessel be now?"
                //   x_pred                 (apply the physics model)
                //   P_pred                 (uncertainty grows)
                //
                // After kf.predict():
                //   kf.getStateEstimation()  → x_pred  [4 doubles: east, north, vx, vy]
                //   kf.getErrorCovariance()  → P_pred  [4×4 matrix]
                kf.predict();

                // Innovation: GPS measurement vs. prediction
                // zArr = raw GPS position for this step (in ENU metres).
                // xPred = what the filter just predicted (the first 2 components, the x & y positions).
                // innov = the discrepancy between observation and prediction.
                //
                //                          https://en.wikipedia.org/wiki/Kalman_filter
                //   innov = z − H × x_pred
                //         = [gps_east − pred_east,  gps_north − pred_north]
                //
                // A small innovation means the GPS confirms the prediction (normal situation).
                // A large innovation means either the GPS is noisy or the vessel changed speed.
                double[] zArr  = {rawEnu[i][0], rawEnu[i][1]};
                double[] xPred = kf.getStateEstimation();
                double[] innov = {zArr[0] - xPred[0], zArr[1] - xPred[1]};

                // Innovation covariance S (2×2)
                // S answers: "What amplitude of innovation is statistically normal right now?"
                RealMatrix PPred = new Array2DRowRealMatrix(kf.getErrorCovariance());
                // S formula sources:
                //          https://medium.com/@sophiezhao_2990/kalman-filter-explained-simply-2b5672429205µ
                //          https://www.anuncommonlab.com/articles/how-kalman-filters-work/part2.html#:~:text=Note%20that%20on%20each%20correction,no%20general%20rules%20to%20follow.
                RealMatrix S     = H.multiply(PPred).multiply(H.transpose()).add(R);

                // Sinv: inverse of S (2×2 analytic formula)
                // S⁻¹ is needed for both the Mahalanobis distance and the Kalman Gain.
                RealMatrix Sinv = invertSymmetric2x2(S);

                // Mahalanobis distance
                //
                // Why?:
                //      https://math.stackexchange.com/questions/4726428/kalman-filter-observation-matrix-of-measurement-equation-and-what-is-a-good-sig
                //      https://gurevich.ca/simple-outlier-detection-in-a-kalman-filter/
                //      https://medium.com/blogyuxiglobal/kalman-filter-the-way-to-remove-outliers-bb6aa616788e
                //
                // To go further?: https://ietresearch.onlinelibrary.wiley.com/doi/full/10.1049/cmu2.12664
                //
                //   Formula: https://math.stackexchange.com/questions/185947/how-to-calculate-the-mahalanobis-distance
                //   mahal = √( innovᵀ × S⁻¹ × innov )
                //
                // Interpretation:
                //   mahal < 1   → innovation within 1σ → completely normal
                //   mahal 1–3   → unusual but acceptable
                //   mahal > 3   → beyond 3σ threshold → probable GPS outlier
                //
                // Java decomposition:
                //   Sinv.preMultiply(yVec) → S⁻¹ × innov    (vector 2D)
                //   .dotProduct(yVec)      → innovᵀ × (S⁻¹ × innov)  (scalar)
                //   Math.sqrt(...)         → √(...)           (final distance)
                RealVector yVec = new ArrayRealVector(innov);
                double mahal    = Math.sqrt(Sinv.preMultiply(yVec).dotProduct(yVec));

                boolean isOutlier = mahal > gate;

                if (isOutlier && dropOutliers) {
                    // GPS measurement is statistically implausible (> 3σ away from prediction)
                    // and outlier dropping is enabled: keep the predicted position as it is.
                    smoothed[i][0] = xPred[0];
                    smoothed[i][1] = xPred[1];

                } else {
                    // CORRECTION step
                    // Incorporate the GPS measurement to refine the predicted state.
                    // kf.correct(zArr) performs three internal calculations:
                    //
                    //   1. Kalman Gain K (4×2):
                    //      K is the "trust cursor" between model and GPS:
                    //        - Large P_pred (uncertain prediction) → large K → trust GPS more.
                    //        - Large R (noisy GPS) → small S⁻¹ → small K → trust model more.
                    //      K values are between 0 (ignore GPS) and 1 (fully trust GPS).
                    //
                    //   2. State correction
                    //
                    //   3. Covariance update
                    //       Incorporating a measurement reduces uncertainty
                    kf.correct(zArr);

                    double[] xCorr = kf.getStateEstimation();
                    smoothed[i][0] = xCorr[0];   // smoothed east  (metres from origin)
                    smoothed[i][1] = xCorr[1];   // smoothed north (metres from origin)
                }

                // Propagation: carry state forward to the next iteration
                // The corrected state x_corr and its covariance P_corr become the
                // starting point for the next predict/correct cycle.
                x = new ArrayRealVector(kf.getStateEstimation());
                P = new Array2DRowRealMatrix(kf.getErrorCovariance());
            }

            return smoothed;
        }

        // =====================================================================
        // Helper: analytic 2×2 matrix inversion
        // =====================================================================

        /**
         * Inverts a symmetric 2×2 matrix.
         *
         * <p>For a 2×2 matrix {@code M = [[a, b], [c, d]]}, the inverse is:
         * <pre>
         *   M⁻¹ = (1/det) × [[ d, -b],
         *                     [-c,  a]]
         *   where det = a×d − b×c
         * </pre>
         *
         * https://www.mathcentre.ac.uk/resources/uploaded/sigma-matrices7-2009-1.pdf
         * https://math.stackexchange.com/questions/21533/shortcut-for-finding-a-inverse-of-matrix
         *
         * @param M a 2×2 real matrix (assumed symmetric and positive-definite)
         * @return M⁻¹
         */
        private RealMatrix invertSymmetric2x2(RealMatrix M) {
            double a = M.getEntry(0, 0), b = M.getEntry(0, 1),
                    c = M.getEntry(1, 0), d = M.getEntry(1, 1);
            double det = a * d - b * c;
            return new Array2DRowRealMatrix(new double[][]{
                    { d / det, -b / det},
                    {-c / det,  a / det}
            });
        }

        // =====================================================================
        // ENU ↔ WGS-84 projection utilities
        // =====================================================================

        /**
         * Converts a WGS-84 (lat, lon) point to local ENU (east, north) coordinates
         * in metres, relative to a given origin.
         *
         * <p>Latitude/longitude degrees are <em>not</em> a uniform distance unit.
         * The metric length of 1° of longitude depends on latitude:
         * At lat=0° (equator): ≈ 111 km/°; at lat=51° (Brussels): ≈ 70 km/°;
         * at lat=90° (pole): 0 km/°.
         *
         * <p>The Kalman Filter computes differences and distances between coordinates.
         * Those operations require a uniform metric space. ENU provides that.
         *
         * https://stackoverflow.com/questions/3024404/transform-longitude-latitude-into-meters
         *
         * @param lat       WGS-84 latitude of the point (degrees)
         * @param lon       WGS-84 longitude of the point (degrees)
         * @param originLat WGS-84 latitude of the ENU origin (degrees)
         * @param originLon WGS-84 longitude of the ENU origin (degrees)
         * @return [east (m), north (m)] relative to the origin
         */
        private double[] latLonToEnu(double lat, double lon, double originLat, double originLon) {
            double dLat   = Math.toRadians(lat - originLat);   // angular difference in latitude
            double dLon   = Math.toRadians(lon - originLon);   // angular difference in longitude
            double latRad = Math.toRadians(originLat);         // origin latitude in radians

            // North: 1° of latitude is always ≈ 111 km so no correction needed.
            double north = dLat * EARTH_RADIUS;

            // East: 1° of longitude shrinks with latitude → multiply by cos(lat).
            // At Brussels (51°N): cos(51°) ≈ 0.629 → 1° ≈ 70 km instead of 111 km.
            double east  = dLon * EARTH_RADIUS * Math.cos(latRad);

            return new double[]{east, north};
        }

        /**
         * Converts local ENU (east, north) coordinates in metres back to WGS-84
         * (lat, lon) degrees.
         *
         * https://stackoverflow.com/questions/3024404/transform-longitude-latitude-into-meters
         *
         * <p>This is the exact inverse of {@link #latLonToEnu}: divides metres by the
         * appropriate scale factor to recover degree offsets, then adds the origin.
         *
         * @param east      ENU east offset in metres
         * @param north     ENU north offset in metres
         * @param originLat WGS-84 latitude of the ENU origin (degrees)
         * @param originLon WGS-84 longitude of the ENU origin (degrees)
         * @return [lat (degrees), lon (degrees)] in WGS-84
         */
        private double[] enuToLatLon(double east, double north, double originLat, double originLon) {
            double latRad = Math.toRadians(originLat);

            // Latitude: divide north metres by earth radius to get degrees offset.
            double lat = originLat + Math.toDegrees(north / EARTH_RADIUS);

            // Longitude: divide east metres by (earth_radius × cos(lat)) to undo the
            // longitude compression that was applied in latLonToEnu.
            double lon = originLon + Math.toDegrees(east / (EARTH_RADIUS * Math.cos(latRad)));

            return new double[]{lat, lon};
        }

        /**
         * Formats a Unix epoch millisecond timestamp as an ISO-8601 string with UTC offset,
         * compatible with the MEOS {@code tgeogpoint_in()} WKT format.
         *
         * @param millis epoch milliseconds
         * @return formatted timestamp, e.g. {@code "2024-08-02 10:05:48+00"}
         */
        private String millisToTimestamp(long millis) {
            OffsetDateTime dt = Instant.ofEpochMilli(millis).atOffset(ZoneOffset.UTC);
            return dt.format(TIMESTAMP_FMT);
        }
    }
}