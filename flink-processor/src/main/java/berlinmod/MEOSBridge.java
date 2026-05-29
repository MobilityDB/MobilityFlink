package berlinmod;

import functions.GeneratedFunctions;
import jnr.ffi.Pointer;
import org.mobilitydb.flink.meos.wirings.MeosWiringRuntime;

/**
 * Thin wiring from the BerlinMOD streaming-form predicates to MEOS via JMEOS.
 *
 * <p>Every spatial predicate exercised by the BerlinMOD-9 × 3-form scaffold
 * flows through this class, and every predicate evaluates through MEOS. The
 * within-distance predicate is the canonical temporal operator
 * {@code edwithin_tgeo_geo} — ever-within between the vehicle's {@code tgeogpoint}
 * instant and the query geography, in metres on the WGS84 spheroid — the same
 * MEOS operator the streaming-form parity contract names. Distances are
 * {@code geog_distance} over WGS84 geographies. This class holds no spatial
 * mathematics of its own: it constructs the MEOS inputs and delegates the
 * computation to libmeos.
 *
 * <p>{@link MeosWiringRuntime#ensureInitializedOnThread()} initialises MEOS on
 * the calling task thread (idempotent per thread) before the first call, since
 * MEOS keeps its session state per OS thread.
 */
public final class MEOSBridge {

    private MEOSBridge() {
        // utility
    }

    // ----------------------------------------------------------------------
    // Public predicate surface — all evaluation delegated to MEOS.
    // ----------------------------------------------------------------------

    /**
     * @return {@code true} iff the WGS84 spheroidal distance from
     *         {@code (lon1, lat1)} to {@code (lon2, lat2)} is at most
     *         {@code radiusMetres}, via MEOS {@code edwithin_tgeo_geo} between
     *         the {@code (lon1, lat1)} {@code tgeogpoint} instant and the
     *         {@code (lon2, lat2)} point geography.
     */
    public static boolean dwithinMetres(double lon1, double lat1,
                                        double lon2, double lat2,
                                        double radiusMetres) {
        MeosWiringRuntime.ensureInitializedOnThread();
        return GeneratedFunctions.edwithin_tgeo_geo(
                tgeogInst(lon1, lat1), pointGeog(lon2, lat2), radiusMetres) == 1;
    }

    /**
     * @return {@code true} iff the WGS84 spheroidal distance from
     *         {@code (pLon, pLat)} to the LineString {@code (s1, s2)} is at most
     *         {@code radiusMetres}, via MEOS {@code edwithin_tgeo_geo} between
     *         the point {@code tgeogpoint} instant and the line geography.
     */
    public static boolean dwithinSegmentMetres(double pLon, double pLat,
                                               double s1Lon, double s1Lat,
                                               double s2Lon, double s2Lat,
                                               double radiusMetres) {
        MeosWiringRuntime.ensureInitializedOnThread();
        return GeneratedFunctions.edwithin_tgeo_geo(
                tgeogInst(pLon, pLat),
                lineGeog(s1Lon, s1Lat, s2Lon, s2Lat), radiusMetres) == 1;
    }

    /**
     * @return {@code true} iff {@code (lon, lat)} lies in the axis-aligned box
     *         {@code [xmin, xmax] × [ymin, ymax]}, via MEOS
     *         {@code eintersects_tgeo_geo} between the point's {@code tgeompoint}
     *         instant and the box polygon (planar, SRID 4326).
     */
    public static boolean intersectsBox(double lon, double lat,
                                        double xmin, double ymin,
                                        double xmax, double ymax) {
        MeosWiringRuntime.ensureInitializedOnThread();
        return GeneratedFunctions.eintersects_tgeo_geo(
                tgeomInst(lon, lat), boxPolygon(xmin, ymin, xmax, ymax)) == 1;
    }

    /**
     * @return the WGS84 spheroidal distance in metres between two points, via
     *         MEOS {@code geog_distance}.
     */
    public static double distanceMetres(double lon1, double lat1,
                                        double lon2, double lat2) {
        MeosWiringRuntime.ensureInitializedOnThread();
        return GeneratedFunctions.geog_distance(pointGeog(lon1, lat1), pointGeog(lon2, lat2));
    }

    /**
     * @return the WGS84 spheroidal distance in metres from {@code (pLon, pLat)}
     *         to the LineString {@code (s1, s2)}, via MEOS {@code geog_distance}.
     */
    public static double distanceSegmentMetres(double pLon, double pLat,
                                               double s1Lon, double s1Lat,
                                               double s2Lon, double s2Lat) {
        MeosWiringRuntime.ensureInitializedOnThread();
        return GeneratedFunctions.geog_distance(
                pointGeog(pLon, pLat), lineGeog(s1Lon, s1Lat, s2Lon, s2Lat));
    }

    // ----------------------------------------------------------------------
    // Internal helpers — construct the MEOS temporal / geography inputs.
    // ----------------------------------------------------------------------

    private static Pointer tgeogInst(double lon, double lat) {
        return GeneratedFunctions.tgeogpoint_in(
                String.format("SRID=4326;Point(%.7f %.7f)@2000-01-01", lon, lat));
    }

    private static Pointer tgeomInst(double lon, double lat) {
        return GeneratedFunctions.tgeompoint_in(
                String.format("SRID=4326;Point(%.7f %.7f)@2000-01-01", lon, lat));
    }

    private static Pointer boxPolygon(double xmin, double ymin,
                                      double xmax, double ymax) {
        return GeneratedFunctions.geom_in(String.format(
                "SRID=4326;Polygon((%.7f %.7f, %.7f %.7f, %.7f %.7f, %.7f %.7f, %.7f %.7f))",
                xmin, ymin, xmax, ymin, xmax, ymax, xmin, ymax, xmin, ymin), -1);
    }

    private static Pointer pointGeog(double lon, double lat) {
        return GeneratedFunctions.geom_to_geog(
                GeneratedFunctions.geom_in(String.format("SRID=4326;Point(%.7f %.7f)", lon, lat), -1));
    }

    private static Pointer lineGeog(double s1Lon, double s1Lat,
                                    double s2Lon, double s2Lat) {
        return GeneratedFunctions.geom_to_geog(
                GeneratedFunctions.geom_in(String.format(
                        "SRID=4326;LineString(%.7f %.7f, %.7f %.7f)", s1Lon, s1Lat, s2Lon, s2Lat), -1));
    }
}
