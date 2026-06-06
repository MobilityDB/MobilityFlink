package berlinmod;

import functions.functions;
import jnr.ffi.Pointer;
import utils.spatial.PointToSegment;

/**
 * Runtime bridge from MobilityFlink BerlinMOD streaming-form predicates to
 * MEOS via JMEOS.
 *
 * <p>All spatial predicates exercised by the BerlinMOD-9 × 3-form scaffold flow
 * through this class and evaluate through MEOS' WGS84 geography surface
 * ({@code geom_to_geog} + {@code geog_dwithin}/{@code geog_distance}). There is
 * no pure-Java approximation: a hand-rolled great-circle would diverge from
 * MEOS' spheroidal result and could silently mask a missing libmeos, so the
 * bridge requires MEOS and fails loudly when it is absent — exactly as the
 * {@code MeosOps*} surface does ({@code "requires libmeos — set
 * -Dmobilityflink.meos.enabled=true"}).
 */
public final class MEOSBridge {

    static {
        try {
            functions.meos_initialize();
        } catch (Throwable t) {
            throw new IllegalStateException(
                "MEOSBridge requires libmeos: its spatial predicates are MEOS geog_dwithin/"
                + "geog_distance with no pure-Java fallback. Put libmeos on java.library.path.", t);
        }
    }

    private MEOSBridge() {
        // utility
    }

    // ----------------------------------------------------------------------
    // Public bridge surface — MEOS geography predicates, no fallback.
    // ----------------------------------------------------------------------

    /**
     * @return {@code true} if the WGS84 spheroidal distance from
     *         {@code (lon1, lat1)} to {@code (lon2, lat2)} is at most
     *         {@code radiusMetres}, via MEOS {@code geog_dwithin}.
     */
    public static boolean dwithinMetres(double lon1, double lat1,
                                        double lon2, double lat2,
                                        double radiusMetres) {
        return functions.geog_dwithin(pointGeog(lon1, lat1), pointGeog(lon2, lat2), radiusMetres, true);
    }

    /**
     * @return {@code true} if the spheroidal distance from {@code (pLon, pLat)}
     *         to the LineString {@code (s1, s2)} is at most {@code radiusMetres},
     *         via MEOS {@code geog_dwithin} on geographies built from the point
     *         and line WKTs.
     */
    public static boolean dwithinSegmentMetres(double pLon, double pLat,
                                               double s1Lon, double s1Lat,
                                               double s2Lon, double s2Lat,
                                               double radiusMetres) {
        return functions.geog_dwithin(
            pointGeog(pLon, pLat), lineGeog(s1Lon, s1Lat, s2Lon, s2Lat), radiusMetres, true);
    }

    /**
     * @return the WGS84 spheroidal distance in metres between two points, via
     *         {@code utils.spatial.Haversine.distance} (MEOS {@code geog_distance}
     *         over two POINT geographies).
     */
    public static double distanceMetres(double lon1, double lat1,
                                        double lon2, double lat2) {
        return utils.spatial.Haversine.distance(lon1, lat1, lon2, lat2);
    }

    /**
     * @return the spheroidal distance in metres from {@code (pLon, pLat)} to the
     *         LineString {@code (s1, s2)}, via {@code utils.spatial.PointToSegment.distance}
     *         (MEOS {@code geog_distance} over POINT/LINESTRING geographies).
     */
    public static double distanceSegmentMetres(double pLon, double pLat,
                                               double s1Lon, double s1Lat,
                                               double s2Lon, double s2Lat) {
        return PointToSegment.distance(pLon, pLat, s1Lon, s1Lat, s2Lon, s2Lat);
    }

    // ----------------------------------------------------------------------
    // Internal helpers — WKT → geometry → geography in one MEOS-side step.
    // ----------------------------------------------------------------------

    private static Pointer pointGeog(double lon, double lat) {
        String wkt = String.format("SRID=4326;Point(%.7f %.7f)", lon, lat);
        return functions.geom_to_geog(functions.geom_in(wkt, -1));
    }

    private static Pointer lineGeog(double s1Lon, double s1Lat,
                                    double s2Lon, double s2Lat) {
        String wkt = String.format("SRID=4326;LineString(%.7f %.7f, %.7f %.7f)",
                                   s1Lon, s1Lat, s2Lon, s2Lat);
        return functions.geom_to_geog(functions.geom_in(wkt, -1));
    }
}
