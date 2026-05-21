package berlinmod;

import functions.functions;
import jnr.ffi.Pointer;
import utils.spatial.PointToSegment;

/**
 * Runtime bridge from MobilityFlink BerlinMOD streaming-form predicates to
 * MEOS via JMEOS.
 *
 * <p>All spatial predicates exercised by the BerlinMOD-9 × 3-form scaffold
 * flow through this class. When the JMEOS native libmeos shared object is
 * present and loadable, each predicate evaluates through MEOS' WGS84
 * geography surface ({@code geom_to_geog} + {@code geog_dwithin}). When
 * libmeos is not available, each predicate falls back to the corresponding
 * pure-Java implementation in {@link Haversine} or {@link SegmentDistance}
 * so the BerlinMOD mini-cluster local tests stay runnable on systems
 * without a MEOS install.
 *
 * <p>The fallback is gated by the {@link #MEOS_AVAILABLE} static flag, set
 * once at class-load time:
 * <ul>
 *   <li>{@code -Dmobilityflink.meos.enabled=false} forces the pure-Java path
 *       even when libmeos is present (used by {@code BerlinMODQ*LocalTest}).
 *   <li>Otherwise {@code MEOS_AVAILABLE} is {@code true} iff
 *       {@code functions.meos_initialize()} returns without throwing.
 * </ul>
 */
public final class MEOSBridge {

    /**
     * {@code true} iff MEOS is available on this runtime and the bridge
     * routes through it; {@code false} iff the bridge will use the pure-Java
     * fallbacks.
     */
    public static final boolean MEOS_AVAILABLE;

    static {
        boolean enabled =
                Boolean.parseBoolean(System.getProperty("mobilityflink.meos.enabled", "true"));
        boolean ok = false;
        if (enabled) {
            try {
                functions.meos_initialize();
                ok = true;
            } catch (Throwable t) {
                // libmeos shared object not loadable on this runtime — fall back.
                ok = false;
            }
        }
        MEOS_AVAILABLE = ok;
    }

    private MEOSBridge() {
        // utility
    }

    // ----------------------------------------------------------------------
    // Public bridge surface — same shape as Haversine + SegmentDistance.
    // ----------------------------------------------------------------------

    /**
     * @return {@code true} if the great-circle distance from {@code (lon1, lat1)}
     *         to {@code (lon2, lat2)} on the WGS84 spheroid is at most
     *         {@code radiusMetres}. MEOS-backed via {@code geog_dwithin} when
     *         available, else pure-Java {@link Haversine#withinMetres}.
     */
    public static boolean dwithinMetres(double lon1, double lat1,
                                        double lon2, double lat2,
                                        double radiusMetres) {
        if (!MEOS_AVAILABLE) {
            return Haversine.withinMetres(lon1, lat1, lon2, lat2, radiusMetres);
        }
        Pointer g1 = pointGeog(lon1, lat1);
        Pointer g2 = pointGeog(lon2, lat2);
        if (g1 == null || g2 == null) {
            return Haversine.withinMetres(lon1, lat1, lon2, lat2, radiusMetres);
        }
        return functions.geog_dwithin(g1, g2, radiusMetres, true);
    }

    /**
     * @return {@code true} if the spheroidal distance from {@code (pLon, pLat)}
     *         to the LineString {@code (s1, s2)} is at most {@code radiusMetres}.
     *         MEOS-backed via {@code geog_dwithin} on geographies built from
     *         the point and line WKTs, else pure-Java
     *         {@link SegmentDistance#withinMetres}.
     */
    public static boolean dwithinSegmentMetres(double pLon, double pLat,
                                               double s1Lon, double s1Lat,
                                               double s2Lon, double s2Lat,
                                               double radiusMetres) {
        if (!MEOS_AVAILABLE) {
            return SegmentDistance.withinMetres(pLon, pLat, s1Lon, s1Lat, s2Lon, s2Lat, radiusMetres);
        }
        Pointer pg = pointGeog(pLon, pLat);
        Pointer lg = lineGeog(s1Lon, s1Lat, s2Lon, s2Lat);
        if (pg == null || lg == null) {
            return SegmentDistance.withinMetres(pLon, pLat, s1Lon, s1Lat, s2Lon, s2Lat, radiusMetres);
        }
        return functions.geog_dwithin(pg, lg, radiusMetres, true);
    }

    /**
     * @return the spheroidal distance in metres between two WGS84 points.
     *         MEOS-backed via {@code utils.spatial.Haversine.distance}
     *         (which calls MEOS' {@code geog_distance} over two POINT
     *         geographies) when libmeos is loadable, else pure-Java
     *         {@link Haversine#distanceMetres}.
     */
    public static double distanceMetres(double lon1, double lat1,
                                        double lon2, double lat2) {
        if (!MEOS_AVAILABLE) {
            return Haversine.distanceMetres(lon1, lat1, lon2, lat2);
        }
        return utils.spatial.Haversine.distance(lon1, lat1, lon2, lat2);
    }

    /**
     * @return the spheroidal distance in metres from {@code (pLon, pLat)} to
     *         the LineString {@code (s1, s2)}. MEOS-backed via
     *         {@code utils.spatial.PointToSegment.distance} when libmeos is
     *         loadable, else pure-Java
     *         {@link SegmentDistance#distanceMetres}.
     */
    public static double distanceSegmentMetres(double pLon, double pLat,
                                               double s1Lon, double s1Lat,
                                               double s2Lon, double s2Lat) {
        if (!MEOS_AVAILABLE) {
            return SegmentDistance.distanceMetres(pLon, pLat, s1Lon, s1Lat, s2Lon, s2Lat);
        }
        return PointToSegment.distance(pLon, pLat, s1Lon, s1Lat, s2Lon, s2Lat);
    }

    // ----------------------------------------------------------------------
    // Internal helpers — WKT → geometry → geography in one MEOS-side step.
    // ----------------------------------------------------------------------

    private static Pointer pointGeog(double lon, double lat) {
        String wkt = String.format("SRID=4326;Point(%.7f %.7f)", lon, lat);
        Pointer g = functions.geom_in(wkt, -1);
        if (g == null) {
            return null;
        }
        return functions.geom_to_geog(g);
    }

    private static Pointer lineGeog(double s1Lon, double s1Lat,
                                    double s2Lon, double s2Lat) {
        String wkt = String.format("SRID=4326;LineString(%.7f %.7f, %.7f %.7f)",
                                   s1Lon, s1Lat, s2Lon, s2Lat);
        Pointer g = functions.geom_in(wkt, -1);
        if (g == null) {
            return null;
        }
        return functions.geom_to_geog(g);
    }
}
