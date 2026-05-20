package berlinmod;

/**
 * Great-circle distance in metres between two WGS84 (lon, lat) points.
 *
 * <p>Used by the BerlinMOD-Q3 scaffold for "is this vehicle within {@code d}
 * metres of point P" predicates. This is the same semantic as the MEOS
 * {@code edwithin_tgeo_geo} operator used by {@code MobilityNebula/Queries/Query1.yaml};
 * keeping the predicate as pure Java here lets the scaffold compile and run
 * before the JMEOS bridge for {@code edwithin_tgeo_geo} is wired through.
 */
public final class Haversine {

    private static final double EARTH_RADIUS_METRES = 6_371_000.0;

    private Haversine() {
        // utility
    }

    /**
     * @return great-circle distance in metres between (lon1, lat1) and (lon2, lat2)
     */
    public static double distanceMetres(double lon1, double lat1, double lon2, double lat2) {
        double phi1 = Math.toRadians(lat1);
        double phi2 = Math.toRadians(lat2);
        double dPhi = Math.toRadians(lat2 - lat1);
        double dLambda = Math.toRadians(lon2 - lon1);

        double a = Math.sin(dPhi / 2) * Math.sin(dPhi / 2)
                 + Math.cos(phi1) * Math.cos(phi2)
                 * Math.sin(dLambda / 2) * Math.sin(dLambda / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return EARTH_RADIUS_METRES * c;
    }

    /**
     * @return true if the great-circle distance from (lon, lat) to (pLon, pLat)
     *         is ≤ {@code radiusMetres}
     */
    public static boolean withinMetres(double lon, double lat, double pLon, double pLat, double radiusMetres) {
        return distanceMetres(lon, lat, pLon, pLat) <= radiusMetres;
    }
}
