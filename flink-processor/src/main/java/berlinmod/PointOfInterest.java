package berlinmod;

import java.io.Serializable;

/**
 * Simple point-of-interest record for BerlinMOD-Q7 — a (lon, lat) plus a
 * proximity radius in metres and an integer id. Serializable for use in
 * Flink operator state and configuration.
 */
public final class PointOfInterest implements Serializable {
    private static final long serialVersionUID = 1L;

    public final int id;
    public final double lon;
    public final double lat;
    public final double radiusMetres;

    public PointOfInterest(int id, double lon, double lat, double radiusMetres) {
        this.id = id;
        this.lon = lon;
        this.lat = lat;
        this.radiusMetres = radiusMetres;
    }
}
