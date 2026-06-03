package berlinmod;

/**
 * Plain data class for a single GPS event from a BerlinMOD trip.
 *
 * <p>Matches the {@code aisdata.AISData} field set but uses the BerlinMOD vehicle
 * identifier {@code vehicleId} instead of an AIS {@code mmsi} and drops the
 * AIS-specific {@code speed}/{@code course} channels (BerlinMOD's generator
 * does not export those for the streaming form).
 */
public class BerlinMODTrip {
    private long timestamp; // epoch milliseconds (event time)
    private int vehicleId;
    private double lon;
    private double lat;

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public int getVehicleId() {
        return vehicleId;
    }

    public void setVehicleId(int vehicleId) {
        this.vehicleId = vehicleId;
    }

    public double getLon() {
        return lon;
    }

    public void setLon(double lon) {
        this.lon = lon;
    }

    public double getLat() {
        return lat;
    }

    public void setLat(double lat) {
        this.lat = lat;
    }
}
