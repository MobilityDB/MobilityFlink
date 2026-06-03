package sncbdata;

/**
 * Data model for SNCB train telemetry events.
 *
 * <p>Maps to the CSV schema defined in {@code sncb_brake_monitoring.yaml}:
 * <pre>
 *   col 0  : time_utc    → timestamp (Unix epoch seconds, stored as millis)
 *   col 1  : device_id   → deviceId  (train identifier, replaces MMSI)
 *   col 2  : Vbat        → vbat
 *   col 3  : PCFA_mbar   → pcfaMbar  (automatic brake pipe pressure)
 *   col 4  : PCFF_mbar   → pcffMbar  (fictitious brake cylinder pressure)
 *   col 5  : PCF1_mbar   → pcf1Mbar
 *   col 6  : PCF2_mbar   → pcf2Mbar
 *   col 7  : T1_mbar     → t1Mbar
 *   col 8  : T2_mbar     → t2Mbar
 *   col 9  : Code1       → code1
 *   col 10 : Code2       → code2
 *   col 11 : gps_speed   → gpsSpeed  (speed in km/h)
 *   col 12 : gps_lat     → lat
 *   col 13 : gps_lon     → lon
 * </pre>
 */
public class SNCBData {

    // Core fields used by all queries
    private long   timestamp;   // col 0: time_utc in milliseconds
    private int    deviceId;    // col 1: device_id (train identifier)
    private double lat;         // col 12: gps_lat
    private double lon;         // col 13: gps_lon
    private double gpsSpeed;    // col 11: gps_speed (km/h)

    // Brake pressure fields — used by Query 2
    private double pcfaMbar;    // col 3: PCFA_mbar (automatic brake pipe pressure)
    private double pcffMbar;    // col 4: PCFF_mbar (fictitious brake cylinder pressure)

    // Additional sensor fields (available if needed for future queries)
    private double vbat;        // col 2: battery voltage
    private double pcf1Mbar;   // col 5: PCF1_mbar
    private double pcf2Mbar;   // col 6: PCF2_mbar
    private double t1Mbar;     // col 7: T1_mbar
    private double t2Mbar;     // col 8: T2_mbar
    private double code1;      // col 9: Code1
    private double code2;      // col 10: Code2

    // -----------------------------------------------------------------------
    // Constructors
    // -----------------------------------------------------------------------

    public SNCBData() {}

    public SNCBData(long timestamp, int deviceId, double lat, double lon,
                    double gpsSpeed, double pcfaMbar, double pcffMbar) {
        this.timestamp = timestamp;
        this.deviceId  = deviceId;
        this.lat       = lat;
        this.lon       = lon;
        this.gpsSpeed  = gpsSpeed;
        this.pcfaMbar  = pcfaMbar;
        this.pcffMbar  = pcffMbar;
    }

    // -----------------------------------------------------------------------
    // Getters & Setters
    // -----------------------------------------------------------------------

    public long   getTimestamp()  { return timestamp; }
    public void   setTimestamp(long timestamp)  { this.timestamp = timestamp; }

    public int    getDeviceId()   { return deviceId; }
    public void   setDeviceId(int deviceId)     { this.deviceId = deviceId; }

    public double getLat()        { return lat; }
    public void   setLat(double lat)             { this.lat = lat; }

    public double getLon()        { return lon; }
    public void   setLon(double lon)             { this.lon = lon; }

    public double getGpsSpeed()   { return gpsSpeed; }
    public void   setGpsSpeed(double gpsSpeed)   { this.gpsSpeed = gpsSpeed; }

    public double getPcfaMbar()   { return pcfaMbar; }
    public void   setPcfaMbar(double pcfaMbar)   { this.pcfaMbar = pcfaMbar; }

    public double getPcffMbar()   { return pcffMbar; }
    public void   setPcffMbar(double pcffMbar)   { this.pcffMbar = pcffMbar; }

    public double getVbat()       { return vbat; }
    public void   setVbat(double vbat)           { this.vbat = vbat; }

    public double getPcf1Mbar()  { return pcf1Mbar; }
    public void   setPcf1Mbar(double v)          { this.pcf1Mbar = v; }

    public double getPcf2Mbar()  { return pcf2Mbar; }
    public void   setPcf2Mbar(double v)          { this.pcf2Mbar = v; }

    public double getT1Mbar()    { return t1Mbar; }
    public void   setT1Mbar(double v)            { this.t1Mbar = v; }

    public double getT2Mbar()    { return t2Mbar; }
    public void   setT2Mbar(double v)            { this.t2Mbar = v; }

    public double getCode1()     { return code1; }
    public void   setCode1(double v)             { this.code1 = v; }

    public double getCode2()     { return code2; }
    public void   setCode2(double v)             { this.code2 = v; }


    @Override
    public String toString() {
        return String.format("SNCBData{ts=%d, deviceId=%d, lat=%.4f, lon=%.4f, "
                        + "gpsSpeed=%.2f, pcfaMbar=%.3f, pcffMbar=%.3f}",
                timestamp, deviceId, lat, lon, gpsSpeed, pcfaMbar, pcffMbar);
    }
}