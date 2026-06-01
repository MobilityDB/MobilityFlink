/*****************************************************************************
 *
 * This MobilityDB code is provided under The PostgreSQL License.
 * Copyright (c) 2020-2026, Université libre de Bruxelles and MobilityDB
 * contributors
 *
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for any purpose, without fee, and without a written
 * agreement is hereby granted, provided that the above copyright notice and
 * this paragraph and the following two paragraphs appear in all copies.
 *
 * IN NO EVENT SHALL UNIVERSITE LIBRE DE BRUXELLES BE LIABLE TO ANY PARTY FOR
 * DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
 * LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION,
 * EVEN IF UNIVERSITE LIBRE DE BRUXELLES HAS BEEN ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 *
 * UNIVERSITE LIBRE DE BRUXELLES SPECIFICALLY DISCLAIMS ANY WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
 * AND FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON
 * AN "AS IS" BASIS, AND UNIVERSITE LIBRE DE BRUXELLES HAS NO OBLIGATIONS TO
 * PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 *
 *****************************************************************************/

package berlinmod;

import functions.GeneratedFunctions;
import jnr.ffi.Pointer;
import org.mobilitydb.flink.meos.wirings.MeosWiringRuntime;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;
import java.util.stream.Stream;

/**
 * Corpus loader and query-parameter derivation for the BerlinMOD streaming
 * benchmark.
 *
 * <p>Supplies either a deterministic synthetic corpus or the real BerlinMOD
 * instants corpus read from the {@code berlinmod_instants.csv} produced by the
 * BerlinMOD generator. Real instants are stored in EPSG:3857; they are
 * reprojected to EPSG:4326 through MEOS {@code geo_transform} at load — the
 * loader holds no projection mathematics of its own.
 *
 * <p>{@link Params} fixes the per-query parameters from the corpus itself (its
 * centroid, bounding box, vehicle ids, and time span) so every spatial cell is
 * selective and the windowing granularity yields a comparable number of windows
 * regardless of the corpus time span.
 */
public final class BerlinMODCorpus {

    private static final DateTimeFormatter TS = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd HH:mm:ss")
            .optionalStart().appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true).optionalEnd()
            .appendOffset("+HH", "Z")
            .toFormatter();

    private BerlinMODCorpus() { /* utility */ }

    /** Query parameters derived from a corpus. */
    public static final class Params {
        public final double pLon, pLat, radiusMetres, dMeetMetres;
        public final double xmin, ymin, xmax, ymax;
        public final double s1Lon, s1Lat, s2Lon, s2Lat;
        public final List<PointOfInterest> pois;
        public final int targetId, xId, yId;
        public final long windowSeconds, snapshotTickMillis;

        Params(double pLon, double pLat, double radiusMetres, double dMeetMetres,
               double xmin, double ymin, double xmax, double ymax,
               double s1Lon, double s1Lat, double s2Lon, double s2Lat,
               List<PointOfInterest> pois, int targetId, int xId, int yId,
               long windowSeconds, long snapshotTickMillis) {
            this.pLon = pLon; this.pLat = pLat; this.radiusMetres = radiusMetres; this.dMeetMetres = dMeetMetres;
            this.xmin = xmin; this.ymin = ymin; this.xmax = xmax; this.ymax = ymax;
            this.s1Lon = s1Lon; this.s1Lat = s1Lat; this.s2Lon = s2Lon; this.s2Lat = s2Lat;
            this.pois = pois; this.targetId = targetId; this.xId = xId; this.yId = yId;
            this.windowSeconds = windowSeconds; this.snapshotTickMillis = snapshotTickMillis;
        }
    }

    /** Deterministic synthetic corpus: vehicles on a disc around Brussels centre,
     * drifting per event, with monotonically increasing timestamps. */
    public static List<BerlinMODTrip> synthetic(int vehicles, int perVehicle) {
        final double centreLon = 4.3517, centreLat = 50.8503, spread = 0.12;
        final long t0 = 1_735_711_200_000L, spanMillis = 600_000L;
        int total = vehicles * perVehicle;
        long step = Math.max(1L, spanMillis / total);
        List<BerlinMODTrip> events = new ArrayList<>(total);
        long g = 0;
        for (int e = 0; e < perVehicle; e++) {
            for (int v = 0; v < vehicles; v++) {
                double ang = (v * 2.399963) % (2 * Math.PI);
                double rad = spread * ((v % 17) / 17.0);
                double drift = 0.0005 * Math.sin((e + v) * 0.13);
                events.add(make(100 + v, t0 + g * step,
                        centreLon + rad * Math.cos(ang) + drift,
                        centreLat + rad * Math.sin(ang) + drift));
                g++;
            }
        }
        return events;
    }

    /** Real BerlinMOD instants from {@code berlinmod_instants.csv}
     * (columns {@code tripid,vehid,day,seqno,geom,t}), reprojected 3857→4326
     * through MEOS, sorted by timestamp. {@code maxRows <= 0} loads all rows. */
    public static List<BerlinMODTrip> fromInstantsCsv(String path, int maxRows) throws Exception {
        MeosWiringRuntime.ensureInitializedOnThread();
        List<BerlinMODTrip> events = new ArrayList<>();
        try (Stream<String> lines = Files.lines(Paths.get(path))) {
            java.util.Iterator<String> it = lines.iterator();
            if (it.hasNext()) {
                it.next(); // header
            }
            while (it.hasNext() && (maxRows <= 0 || events.size() < maxRows)) {
                String[] f = it.next().split(",");
                int vid = Integer.parseInt(f[1].trim());
                long ms = OffsetDateTime.parse(f[5].trim(), TS).toInstant().toEpochMilli();
                Pointer g4326 = GeneratedFunctions.geo_transform(
                        GeneratedFunctions.geom_in(f[4].trim(), -1), 4326);
                String txt = GeneratedFunctions.geo_as_text(g4326, 7); // POINT(lon lat)
                String[] xy = txt.substring(txt.indexOf('(') + 1, txt.indexOf(')')).trim().split("\\s+");
                events.add(make(vid, ms, Double.parseDouble(xy[0]), Double.parseDouble(xy[1])));
            }
        }
        events.sort((a, b) -> Long.compare(a.getTimestamp(), b.getTimestamp()));
        return events;
    }

    /** Derive selective per-query parameters and a window/tick granularity that
     * yields ~200 windows over the corpus time span. */
    public static Params derive(List<BerlinMODTrip> corpus) {
        double sumLon = 0, sumLat = 0, minLon = Double.MAX_VALUE, minLat = Double.MAX_VALUE,
                maxLon = -Double.MAX_VALUE, maxLat = -Double.MAX_VALUE, minT = Double.MAX_VALUE, maxT = -Double.MAX_VALUE;
        TreeSet<Integer> ids = new TreeSet<>();
        for (BerlinMODTrip t : corpus) {
            sumLon += t.getLon(); sumLat += t.getLat();
            minLon = Math.min(minLon, t.getLon()); maxLon = Math.max(maxLon, t.getLon());
            minLat = Math.min(minLat, t.getLat()); maxLat = Math.max(maxLat, t.getLat());
            minT = Math.min(minT, t.getTimestamp()); maxT = Math.max(maxT, t.getTimestamp());
            ids.add(t.getVehicleId());
        }
        int n = corpus.size();
        double cLon = sumLon / n, cLat = sumLat / n;
        double exLon = maxLon - minLon, exLat = maxLat - minLat;
        List<Integer> idList = new ArrayList<>(ids);
        int targetId = idList.get(idList.size() / 2);
        int xId = idList.get(0);
        int yId = idList.get(Math.min(idList.size() - 1, idList.size() / 2));
        long span = (long) (maxT - minT);
        long windowSeconds = Math.max(1L, span / 1000 / 200);
        long tickMillis = Math.max(1000L, windowSeconds * 1000L / 2);
        List<PointOfInterest> pois = Arrays.asList(
                new PointOfInterest(1, cLon, cLat, 2_000.0),
                new PointOfInterest(2, cLon + 0.1 * exLon, cLat + 0.1 * exLat, 1_000.0),
                new PointOfInterest(3, cLon - 0.1 * exLon, cLat - 0.1 * exLat, 2_000.0));
        return new Params(cLon, cLat, 5_000.0, 5_000.0,
                cLon - 0.25 * exLon, cLat - 0.25 * exLat, cLon + 0.25 * exLon, cLat + 0.25 * exLat,
                minLon + 0.25 * exLon, cLat, maxLon - 0.25 * exLon, cLat,
                pois, targetId, xId, yId, windowSeconds, tickMillis);
    }

    private static BerlinMODTrip make(int vid, long t, double lon, double lat) {
        BerlinMODTrip trip = new BerlinMODTrip();
        trip.setVehicleId(vid);
        trip.setTimestamp(t);
        trip.setLon(lon);
        trip.setLat(lat);
        return trip;
    }
}
