package berlinmod;

import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * BerlinMOD-Q7 — <b>windowed form</b>.
 *
 * <p><i>"Per N-second tumbling window, for each (vehicle, POI), what was the
 * vehicle's first event in the window that placed it inside the POI's
 * radius?"</i>
 *
 * <p>Intra-window scoping (no cross-window first-passage state). Per window:
 * group events by vehicleId, sort by time, walk and for each POI emit the
 * timestamp of the first event in the window where the vehicle is inside
 * that POI.
 */
public class Q7WindowedFunction
        extends ProcessAllWindowFunction<BerlinMODTrip, Tuple5<Long, Long, Integer, Integer, Long>, TimeWindow> {

    private final List<PointOfInterest> pois;

    public Q7WindowedFunction(List<PointOfInterest> pois) {
        this.pois = pois;
    }

    @Override
    public void process(
            Context ctx,
            Iterable<BerlinMODTrip> elements,
            Collector<Tuple5<Long, Long, Integer, Integer, Long>> out) {
        Map<Integer, List<BerlinMODTrip>> perVehicle = new HashMap<>();
        for (BerlinMODTrip trip : elements) {
            perVehicle.computeIfAbsent(trip.getVehicleId(), k -> new ArrayList<>()).add(trip);
        }
        // For deterministic output, iterate vehicles in id order
        List<Integer> vehicleIds = new ArrayList<>(perVehicle.keySet());
        vehicleIds.sort(Comparator.naturalOrder());
        for (Integer vid : vehicleIds) {
            List<BerlinMODTrip> sorted = perVehicle.get(vid);
            sorted.sort((a, b) -> Long.compare(a.getTimestamp(), b.getTimestamp()));
            Set<Integer> emittedPois = new HashSet<>();
            for (BerlinMODTrip trip : sorted) {
                for (PointOfInterest poi : pois) {
                    if (emittedPois.contains(poi.id)) {
                        continue;
                    }
                    if (MEOSBridge.dwithinMetres(trip.getLon(), trip.getLat(),
                                               poi.lon, poi.lat, poi.radiusMetres)) {
                        emittedPois.add(poi.id);
                        out.collect(new Tuple5<>(
                                ctx.window().getStart(), ctx.window().getEnd(),
                                vid, poi.id, trip.getTimestamp()));
                    }
                }
            }
        }
    }
}
