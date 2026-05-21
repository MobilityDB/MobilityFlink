package berlinmod;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * BerlinMOD-Q5 — <b>windowed form</b>.
 *
 * <p><i>"Per N-second tumbling window, which pairs of vehicles met near P
 * (using each vehicle's last-seen-in-window position)?"</i>
 *
 * <p>Within each window, take each vehicle's latest position from the
 * window's events. Run the same near-P-and-within-meet-distance pair check
 * as the continuous form. Emit {@code (windowStart, windowEnd, a, b,
 * distanceMetres)} per meeting pair.
 */
public class Q5WindowedFunction
        extends ProcessAllWindowFunction<BerlinMODTrip, Tuple5<Long, Long, Integer, Integer, Double>, TimeWindow> {

    private final double pLon, pLat, dPMetres, dMeetMetres;

    public Q5WindowedFunction(double pLon, double pLat, double dPMetres, double dMeetMetres) {
        this.pLon = pLon;
        this.pLat = pLat;
        this.dPMetres = dPMetres;
        this.dMeetMetres = dMeetMetres;
    }

    @Override
    public void process(
            Context ctx,
            Iterable<BerlinMODTrip> elements,
            Collector<Tuple5<Long, Long, Integer, Integer, Double>> out) {
        // Last position per vehicle within the window
        Map<Integer, BerlinMODTrip> latest = new HashMap<>();
        for (BerlinMODTrip trip : elements) {
            BerlinMODTrip prev = latest.get(trip.getVehicleId());
            if (prev == null || trip.getTimestamp() > prev.getTimestamp()) {
                latest.put(trip.getVehicleId(), trip);
            }
        }

        // Filter to vehicles near P
        List<Map.Entry<Integer, Tuple2<Double, Double>>> nearP = new ArrayList<>();
        for (Map.Entry<Integer, BerlinMODTrip> e : latest.entrySet()) {
            BerlinMODTrip t = e.getValue();
            if (MEOSBridge.dwithinMetres(t.getLon(), t.getLat(), pLon, pLat, dPMetres)) {
                nearP.add(new HashMap.SimpleEntry<>(e.getKey(), new Tuple2<>(t.getLon(), t.getLat())));
            }
        }
        nearP.sort(Comparator.comparingInt(Map.Entry::getKey));

        for (int i = 0; i < nearP.size(); i++) {
            for (int j = i + 1; j < nearP.size(); j++) {
                Tuple2<Double, Double> a = nearP.get(i).getValue();
                Tuple2<Double, Double> b = nearP.get(j).getValue();
                double d = MEOSBridge.distanceMetres(a.f0, a.f1, b.f0, b.f1);
                if (d <= dMeetMetres) {
                    out.collect(new Tuple5<>(
                            ctx.window().getStart(), ctx.window().getEnd(),
                            nearP.get(i).getKey(), nearP.get(j).getKey(), d));
                }
            }
        }
    }
}
