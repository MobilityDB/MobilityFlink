package berlinmod;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * BerlinMOD-Q7 — <b>continuous form</b>.
 *
 * <p><i>"For each (vehicle, POI) pair, when did the vehicle first come within
 * the POI's radius?"</i>
 *
 * <p>Keyed by vehicleId. State is a per-vehicle {@code MapState<poiId,
 * firstPassageTime>}. On each event, walk the POI list; if the vehicle is
 * within a POI's radius AND no first-passage has been recorded for that
 * (vehicle, POI), record it and emit {@code (vehicleId, poiId, firstPassageTime)}.
 */
public class Q7ContinuousFunction
        extends KeyedProcessFunction<Integer, BerlinMODTrip, Tuple3<Integer, Integer, Long>> {

    private final List<PointOfInterest> pois;
    private transient MapState<Integer, Long> firstPassed;

    public Q7ContinuousFunction(List<PointOfInterest> pois) {
        this.pois = pois;
    }

    @Override
    public void open(Configuration parameters) {
        firstPassed = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("q7FirstPassed",
                        TypeInformation.of(Integer.class),
                        TypeInformation.of(Long.class)));
    }

    @Override
    public void processElement(
            BerlinMODTrip trip,
            Context ctx,
            Collector<Tuple3<Integer, Integer, Long>> out) throws Exception {
        for (PointOfInterest poi : pois) {
            if (firstPassed.contains(poi.id)) {
                continue;
            }
            if (MEOSBridge.dwithinMetres(trip.getLon(), trip.getLat(), poi.lon, poi.lat, poi.radiusMetres)) {
                firstPassed.put(poi.id, trip.getTimestamp());
                out.collect(new Tuple3<>(trip.getVehicleId(), poi.id, trip.getTimestamp()));
            }
        }
    }
}
