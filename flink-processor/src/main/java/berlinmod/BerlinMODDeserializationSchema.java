package berlinmod;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * JSON → {@link BerlinMODTrip} deserializer for the Kafka {@code berlinmod} topic.
 *
 * <p>Expected JSON shape per record:
 * <pre>
 *   { "t": "2007-05-28 06:00:00", "vehicle_id": 42, "lon": 4.36, "lat": 50.84 }
 * </pre>
 *
 * <p>The timestamp format is the same {@code yyyy-MM-dd HH:mm:ss} the BerlinMOD
 * generator emits in {@code generate_berlinmod_trips.sql}; we parse it as UTC
 * to match the AIS pipeline's event-time convention.
 */
public class BerlinMODDeserializationSchema implements DeserializationSchema<BerlinMODTrip> {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final DateTimeFormatter TS_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public BerlinMODDeserializationSchema() {
        OBJECT_MAPPER.configure(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS, true);
        OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Override
    public BerlinMODTrip deserialize(byte[] message) throws IOException {
        JsonNode node = OBJECT_MAPPER.readTree(message);
        BerlinMODTrip trip = new BerlinMODTrip();
        trip.setTimestamp(parseTimestamp(node.get("t").asText()));
        trip.setVehicleId(node.get("vehicle_id").asInt());
        trip.setLon(node.get("lon").asDouble());
        trip.setLat(node.get("lat").asDouble());
        return trip;
    }

    private long parseTimestamp(String s) {
        LocalDateTime dt = LocalDateTime.parse(s, TS_FORMATTER);
        return dt.atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();
    }

    @Override
    public boolean isEndOfStream(BerlinMODTrip nextElement) {
        return false;
    }

    @Override
    public TypeInformation<BerlinMODTrip> getProducedType() {
        return TypeExtractor.getForClass(BerlinMODTrip.class);
    }
}
