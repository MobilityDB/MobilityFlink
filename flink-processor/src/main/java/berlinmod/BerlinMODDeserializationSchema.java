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
