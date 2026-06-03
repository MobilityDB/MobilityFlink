package sncbdata;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Flink {@link DeserializationSchema} for SNCB train telemetry JSON messages.
 *
 * <p>Parses JSON objects produced by the Python Kafka producer. The producer
 * serializes each row of {@code input_sncb.csv} as a JSON object with named keys.
 *
 * <p>Expected JSON format (produced by {@code python-producer.py}):
 * <pre>
 * {
 *   "t":        1722520348.0,  // time_utc (Unix epoch seconds, float)
 *   "deviceId": 3.0,           // device_id (sent as float by pandas)
 *   "vbat":     29.4,
 *   "pcfaMbar": 4.376,         // PCFA_mbar — automatic brake pipe pressure
 *   "pcffMbar": 1.451,         // PCFF_mbar — fictitious brake cylinder pressure
 *   "pcf1Mbar": 0.003,
 *   "pcf2Mbar": 1.316,
 *   "t1mbar":   46.18,
 *   "t2mbar":   4.518,
 *   "code1":    0.0,
 *   "code2":    1296.0,
 *   "gpsSpeed": 1.4671,        // km/h
 *   "lat":      50.6456,
 *   "lon":      4.3658
 * }
 * </pre>
 *
 * <p>Note: {@code "t"} is a float Unix epoch in seconds → multiplied by 1000
 * and cast to long to obtain milliseconds for Flink event-time watermarks.
 * {@code "deviceId"} is sent as float by pandas (e.g. 3.0) → cast to int.
 */
public class SNCBDataDeserializationSchema implements DeserializationSchema<SNCBData> {

    private static final Logger log = LoggerFactory.getLogger(SNCBDataDeserializationSchema.class);

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public SNCBDataDeserializationSchema() {
        objectMapper.configure(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS, true);
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Override
    public SNCBData deserialize(byte[] message) throws IOException {
        try {
            JsonNode node = objectMapper.readTree(message);
            SNCBData data = new SNCBData();

            // "t" is a float Unix epoch in seconds (e.g. 1722520348.0) → millis
            data.setTimestamp((long) (node.get("t").asDouble() * 1000L));

            // "deviceId" is sent as float (e.g. 3.0) → cast to int
            data.setDeviceId((int) node.get("deviceId").asDouble());

            data.setVbat(node.get("vbat").asDouble());
            data.setPcfaMbar(node.get("pcfaMbar").asDouble());
            data.setPcffMbar(node.get("pcffMbar").asDouble());
            data.setPcf1Mbar(node.get("pcf1Mbar").asDouble());
            data.setPcf2Mbar(node.get("pcf2Mbar").asDouble());
            data.setT1Mbar(node.get("t1mbar").asDouble());
            data.setT2Mbar(node.get("t2mbar").asDouble());
            data.setCode1(node.get("code1").asDouble());
            data.setCode2(node.get("code2").asDouble());
            data.setGpsSpeed(node.get("gpsSpeed").asDouble());
            data.setLat(node.get("lat").asDouble());
            data.setLon(node.get("lon").asDouble());

            return data;

        } catch (Exception e) {
            log.error("Failed to parse SNCB JSON message: {} — {}",
                    new String(message), e.getMessage());
            return null;
        }
    }

    @Override
    public boolean isEndOfStream(SNCBData nextElement) {
        return false;
    }

    @Override
    public TypeInformation<SNCBData> getProducedType() {
        return TypeExtractor.getForClass(SNCBData.class);
    }
}