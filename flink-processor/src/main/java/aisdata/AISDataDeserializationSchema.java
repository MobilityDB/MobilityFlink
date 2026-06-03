package aisdata;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;


public class AISDataDeserializationSchema implements DeserializationSchema<AISData> {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public AISDataDeserializationSchema() {
        objectMapper.configure(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS, true);
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Override
    public AISData deserialize(byte[] message) throws IOException {
        JsonNode jsonNode = objectMapper.readTree(message);
        AISData data = new AISData();

        data.setTimestamp(parseTimestamp(jsonNode.get("t").asText()));
        data.setMmsi(jsonNode.get("mmsi").asInt());
        data.setLon(jsonNode.get("lon").asDouble());
        data.setLat(jsonNode.get("lat").asDouble());
        data.setSpeed(jsonNode.get("speed").asDouble());
        data.setCourse(jsonNode.get("course").asDouble());

        return data;
    }

    private long parseTimestamp(String timestampStr) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime dateTime = LocalDateTime.parse(timestampStr, formatter);
        return dateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
    }


    @Override
    public boolean isEndOfStream(AISData nextElement) {
        return false;
    }

    @Override
    public TypeInformation<AISData> getProducedType() {
        return TypeExtractor.getForClass(AISData.class);
    }
}
