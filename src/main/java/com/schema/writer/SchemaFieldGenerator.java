package com.schema.writer;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class SchemaFieldGenerator {
    private final Random random = new Random();

    public Object generateRandomValue(Schema schema) {
        switch (schema.getType()) {
            case NULL:
                return null;
            case BOOLEAN:
                return random.nextBoolean();
            case INT:
                if (LogicalTypes.date().equals(schema.getLogicalType())) {
                    return generateRandomDate();
                }
                return random.nextInt(1000);
            case LONG:
                if (LogicalTypes.timeMicros().equals(schema.getLogicalType())) {
                    return generateRandomTime();
                } else if (LogicalTypes.timestampMicros().equals(schema.getLogicalType())) {
                    return generateRandomTimestamp();
                }
                return random.nextLong(1000);
            case FLOAT:
                return random.nextFloat();
            case DOUBLE:
                return random.nextDouble();
            case STRING:
                return generateMeaningfulString("default");
            case BYTES:
                if (schema.getLogicalType() instanceof LogicalTypes.Decimal) {
                    return generateRandomDecimal((LogicalTypes.Decimal) schema.getLogicalType());
                }
                return generateRandomBytes();
            case ENUM:
                return generateRandomEnum(schema);
            case ARRAY:
                return generateRandomArray(schema);
            case MAP:
                return generateRandomMap(schema);
            case RECORD:
                return generateRandomRecord(schema);
            case FIXED:
                return generateRandomFixed(schema);
            case UNION:
                return generateRandomUnion(schema);
            default:
                throw new IllegalArgumentException("Unsupported schema type: " + schema.getType());
        }
    }

    private String generateMeaningfulString(String fieldName) {
        switch (fieldName.toLowerCase()) {
            case "tenant_id":
                return "tenant_" + String.format("%04d", random.nextInt(1000));
            case "event_name":
                String[] eventNames = {"user_login", "page_view", "button_click", "form_submit", "purchase"};
                return eventNames[random.nextInt(eventNames.length)];
            case "event_id":
                return "evt_" + UUID.randomUUID().toString().substring(0, 8);
            case "session_id":
                return "sess_" + UUID.randomUUID().toString().substring(0, 8);
            case "scribe_client_type":
                String[] clientTypes = {"web", "mobile", "desktop", "api"};
                return clientTypes[random.nextInt(clientTypes.length)];
            case "scribe_api_version":
                String[] versions = {"v1.0.0", "v1.1.0", "v2.0.0"};
                return versions[random.nextInt(versions.length)];
            case "channel":
                String[] channels = {"web", "mobile_app", "email", "sms", "push"};
                return channels[random.nextInt(channels.length)];
            case "event_type":
                String[] eventTypes = {"track", "page", "screen", "identify", "group"};
                return eventTypes[random.nextInt(eventTypes.length)];
            default:
                return UUID.randomUUID().toString();
        }
    }

    private ByteBuffer generateRandomBytes() {
        byte[] bytes = new byte[10];
        random.nextBytes(bytes);
        return ByteBuffer.wrap(bytes);
    }

    private GenericData.EnumSymbol generateRandomEnum(Schema schema) {
        List<String> symbols = schema.getEnumSymbols();
        String symbol = symbols.get(random.nextInt(symbols.size()));
        return new GenericData.EnumSymbol(schema, symbol);
    }

    private List<Object> generateRandomArray(Schema schema) {
        int size = random.nextInt(5) + 1;
        List<Object> array = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            array.add(generateRandomValue(schema.getElementType()));
        }
        return array;
    }

    private Map<String, Object> generateRandomMap(Schema schema) {
        int size = random.nextInt(3) + 1; // Reduced size for more realistic data
        Map<String, Object> map = new HashMap<>(size);
        if (schema.getName() != null && schema.getName().equals("custom_params_map")) {
            // Generate meaningful custom parameters
            String[] possibleKeys = {"source", "medium", "campaign", "content", "term"};
            String[] possibleValues = {"google", "facebook", "email", "spring_sale", "product_launch"};
            for (int i = 0; i < size; i++) {
                String key = possibleKeys[random.nextInt(possibleKeys.length)];
                String value = possibleValues[random.nextInt(possibleValues.length)];
                map.put(key, value);
            }
        } else {
            // Default map generation
            for (int i = 0; i < size; i++) {
                map.put("key" + i, generateRandomValue(schema.getValueType()));
            }
        }
        return map;
    }

    public GenericData.Record generateRandomRecord(Schema schema) {
        GenericData.Record record = new GenericData.Record(schema);
        for (Schema.Field field : schema.getFields()) {
            if (field.schema().getType() == Schema.Type.STRING) {
                record.put(field.name(), generateMeaningfulString(field.name()));
            } else {
                record.put(field.name(), generateRandomValue(field.schema()));
            }
        }
        return record;
    }

    private GenericData.Fixed generateRandomFixed(Schema schema) {
        byte[] bytes = new byte[schema.getFixedSize()];
        random.nextBytes(bytes);
        return new GenericData.Fixed(schema, bytes);
    }

    private Object generateRandomUnion(Schema schema) {
        List<Schema> types = schema.getTypes();
        return generateRandomValue(types.get(random.nextInt(types.size())));
    }

    private int generateRandomDate() {
        LocalDate startDate = LocalDate.of(2020, 1, 1);
        long randomDays = ThreadLocalRandom.current().nextLong(365 * 2); // 2 years range
        return (int) startDate.plusDays(randomDays).toEpochDay();
    }

    private long generateRandomTime() {
        LocalTime time = LocalTime.of(
            random.nextInt(24),
            random.nextInt(60),
            random.nextInt(60),
            random.nextInt(1000000)
        );
        return time.toNanoOfDay() / 1000;
    }

    private long generateRandomTimestamp() {
        Instant now = Instant.now();
        long randomSeconds = ThreadLocalRandom.current().nextLong(
            now.minusSeconds(365 * 24 * 60 * 60).getEpochSecond(),
            now.getEpochSecond()
        );
        return randomSeconds * 1_000_000 + random.nextInt(1000000);
    }

    private ByteBuffer generateRandomDecimal(LogicalTypes.Decimal decimal) {
        BigDecimal value = BigDecimal.valueOf(random.nextDouble() * Math.pow(10, decimal.getPrecision()))
            .setScale(decimal.getScale(), BigDecimal.ROUND_HALF_UP);
        return ByteBuffer.wrap(value.unscaledValue().toByteArray());
    }
}
