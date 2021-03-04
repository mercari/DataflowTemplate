package com.mercari.solution.util.converter;

import com.mercari.solution.util.schema.AvroSchemaUtil;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.*;
import java.util.stream.Collectors;

public class RecordToMapConverter {

    public static Map<String, Object> convert(final GenericRecord record) {
        final Map<String, Object> map = new HashMap<>();
        if(record == null) {
            return map;
        }
        for(final Schema.Field field : record.getSchema().getFields()) {
            map.put(field.name(), getValue(field.schema(), record.get(field.name())));
        }
        return map;
    }

    private static Object getValue(final Schema schema, final Object value) {
        if(value == null) {
            return null;
        }
        final Schema unnestedSchema = AvroSchemaUtil.unnestUnion(schema);
        switch (unnestedSchema.getType()) {
            case INT: {
                final Integer intValue = (Integer) value;
                if (LogicalTypes.date().equals(unnestedSchema.getLogicalType())) {
                    return LocalDate.ofEpochDay(intValue);
                } else if (LogicalTypes.timeMillis().equals(unnestedSchema.getLogicalType())) {
                    return LocalTime.ofNanoOfDay(new Long(intValue) * 1000 * 1000);
                } else {
                    return intValue;
                }
            }
            case LONG: {
                final Long longValue = (Long) value;
                if (LogicalTypes.timestampMillis().equals(unnestedSchema.getLogicalType())) {
                    return java.time.Instant.ofEpochMilli(longValue);
                } else if (LogicalTypes.timestampMicros().equals(unnestedSchema.getLogicalType())) {
                    return java.time.Instant.ofEpochMilli(longValue / 1000);
                } else if (LogicalTypes.timeMicros().equals(unnestedSchema.getLogicalType())) {
                    return LocalTime.ofNanoOfDay(longValue * 1000);
                } else {
                    return longValue;
                }
            }
            case BOOLEAN:
            case FLOAT:
            case DOUBLE:
                return value;
            case ENUM:
            case STRING:
                return value.toString();
            case FIXED:
            case BYTES:
                return Base64.getEncoder().encodeToString(((ByteBuffer) value).array());
            case RECORD:
                return convert((GenericRecord) value);
            case ARRAY:
                return ((List<Object>) value).stream()
                        .map(o -> getValue(schema.getElementType(), o))
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());
            case MAP:
            case UNION:
            case NULL:
            default:
                return null;
        }
    }

}
