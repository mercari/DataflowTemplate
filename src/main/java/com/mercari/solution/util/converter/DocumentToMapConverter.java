package com.mercari.solution.util.converter;

import com.google.firestore.v1.Value;
import com.google.firestore.v1.Document;
import com.google.protobuf.Timestamp;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class DocumentToMapConverter {

    public static Map<String, Object> convert(final Document document) {
        if(document == null) {
            return new HashMap<>();
        }
        return convert(document.getFieldsMap());
    }

    public static Map<String, Object> convert(final Map<String, Value> values) {
        final Map<String, Object> map = new HashMap<>();
        if(values == null) {
            return map;
        }
        for(final Map.Entry<String, Value> entry : values.entrySet()) {
            map.put(entry.getKey(), getValue(entry.getValue()));
        }
        return map;
    }

    private static Object getValue(final Value value) {
        if(value == null) {
            return null;
        }
        switch (value.getValueTypeCase()) {
            case BOOLEAN_VALUE:
                return value.getBooleanValue();
            case STRING_VALUE:
                return value.getStringValue();
            case BYTES_VALUE:
                return value.getBytesValue().asReadOnlyByteBuffer();
            case INTEGER_VALUE:
                return value.getIntegerValue();
            case DOUBLE_VALUE:
                return value.getDoubleValue();
            case TIMESTAMP_VALUE: {
                final Timestamp timestamp = value.getTimestampValue();
                return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
            }
            case MAP_VALUE:
                return convert(value.getMapValue().getFieldsMap());
            case ARRAY_VALUE: {
                return value.getArrayValue().getValuesList().stream()
                        .map(DocumentToMapConverter::getValue)
                        .collect(Collectors.toList());
            }
            case REFERENCE_VALUE:
            case NULL_VALUE:
            case VALUETYPE_NOT_SET:
            case GEO_POINT_VALUE:
            default:
                return null;
        }
    }

}
