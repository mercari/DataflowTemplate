package com.mercari.solution.util.converter;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import org.joda.time.Instant;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class StructToMapConverter {

    public static Map<String, Object> convert(final Struct struct) {
        return convertWithFields(struct, null);
    }

    public static Map<String, Object> convertWithFields(final Struct struct, final Collection<String> fields) {
        final Map<String, Object> map = new HashMap<>();
        if(struct == null) {
            return map;
        }
        for(final Type.StructField field : struct.getType().getStructFields()) {
            if(fields == null || fields.contains(field.getName())) {
                map.put(field.getName(), getValue(field, struct));
            }
        }
        return map;
    }

    private static Object getValue(final Type.StructField field, final Struct struct) {
        if(struct.isNull(field.getName())) {
            return null;
        }
        switch (field.getType().getCode()) {
            case BOOL:
                return struct.getBoolean(field.getName());
            case BYTES:
                return struct.getBytes(field.getName()).toBase64();
            case STRING:
                return struct.getString(field.getName());
            case INT64:
                return struct.getLong(field.getName());
            case FLOAT64:
                return struct.getDouble(field.getName());
            case NUMERIC:
                return struct.getBigDecimal(field.getName()).toPlainString();
            case DATE: {
                final Date date = struct.getDate(field.getName());
                return LocalDate.of(date.getYear(), date.getMonth(), date.getDayOfMonth());
            }
            case TIMESTAMP: {
                final Timestamp timestamp = struct.getTimestamp(field.getName());
                return Instant.ofEpochMilli(timestamp.toSqlTimestamp().toInstant().toEpochMilli());
            }
            case STRUCT:
                return convert(struct);
            case ARRAY: {
                switch (field.getType().getArrayElementType().getCode()) {
                    case BOOL:
                        return struct.getBooleanList(field.getName());
                    case STRING:
                        return struct.getStringList(field.getName());
                    case BYTES:
                        return struct.getBytesList(field.getName())
                                .stream()
                                .map(ByteArray::toBase64)
                                .collect(Collectors.toList());
                    case INT64:
                        return struct.getLongList(field.getName());
                    case FLOAT64:
                        return struct.getDoubleList(field.getName());
                    case DATE:
                        return struct.getDateList(field.getName())
                                .stream()
                                .map(date -> LocalDate.of(date.getYear(), date.getMonth(), date.getDayOfMonth()))
                                .collect(Collectors.toList());
                    case TIMESTAMP:
                        return struct.getTimestampList(field.getName())
                                .stream()
                                .map(timestamp -> Instant.ofEpochMilli(timestamp.toSqlTimestamp().toInstant().toEpochMilli()))
                                .collect(Collectors.toList());
                    case NUMERIC:
                        return struct.getBigDecimalList(field.getName())
                                .stream()
                                .map(BigDecimal::toPlainString)
                                .collect(Collectors.toList());
                    case STRUCT:
                        return struct.getStructList(field.getName())
                                .stream()
                                .map(StructToMapConverter::convert)
                                .collect(Collectors.toList());
                    case ARRAY:
                        throw new IllegalStateException("Array in Array is not supported!");
                    default:
                        return null;
                }
            }
            default:
                return null;
        }
    }
}
