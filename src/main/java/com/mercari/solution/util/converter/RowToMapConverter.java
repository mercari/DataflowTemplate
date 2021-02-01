package com.mercari.solution.util.converter;

import com.google.cloud.Date;
import com.mercari.solution.util.RowSchemaUtil;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;
import org.joda.time.ReadableDateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;

public class RowToMapConverter {

    private static final DateTimeFormatter FORMATTER_HH_MM_SS   = DateTimeFormat.forPattern("HH:mm:ss");

    public static Map<String, Object> convert(final Row row) {
        final Map<String, Object> map = new HashMap<>();
        if(row == null) {
            return map;
        }
        for(final Schema.Field field : row.getSchema().getFields()) {
            map.put(field.getName(), getValue(field.getType(), row.getValue(field.getName())));
        }
        return map;
    }

    private static Object getValue(final Schema.FieldType type, final Object value) {
        if(value == null) {
            return null;
        }
        switch (type.getTypeName()) {
            case BYTE:
            case INT16:
            case INT32:
            case INT64:
            case BOOLEAN:
            case FLOAT:
            case DOUBLE:
            case STRING:
            case DECIMAL:
                return value;
            case BYTES:
                return Base64.getEncoder().encodeToString(((ByteBuffer) value).array());
            case DATETIME:
                return ((ReadableDateTime) value).toInstant();
            case LOGICAL_TYPE: {
                if(RowSchemaUtil.isLogicalTypeDate(type)) {
                    final LocalDate localDate = (LocalDate) value;
                    return Date.fromYearMonthDay(
                            localDate.getYear(), localDate.getMonthValue(), localDate.getDayOfMonth());
                } else if(RowSchemaUtil.isLogicalTypeTime(type)) {
                    return ((Instant) value).toString(FORMATTER_HH_MM_SS);
                } else if(RowSchemaUtil.isLogicalTypeTimestamp(type)) {
                    return ((ReadableDateTime) value).toInstant();
                } else {
                    throw new IllegalArgumentException("Unsupported Beam logical type: " + type.getLogicalType().getIdentifier());
                }
            }
            case ROW:
                return convert((Row) value);
            case ITERABLE:
            case ARRAY:
                return ((List<Object>) value).stream()
                        .map(o -> getValue(type.getCollectionElementType(), o))
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());
            case MAP:
            default:
                return null;
        }
    }

}
