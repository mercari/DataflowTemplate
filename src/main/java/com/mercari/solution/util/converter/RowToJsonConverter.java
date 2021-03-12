package com.mercari.solution.util.converter;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.mercari.solution.util.schema.RowSchemaUtil;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;
import org.joda.time.ReadableInstant;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

public class RowToJsonConverter {

    private RowToJsonConverter() {}

    public static String convert(final Row row, final List<String> fields) {
        return convert(row);
    }

    public static String convert(final Row row) {
        return convertRow(row).toString();

    }

    private static JsonObject convertRow(final Row row) {
        final JsonObject obj = new JsonObject();
        if(row == null) {
            return obj;
        }
        row.getSchema().getFields().forEach(f -> setValue(obj, f, row));
        return obj;
    }

    private static void setValue(final JsonObject obj, final Schema.Field field, final Row row) {
        final String fieldName = field.getName();
        final boolean isNullField = row.getValue(fieldName) == null;
        switch (field.getType().getTypeName()) {
            case BOOLEAN:
                obj.addProperty(fieldName, isNullField ? null : row.getBoolean(fieldName));
                break;
            case BYTE:
                obj.addProperty(fieldName, isNullField ? null : row.getByte(fieldName));
                break;
            case INT16:
                obj.addProperty(fieldName, isNullField ? null : row.getInt16(fieldName));
                break;
            case INT32:
                obj.addProperty(fieldName, isNullField ? null : row.getInt32(fieldName));
                break;
            case INT64:
                obj.addProperty(fieldName, isNullField ? null : row.getInt64(fieldName));
                break;
            case FLOAT:
                obj.addProperty(fieldName, isNullField ? null : row.getFloat(fieldName));
                break;
            case DOUBLE:
                obj.addProperty(fieldName, isNullField ? null : row.getDouble(fieldName));
                break;
            case STRING:
                obj.addProperty(fieldName, isNullField ? null : row.getString(fieldName));
                break;
            case BYTES:
                obj.addProperty(fieldName, isNullField ? null : java.util.Base64.getEncoder().encodeToString(row.getBytes(fieldName)));
                break;
            case DATETIME:
                obj.addProperty(fieldName, isNullField ? null : row.getDateTime(fieldName).toString());
                break;
            case DECIMAL:
                obj.addProperty(fieldName, isNullField ? null : row.getDecimal(fieldName).toString());
                break;
            case LOGICAL_TYPE: {
                if(RowSchemaUtil.isLogicalTypeDate(field.getType())) {
                    final LocalDate localDate = row.getLogicalTypeValue(fieldName, LocalDate.class);
                    obj.addProperty(fieldName, isNullField ? null : localDate.toString());
                } else if(RowSchemaUtil.isLogicalTypeTime(field.getType())) {
                    final LocalTime localTime = row.getLogicalTypeValue(fieldName, LocalTime.class);
                    obj.addProperty(fieldName, isNullField ? null : localTime.toString());
                } else if(RowSchemaUtil.isLogicalTypeTimestamp(field.getType())) {
                    final Instant instant = row.getLogicalTypeValue(fieldName, Instant.class);
                    obj.addProperty(fieldName, isNullField ? null : instant.toString());
                }
                break;
            }
            case ROW:
                obj.add(fieldName, isNullField ? null : convertRow(row.getRow(fieldName)));
                break;
            case ITERABLE:
            case ARRAY:
                obj.add(field.getName(), convertArray(field, row.getArray(fieldName)));
                break;
            case MAP:
            default:
                break;
        }
    }

    private static JsonArray convertArray(final Schema.Field field, final Collection<?> arrayValue) {
        final JsonArray array = new JsonArray();
        if(arrayValue == null || arrayValue.size() == 0) {
            return array;
        }
        switch (field.getType().getCollectionElementType().getTypeName()) {
            case BOOLEAN:
                arrayValue.stream()
                        .filter(Objects::nonNull)
                        .map(v -> (Boolean) v)
                        .forEach(array::add);
                break;
            case BYTE:
                arrayValue.stream()
                        .filter(Objects::nonNull)
                        .map(v -> (Byte) v)
                        .forEach(array::add);
                break;
            case INT16:
                arrayValue.stream()
                        .filter(Objects::nonNull)
                        .map(v -> (Short) v)
                        .forEach(array::add);
                break;
            case INT32:
                arrayValue.stream()
                        .filter(Objects::nonNull)
                        .map(v -> (Integer) v)
                        .forEach(array::add);
                break;
            case INT64:
                arrayValue.stream()
                        .filter(Objects::nonNull)
                        .map(v -> (Long) v)
                        .forEach(array::add);
                break;
            case FLOAT:
                arrayValue.stream()
                        .filter(Objects::nonNull)
                        .map(v -> (Float) v)
                        .forEach(array::add);
                break;
            case DOUBLE:
                arrayValue.stream()
                        .filter(Objects::nonNull)
                        .map(v -> (Double) v)
                        .forEach(array::add);
                break;
            case STRING:
                arrayValue.stream()
                        .filter(Objects::nonNull)
                        .map(Object::toString)
                        .forEach(array::add);
                break;
            case BYTES:
                arrayValue.stream()
                        .filter(Objects::nonNull)
                        .map((bytes) -> java.util.Base64.getEncoder().encodeToString((byte[])bytes))
                        .forEach(array::add);
                break;
            case DATETIME:
                arrayValue.stream()
                        .filter(Objects::nonNull)
                        .map(v -> (ReadableInstant) v)
                        .map(Object::toString)
                        .forEach(array::add);
                break;
            case DECIMAL:
                arrayValue.stream()
                        .filter(Objects::nonNull)
                        .map(v -> (BigDecimal) v)
                        .map(BigDecimal::doubleValue)
                        .forEach(array::add);
                break;
            case LOGICAL_TYPE: {
                arrayValue.stream()
                        .filter(Objects::nonNull)
                        .map(Object::toString)
                        .forEach(array::add);
                break;
            }
            case ROW:
                arrayValue.stream()
                        .map(o -> (Row)o)
                        .map(RowToJsonConverter::convertRow)
                        .forEach(array::add);
                break;
            case ITERABLE:
            case ARRAY:
            case MAP:
            default:
                break;
        }
        return array;
    }

}
