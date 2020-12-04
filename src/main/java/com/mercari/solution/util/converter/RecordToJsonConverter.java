package com.mercari.solution.util.converter;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.mercari.solution.util.AvroSchemaUtil;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.List;
import java.util.Objects;

public class RecordToJsonConverter {

    public static String convert(final GenericRecord record, final List<String> fields) {
        return convertRecord(record, fields).toString();
    }

    public static String convert(final GenericRecord record) {
        return convert(record, null);
    }

    private static JsonObject convertRecord(final GenericRecord record) {
        return convertRecord(record, null);
    }

    private static JsonObject convertRecord(final GenericRecord record, final List<String> fields) {
        final JsonObject obj = new JsonObject();
        if(record == null) {
            return obj;
        }
        if(fields != null && fields.size() > 0) {
            record.getSchema().getFields().stream()
                    .filter(f -> fields.contains(f.name()))
                    .forEach(f -> setValue(obj, f, record));
        } else {
            record.getSchema().getFields()
                    .forEach(f -> setValue(obj, f, record));
        }
        return obj;
    }

    private static void setValue(final JsonObject obj, final Schema.Field field, final GenericRecord record) {
        final String fieldName = field.name();
        final Object value = record.get(fieldName);
        final boolean isNullField = value == null;
        switch (AvroSchemaUtil.unnestUnion(field.schema()).getType()) {
            case BOOLEAN:
                obj.addProperty(fieldName, (Boolean) value);
                break;
            case INT:
                if (LogicalTypes.date().equals(field.schema().getLogicalType())) {
                    obj.addProperty(fieldName, isNullField ? null : LocalDate.ofEpochDay((int) value).format(DateTimeFormatter.ISO_LOCAL_DATE));
                } else if (LogicalTypes.timeMillis().equals(field.schema().getLogicalType())) {
                    obj.addProperty(fieldName, isNullField ? null : LocalTime.ofNanoOfDay(new Long((Integer) value) * 1000 * 1000).format(DateTimeFormatter.ISO_LOCAL_TIME));
                } else {
                    obj.addProperty(fieldName, (Integer) value);
                }
                break;
            case LONG: {
                final Long longValue = (Long) value;
                if (LogicalTypes.timestampMillis().equals(field.schema().getLogicalType())) {
                    obj.addProperty(fieldName, isNullField ? null : DateTimeFormatter.ISO_INSTANT.format(java.time.Instant.ofEpochMilli(longValue)));
                } else if (LogicalTypes.timestampMicros().equals(field.schema().getLogicalType())) {
                    obj.addProperty(fieldName, isNullField ? null : DateTimeFormatter.ISO_INSTANT.format(java.time.Instant.ofEpochMilli(longValue / 1000)));
                } else if (LogicalTypes.timeMicros().equals(field.schema().getLogicalType())) {
                    obj.addProperty(fieldName, isNullField ? null : LocalTime.ofNanoOfDay(longValue * 1000).format(DateTimeFormatter.ISO_LOCAL_TIME));
                } else {
                    obj.addProperty(fieldName, longValue);
                }
                break;
            }
            case FLOAT:
                obj.addProperty(fieldName, (Float) value);
                break;
            case DOUBLE:
                obj.addProperty(fieldName, (Double) value);
                break;
            case ENUM:
            case STRING:
                obj.addProperty(fieldName, isNullField ? null : value.toString());
                break;
            case FIXED:
            case BYTES:
                obj.addProperty(fieldName, isNullField ? null : Base64.getEncoder().encodeToString(((ByteBuffer) value).array()));
                break;
            case RECORD:
                obj.add(fieldName, isNullField ? null : convertRecord((GenericRecord) value));
                break;
            case ARRAY:
                obj.add(fieldName, isNullField ? null : convertArray(AvroSchemaUtil.unnestUnion(field.schema().getElementType()), (List<?>) value));
                break;
            case MAP:
            case UNION:
            case NULL:
            default:
                break;
        }
    }

    private static JsonArray convertArray(final Schema schema, final List<?> arrayValue) {
        final JsonArray array = new JsonArray();
        if(arrayValue == null || arrayValue.size() == 0) {
            return array;
        }
        switch (schema.getType()) {
            case BOOLEAN:
                arrayValue.stream()
                        .filter(Objects::nonNull)
                        .map(v -> (Boolean) v)
                        .forEach(array::add);
                break;
            case INT:
                if (LogicalTypes.date().equals(schema.getLogicalType())) {
                    arrayValue.stream()
                            .filter(Objects::nonNull)
                            .map(v -> (Integer) v)
                            .map(LocalDate::ofEpochDay)
                            .map(ld -> ld.format(DateTimeFormatter.ISO_LOCAL_DATE))
                            .forEach(array::add);
                } else if (LogicalTypes.timeMillis().equals(schema.getLogicalType())) {
                    arrayValue.stream()
                            .filter(Objects::nonNull)
                            .map(v -> (Long) v * 1000 * 1000)
                            .map(LocalTime::ofNanoOfDay)
                            .map(lt -> lt.format(DateTimeFormatter.ISO_LOCAL_TIME))
                            .forEach(array::add);
                } else {
                    arrayValue.stream()
                            .filter(Objects::nonNull)
                            .map(v -> (Integer) v)
                            .forEach(array::add);
                }
                break;
            case LONG:
                if (LogicalTypes.timestampMillis().equals(schema.getLogicalType())) {
                    arrayValue.stream()
                            .filter(Objects::nonNull)
                            .map(v -> (Long) v)
                            .map(java.time.Instant::ofEpochMilli)
                            .map(DateTimeFormatter.ISO_INSTANT::format)
                            .forEach(array::add);
                } else if (LogicalTypes.timestampMicros().equals(schema.getLogicalType())) {
                    arrayValue.stream()
                            .filter(Objects::nonNull)
                            .map(v -> (Long) v / 1000)
                            .map(java.time.Instant::ofEpochMilli)
                            .map(DateTimeFormatter.ISO_INSTANT::format)
                            .forEach(array::add);
                } else if (LogicalTypes.timeMicros().equals(schema.getLogicalType())) {
                    arrayValue.stream()
                            .filter(Objects::nonNull)
                            .map(v -> (Long) v * 1000)
                            .map(LocalTime::ofNanoOfDay)
                            .map(lt -> lt.format(DateTimeFormatter.ISO_LOCAL_TIME))
                            .forEach(array::add);
                } else {
                    arrayValue.stream()
                            .filter(Objects::nonNull)
                            .map(v -> (Long) v)
                            .forEach(array::add);
                }
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
            case RECORD:
                arrayValue.stream()
                        .map(o -> (GenericRecord)o)
                        .map(RecordToJsonConverter::convertRecord)
                        .forEach(array::add);
                break;
            case ARRAY:
            case MAP:
            default:
                break;
        }
        return array;
    }
}
