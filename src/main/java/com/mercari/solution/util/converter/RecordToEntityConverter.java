package com.mercari.solution.util.converter;

import com.google.cloud.Timestamp;
import com.google.datastore.v1.*;
import com.google.protobuf.ByteString;
import com.google.protobuf.NullValue;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.gcp.DatastoreUtil;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class RecordToEntityConverter {

    private static final String KEY_FIELD_NAME = "__key__";

    public static Entity.Builder convertBuilder(final Schema schema, final GenericRecord record) {
        final Entity.Builder builder = Entity.newBuilder();
        for(final Schema.Field field : schema.getFields()) {
            if(KEY_FIELD_NAME.equals(field.name()) && field.schema().getType().equals(Schema.Type.RECORD)) {
                final GenericRecord keyRecord = (GenericRecord) record.get(KEY_FIELD_NAME);
                final Key key = createPathElement(KEY_FIELD_NAME, keyRecord);
                builder.setKey(key);
            } else {
                builder.putProperties(field.name(), convertValue(field.schema(), record.get(field.name())));
            }
        }
        return builder;
    }

    public static Entity convert(final Schema schema, final GenericRecord record,
                                 final String kind, final List<String> keyFields, final String keySplitter) {

        final Key key;
        if(keyFields != null && keyFields.size() > 0) {
            if(keyFields.size() > 1) {
                final StringBuilder sb = new StringBuilder();
                for (final String keyField : keyFields) {
                    final String keyValue = AvroSchemaUtil.getAsString(record, keyField);
                    sb.append(keyValue);
                    sb.append(keySplitter);
                }
                sb.deleteCharAt(sb.length() - keySplitter.length());
                final Key.PathElement pathElement = Key.PathElement.newBuilder().setKind(kind).setName(sb.toString()).build();
                key = Key.newBuilder().addPath(pathElement).build();
            } else {
                final String keyField = keyFields.get(0);
                switch (AvroSchemaUtil.unnestUnion(schema.getField(keyField).schema()).getType()) {
                    case INT:
                    case LONG: {
                        final Key.PathElement pathElement = Key.PathElement.newBuilder()
                                .setKind(kind)
                                .setId((long)record.get(keyField))
                                .build();
                        key = Key.newBuilder().addPath(pathElement).build();
                        break;
                    }
                    case RECORD: {
                        final GenericRecord keyRecord = (GenericRecord) record.get(keyField);
                        key = createPathElement(kind, keyRecord);
                        break;
                    }
                    default: {
                        final Key.PathElement pathElement = Key.PathElement.newBuilder()
                                .setKind(kind)
                                .setName(record.get(keyField).toString())
                                .build();
                        key = Key.newBuilder().addPath(pathElement).build();
                        break;
                    }
                }
            }
        } else if(schema.getFields().stream().anyMatch(f ->
                f.name().equals(KEY_FIELD_NAME) && f.schema().getType().equals(Schema.Type.RECORD))) {

            final GenericRecord keyRecord = (GenericRecord) record.get(KEY_FIELD_NAME);
            key = createPathElement(kind, keyRecord);
        } else {
            final Key.PathElement pathElement = Key.PathElement.newBuilder()
                    .setKind(kind)
                    .setName(UUID.randomUUID().toString())
                    .build();
            key = Key.newBuilder().addPath(pathElement).build();
        }

        final Entity.Builder builder = Entity.newBuilder().setKey(key);
        for(final Schema.Field field : schema.getFields()) {
            if(KEY_FIELD_NAME.equals(field.name())) {
                continue;
            }

            builder.putProperties(field.name(), convertValue(field.schema(), record.get(field.name())));
        }
        return builder.build();
    }

    private static Value convertValue(final Schema schema, final Object value) {
        if(value == null) {
            return Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build();
        }
        switch (schema.getType()) {
            case BOOLEAN:
                return Value.newBuilder().setBooleanValue((Boolean) value).build();
            case FIXED:
            case BYTES: {
                final ByteString byteString = ByteString.copyFrom((ByteBuffer) value);
                if(byteString.size() >= DatastoreUtil.QUOTE_VALUE_SIZE) {
                    return Value.newBuilder().setBlobValue(byteString).setExcludeFromIndexes(true).build();
                } else {
                    return Value.newBuilder().setBlobValue(byteString).build();
                }
            }
            case ENUM:
            case STRING: {
                final String stringValue = value.toString();
                if(stringValue.getBytes().length >= DatastoreUtil.QUOTE_VALUE_SIZE) {
                    return Value.newBuilder().setStringValue(stringValue).setExcludeFromIndexes(true).build();
                } else {
                    return Value.newBuilder().setStringValue(stringValue).build();
                }
            }
            case INT:
                final int intValue = (int) value;
                if (LogicalTypes.date().equals(schema.getLogicalType())) {
                    return Value.newBuilder()
                            .setStringValue(LocalDate
                                    .ofEpochDay(intValue)
                                    .format(DateTimeFormatter.ISO_LOCAL_DATE))
                            .build();
                } else if (LogicalTypes.timeMillis().equals(schema.getLogicalType())) {
                    return Value.newBuilder()
                            .setStringValue(LocalTime
                                    .ofNanoOfDay(intValue * 1000_000)
                                    .format(DateTimeFormatter.ISO_LOCAL_TIME))
                            .build();
                } else {
                    return Value.newBuilder().setIntegerValue(intValue).build();
                }
            case LONG:
                if(LogicalTypes.timestampMillis().equals(schema.getLogicalType())) {
                    return Value.newBuilder().setTimestampValue(Timestamp.ofTimeMicroseconds(1000 * (long) value).toProto()).build();
                } else if(LogicalTypes.timestampMicros().equals(schema.getLogicalType())) {
                    return Value.newBuilder().setTimestampValue(Timestamp.ofTimeMicroseconds((long) value).toProto()).build();
                } else if(LogicalTypes.timeMicros().equals(schema.getLogicalType())) {
                    return Value.newBuilder()
                            .setStringValue(LocalTime
                                    .ofNanoOfDay(((long) value) * 1000)
                                    .format(DateTimeFormatter.ISO_LOCAL_TIME))
                            .build();
                }
                return Value.newBuilder().setIntegerValue((Long) value).build();
            case FLOAT:
                return Value.newBuilder().setDoubleValue((Float) value).build();
            case DOUBLE:
                return Value.newBuilder().setDoubleValue((Double) value).build();
            case RECORD:
                final GenericRecord childRecord = (GenericRecord) value;
                final Entity.Builder builder = Entity.newBuilder();
                for(final Schema.Field field : schema.getFields()) {
                    builder.putProperties(field.name(), convertValue(field.schema(), childRecord.get(field.name())));
                }
                return Value.newBuilder().setEntityValue(builder.build()).setExcludeFromIndexes(true).build();
            case ARRAY:
                return Value.newBuilder().setArrayValue(ArrayValue.newBuilder()
                        .addAllValues(((List<Object>) value).stream()
                                .map(o -> convertValue(schema.getElementType(), o))
                                .collect(Collectors.toList()))
                        .build())
                        .build();
            case UNION:
                return convertValue(AvroSchemaUtil.unnestUnion(schema), value);
            case MAP:
            case NULL:
            default:
                return Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build();
        }
    }

    private static Key createPathElement(final String kind, final GenericRecord keyRecord) {

        Key.Builder keyBuilder = Key.newBuilder();
        final String path = keyRecord.get("path").toString();
        final String[] paths = path.split(",");
        for(int i=0; i<paths.length - 2; i+=2) {
            final String k = paths[i];
            final String v = paths[i+1];
            if(v.contains("\"")) {
                keyBuilder = keyBuilder.addPath(Key.PathElement.newBuilder()
                        .setKind(k.replaceAll("\"", ""))
                        .setName(v.replaceAll("\"", "")));
            } else {
                keyBuilder = keyBuilder.addPath(Key.PathElement.newBuilder()
                        .setKind(k.replaceAll("\"", ""))
                        .setId(Long.valueOf(v)));
            }
        }

        Key.PathElement.Builder lastPathBuilder = Key.PathElement.newBuilder()
                .setKind(kind == null ? keyRecord.get("kind").toString() : kind);
        if(keyRecord.get("id") != null && (long)keyRecord.get("id") != 0) {
            lastPathBuilder = lastPathBuilder.setId((long) keyRecord.get("id"));
        } else if(keyRecord.get("name") != null) {
            lastPathBuilder = lastPathBuilder.setName(keyRecord.get("name").toString());
        } else {
            throw new IllegalArgumentException("Entity field value must not be null id or name.");
        }

        return keyBuilder.addPath(lastPathBuilder).build();
    }

}
