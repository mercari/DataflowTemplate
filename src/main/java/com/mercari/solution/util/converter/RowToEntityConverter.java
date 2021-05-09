package com.mercari.solution.util.converter;

import com.google.cloud.Timestamp;
import com.google.datastore.v1.ArrayValue;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.Value;
import com.google.protobuf.ByteString;
import com.google.protobuf.NullValue;
import com.mercari.solution.util.schema.RowSchemaUtil;
import com.mercari.solution.util.gcp.DatastoreUtil;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.joda.time.ReadableDateTime;
import org.joda.time.format.ISODateTimeFormat;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class RowToEntityConverter {

    public static Entity convert(final Schema schema, final Row row,
                                 final String kind, final Iterable<String> keyFields, final String splitter) {

        final Key.PathElement pathElement;
        if(keyFields != null) {
            final StringBuilder sb = new StringBuilder();
            for (final String keyField : keyFields) {
                final String keyValue = RowSchemaUtil.getAsString(row, keyField);
                sb.append(keyValue);
                sb.append(splitter);
            }
            sb.deleteCharAt(sb.length() - splitter.length());
            pathElement = Key.PathElement.newBuilder().setKind(kind).setName(sb.toString()).build();
        } else {
            pathElement = Key.PathElement.newBuilder().setKind(kind).setName(UUID.randomUUID().toString()).build();
        }

        final Key key = Key.newBuilder()
                //.setPartitionId(PartitionId.newBuilder().setProjectId("").setNamespaceId("").build())
                .addPath(pathElement)
                .build();

        Entity.Builder builder = Entity.newBuilder();
        builder.setKey(key);
        for(Schema.Field field : schema.getFields()) {
            builder.putProperties(field.getName(), convertValue(field.getType(), row.getValue(field.getName())));
        }
        return builder.build();
    }

    private static Value convertValue(final Schema.FieldType fieldType, final Object value) {
        if(value == null) {
            return Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build();
        }
        switch (fieldType.getTypeName()) {
            case BOOLEAN:
                return Value.newBuilder().setBooleanValue((Boolean) value).build();
            case BYTES: {
                final ByteString byteString = ByteString.copyFrom((ByteBuffer) value);
                if(byteString.size() > DatastoreUtil.QUOTE_VALUE_SIZE) {
                    return Value.newBuilder().setBlobValue(byteString).setExcludeFromIndexes(true).build();
                } else {
                    return Value.newBuilder().setBlobValue(byteString).build();
                }
            }
            case DECIMAL:
            case STRING: {
                final String string = value.toString();
                if(string.getBytes().length > DatastoreUtil.QUOTE_VALUE_SIZE) {
                    return Value.newBuilder().setStringValue(value.toString()).setExcludeFromIndexes(true).build();
                } else {
                    return Value.newBuilder().setStringValue(value.toString()).build();
                }
            }
            case BYTE:
                return Value.newBuilder().setIntegerValue((Byte) value).build();
            case INT16:
                return Value.newBuilder().setIntegerValue((Short) value).build();
            case INT32:
                return Value.newBuilder().setIntegerValue((Integer) value).build();
            case INT64:
                return Value.newBuilder().setIntegerValue((Long) value).build();
            case FLOAT:
                return Value.newBuilder().setDoubleValue((Float) value).build();
            case DOUBLE:
                return Value.newBuilder().setDoubleValue((Double) value).build();
            case DATETIME:
                return Value.newBuilder().setTimestampValue(Timestamp
                        .parseTimestamp(((ReadableDateTime) value)
                                .toDateTime()
                                .toString(ISODateTimeFormat.dateTime())).toProto()).build();
            case LOGICAL_TYPE:
                // FIXME
                if(RowSchemaUtil.isLogicalTypeDate(fieldType)) {
                    return Value.newBuilder().setStringValue(value.toString()).build();
                } else if(RowSchemaUtil.isLogicalTypeTime(fieldType)) {
                    return Value.newBuilder().setStringValue(value.toString()).build();
                } else if(RowSchemaUtil.isLogicalTypeTimestamp(fieldType)) {
                    return Value.newBuilder().setStringValue(value.toString()).build();
                } else {
                    throw new IllegalArgumentException(
                            "Unsupported Beam logical type: " + fieldType.getLogicalType());
                }
            case ROW:
                final Row childRow = (Row) value;
                final Entity.Builder builder = Entity.newBuilder();
                for(Schema.Field field : fieldType.getRowSchema().getFields()) {
                    builder.putProperties(field.getName(), convertValue(field.getType(), childRow.getValue(field.getName())));
                }
                return Value.newBuilder().setEntityValue(builder.build()).setExcludeFromIndexes(true).build();
            case ITERABLE:
            case ARRAY:
                return Value.newBuilder().setArrayValue(ArrayValue.newBuilder()
                        .addAllValues(((List<Object>) value).stream()
                                .map(o -> convertValue(fieldType.getCollectionElementType(), o))
                                .collect(Collectors.toList()))
                        .build())
                        .build();
            case MAP:
            default:
                return Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build();
        }
    }

}
