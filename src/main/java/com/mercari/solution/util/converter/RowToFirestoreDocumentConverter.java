package com.mercari.solution.util.converter;

import com.google.firestore.v1.ArrayValue;
import com.google.firestore.v1.Document;
import com.google.firestore.v1.MapValue;
import com.google.firestore.v1.Value;
import com.google.protobuf.ByteString;
import com.google.protobuf.NullValue;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.schema.RowSchemaUtil;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;

import java.math.BigDecimal;
import java.util.List;
import java.util.stream.Collectors;

public class RowToFirestoreDocumentConverter {

    public static Document.Builder convert(final Schema schema, final Row row) {
        final Document.Builder builder = Document.newBuilder();
        for(final Schema.Field field : schema.getFields()) {
            builder.putFields(field.getName(), getValue(field.getType(), row.getValue(field.getName())));
        }
        return builder;
    }

    public static MapValue convertMapValue(final Schema schema, final Row row) {
        final MapValue.Builder builder = MapValue.newBuilder();
        for(final Schema.Field field : schema.getFields()) {
            builder.putFields(field.getName(), getValue(field.getType(), row.getValue(field.getName())));
        }
        return builder.build();
    }

    private static Value getValue(final Schema.FieldType fieldType, final Object v) {
        if(v == null) {
            return Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build();
        }
        switch (fieldType.getTypeName()) {
            case BOOLEAN:
                return Value.newBuilder().setBooleanValue(((Boolean) v)).build();
            case STRING:
                return Value.newBuilder().setStringValue(((String) v)).build();
            case BYTES:
                return Value.newBuilder().setBytesValue(ByteString.copyFrom((byte[]) v)).build();
            case BYTE:
                return Value.newBuilder().setIntegerValue(((Byte) v).longValue()).build();
            case INT16:
                return Value.newBuilder().setIntegerValue(((Short) v).longValue()).build();
            case INT32:
                return Value.newBuilder().setIntegerValue(((Integer) v).longValue()).build();
            case INT64:
                return Value.newBuilder().setIntegerValue(((Long) v)).build();
            case FLOAT:
                return Value.newBuilder().setDoubleValue(((Float) v)).build();
            case DOUBLE:
                return Value.newBuilder().setDoubleValue(((Double) v)).build();
            case DECIMAL:
                return Value.newBuilder().setStringValue(((BigDecimal) v).toString()).build();
            case DATETIME:
                return Value.newBuilder().setTimestampValue(DateTimeUtil.toProtoTimestamp((Instant) v)).build();
            case LOGICAL_TYPE: {
                if(RowSchemaUtil.isLogicalTypeDate(fieldType)) {
                    return Value.newBuilder().setStringValue((String)v).build();
                } else if(RowSchemaUtil.isLogicalTypeTime(fieldType)) {
                    return Value.newBuilder().setStringValue((String)v).build();
                } else if(RowSchemaUtil.isLogicalTypeTimestamp(fieldType)) {
                    return Value.newBuilder().setTimestampValue(DateTimeUtil.toProtoTimestamp((Instant)v)).build();
                } else if(RowSchemaUtil.isLogicalTypeEnum(fieldType)) {
                    return Value.newBuilder().setStringValue((String)v).build();
                } else {
                    throw new IllegalArgumentException(
                            "Unsupported Beam logical type: " + fieldType.getLogicalType().getIdentifier());
                }
            }
            case ROW:
                return Value.newBuilder().setMapValue(convertMapValue(fieldType.getRowSchema(), (Row) v)).build();
            case ARRAY:
            case ITERABLE:
                return Value.newBuilder()
                        .setArrayValue(ArrayValue.newBuilder()
                                .addAllValues(((List<Object>)v).stream()
                                        .map(c -> getValue(fieldType.getCollectionElementType(), c))
                                        .collect(Collectors.toList()))
                                .build())
                        .build();
            case MAP:
            default: {
                throw new RuntimeException("Not supported fieldType: " + fieldType);
            }

        }
    }

}
