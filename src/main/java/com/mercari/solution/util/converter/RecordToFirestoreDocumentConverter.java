package com.mercari.solution.util.converter;

import com.google.firestore.v1.ArrayValue;
import com.google.firestore.v1.Document;
import com.google.firestore.v1.MapValue;
import com.google.firestore.v1.Value;
import com.google.protobuf.ByteString;
import com.google.protobuf.NullValue;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.stream.Collectors;


public class RecordToFirestoreDocumentConverter {

    public static Document.Builder convert(final Schema schema, final GenericRecord record) {
        final Document.Builder builder = Document.newBuilder();
        for(final Schema.Field field : schema.getFields()) {
            builder.putFields(field.name(), getValue(field.schema(), record.get(field.name())));
        }
        return builder;
    }

    public static MapValue convertMapValue(final Schema schema, final GenericRecord record) {
        final MapValue.Builder builder = MapValue.newBuilder();
        for(final Schema.Field field : schema.getFields()) {
            builder.putFields(field.name(), getValue(field.schema(), record.get(field.name())));
        }
        return builder.build();
    }

    private static Value getValue(final Schema schema, final Object v) {
        if(v == null) {
            return Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build();
        }
        switch (schema.getType()) {
            case BOOLEAN:
                return Value.newBuilder().setBooleanValue(((Boolean) v)).build();
            case ENUM:
            case STRING:
                return Value.newBuilder().setStringValue(v.toString()).build();
            case FIXED:
            case BYTES: {
                if(AvroSchemaUtil.isLogicalTypeDecimal(schema)) {
                    final BigDecimal decimal = AvroSchemaUtil.getAsBigDecimal(schema, (ByteBuffer) v);
                    return Value.newBuilder().setStringValue(decimal.toString()).build();
                }
                return Value.newBuilder().setBytesValue(ByteString.copyFrom((ByteBuffer) v)).build();
            }
            case INT: {
                final Integer i = (Integer) v;
                if (LogicalTypes.date().equals(schema.getLogicalType())) {
                    return Value.newBuilder()
                            .setStringValue(LocalDate.ofEpochDay(i.longValue()).format(DateTimeFormatter.ISO_LOCAL_DATE))
                            .build();
                } else if (LogicalTypes.timeMillis().equals(schema.getLogicalType())) {
                    return Value.newBuilder()
                            .setStringValue(LocalTime.ofNanoOfDay(i.longValue() * 1000_000L).format(DateTimeFormatter.ISO_LOCAL_TIME))
                            .build();
                } else {
                    return Value.newBuilder().setIntegerValue(i.longValue()).build();
                }
            }
            case LONG: {
                final Long l = (Long) v;
                if(LogicalTypes.timestampMillis().equals(schema.getLogicalType())) {
                    return Value.newBuilder().setTimestampValue(DateTimeUtil.toProtoTimestamp(l/1000L)).build();
                } else if(LogicalTypes.timestampMicros().equals(schema.getLogicalType())) {
                    return Value.newBuilder().setTimestampValue(DateTimeUtil.toProtoTimestamp(l)).build();
                } else if(LogicalTypes.timeMicros().equals(schema.getLogicalType())) {
                    return Value.newBuilder()
                            .setStringValue(LocalTime.ofNanoOfDay(l * 1000L).format(DateTimeFormatter.ISO_LOCAL_TIME))
                            .build();
                } else {
                    return Value.newBuilder().setIntegerValue(l).build();
                }
            }
            case FLOAT:
                return Value.newBuilder().setDoubleValue(((Float) v)).build();
            case DOUBLE:
                return Value.newBuilder().setDoubleValue(((Double) v)).build();
            case RECORD:
                return Value.newBuilder().setMapValue(convertMapValue(schema, (GenericRecord) v)).build();
            case ARRAY:
                return Value.newBuilder()
                        .setArrayValue(ArrayValue.newBuilder()
                                .addAllValues(((List<Object>)v).stream()
                                        .map(c -> getValue(schema.getElementType(), c))
                                        .collect(Collectors.toList()))
                                .build())
                        .build();
            case UNION: {
                return getValue(AvroSchemaUtil.unnestUnion(schema), v);
            }
            case NULL:
            case MAP:
            default: {
                throw new RuntimeException("Not supported fieldType: " + schema);
            }

        }
    }

}
