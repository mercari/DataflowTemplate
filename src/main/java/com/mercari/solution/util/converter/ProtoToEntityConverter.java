package com.mercari.solution.util.converter;

import com.google.datastore.v1.ArrayValue;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Value;
import com.google.protobuf.*;
import com.google.protobuf.util.JsonFormat;
import com.google.type.Date;
import com.google.type.DateTime;
import com.google.type.LatLng;
import com.google.type.TimeOfDay;
import com.mercari.solution.util.ProtoUtil;
import org.apache.beam.sdk.schemas.Schema;

import java.time.*;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class ProtoToEntityConverter {

    public static Schema convertSchema(final Descriptors.Descriptor messageType) {
        return ProtoToRowConverter.convertSchema(messageType);
    }

    public static Entity convert(final Schema schema,
                                 final Descriptors.Descriptor messageDescriptor,
                                 final byte[] bytes,
                                 final JsonFormat.Printer printer) {

        try {
            final DynamicMessage message = DynamicMessage
                    .newBuilder(messageDescriptor)
                    .mergeFrom(bytes)
                    .build();
            return convert(schema, messageDescriptor, message, printer);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException(e);
        }
    }

    public static Entity convert(final Schema schema,
                                 final Descriptors.Descriptor messageDescriptor,
                                 final DynamicMessage message,
                                 final JsonFormat.Printer printer) {

        final Entity.Builder builder = Entity.newBuilder();
        for (final Descriptors.FieldDescriptor field : messageDescriptor.getFields()) {
            builder.putProperties(field.getName(), convertValue(field, message.getField(field), printer));
        }
        return builder.build();
    }

    private static Value convertValue(final Descriptors.FieldDescriptor field,
                                      final Object value,
                                      final JsonFormat.Printer printer) {

        if(field.isRepeated()) {
            if(field.isMapField()) {
                if(value == null) {
                    return Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build();
                }
                return Value.newBuilder().setArrayValue(ArrayValue.newBuilder()
                        .addAllValues(((List<DynamicMessage>) value).stream()
                        .filter(Objects::nonNull)
                        .map(m -> Entity.newBuilder()
                                .putProperties("key", convertValue(field.getMessageType().findFieldByName("key"), m.getField(field.getMessageType().findFieldByName("key")), printer))
                                .putProperties("value", convertValue(field.getMessageType().findFieldByName("value"), m.getField(field.getMessageType().findFieldByName("value")), printer))
                                .build())
                        .map(e -> Value.newBuilder().setEntityValue(e).build())
                        .collect(Collectors.toList())).build()).build();
            }
            if(value == null) {
                return Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build();
            }
            return Value.newBuilder().setArrayValue(ArrayValue.newBuilder()
                    .addAllValues(((List<Object>) value).stream()
                            .map(v -> getValue(field, v, printer))
                            .collect(Collectors.toList()))
                    .build())
                    .build();
        }
        return getValue(field, value, printer);
    }

    private static Value getValue(final Descriptors.FieldDescriptor field,
                                  final Object value,
                                  final JsonFormat.Printer printer) {

        switch (field.getJavaType()) {
            case BOOLEAN:
                return Value.newBuilder().setBooleanValue((Boolean)value).build();
            case LONG:
                return Value.newBuilder().setIntegerValue((Long)value).build();
            case INT:
                return Value.newBuilder().setIntegerValue((Integer) value).build();
            case FLOAT:
                return Value.newBuilder().setDoubleValue((Float)value).build();
            case DOUBLE:
                return Value.newBuilder().setDoubleValue((Double)value).build();
            case STRING:
                return Value.newBuilder().setStringValue((String)value).build();
            case ENUM: {
                return Value.newBuilder().setStringValue(((Descriptors.EnumValueDescriptor)value).getName()).build();
            }
            case BYTE_STRING:
                return Value.newBuilder().setBlobValue((ByteString) value).build();
            case MESSAGE: {
                final Object object = ProtoUtil
                        .convertBuildInValue(field.getMessageType().getFullName(), (DynamicMessage) value);
                switch (ProtoUtil.ProtoType.of(field.getMessageType().getFullName())) {
                    case BOOL_VALUE:
                        return Value.newBuilder().setBooleanValue(((BoolValue) object).getValue()).build();
                    case BYTES_VALUE:
                        return Value.newBuilder().setBlobValue(((BytesValue) object).getValue()).build();
                    case STRING_VALUE:
                        return Value.newBuilder().setStringValue(((StringValue) object).getValue()).build();
                    case INT32_VALUE:
                        return Value.newBuilder().setIntegerValue(((Int32Value) object).getValue()).build();
                    case INT64_VALUE:
                        return Value.newBuilder().setIntegerValue(((Int64Value) object).getValue()).build();
                    case UINT32_VALUE:
                        return Value.newBuilder().setIntegerValue(((UInt32Value) object).getValue()).build();
                    case UINT64_VALUE:
                        return Value.newBuilder().setIntegerValue(((UInt64Value) object).getValue()).build();
                    case FLOAT_VALUE:
                        return Value.newBuilder().setDoubleValue(((FloatValue) object).getValue()).build();
                    case DOUBLE_VALUE:
                        return Value.newBuilder().setDoubleValue(((DoubleValue) object).getValue()).build();
                    case DATE: {
                        final Date date = (Date) object;
                        return Value.newBuilder().setStringValue(String
                                .format("%04d-%02d-%02d", date.getYear(), date.getMonth(), date.getDay()))
                                .build();
                    }
                    case TIME: {
                        final TimeOfDay timeOfDay = (TimeOfDay) object;
                        return Value.newBuilder().setStringValue(String
                                .format("%02d:%02d:%02d", timeOfDay.getHours(), timeOfDay.getMinutes(), timeOfDay.getSeconds()))
                                .build();
                    }
                    case DATETIME: {
                        final DateTime dt = (DateTime) object;
                        Instant instant = LocalDateTime.of(
                                dt.getYear(), dt.getMonth(), dt.getDay(),
                                dt.getHours(), dt.getMinutes(), dt.getSeconds(), dt.getNanos())
                                .atOffset(ZoneOffset.ofTotalSeconds((int)dt.getUtcOffset().getSeconds()))
                                .toInstant();
                        return Value.newBuilder()
                                .setTimestampValue(Timestamp.newBuilder()
                                        .setSeconds(instant.getEpochSecond())
                                        .setNanos(instant.getNano()).build())
                                .build();
                    }
                    case TIMESTAMP:
                        return Value.newBuilder().setTimestampValue((Timestamp)object).build();
                    case ANY: {
                        final Any any = (Any) object;
                        try {
                            return Value.newBuilder().setStringValue(printer.print(any)).build();
                        } catch (InvalidProtocolBufferException e) {
                            return Value.newBuilder().setStringValue(any.getValue().toStringUtf8()).build();
                        }
                    }
                    case LATLNG: {
                        final LatLng ll = (LatLng) object;
                        return Value.newBuilder().setStringValue(String.format("%f,%f", ll.getLatitude(), ll.getLongitude())).build();
                    }
                    case EMPTY:
                    case NULL_VALUE:
                        return Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build();
                    case CUSTOM:
                    default: {
                        return Value.newBuilder()
                                .setEntityValue(convert(null, field.getMessageType(), (DynamicMessage) value, printer))
                                .build();
                    }
                }
            }
            default:
                return Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build();
        }
    }

}
