package com.mercari.solution.util.converter;

import com.google.protobuf.*;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.Timestamps;
import com.google.type.Date;
import com.google.type.DateTime;
import com.google.type.LatLng;
import com.google.type.TimeOfDay;
import com.mercari.solution.util.ProtoUtil;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.apache.beam.sdk.values.Row;

import java.time.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class ProtoToRowConverter {

    public static Schema convertSchema(final Descriptors.Descriptor messageType) {
        final Schema.Builder builder = Schema.builder();
        for(final Descriptors.FieldDescriptor field : messageType.getFields()) {
            builder.addField(field.getName(), convertFieldType(field));
        }
        return builder.build();
    }

    public static Row convert(final Schema schema,
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

    public static Row convert(final Schema schema,
                              final Descriptors.Descriptor messageDescriptor,
                              final DynamicMessage message,
                              final JsonFormat.Printer printer) {

        final Row.Builder builder = Row.withSchema(schema);
        for (final Schema.Field field : schema.getFields()) {
            final Descriptors.FieldDescriptor fieldDescriptor = messageDescriptor.findFieldByName(field.getName());
            if(fieldDescriptor == null || (!fieldDescriptor.isRepeated() && !message.hasField(fieldDescriptor))) {
                builder.addValue(null);
            } else {
                builder.addValue(convertValue(fieldDescriptor, message.getField(fieldDescriptor), printer));
            }
        }
        return builder.build();
    }

    private static Schema.FieldType convertFieldType(Descriptors.FieldDescriptor field) {
        final boolean nullable = !field.isRequired() && (field.hasDefaultValue() || field.isOptional());
        final Schema.FieldType elementFieldType;
        switch (field.getJavaType()) {
            case BOOLEAN:
                elementFieldType = Schema.FieldType.BOOLEAN.withNullable(nullable);
                break;
            case ENUM: {
                final List<String> enumNames = field.getEnumType().getValues().stream()
                        .map(Descriptors.EnumValueDescriptor::getName)
                        .collect(Collectors.toList());
                elementFieldType = Schema.FieldType.logicalType(EnumerationType.create(enumNames)).withNullable(nullable);
                break;
            }
            case STRING:
                elementFieldType = Schema.FieldType.STRING.withNullable(nullable);
                break;
            case BYTE_STRING:
                elementFieldType = Schema.FieldType.BYTES.withNullable(nullable);
                break;
            case INT:
                elementFieldType = Schema.FieldType.INT32.withNullable(nullable);
                break;
            case LONG:
                elementFieldType = Schema.FieldType.INT64.withNullable(nullable);
                break;
            case FLOAT:
                elementFieldType = Schema.FieldType.FLOAT.withNullable(nullable);
                break;
            case DOUBLE:
                elementFieldType = Schema.FieldType.DOUBLE.withNullable(nullable);
                break;
            case MESSAGE: {
                if(field.isMapField()) {
                    Descriptors.FieldDescriptor keyField = null;
                    Descriptors.FieldDescriptor valueField = null;
                    for(Descriptors.FieldDescriptor mapField : field.getMessageType().getFields()) {
                        if("key".equals(mapField.getName())) {
                            keyField = mapField;
                        } else if("value".equals(mapField.getName())) {
                            valueField = mapField;
                        }
                    }
                    elementFieldType = Schema.FieldType.map(
                            convertFieldType(keyField),
                            convertFieldType(valueField))
                            .withNullable(nullable);
                    break;
                }
                switch (ProtoUtil.ProtoType.of(field.getMessageType().getFullName())) {
                    case BOOL_VALUE:
                        elementFieldType = Schema.FieldType.BOOLEAN.withNullable(nullable);
                        break;
                    case STRING_VALUE:
                        elementFieldType = Schema.FieldType.STRING.withNullable(nullable);
                        break;
                    case BYTES_VALUE:
                        elementFieldType = Schema.FieldType.BYTES.withNullable(nullable);
                        break;
                    case INT32_VALUE:
                        elementFieldType = Schema.FieldType.INT32.withNullable(nullable);
                        break;
                    case INT64_VALUE:
                        elementFieldType = Schema.FieldType.INT64.withNullable(nullable);
                        break;
                    case FLOAT_VALUE:
                        elementFieldType = Schema.FieldType.FLOAT.withNullable(nullable);
                        break;
                    case DOUBLE_VALUE:
                        elementFieldType = Schema.FieldType.DOUBLE.withNullable(nullable);
                        break;
                    case UINT32_VALUE:
                        elementFieldType = Schema.FieldType.INT32.withNullable(nullable);
                        break;
                    case UINT64_VALUE:
                        elementFieldType = Schema.FieldType.INT64.withNullable(nullable);
                        break;
                    case DATE:
                        elementFieldType = nullable ? CalciteUtils.NULLABLE_DATE : CalciteUtils.DATE;
                        break;
                    case TIME:
                        elementFieldType = nullable ? CalciteUtils.NULLABLE_TIME : CalciteUtils.TIME;
                        break;
                    case DATETIME:
                    case TIMESTAMP:
                        elementFieldType = Schema.FieldType.DATETIME.withNullable(nullable);
                        break;
                    case LATLNG:
                        elementFieldType = Schema.FieldType.STRING.withNullable(nullable);
                        break;
                    case ANY:
                        elementFieldType = Schema.FieldType.STRING.withNullable(nullable);
                        break;
                    case NULL_VALUE:
                    case EMPTY:
                        elementFieldType = Schema.FieldType.STRING.withNullable(nullable);
                        break;
                    case CUSTOM:
                    default:
                        elementFieldType = Schema.FieldType.row(convertSchema(field.getMessageType())).withNullable(nullable);
                        break;
                }
                break;
            }
            default:
                throw new IllegalArgumentException(field.getName() + " is not supported for bigquery.");
        }

        if(field.isRepeated() && !field.isMapField()) {
            return Schema.FieldType.array(elementFieldType).withNullable(nullable);
        } else {
            return elementFieldType;
        }

    }

    private static Object convertValue(final Descriptors.FieldDescriptor field,
                                       final Object value,
                                       final JsonFormat.Printer printer) {

        if(field.isRepeated()) {
            if(field.isMapField()) {
                if(value == null) {
                    return new HashMap<>();
                }
                return ((List<DynamicMessage>) value).stream()
                        .collect(Collectors.toMap(
                                e -> e.getField(field.getMessageType().findFieldByName("key")),
                                e -> e.getField(field.getMessageType().findFieldByName("value"))));
            }
            if(value == null) {
                return new ArrayList<>();
            }
            return ((List<Object>) value).stream()
                    .map(v -> getValue(field, v, printer))
                    .collect(Collectors.toList());
        }
        return getValue(field, value, printer);
    }

    private static Object getValue(final Descriptors.FieldDescriptor field,
                                   final Object value,
                                   final JsonFormat.Printer printer) {

        switch (field.getJavaType()) {
            case BOOLEAN:
            case LONG:
            case INT:
            case FLOAT:
            case DOUBLE:
            case STRING:
                return value;
            case ENUM: {
                return new EnumerationType.Value(((Descriptors.EnumValueDescriptor)value).getIndex());
            }
            case BYTE_STRING:
                return ((ByteString) value).toByteArray();
            case MESSAGE: {
                final Object object = ProtoUtil
                        .convertBuildInValue(field.getMessageType().getFullName(), (DynamicMessage) value);
                switch (ProtoUtil.ProtoType.of(field.getMessageType().getFullName())) {
                    case BOOL_VALUE:
                        return ((BoolValue) object).getValue();
                    case BYTES_VALUE:
                        return ((BytesValue) object).getValue().asReadOnlyByteBuffer();
                    case STRING_VALUE:
                        return ((StringValue) object).getValue();
                    case INT32_VALUE:
                        return ((Int32Value) object).getValue();
                    case INT64_VALUE:
                        return ((Int64Value) object).getValue();
                    case UINT32_VALUE:
                        return ((UInt32Value) object).getValue();
                    case UINT64_VALUE:
                        return ((UInt64Value) object).getValue();
                    case FLOAT_VALUE:
                        return ((FloatValue) object).getValue();
                    case DOUBLE_VALUE:
                        return ((DoubleValue) object).getValue();
                    case DATE: {
                        final Date date = (Date) object;
                        return LocalDate.of(date.getYear(), date.getMonth(), date.getDay());
                    }
                    case TIME: {
                        final TimeOfDay timeOfDay = (TimeOfDay) object;
                        return LocalTime.of(timeOfDay.getHours(), timeOfDay.getMinutes(), timeOfDay.getSeconds(), timeOfDay.getNanos());
                    }
                    case DATETIME: {
                        final DateTime dt = (DateTime) object;
                        long epochMilli = LocalDateTime.of(
                                dt.getYear(), dt.getMonth(), dt.getDay(),
                                dt.getHours(), dt.getMinutes(), dt.getSeconds(), dt.getNanos())
                                .atOffset(ZoneOffset.ofTotalSeconds((int)dt.getUtcOffset().getSeconds()))
                                .toInstant()
                                .toEpochMilli();
                        return org.joda.time.Instant.ofEpochMilli(epochMilli);
                    }
                    case TIMESTAMP:
                        return org.joda.time.Instant.ofEpochMilli(Timestamps.toMillis((Timestamp) object));
                    case ANY: {
                        final Any any = (Any) object;
                        try {
                            return printer.print(any);
                        } catch (InvalidProtocolBufferException e) {
                            return any.getValue().toStringUtf8();
                        }
                    }
                    case LATLNG: {
                        final LatLng ll = (LatLng) object;
                        return String.format("%f,%f", ll.getLatitude(), ll.getLongitude());
                    }
                    case EMPTY:
                    case NULL_VALUE:
                        return null;
                    case CUSTOM:
                    default: {
                        final Schema schema = convertSchema(field.getMessageType());
                        return convert(schema, field.getMessageType(), (DynamicMessage) value, printer);
                    }
                }
            }
            default:
                return null;
        }
    }

}
