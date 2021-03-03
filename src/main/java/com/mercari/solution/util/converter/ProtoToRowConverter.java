package com.mercari.solution.util.converter;

import com.google.cloud.ByteArray;
import com.google.protobuf.*;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.Timestamps;
import com.google.type.Date;
import com.google.type.DateTime;
import com.google.type.TimeOfDay;
import com.mercari.solution.util.schema.ProtoSchemaUtil;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.apache.beam.sdk.values.Row;

import java.nio.ByteBuffer;
import java.time.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
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
            final Descriptors.FieldDescriptor fieldDescriptor = ProtoSchemaUtil.getField(messageDescriptor, field.getName());
            builder.addValue(convertValue(fieldDescriptor, message.getField(fieldDescriptor), printer));
        }
        return builder.build();
    }

    private static Schema.FieldType convertFieldType(Descriptors.FieldDescriptor field) {
        //final boolean nullable = !field.isRequired() && (field.hasDefaultValue() || field.isOptional());
        final Schema.FieldType elementFieldType;
        switch (field.getJavaType()) {
            case BOOLEAN:
                elementFieldType = Schema.FieldType.BOOLEAN.withNullable(false);
                break;
            case ENUM: {
                final List<String> enumNames = field.getEnumType().getValues().stream()
                        .map(Descriptors.EnumValueDescriptor::getName)
                        .collect(Collectors.toList());
                elementFieldType = Schema.FieldType.logicalType(EnumerationType.create(enumNames)).withNullable(false);
                break;
            }
            case STRING:
                elementFieldType = Schema.FieldType.STRING.withNullable(!field.isRepeated());
                break;
            case BYTE_STRING:
                elementFieldType = Schema.FieldType.BYTES.withNullable(false);
                break;
            case INT:
                elementFieldType = Schema.FieldType.INT32.withNullable(false);
                break;
            case LONG:
                elementFieldType = Schema.FieldType.INT64.withNullable(false);
                break;
            case FLOAT:
                elementFieldType = Schema.FieldType.FLOAT.withNullable(false);
                break;
            case DOUBLE:
                elementFieldType = Schema.FieldType.DOUBLE.withNullable(false);
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
                            .withNullable(false);
                    break;
                }
                switch (ProtoSchemaUtil.ProtoType.of(field.getMessageType().getFullName())) {
                    case BOOL_VALUE:
                        elementFieldType = Schema.FieldType.BOOLEAN.withNullable(false);
                        break;
                    case STRING_VALUE:
                        elementFieldType = Schema.FieldType.STRING.withNullable(false);
                        break;
                    case BYTES_VALUE:
                        elementFieldType = Schema.FieldType.BYTES.withNullable(false);
                        break;
                    case INT32_VALUE:
                        elementFieldType = Schema.FieldType.INT32.withNullable(false);
                        break;
                    case INT64_VALUE:
                        elementFieldType = Schema.FieldType.INT64.withNullable(false);
                        break;
                    case FLOAT_VALUE:
                        elementFieldType = Schema.FieldType.FLOAT.withNullable(false);
                        break;
                    case DOUBLE_VALUE:
                        elementFieldType = Schema.FieldType.DOUBLE.withNullable(false);
                        break;
                    case UINT32_VALUE:
                        elementFieldType = Schema.FieldType.INT32.withNullable(false);
                        break;
                    case UINT64_VALUE:
                        elementFieldType = Schema.FieldType.INT64.withNullable(false);
                        break;
                    case DATE:
                        elementFieldType = CalciteUtils.DATE;
                        break;
                    case TIME:
                        elementFieldType = CalciteUtils.TIME;
                        break;
                    case DATETIME:
                    case TIMESTAMP:
                        elementFieldType = Schema.FieldType.DATETIME.withNullable(false);
                        break;
                    case ANY:
                        elementFieldType = Schema.FieldType.STRING.withNullable(false);
                        break;
                    case NULL_VALUE:
                    case EMPTY:
                        elementFieldType = Schema.FieldType.STRING.withNullable(true);
                        break;
                    case CUSTOM:
                    default:
                        elementFieldType = Schema.FieldType.row(convertSchema(field.getMessageType())).withNullable(true);
                        break;
                }
                break;
            }
            default:
                throw new IllegalArgumentException(field.getName() + " is not supported for beam row type.");
        }

        if(field.isRepeated() && !field.isMapField()) {
            return Schema.FieldType.array(elementFieldType).withNullable(false);
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
                final Descriptors.FieldDescriptor keyFieldDescriptor = field.getMessageType().findFieldByName("key");
                final Descriptors.FieldDescriptor valueFieldDescriptor = field.getMessageType().getFields().stream()
                        .filter(f -> f.getName().equals("value"))
                        .findAny()
                        .orElseThrow(() -> new IllegalStateException("Map value not found for field: " + field));
                return ((List<DynamicMessage>) value).stream()
                        .collect(Collectors.toMap(
                                e -> e.getField(keyFieldDescriptor),
                                e -> convertValue(valueFieldDescriptor, e.getField(field.getMessageType().findFieldByName("value")), printer)));
            }
            if(value == null) {
                return new ArrayList<>();
            }
            return ((List<Object>) value).stream()
                    .map(v -> getValue(field, v, printer))
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
        }
        return getValue(field, value, printer);
    }

    public static Object getValue(final Descriptors.FieldDescriptor field,
                                  final Object value,
                                  final JsonFormat.Printer printer) {

        boolean isNull = value == null;

        switch (field.getJavaType()) {
            case BOOLEAN:
                return !isNull && (Boolean)value;
            case INT:
                return isNull ? 0 : value;
            case LONG:
                return isNull ? 0l : value;
            case FLOAT:
                return isNull ? 0f : value;
            case DOUBLE:
                return isNull ? 0d : value;
            case STRING:
                return isNull ? "" : value;
            case ENUM: {
                if(isNull) {
                    return new EnumerationType.Value(0);
                }
                return new EnumerationType.Value(((Descriptors.EnumValueDescriptor)value).getIndex());
            }
            case BYTE_STRING:
                return isNull ? ByteArray.copyFrom("").toByteArray() : ((ByteString) value).toByteArray();
            case MESSAGE: {
                final Object object = ProtoSchemaUtil
                        .convertBuildInValue(field.getMessageType().getFullName(), (DynamicMessage) value);
                isNull = object == null;
                switch (ProtoSchemaUtil.ProtoType.of(field.getMessageType().getFullName())) {
                    case BOOL_VALUE:
                        return !isNull && ((BoolValue) object).getValue();
                    case BYTES_VALUE:
                        return isNull ? ByteArray.copyFrom("").toByteArray() : ByteBuffer.wrap(((BytesValue) object).getValue().toByteArray());
                    case STRING_VALUE:
                        return isNull ? "" : ((StringValue) object).getValue();
                    case INT32_VALUE:
                        return isNull ? 0 : ((Int32Value) object).getValue();
                    case INT64_VALUE:
                        return isNull ? 0 :((Int64Value) object).getValue();
                    case UINT32_VALUE:
                        return isNull ? 0 :((UInt32Value) object).getValue();
                    case UINT64_VALUE:
                        return isNull ? 0 :((UInt64Value) object).getValue();
                    case FLOAT_VALUE:
                        return isNull ? 0f :((FloatValue) object).getValue();
                    case DOUBLE_VALUE:
                        return isNull ? 0d :((DoubleValue) object).getValue();
                    case DATE: {
                        if(isNull) {
                            return LocalDate.of(1, 1, 1);
                        }
                        final Date date = (Date) object;
                        return LocalDate.of(date.getYear(), date.getMonth(), date.getDay());
                    }
                    case TIME: {
                        if(isNull) {
                            return LocalTime.of(0, 0, 0, 0);
                        }
                        final TimeOfDay timeOfDay = (TimeOfDay) object;
                        return LocalTime.of(timeOfDay.getHours(), timeOfDay.getMinutes(), timeOfDay.getSeconds(), timeOfDay.getNanos());
                    }
                    case DATETIME: {
                        if(isNull) {
                            long epochMilli = LocalDateTime.of(
                                    1, 1, 1,
                                    0, 0, 0, 0)
                                    .atOffset(ZoneOffset.UTC)
                                    .toInstant()
                                    .toEpochMilli();
                            return org.joda.time.Instant.ofEpochMilli(epochMilli);
                        }
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
                        if(isNull) {
                            long epochMilli = LocalDateTime.of(
                                    1, 1, 1,
                                    0, 0, 0, 0)
                                    .atOffset(ZoneOffset.UTC)
                                    .toInstant()
                                    .toEpochMilli();
                            return org.joda.time.Instant.ofEpochMilli(epochMilli);
                        }
                        return org.joda.time.Instant.ofEpochMilli(Timestamps.toMillis((Timestamp) object));
                    case ANY: {
                        if(isNull) {
                            return "";
                        }
                        final Any any = (Any) object;
                        try {
                            return printer.print(any);
                        } catch (InvalidProtocolBufferException e) {
                            return any.getValue().toStringUtf8();
                        }
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
                throw new IllegalStateException("Not support data type: " + field);
        }
    }

}
