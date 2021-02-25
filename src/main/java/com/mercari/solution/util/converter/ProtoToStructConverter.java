package com.mercari.solution.util.converter;

import com.google.cloud.ByteArray;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.protobuf.*;
import com.google.protobuf.util.JsonFormat;
import com.google.type.Date;
import com.google.type.DateTime;
import com.google.type.LatLng;
import com.google.type.TimeOfDay;
import com.mercari.solution.util.ProtoUtil;

import java.time.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class ProtoToStructConverter {

    public static Type convertSchema(final Descriptors.Descriptor messageType) {
        final List<Type.StructField> fields = new ArrayList<>();
        for(final Descriptors.FieldDescriptor field : messageType.getFields()) {
            fields.add(Type.StructField.of(field.getName(), convertSchema(field)));
        }
        return Type.struct(fields);
    }

    public static Struct convert(final Type type,
                                 final Descriptors.Descriptor messageDescriptor,
                                 final byte[] bytes,
                                 final JsonFormat.Printer printer) {

        try {
            final DynamicMessage message = DynamicMessage
                    .newBuilder(messageDescriptor)
                    .mergeFrom(bytes)
                    .build();
            return convert(type, messageDescriptor, message, printer);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException(e);
        }
    }

    public static Struct convert(final Type type,
                                 final Descriptors.Descriptor messageDescriptor,
                                 final DynamicMessage message,
                                 final JsonFormat.Printer printer) {

        final Struct.Builder builder = Struct.newBuilder();
        for (final Descriptors.FieldDescriptor field : messageDescriptor.getFields()) {
            setValue(builder, field, message.getField(field), printer);
        }
        return builder.build();
    }

    private static Type convertSchema(Descriptors.FieldDescriptor field) {
        final Type elementType;
        switch (field.getJavaType()) {
            case BOOLEAN:
                elementType = Type.bool();
                break;
            case ENUM:
            case STRING:
                elementType =  Type.string();
                break;
            case BYTE_STRING:
                elementType =  Type.bytes();
                break;
            case INT:
            case LONG:
                elementType =  Type.int64();
                break;
            case DOUBLE:
                elementType =  Type.float64();
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
                    elementType = Type.struct(
                            Type.StructField.of("key", convertSchema(keyField)),
                            Type.StructField.of("value", convertSchema(valueField)));
                    break;
                }
                switch (ProtoUtil.ProtoType.of(field.getMessageType().getFullName())) {
                    case BOOL_VALUE:
                        elementType = Type.bool();
                        break;
                    case STRING_VALUE:
                        elementType = Type.string();
                        break;
                    case BYTES_VALUE:
                        elementType = Type.bytes();
                        break;
                    case INT32_VALUE:
                    case INT64_VALUE:
                    case UINT32_VALUE:
                    case UINT64_VALUE:
                        elementType = Type.int64();
                        break;
                    case FLOAT_VALUE:
                    case DOUBLE_VALUE:
                        elementType = Type.float64();
                        break;
                    case DATE:
                        elementType = Type.date();
                        break;
                    case TIME:
                        elementType = Type.string();
                        break;
                    case DATETIME:
                    case TIMESTAMP:
                        elementType = Type.timestamp();
                        break;
                    case LATLNG:
                        elementType = Type.string();
                        break;
                    case ANY:
                        elementType = Type.string();
                        break;
                    case NULL_VALUE:
                    case EMPTY:
                        elementType = Type.string();
                        break;
                    case CUSTOM:
                    default:
                        elementType = convertSchema(field.getMessageType());
                        break;
                }
                break;
            }
            default:
                throw new IllegalArgumentException(field.getName() + " is not supported for bigquery.");
        }

        if(field.isRepeated()) {
            return Type.array(elementType);
        } else {
            return elementType;
        }

    }

    private static void setValue(final Struct.Builder builder,
                                 final Descriptors.FieldDescriptor field,
                                 final Object value,
                                 final JsonFormat.Printer printer) {

        boolean isNull = value == null;

        if(field.isRepeated()) {
            final List<Object> array = (List<Object>) value;
            switch (field.getJavaType()) {
                case BOOLEAN:
                    builder.set(field.getName()).toBoolArray(isNull ? new ArrayList<>() : array.stream()
                            .filter(Objects::nonNull)
                            .map(o -> (Boolean)o)
                            .collect(Collectors.toList()));
                    return;
                case LONG:
                    builder.set(field.getName()).toInt64Array(isNull ? new ArrayList<>() : array.stream()
                            .filter(Objects::nonNull)
                            .map(o -> (Long)o)
                            .collect(Collectors.toList()));
                    return;
                case INT:
                    builder.set(field.getName()).toInt64Array(isNull ? new ArrayList<>() : array.stream()
                            .filter(Objects::nonNull)
                            .map(o -> (Integer)o)
                            .map(Integer::longValue)
                            .collect(Collectors.toList()));
                    return;
                case FLOAT:
                    builder.set(field.getName()).toFloat64Array(isNull ? new ArrayList<>() : array.stream()
                            .filter(Objects::nonNull)
                            .map(o -> (Float)o)
                            .map(Float::doubleValue)
                            .collect(Collectors.toList()));
                    return;
                case DOUBLE:
                    builder.set(field.getName()).toFloat64Array(isNull ? new ArrayList<>() : array.stream()
                            .filter(Objects::nonNull)
                            .map(o -> (Double)o)
                            .collect(Collectors.toList()));
                    return;
                case STRING:
                    builder.set(field.getName()).toStringArray(isNull ? new ArrayList<>() : array.stream()
                            .filter(Objects::nonNull)
                            .map(Object::toString)
                            .collect(Collectors.toList()));
                    return;
                case ENUM:
                    builder.set(field.getName()).toStringArray(isNull ? new ArrayList<>() : array.stream()
                            .filter(Objects::nonNull)
                            .map(o -> (Descriptors.EnumValueDescriptor)o)
                            .map(Descriptors.EnumValueDescriptor::getName)
                            .collect(Collectors.toList()));
                    return;
                case BYTE_STRING:
                    builder.set(field.getName()).toBytesArray(isNull ? new ArrayList<>() : array.stream()
                            .filter(Objects::nonNull)
                            .map(o -> (ByteString)o)
                            .map(ByteString::toByteArray)
                            .map(ByteArray::copyFrom)
                            .collect(Collectors.toList()));
                    return;
                case MESSAGE: {
                    if(field.isMapField()) {
                        final Type type = convertSchema(field.getMessageType());
                        builder.set(field.getName()).toStructArray(type, isNull ? new ArrayList<>() : array.stream()
                                .filter(Objects::nonNull)
                                .map(o -> (DynamicMessage)o)
                                .map(m -> convert(type, field.getMessageType(), m, printer))
                                .collect(Collectors.toList()));
                        return;
                    }
                    switch (ProtoUtil.ProtoType.of(field.getMessageType().getFullName())) {
                        case BOOL_VALUE:
                            builder.set(field.getName()).toBoolArray(isNull ? new ArrayList<>() : array.stream()
                                    .filter(Objects::nonNull)
                                    .map(o -> (DynamicMessage)o)
                                    .map(m -> ProtoUtil.convertBuildInValue(field.getMessageType().getFullName(), m))
                                    .map(o -> (BoolValue)o)
                                    .map(BoolValue::getValue)
                                    .collect(Collectors.toList()));
                            return;
                        case BYTES_VALUE:
                            builder.set(field.getName()).toBytesArray(isNull ? new ArrayList<>() : array.stream()
                                    .filter(Objects::nonNull)
                                    .map(o -> (DynamicMessage)o)
                                    .map(m -> ProtoUtil.convertBuildInValue(field.getMessageType().getFullName(), m))
                                    .map(o -> (BytesValue)o)
                                    .map(BytesValue::getValue)
                                    .map(ByteString::toByteArray)
                                    .map(ByteArray::copyFrom)
                                    .collect(Collectors.toList()));
                            return;
                        case STRING_VALUE:
                            builder.set(field.getName()).toStringArray(isNull ? new ArrayList<>() : array.stream()
                                    .filter(Objects::nonNull)
                                    .map(o -> (DynamicMessage)o)
                                    .map(m -> ProtoUtil.convertBuildInValue(field.getMessageType().getFullName(), m))
                                    .map(o -> (StringValue)o)
                                    .map(StringValue::getValue)
                                    .collect(Collectors.toList()));
                            return;
                        case INT32_VALUE:
                            builder.set(field.getName()).toInt64Array(isNull ? new ArrayList<>() : array.stream()
                                    .filter(Objects::nonNull)
                                    .map(o -> (DynamicMessage)o)
                                    .map(m -> ProtoUtil.convertBuildInValue(field.getMessageType().getFullName(), m))
                                    .map(o -> (Int32Value)o)
                                    .map(Int32Value::getValue)
                                    .map(i -> (long) i)
                                    .collect(Collectors.toList()));
                            return;
                        case INT64_VALUE:
                            builder.set(field.getName()).toInt64Array(isNull ? new ArrayList<>() : array.stream()
                                    .filter(Objects::nonNull)
                                    .map(o -> (DynamicMessage)o)
                                    .map(m -> ProtoUtil.convertBuildInValue(field.getMessageType().getFullName(), m))
                                    .map(o -> (Int64Value)o)
                                    .map(Int64Value::getValue)
                                    .collect(Collectors.toList()));
                            return;
                        case UINT32_VALUE:
                            builder.set(field.getName()).toInt64Array(isNull ? new ArrayList<>() : array.stream()
                                    .filter(Objects::nonNull)
                                    .map(o -> (DynamicMessage)o)
                                    .map(m -> ProtoUtil.convertBuildInValue(field.getMessageType().getFullName(), m))
                                    .map(o -> (UInt32Value)o)
                                    .map(UInt32Value::getValue)
                                    .map(i -> (long) i)
                                    .collect(Collectors.toList()));
                            return;
                        case UINT64_VALUE:
                            builder.set(field.getName()).toInt64Array(isNull ? new ArrayList<>() : array.stream()
                                    .filter(Objects::nonNull)
                                    .map(o -> (UInt64Value)o)
                                    .map(UInt64Value::getValue)
                                    .collect(Collectors.toList()));
                            return;
                        case FLOAT_VALUE:
                            builder.set(field.getName()).toFloat64Array(isNull ? new ArrayList<>() : array.stream()
                                    .filter(Objects::nonNull)
                                    .map(o -> (DynamicMessage)o)
                                    .map(m -> ProtoUtil.convertBuildInValue(field.getMessageType().getFullName(), m))
                                    .map(o -> (FloatValue)o)
                                    .map(FloatValue::getValue)
                                    .map(f -> (double) f)
                                    .collect(Collectors.toList()));
                            return;
                        case DOUBLE_VALUE:
                            builder.set(field.getName()).toFloat64Array(isNull ? new ArrayList<>() : array.stream()
                                    .filter(Objects::nonNull)
                                    .map(o -> (DynamicMessage)o)
                                    .map(m -> ProtoUtil.convertBuildInValue(field.getMessageType().getFullName(), m))
                                    .map(o -> (DoubleValue)o)
                                    .map(DoubleValue::getValue)
                                    .collect(Collectors.toList()));
                            return;
                        case DATE: {
                            builder.set(field.getName()).toDateArray(isNull ? new ArrayList<>() : array.stream()
                                    .filter(Objects::nonNull)
                                    .map(o -> (DynamicMessage)o)
                                    .map(m -> ProtoUtil.convertBuildInValue(field.getMessageType().getFullName(), m))
                                    .map(o -> (Date)o)
                                    .map(d -> com.google.cloud.Date.fromYearMonthDay(d.getYear(), d.getMonth(), d.getDay()))
                                    .collect(Collectors.toList()));
                            return;
                        }
                        case TIME: {
                            builder.set(field.getName()).toStringArray(isNull ? new ArrayList<>() : array.stream()
                                    .filter(Objects::nonNull)
                                    .map(o -> (DynamicMessage)o)
                                    .map(m -> ProtoUtil.convertBuildInValue(field.getMessageType().getFullName(), m))
                                    .map(o -> (TimeOfDay)o)
                                    .map(TimeOfDay::toString)
                                    .collect(Collectors.toList()));
                            return;
                        }
                        case DATETIME: {
                            builder.set(field.getName()).toTimestampArray(isNull ? new ArrayList<>() : array.stream()
                                    .filter(Objects::nonNull)
                                    .map(o -> (DynamicMessage)o)
                                    .map(m -> ProtoUtil.convertBuildInValue(field.getMessageType().getFullName(), m))
                                    .map(o -> (DateTime)o)
                                    .map(dt -> LocalDateTime
                                            .of(dt.getYear(), dt.getMonth(), dt.getDay(), dt.getHours(), dt.getMinutes(), dt.getSeconds(), dt.getNanos())
                                            .atOffset(ZoneOffset.ofTotalSeconds((int)dt.getUtcOffset().getSeconds()))
                                            .toInstant())
                                    .map(i -> com.google.cloud.Timestamp
                                            .ofTimeSecondsAndNanos(i.getEpochSecond(), i.getNano()))
                                    .collect(Collectors.toList()));
                            return;
                        }
                        case TIMESTAMP: {
                            builder.set(field.getName()).toTimestampArray(isNull ? new ArrayList<>() : array.stream()
                                    .filter(Objects::nonNull)
                                    .map(o -> (DynamicMessage)o)
                                    .map(m -> ProtoUtil.convertBuildInValue(field.getMessageType().getFullName(), m))
                                    .map(o -> (Timestamp)o)
                                    .map(t -> com.google.cloud.Timestamp
                                            .ofTimeSecondsAndNanos(t.getSeconds(), t.getNanos()))
                                    .collect(Collectors.toList()));
                            return;
                        }
                        case ANY: {
                            builder.set(field.getName()).toStringArray(isNull ? new ArrayList<>() : array.stream()
                                    .filter(Objects::nonNull)
                                    .map(o -> (DynamicMessage)o)
                                    .map(m -> ProtoUtil.convertBuildInValue(field.getMessageType().getFullName(), m))
                                    .map(o -> (Any)o)
                                    .map(a -> {
                                        try {
                                            return printer.print(a);
                                        } catch (InvalidProtocolBufferException e) {
                                            return a.getValue().toStringUtf8();
                                        }
                                    })
                                    .collect(Collectors.toList()));
                            return;
                        }
                        case LATLNG:
                            builder.set(field.getName()).toStringArray(isNull ? new ArrayList<>() : array.stream()
                                    .filter(Objects::nonNull)
                                    .map(o -> (DynamicMessage)o)
                                    .map(m -> ProtoUtil.convertBuildInValue(field.getMessageType().getFullName(), m))
                                    .map(o -> (LatLng)o)
                                    .map(LatLng::toString)
                                    .collect(Collectors.toList()));
                            return;
                        case EMPTY:
                        case NULL_VALUE:
                            builder.set(field.getName()).toStringArray(new ArrayList<>());
                            return;
                        case CUSTOM:
                        default: {
                            final Type type = convertSchema(field.getMessageType());
                            builder.set(field.getName()).toStructArray(type, isNull ? new ArrayList<>() : array.stream()
                                    .filter(Objects::nonNull)
                                    .map(o -> (DynamicMessage)o)
                                    .map(m -> convert(type, field.getMessageType(), m, printer))
                                    .collect(Collectors.toList()));
                            return;
                        }
                    }
                }
                default:
                    builder.set(field.getName()).to((String) null);
            }
        } else {
            switch (field.getJavaType()) {
                case BOOLEAN:
                    builder.set(field.getName()).to(isNull ? null : (Boolean)value);
                    return;
                case LONG:
                    builder.set(field.getName()).to(isNull ? null : (Long)value);
                    return;
                case INT:
                    builder.set(field.getName()).to(isNull ? null : ((Integer)value).longValue());
                    return;
                case FLOAT:
                    builder.set(field.getName()).to(isNull ? null : ((Float)value).doubleValue());
                    return;
                case DOUBLE:
                    builder.set(field.getName()).to(isNull ? null : (Double)value);
                    return;
                case STRING:
                    builder.set(field.getName()).to(isNull ? null : value.toString());
                    return;
                case ENUM:
                    builder.set(field.getName()).to(isNull ? null : ((Descriptors.EnumValueDescriptor)value).getName());
                    return;
                case BYTE_STRING:
                    builder.set(field.getName()).to(isNull ? null : ByteArray.copyFrom(((ByteString) value).toByteArray()));
                    return;
                case MESSAGE: {
                    final Object object = ProtoUtil
                            .convertBuildInValue(field.getMessageType().getFullName(), (DynamicMessage) value);
                    isNull = object == null;
                    switch (ProtoUtil.ProtoType.of(field.getMessageType().getFullName())) {
                        case BOOL_VALUE:
                            builder.set(field.getName()).to(isNull ? null : ((BoolValue) object).getValue());
                            return;
                        case BYTES_VALUE:
                            builder.set(field.getName()).to(isNull ? null : ByteArray.copyFrom(((BytesValue) object).getValue().toByteArray()));
                            return;
                        case STRING_VALUE:
                            builder.set(field.getName()).to(isNull ? null : ((StringValue) object).getValue());
                            return;
                        case INT32_VALUE:
                            builder.set(field.getName()).to(isNull ? null : (long)((Int32Value) object).getValue());
                            return;
                        case INT64_VALUE:
                            builder.set(field.getName()).to(isNull ? null : ((Int64Value) object).getValue());
                            return;
                        case UINT32_VALUE:
                            builder.set(field.getName()).to(isNull ? null : (long)((UInt32Value) object).getValue());
                            return;
                        case UINT64_VALUE:
                            builder.set(field.getName()).to(isNull ? null : ((UInt64Value) object).getValue());
                            return;
                        case FLOAT_VALUE:
                            builder.set(field.getName()).to(isNull ? null : (double)((FloatValue) object).getValue());
                            return;
                        case DOUBLE_VALUE:
                            builder.set(field.getName()).to(isNull ? null : ((DoubleValue) object).getValue());
                            return;
                        case DATE: {
                            final Date date = (Date) object;
                            builder.set(field.getName()).to(isNull ? null : com.google.cloud.Date.fromYearMonthDay(date.getYear(), date.getMonth(), date.getDay()));
                            return;
                        }
                        case TIME: {
                            final TimeOfDay timeOfDay = (TimeOfDay) object;
                            builder.set(field.getName()).to(isNull ? null : timeOfDay.toString());
                            return;
                        }
                        case DATETIME: {
                            final DateTime dt = (DateTime) object;
                            final Instant ldt = LocalDateTime.of(
                                    dt.getYear(), dt.getMonth(), dt.getDay(),
                                    dt.getHours(), dt.getMinutes(), dt.getSeconds(), dt.getNanos())
                                    .atOffset(ZoneOffset.ofTotalSeconds((int)dt.getUtcOffset().getSeconds()))
                                    .toInstant();
                            builder.set(field.getName()).to(isNull ? null : com.google.cloud.Timestamp
                                    .ofTimeSecondsAndNanos(ldt.getEpochSecond(), ldt.getNano()));
                            return;
                        }
                        case TIMESTAMP: {
                            final Timestamp timestamp = (Timestamp) object;
                            builder.set(field.getName()).to(com.google.cloud.Timestamp
                                    .ofTimeSecondsAndNanos(timestamp.getSeconds(), timestamp.getNanos()));
                            return;
                        }
                        case ANY: {
                            final Any any = (Any) object;
                            try {
                                builder.set(field.getName()).to(isNull ? null : printer.print(any));
                            } catch (InvalidProtocolBufferException e) {
                                builder.set(field.getName()).to(isNull ? null : any.getValue().toStringUtf8());
                            }
                            return;
                        }
                        case LATLNG:
                            builder.set(field.getName()).to(isNull ? null : object.toString());
                            return;
                        case EMPTY:
                        case NULL_VALUE:
                            builder.set(field.getName()).to((String) null);
                            return;
                        case CUSTOM:
                        default: {
                            final Type type = convertSchema(field.getMessageType());
                            final Struct struct = convert(type, field.getMessageType(), (DynamicMessage) value, printer);
                            builder.set(field.getName()).to(type, struct);
                            return;
                        }
                    }
                }
                default:
                    builder.set(field.getName()).to((String) null);
            }
        }

    }

}
