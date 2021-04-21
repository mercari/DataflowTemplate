package com.mercari.solution.util.converter;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.spanner.Struct;
import com.google.protobuf.*;
import com.mercari.solution.util.schema.ProtoSchemaUtil;

import java.time.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class StructToProtoConverter {

    public static DynamicMessage convert(final Descriptors.Descriptor messageDescriptor, final Struct struct) {
        final DynamicMessage.Builder builder = DynamicMessage.newBuilder(messageDescriptor);
        if(struct == null) {
            return builder.build();
        }
        for(final Descriptors.FieldDescriptor field : messageDescriptor.getFields()) {
            if(field.isRepeated()) {
                if(field.isMapField()) {
                    // NOT support map
                    continue;
                } else {
                    final List<?> objects = convertValues(field, struct);
                    for(final Object value : objects) {
                        builder.addRepeatedField(field, value);
                    }
                }
            } else {
                final Object value = convertValue(field, struct);
                builder.setField(field, value);
            }
        }
        return builder.build();
    }

    private static Object convertValue(final Descriptors.FieldDescriptor field, final Struct struct) {

        if(struct == null) {
            return null;
        }
        if(struct.isNull(field.getName())) {
            return null;
        }

        switch (field.getJavaType()) {
            case BOOLEAN:
                return struct.getBoolean(field.getName());
            case INT:
                return Long.valueOf(struct.getLong(field.getName())).intValue();
            case LONG:
                return struct.getLong(field.getName());
            case FLOAT:
                return Double.valueOf(struct.getDouble(field.getName())).floatValue();
            case DOUBLE:
                return struct.getDouble(field.getName());
            case STRING:
                return struct.getString(field.getName());
            case ENUM:
                return field.getEnumType().findValueByName(struct.getString(field.getName()));
            case BYTE_STRING:
                return ByteString.copyFrom(struct.getBytes(field.getName()).toByteArray());
            case MESSAGE: {
                switch (ProtoSchemaUtil.ProtoType.of(field.getMessageType().getFullName())) {
                    case BOOL_VALUE:
                        return DynamicMessage.newBuilder(field.getMessageType())
                                .setField(field.getMessageType().findFieldByName("value"), struct.getBoolean(field.getName()))
                                .build();
                    case BYTES_VALUE:
                        return DynamicMessage.newBuilder(field.getMessageType())
                                .setField(field.getMessageType().findFieldByName("value"), ByteString.copyFrom(struct.getBytes(field.getName()).toByteArray()))
                                .build();
                    case STRING_VALUE:
                        return DynamicMessage.newBuilder(field.getMessageType())
                                .setField(field.getMessageType().findFieldByName("value"), struct.getString(field.getName()))
                                .build();
                    case INT32_VALUE:
                    case UINT32_VALUE:
                        return DynamicMessage.newBuilder(field.getMessageType())
                                .setField(field.getMessageType().findFieldByName("value"), Long.valueOf(struct.getLong(field.getName())).intValue())
                                .build();
                    case INT64_VALUE:
                    case UINT64_VALUE:
                        return DynamicMessage.newBuilder(field.getMessageType())
                                .setField(field.getMessageType().findFieldByName("value"), struct.getLong(field.getName()))
                                .build();
                    case FLOAT_VALUE:
                        return DynamicMessage.newBuilder(field.getMessageType())
                                .setField(field.getMessageType().findFieldByName("value"), Double.valueOf(struct.getDouble(field.getName())).floatValue())
                                .build();
                    case DOUBLE_VALUE:
                        return DynamicMessage.newBuilder(field.getMessageType())
                                .setField(field.getMessageType().findFieldByName("value"), struct.getDouble(field.getName()))
                                .build();
                    case DATE: {
                        final Date date = struct.getDate(field.getName());
                        return DynamicMessage.newBuilder(field.getMessageType())
                                .setField(field.getMessageType().findFieldByName("year"), date.getYear())
                                .setField(field.getMessageType().findFieldByName("month"), date.getMonth())
                                .setField(field.getMessageType().findFieldByName("day"), date.getDayOfMonth())
                                .build();
                    }
                    case TIME: {
                        final LocalTime time = LocalTime.parse(struct.getString(field.getName()));
                        return DynamicMessage.newBuilder(field.getMessageType())
                                .setField(field.getMessageType().findFieldByName("hours"), time.getHour())
                                .setField(field.getMessageType().findFieldByName("minutes"), time.getMinute())
                                .setField(field.getMessageType().findFieldByName("seconds"), time.getSecond())
                                .setField(field.getMessageType().findFieldByName("nanos"), time.getNano())
                                .build();
                    }
                    case DATETIME: {
                        final Descriptors.FieldDescriptor timezone = field.getMessageType().findFieldByName("time_zone");
                        final com.google.cloud.Timestamp timestamp = struct.getTimestamp(field.getName());
                        final ZonedDateTime dt = java.time.Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos()).atZone(ZoneId.of(ZoneOffset.UTC.getId()));
                        return DynamicMessage.newBuilder(field.getMessageType())
                                .setField(timezone,
                                        DynamicMessage.newBuilder(timezone.getMessageType())
                                                .setField(timezone.getMessageType().findFieldByName("id"), ZoneOffset.UTC.getId())
                                                .build())
                                .setField(field.getMessageType().findFieldByName("year"), dt.getYear())
                                .setField(field.getMessageType().findFieldByName("month"), dt.getMonthValue())
                                .setField(field.getMessageType().findFieldByName("day"), dt.getDayOfMonth())
                                .setField(field.getMessageType().findFieldByName("hours"), dt.getHour())
                                .setField(field.getMessageType().findFieldByName("minutes"), dt.getMinute())
                                .setField(field.getMessageType().findFieldByName("seconds"), dt.getSecond())
                                .build();
                    }
                    case TIMESTAMP:
                        final com.google.cloud.Timestamp timestamp = struct.getTimestamp(field.getName());
                        return DynamicMessage.newBuilder(field.getMessageType())
                                .setField(field.getMessageType().findFieldByName("seconds"), timestamp.getSeconds())
                                .setField(field.getMessageType().findFieldByName("nanos"), timestamp.getNanos())
                                .build();
                    case ANY: {
                        return Any.newBuilder().setValue(ByteString.copyFromUtf8(struct.getString(field.getName()))).build();
                    }
                    case EMPTY: {
                        return Empty.newBuilder().build();
                    }
                    case NULL_VALUE:
                        return NullValue.NULL_VALUE;
                    case CUSTOM:
                    default: {
                        return convert(field.getMessageType(), struct.getStruct(field.getName()));
                    }
                }

            }
            default: {
                throw new IllegalArgumentException("");
            }
        }

    }

    private static List<?> convertValues(final Descriptors.FieldDescriptor field, final Struct struct) {
        if(struct == null) {
            return new ArrayList<>();
        }
        if(struct.isNull(field.getName())) {
            return new ArrayList<>();
        }

        switch (field.getJavaType()) {
            case BOOLEAN:
                return struct.getBooleanList(field.getName());
            case INT:
                return struct.getLongList(field.getName()).stream()
                        .map(Long::intValue)
                        .collect(Collectors.toList());
            case LONG:
                return struct.getLongList(field.getName());
            case FLOAT:
                return struct.getDoubleList(field.getName()).stream()
                        .map(Double::floatValue)
                        .collect(Collectors.toList());
            case DOUBLE:
                return struct.getDoubleList(field.getName());
            case STRING:
                return struct.getStringList(field.getName());
            case ENUM:
                return struct.getStringList(field.getName()).stream()
                        .map(s -> field.getEnumType().findValueByName(s))
                        .collect(Collectors.toList());
            case BYTE_STRING:
                return struct.getBytesList(field.getName()).stream()
                        .map(ByteArray::toByteArray)
                        .map(ByteString::copyFrom)
                        .collect(Collectors.toList());
            case MESSAGE: {
                switch (ProtoSchemaUtil.ProtoType.of(field.getMessageType().getFullName())) {
                    case BOOL_VALUE:
                        return struct.getBooleanList(field.getName()).stream()
                                .map(b -> DynamicMessage.newBuilder(field.getMessageType())
                                        .setField(field.getMessageType().findFieldByName("value"), b)
                                        .build())
                                .collect(Collectors.toList());
                    case BYTES_VALUE:
                        return struct.getBytesList(field.getName()).stream()
                                .map(ByteArray::toByteArray)
                                .map(b -> DynamicMessage.newBuilder(field.getMessageType())
                                        .setField(field.getMessageType().findFieldByName("value"), ByteString.copyFrom(b))
                                        .build())
                                .collect(Collectors.toList());
                    case STRING_VALUE:
                        return struct.getStringList(field.getName()).stream()
                                .map(b -> DynamicMessage.newBuilder(field.getMessageType())
                                        .setField(field.getMessageType().findFieldByName("value"), b)
                                        .build())
                                .collect(Collectors.toList());
                    case INT32_VALUE:
                    case UINT32_VALUE:
                        return struct.getLongList(field.getName()).stream()
                                .map(Long::intValue)
                                .map(b -> DynamicMessage.newBuilder(field.getMessageType())
                                        .setField(field.getMessageType().findFieldByName("value"), b)
                                        .build())
                                .collect(Collectors.toList());
                    case INT64_VALUE:
                    case UINT64_VALUE:
                        return struct.getLongList(field.getName()).stream()
                                .map(b -> DynamicMessage.newBuilder(field.getMessageType())
                                        .setField(field.getMessageType().findFieldByName("value"), b)
                                        .build())
                                .collect(Collectors.toList());
                    case FLOAT_VALUE:
                        return struct.getDoubleList(field.getName()).stream()
                                .map(Double::floatValue)
                                .map(b -> DynamicMessage.newBuilder(field.getMessageType())
                                        .setField(field.getMessageType().findFieldByName("value"), b)
                                        .build())
                                .collect(Collectors.toList());
                    case DOUBLE_VALUE:
                        return struct.getDoubleList(field.getName()).stream()
                                .map(b -> DynamicMessage.newBuilder(field.getMessageType())
                                        .setField(field.getMessageType().findFieldByName("value"), b)
                                        .build())
                                .collect(Collectors.toList());
                    case DATE:
                        return struct.getDateList(field.getName()).stream()
                                .map(date -> DynamicMessage.newBuilder(field.getMessageType())
                                        .setField(field.getMessageType().findFieldByName("year"), date.getYear())
                                        .setField(field.getMessageType().findFieldByName("month"), date.getMonth())
                                        .setField(field.getMessageType().findFieldByName("day"), date.getDayOfMonth())
                                        .build())
                                .collect(Collectors.toList());
                    case TIME:
                        return struct.getStringList(field.getName()).stream()
                                .map(LocalTime::parse)
                                .map(time -> DynamicMessage.newBuilder(field.getMessageType())
                                        .setField(field.getMessageType().findFieldByName("hours"), time.getHour())
                                        .setField(field.getMessageType().findFieldByName("minutes"), time.getMinute())
                                        .setField(field.getMessageType().findFieldByName("seconds"), time.getSecond())
                                        .setField(field.getMessageType().findFieldByName("nanos"), time.getNano())
                                        .build())
                                .collect(Collectors.toList());
                    case DATETIME: {
                        final Descriptors.FieldDescriptor timezone = field.getMessageType().findFieldByName("time_zone");
                        return struct.getTimestampList(field.getName()).stream()
                                .map(t -> java.time.Instant.ofEpochSecond(t.getSeconds(), t.getNanos()).atZone(ZoneId.of(ZoneOffset.UTC.getId())))
                                .map(dt -> DynamicMessage.newBuilder(field.getMessageType())
                                        .setField(timezone,
                                                DynamicMessage.newBuilder(timezone.getMessageType())
                                                        .setField(timezone.getMessageType().findFieldByName("id"), ZoneOffset.UTC.getId())
                                                        .build())
                                        .setField(field.getMessageType().findFieldByName("year"), dt.getYear())
                                        .setField(field.getMessageType().findFieldByName("month"), dt.getMonthValue())
                                        .setField(field.getMessageType().findFieldByName("day"), dt.getDayOfMonth())
                                        .setField(field.getMessageType().findFieldByName("hours"), dt.getHour())
                                        .setField(field.getMessageType().findFieldByName("minutes"), dt.getMinute())
                                        .setField(field.getMessageType().findFieldByName("seconds"), dt.getSecond())
                                        .build())
                                .collect(Collectors.toList());
                    }
                    case TIMESTAMP:
                        return struct.getTimestampList(field.getName()).stream()
                                .map(timestamp -> DynamicMessage.newBuilder(field.getMessageType())
                                        .setField(field.getMessageType().findFieldByName("seconds"), timestamp.getSeconds())
                                        .setField(field.getMessageType().findFieldByName("nanos"), timestamp.getNanos())
                                        .build())
                                .collect(Collectors.toList());
                    case ANY:
                        return struct.getStringList(field.getName()).stream()
                                .map(s -> Any.newBuilder().setValue(ByteString.copyFromUtf8(s)).build())
                                .collect(Collectors.toList());
                    case EMPTY:
                    case NULL_VALUE:
                        return new ArrayList<>();
                    case CUSTOM:
                    default:
                        return struct.getStructList(field.getName()).stream()
                                .map(s -> convert(field.getMessageType(), s))
                                .collect(Collectors.toList());
                }

            }
            default: {
                throw new IllegalArgumentException("");
            }
        }
    }

}
