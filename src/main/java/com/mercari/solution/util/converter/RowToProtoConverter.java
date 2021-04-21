package com.mercari.solution.util.converter;

import com.google.protobuf.*;
import com.google.protobuf.util.Timestamps;
import com.mercari.solution.util.schema.ProtoSchemaUtil;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.apache.beam.sdk.values.Row;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;

import java.time.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;


public class RowToProtoConverter {

    public static Descriptors.Descriptor convertSchema(final Schema schema) {
        final DescriptorProtos.DescriptorProto.Builder builder = DescriptorProtos.DescriptorProto.newBuilder();
        for(final Schema.Field field : schema.getFields()) {
            //builder.addField()
        }
        return builder.getDescriptorForType();
    }

    public static DynamicMessage convert(final Descriptors.Descriptor messageDescriptor, final Row row) {
        final DynamicMessage.Builder builder = DynamicMessage.newBuilder(messageDescriptor);
        for(final Descriptors.FieldDescriptor field : messageDescriptor.getFields()) {
            if(field.isRepeated()) {
                if(field.isMapField()) {
                    // NOT support map
                    continue;
                } else {
                    final Collection<Object> objects = Optional.ofNullable(row.getArray(field.getName())).orElse(new ArrayList<>());
                    for(final Object value : objects) {
                        builder.addRepeatedField(field, convertValue(field, value));
                    }
                }
            } else {
                final Object value = convertValue(field, row.getValue(field.getName()));
                builder.setField(field, value);
            }
        }
        return builder.build();
    }

    private static Object convertValue(final Descriptors.FieldDescriptor field, final Object value) {

        if(value == null) {
            return null;
        }

        switch (field.getJavaType()) {
            case BOOLEAN:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case STRING:
                return value;
            case ENUM: {
                final EnumerationType.Value enumValue = (EnumerationType.Value) value;
                return field.getEnumType().findValueByNumber(enumValue.getValue());
            }
            case BYTE_STRING:
                return ByteString.copyFrom(((byte[]) value));
            case MESSAGE: {
                switch (ProtoSchemaUtil.ProtoType.of(field.getMessageType().getFullName())) {
                    case BOOL_VALUE:
                        return DynamicMessage.newBuilder(field.getMessageType())
                                .setField(field.getMessageType().findFieldByName("value"), value)
                                .build();
                    case BYTES_VALUE:
                        return DynamicMessage.newBuilder(field.getMessageType())
                                .setField(field.getMessageType().findFieldByName("value"), ByteString.copyFrom((byte[]) value))
                                .build();
                    case STRING_VALUE:
                        return DynamicMessage.newBuilder(field.getMessageType())
                                .setField(field.getMessageType().findFieldByName("value"), value.toString())
                                .build();
                    case INT32_VALUE:
                    case UINT32_VALUE:
                        return DynamicMessage.newBuilder(field.getMessageType())
                                .setField(field.getMessageType().findFieldByName("value"), (Integer)value)
                                .build();
                    case INT64_VALUE:
                    case UINT64_VALUE:
                        return DynamicMessage.newBuilder(field.getMessageType())
                                .setField(field.getMessageType().findFieldByName("value"), (Long)value)
                                .build();
                    case FLOAT_VALUE:
                        return DynamicMessage.newBuilder(field.getMessageType())
                                .setField(field.getMessageType().findFieldByName("value"), (Float)value)
                                .build();
                    case DOUBLE_VALUE:
                        return DynamicMessage.newBuilder(field.getMessageType())
                                .setField(field.getMessageType().findFieldByName("value"), (Double)value)
                                .build();
                    case DATE: {
                        final LocalDate date = (LocalDate) value;
                        return DynamicMessage.newBuilder(field.getMessageType())
                                .setField(field.getMessageType().findFieldByName("year"), date.getYear())
                                .setField(field.getMessageType().findFieldByName("month"), date.getMonthValue())
                                .setField(field.getMessageType().findFieldByName("day"), date.getDayOfMonth())
                                .build();
                    }
                    case TIME: {
                        final LocalTime time = (LocalTime) value;
                        return DynamicMessage.newBuilder(field.getMessageType())
                                .setField(field.getMessageType().findFieldByName("hours"), time.getHour())
                                .setField(field.getMessageType().findFieldByName("minutes"), time.getMinute())
                                .setField(field.getMessageType().findFieldByName("seconds"), time.getSecond())
                                .setField(field.getMessageType().findFieldByName("nanos"), time.getNano())
                                .build();
                    }
                    case DATETIME: {
                        final Descriptors.FieldDescriptor timezone = field.getMessageType().findFieldByName("time_zone");
                        final org.joda.time.DateTime dt = ((Instant) value).toDateTime(DateTimeZone.UTC);
                        return DynamicMessage.newBuilder(field.getMessageType())
                                .setField(timezone,
                                        DynamicMessage.newBuilder(timezone.getMessageType())
                                                .setField(timezone.getMessageType().findFieldByName("id"), ZoneOffset.UTC.getId())
                                                .build())
                                .setField(field.getMessageType().findFieldByName("year"), dt.getYear())
                                .setField(field.getMessageType().findFieldByName("month"), dt.getMonthOfYear())
                                .setField(field.getMessageType().findFieldByName("day"), dt.getDayOfMonth())
                                .setField(field.getMessageType().findFieldByName("hours"), dt.getHourOfDay())
                                .setField(field.getMessageType().findFieldByName("minutes"), dt.getMinuteOfHour())
                                .setField(field.getMessageType().findFieldByName("seconds"), dt.getSecondOfMinute())
                                .build();
                    }
                    case TIMESTAMP:
                        final Timestamp instant = Timestamps.fromMillis(((Instant) value).getMillis());
                        return DynamicMessage.newBuilder(field.getMessageType())
                                .setField(field.getMessageType().findFieldByName("seconds"), instant.getSeconds())
                                .setField(field.getMessageType().findFieldByName("nanos"), instant.getNanos())
                                .build();
                    case ANY:
                        return Any.newBuilder().setValue(ByteString.copyFromUtf8(value.toString())).build();
                    case EMPTY:
                        return Empty.newBuilder().build();
                    case NULL_VALUE:
                        return NullValue.NULL_VALUE;
                    case CUSTOM:
                    default: {
                        return convert(field.getMessageType(), (Row) value);
                    }
                }

            }
            default: {
                throw new IllegalArgumentException("");
            }
        }
    }

}
