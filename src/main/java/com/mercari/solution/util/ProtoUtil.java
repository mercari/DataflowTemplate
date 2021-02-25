package com.mercari.solution.util;

import com.google.protobuf.*;
import com.google.protobuf.util.JsonFormat;
import com.google.type.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ProtoUtil {

    public enum ProtoType {

        // https://github.com/googleapis/googleapis/tree/master/google/type
        // https://github.com/protocolbuffers/protobuf/tree/master/src/google/protobuf

        DATE("google.type.Date"),
        TIME("google.type.TimeOfDay"),
        DATETIME("google.type.DateTime"),
        LATLNG("google.type.LatLng"),
        ANY("google.protobuf.Any"),
        TIMESTAMP("google.protobuf.Timestamp"),
        //DURATION("google.protobuf.Duration"),
        BOOL_VALUE("google.protobuf.BoolValue"),
        STRING_VALUE("google.protobuf.StringValue"),
        BYTES_VALUE("google.protobuf.BytesValue"),
        INT32_VALUE("google.protobuf.Int32Value"),
        INT64_VALUE("google.protobuf.Int64Value"),
        UINT32_VALUE("google.protobuf.UInt32Value"),
        UINT64_VALUE("google.protobuf.UInt64Value"),
        FLOAT_VALUE("google.protobuf.FloatValue"),
        DOUBLE_VALUE("google.protobuf.DoubleValue"),
        NULL_VALUE("google.protobuf.NullValue"),
        EMPTY("google.protobuf.Empty"),
        CUSTOM("__custom__");

        private final String className;

        public static ProtoType of(final String classFullName) {
            for(final ProtoType type : values()) {
                if(type.className.equals(classFullName)) {
                    return type;
                }
            }
            return CUSTOM;
        }

        ProtoType(final String className) {
            this.className = className;
        }
    }

    public static Map<String, Descriptors.Descriptor> getDescriptors(final byte[] bytes) {
        try {
            final DescriptorProtos.FileDescriptorSet set = DescriptorProtos.FileDescriptorSet
                    .parseFrom(bytes);
            return getDescriptors(set);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException(e);
        }
    }

    public static Map<String, Descriptors.Descriptor> getDescriptors(final InputStream is) {
        try {
            final DescriptorProtos.FileDescriptorSet set = DescriptorProtos.FileDescriptorSet
                    .parseFrom(is);
            return getDescriptors(set);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public static JsonFormat.Printer createJsonPrinter(final Map<String, Descriptors.Descriptor> descriptors) {
        final JsonFormat.TypeRegistry.Builder builder = JsonFormat.TypeRegistry.newBuilder();
        descriptors.values().forEach(builder::add);
        return JsonFormat.printer().usingTypeRegistry(builder.build());
    }

    public static Object convertBuildInValue(final String typeFullName, final DynamicMessage value) {
        switch (ProtoType.of(typeFullName)) {
            case DATE: {
                Integer year = 0;
                Integer month = 0;
                Integer day = 0;
                for(final Map.Entry<Descriptors.FieldDescriptor, Object> entry : value.getAllFields().entrySet()) {
                    if(entry.getValue() == null) {
                        continue;
                    }
                    if("year".equals(entry.getKey().getName())) {
                        year = (Integer) entry.getValue();
                    } else if("month".equals(entry.getKey().getName())) {
                        month = (Integer) entry.getValue();
                    } else if("day".equals(entry.getKey().getName())) {
                        day = (Integer) entry.getValue();
                    }
                }
                return Date.newBuilder().setYear(year).setMonth(month).setDay(day).build();
            }
            case TIME: {
                Integer hours = 0;
                Integer minutes = 0;
                Integer seconds = 0;
                Integer nanos = 0;
                for(final Map.Entry<Descriptors.FieldDescriptor, Object> entry : value.getAllFields().entrySet()) {
                    if(entry.getValue() == null) {
                        continue;
                    }
                    if("hours".equals(entry.getKey().getName())) {
                        hours = (Integer) entry.getValue();
                    } else if("minutes".equals(entry.getKey().getName())) {
                        minutes = (Integer) entry.getValue();
                    } else if("seconds".equals(entry.getKey().getName())) {
                        seconds = (Integer) entry.getValue();
                    } else if("nanos".equals(entry.getKey().getName())) {
                        nanos = (Integer) entry.getValue();
                    }
                }
                return TimeOfDay.newBuilder().setHours(hours).setMinutes(minutes).setSeconds(seconds).setNanos(nanos).build();
            }
            case DATETIME: {
                Integer year = 0;
                Integer month = 0;
                Integer day = 0;
                Integer hours = 0;
                Integer minutes = 0;
                Integer seconds = 0;
                Integer nanos = 0;
                Duration duration = null;
                TimeZone timeZone = null;
                for(final Map.Entry<Descriptors.FieldDescriptor, Object> entry : value.getAllFields().entrySet()) {
                    final Object entryValue = entry.getValue();
                    if(entryValue == null) {
                        continue;
                    }
                    if("year".equals(entry.getKey().getName())) {
                        year = (Integer) entryValue;
                    } else if("month".equals(entry.getKey().getName())) {
                        month = (Integer) entryValue;
                    } else if("day".equals(entry.getKey().getName())) {
                        day = (Integer) entryValue;
                    } else if("hours".equals(entry.getKey().getName())) {
                        hours = (Integer) entryValue;
                    } else if("minutes".equals(entry.getKey().getName())) {
                        minutes = (Integer) entryValue;
                    } else if("seconds".equals(entry.getKey().getName())) {
                        seconds = (Integer) entryValue;
                    } else if("nanos".equals(entry.getKey().getName())) {
                        nanos = (Integer) entryValue;
                    } else if("nanos".equals(entry.getKey().getName())) {
                        nanos = (Integer) entryValue;
                    } else if("time_offset".equals(entry.getKey().getName())) {
                        if(entryValue instanceof Duration) {
                            duration = (Duration) entryValue;
                        } else if(entryValue instanceof TimeZone) {
                            timeZone = (TimeZone) entryValue;
                        }
                    }
                }
                final DateTime.Builder builder = DateTime.newBuilder()
                        .setYear(year)
                        .setMonth(month)
                        .setDay(day)
                        .setHours(hours)
                        .setMinutes(minutes)
                        .setSeconds(seconds)
                        .setNanos(nanos);
                if(duration != null) {
                    return builder.setUtcOffset(duration).build();
                } else if(timeZone != null) {
                    return builder.setTimeZone(timeZone).build();
                } else {
                    return builder.build();
                }
            }
            case LATLNG: {
                double latitude = 0D;
                double longitude = 0d;
                for (final Map.Entry<Descriptors.FieldDescriptor, Object> entry : value.getAllFields().entrySet()) {
                    if ("latitude".equals(entry.getKey().getName())) {
                        latitude = (Double) entry.getValue();
                    } else if ("longitude".equals(entry.getKey().getName())) {
                        longitude = (Double) entry.getValue();

                    }
                }
                return LatLng.newBuilder().setLatitude(latitude).setLongitude(longitude).build();
            }
            case ANY: {
                String typeUrl = null;
                ByteString anyValue = null;
                for (final Map.Entry<Descriptors.FieldDescriptor, Object> entry : value.getAllFields().entrySet()) {
                    if ("type_url".equals(entry.getKey().getName())) {
                        typeUrl = entry.getValue().toString();
                    } else if("value".equals(entry.getKey().getName())) {
                        anyValue = (ByteString) entry.getValue();
                    }
                }
                return Any.newBuilder().setTypeUrl(typeUrl).setValue(anyValue).build();
            }
            /*
            case DURATION: {
                long seconds = 0L;
                int nanos = 0;
                for (final Map.Entry<Descriptors.FieldDescriptor, Object> entry : value.getAllFields().entrySet()) {
                    if ("seconds".equals(entry.getKey().getName())) {
                        seconds = (Long) entry.getValue();
                    } else if("nanos".equals(entry.getKey().getName())) {
                        nanos = (Integer) entry.getValue();
                    }
                }
                return Duration.newBuilder().setSeconds(seconds).setNanos(nanos).build();
            }
            */
            case BOOL_VALUE: {
                for (final Map.Entry<Descriptors.FieldDescriptor, Object> entry : value.getAllFields().entrySet()) {
                    if ("value".equals(entry.getKey().getName())) {
                        return BoolValue.newBuilder().setValue((Boolean) entry.getValue()).build();
                    }
                }
                return BoolValue.newBuilder().build();
            }
            case STRING_VALUE: {
                for(final Map.Entry<Descriptors.FieldDescriptor, Object> entry : value.getAllFields().entrySet()) {
                    if("value".equals(entry.getKey().getName())) {
                        return StringValue.newBuilder().setValue(entry.getValue().toString()).build();
                    }
                }
                return StringValue.newBuilder().build();
            }
            case BYTES_VALUE: {
                for(final Map.Entry<Descriptors.FieldDescriptor, Object> entry : value.getAllFields().entrySet()) {
                    if("value".equals(entry.getKey().getName())) {
                        return BytesValue.newBuilder().setValue((ByteString)entry.getValue()).build();
                    }
                }
                return BytesValue.newBuilder().build();

            }
            case INT32_VALUE: {
                for(final Map.Entry<Descriptors.FieldDescriptor, Object> entry : value.getAllFields().entrySet()) {
                    if("value".equals(entry.getKey().getName())) {
                        return Int32Value.newBuilder().setValue((Integer)entry.getValue()).build();
                    }
                }
                return Int32Value.newBuilder().build();
            }
            case INT64_VALUE: {
                for(final Map.Entry<Descriptors.FieldDescriptor, Object> entry : value.getAllFields().entrySet()) {
                    if("value".equals(entry.getKey().getName())) {
                        return Int64Value.newBuilder().setValue((Long)entry.getValue()).build();
                    }
                }
                return Int64Value.newBuilder().build();
            }
            case UINT32_VALUE: {
                for(final Map.Entry<Descriptors.FieldDescriptor, Object> entry : value.getAllFields().entrySet()) {
                    if("value".equals(entry.getKey().getName())) {
                        return UInt32Value.newBuilder().setValue((Integer)entry.getValue()).build();
                    }
                }
                return UInt32Value.newBuilder().build();
            }
            case UINT64_VALUE: {
                for(final Map.Entry<Descriptors.FieldDescriptor, Object> entry : value.getAllFields().entrySet()) {
                    if("value".equals(entry.getKey().getName())) {
                        return UInt64Value.newBuilder().setValue((Long)entry.getValue()).build();
                    }
                }
                return UInt64Value.newBuilder().build();
            }
            case FLOAT_VALUE: {
                for(final Map.Entry<Descriptors.FieldDescriptor, Object> entry : value.getAllFields().entrySet()) {
                    if("value".equals(entry.getKey().getName())) {
                        return FloatValue.newBuilder().setValue((Float)entry.getValue()).build();
                    }
                }
                return FloatValue.newBuilder().build();
            }
            case DOUBLE_VALUE: {
                for(final Map.Entry<Descriptors.FieldDescriptor, Object> entry : value.getAllFields().entrySet()) {
                    if("value".equals(entry.getKey().getName())) {
                        return DoubleValue.newBuilder().setValue((Double)entry.getValue()).build();
                    }
                }
                return DoubleValue.newBuilder().build();
            }
            case TIMESTAMP: {
                Long seconds = 0L;
                Integer nanos = 0;
                for(final Map.Entry<Descriptors.FieldDescriptor, Object> entry : value.getAllFields().entrySet()) {
                    if(entry.getValue() == null) {
                        continue;
                    }
                    if("seconds".equals(entry.getKey().getName())) {
                        seconds = (Long) entry.getValue();
                    } else if("nanos".equals(entry.getKey().getName())) {
                        nanos = (Integer) entry.getValue();
                    }
                }
                return Timestamp.newBuilder().setSeconds(seconds).setNanos(nanos).build();
            }
            case EMPTY: {
                return Empty.newBuilder().build();
            }
            case NULL_VALUE: {
                return NullValue.NULL_VALUE;
            }
            default:
                return value;
        }
    }

    private static Map<String, Descriptors.Descriptor> getDescriptors(final DescriptorProtos.FileDescriptorSet set) {
        final List<Descriptors.FileDescriptor> fileDescriptors = new ArrayList<>();
        return getDescriptors(set.getFileList(), fileDescriptors);
    }

    private static Map<String, Descriptors.Descriptor> getDescriptors(
            final List<DescriptorProtos.FileDescriptorProto> files,
            final List<Descriptors.FileDescriptor> fileDescriptors) {

        final int processedSize = fileDescriptors.size();
        final Map<String, Descriptors.Descriptor> descriptors = new HashMap<>();
        final List<DescriptorProtos.FileDescriptorProto> failedFiles = new ArrayList<>();
        for(final DescriptorProtos.FileDescriptorProto file : files) {
            try {
                final Descriptors.FileDescriptor fileDescriptor = Descriptors.FileDescriptor
                        .buildFrom(file, fileDescriptors.toArray(new Descriptors.FileDescriptor[fileDescriptors.size()]));
                fileDescriptors.add(fileDescriptor);
                for(final Descriptors.Descriptor messageType : fileDescriptor.getMessageTypes()) {
                    for(final Descriptors.Descriptor childMessageType : messageType.getNestedTypes()) {
                        descriptors.put(childMessageType.getFullName(), childMessageType);
                    }
                    descriptors.put(messageType.getFullName(), messageType);
                }
            } catch (final Descriptors.DescriptorValidationException e) {
                failedFiles.add(file);
            }
        }

        if(processedSize == fileDescriptors.size() && failedFiles.size() > 0) {
            throw new IllegalStateException("Failed to parse descriptors");
        }

        if(failedFiles.size() > 0) {
            descriptors.putAll(getDescriptors(failedFiles, fileDescriptors));
        }

        return descriptors;
    }

}
