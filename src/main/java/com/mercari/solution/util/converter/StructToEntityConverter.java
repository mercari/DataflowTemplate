package com.mercari.solution.util.converter;

import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.datastore.v1.ArrayValue;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.Value;
import com.google.protobuf.ByteString;
import com.google.protobuf.NullValue;
import com.mercari.solution.util.gcp.DatastoreUtil;
import com.mercari.solution.util.gcp.SpannerUtil;
import com.mercari.solution.util.schema.StructSchemaUtil;

import java.util.UUID;
import java.util.stream.Collectors;

public class StructToEntityConverter {


    public static Entity convert(final Type type, final Struct struct,
                                 final String kind, final Iterable<String> keyFields, final String splitter) {

        final Key.PathElement pathElement;
        if(keyFields != null) {
            final StringBuilder sb = new StringBuilder();
            for (final String keyField : keyFields) {
                final String keyValue = StructSchemaUtil.getAsString(struct, keyField);
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
        for(Type.StructField field : type.getStructFields()) {
            builder.putProperties(field.getName(), convertValue(field, struct));
        }
        return builder.build();
    }

    private static Value convertValue(final Type.StructField field, final Struct struct) {
        if(struct.isNull(field.getName())) {
            return Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build();
        }
        switch (field.getType().getCode()) {
            case BOOL:
                return Value.newBuilder().setBooleanValue(struct.getBoolean(field.getName())).build();
            case BYTES: {
                final byte[] bytes = struct.getBytes(field.getName()).toByteArray();
                if(bytes.length > DatastoreUtil.QUOTE_VALUE_SIZE) {
                    return Value.newBuilder().setBlobValue(ByteString.copyFrom(bytes)).setExcludeFromIndexes(true).build();
                } else {
                    return Value.newBuilder().setBlobValue(ByteString.copyFrom(bytes)).build();
                }
            }
            case STRING: {
                final String stringValue = struct.getString(field.getName());
                if(stringValue.getBytes().length > DatastoreUtil.QUOTE_VALUE_SIZE) {
                    return Value.newBuilder().setStringValue(stringValue).setExcludeFromIndexes(true).build();
                } else {
                    return Value.newBuilder().setStringValue(stringValue).build();
                }
            }
            case INT64:
                return Value.newBuilder().setIntegerValue(struct.getLong(field.getName())).build();
            case FLOAT64:
                return Value.newBuilder().setDoubleValue(struct.getDouble(field.getName())).build();
            case DATE:
                return Value.newBuilder().setStringValue(struct.getDate(field.getName()).toString()).build();
            case TIMESTAMP:
                return Value.newBuilder().setTimestampValue(struct.getTimestamp(field.getName()).toProto()).build();
            case STRUCT:
                final Struct childStruct = struct.getStruct(field.getName());
                Entity.Builder builder = Entity.newBuilder();
                for(Type.StructField childField : field.getType().getStructFields()) {
                    builder.putProperties(childField.getName(), convertValue(childField, childStruct));
                }
                return Value.newBuilder().setEntityValue(builder.build()).setExcludeFromIndexes(true).build();
            case ARRAY:
                switch (field.getType().getArrayElementType().getCode()) {
                    case BOOL:
                        return Value.newBuilder().setArrayValue(ArrayValue.newBuilder()
                                .addAllValues((struct.getBooleanList(field.getName()).stream()
                                        .map(o -> Value.newBuilder().setBooleanValue(o).build())
                                        .collect(Collectors.toList())))
                                .build())
                                .build();
                    case BYTES:
                        return Value.newBuilder().setArrayValue(ArrayValue.newBuilder()
                                .addAllValues((struct.getBytesList(field.getName()).stream()
                                        .map(o -> Value.newBuilder().setBlobValue(ByteString.copyFrom(o.toByteArray())).build())
                                        .collect(Collectors.toList())))
                                .build())
                                .build();
                    case STRING:
                        return Value.newBuilder().setArrayValue(ArrayValue.newBuilder()
                                .addAllValues((struct.getStringList(field.getName()).stream()
                                        .map(o -> Value.newBuilder().setStringValue(o).build())
                                        .collect(Collectors.toList())))
                                .build())
                                .build();
                    case INT64:
                        return Value.newBuilder().setArrayValue(ArrayValue.newBuilder()
                                .addAllValues((struct.getLongList(field.getName()).stream()
                                        .map(o -> Value.newBuilder().setIntegerValue(o).build())
                                        .collect(Collectors.toList())))
                                .build())
                                .build();
                    case FLOAT64:
                        return Value.newBuilder().setArrayValue(ArrayValue.newBuilder()
                                .addAllValues((struct.getDoubleList(field.getName()).stream()
                                        .map(o -> Value.newBuilder().setDoubleValue(o).build())
                                        .collect(Collectors.toList())))
                                .build())
                                .build();
                    case DATE:
                        return Value.newBuilder().setArrayValue(ArrayValue.newBuilder()
                                .addAllValues((struct.getDateList(field.getName()).stream()
                                        .map(o -> Value.newBuilder().setStringValue(o.toString()).build())
                                        .collect(Collectors.toList())))
                                .build())
                                .build();
                    case TIMESTAMP:
                        return Value.newBuilder().setArrayValue(ArrayValue.newBuilder()
                                .addAllValues((struct.getTimestampList(field.getName()).stream()
                                        .map(o -> Value.newBuilder().setTimestampValue(o.toProto()).build())
                                        .collect(Collectors.toList())))
                                .build())
                                .build();
                    case STRUCT:
                        return Value.newBuilder().setArrayValue(ArrayValue.newBuilder()
                                .addAllValues((struct.getStructList(field.getName()).stream()
                                        .map(o -> {
                                            Entity.Builder childBuilder = Entity.newBuilder();
                                            for(Type.StructField childField : field.getType().getArrayElementType().getStructFields()) {
                                                childBuilder.putProperties(childField.getName(), convertValue(childField, o));
                                            }
                                            return Value.newBuilder().setEntityValue(childBuilder.build()).build();
                                        })
                                        .collect(Collectors.toList())))
                                .build())
                                .build();
                    case ARRAY:
                    default:
                        throw new IllegalArgumentException("Array in Array is not supported!");
                }
            default:
                return Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build();
        }
    }

}
