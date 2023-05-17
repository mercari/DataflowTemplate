package com.mercari.solution.util.schema;

import com.google.firestore.v1.MapValue;
import com.google.firestore.v1.Value;
import com.google.firestore.v1.Document;
import com.google.protobuf.ByteString;
import com.google.protobuf.NullValue;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import com.mercari.solution.util.DateTimeUtil;
import org.apache.beam.sdk.schemas.Schema;
import org.joda.time.Instant;

import java.util.*;
import java.util.stream.Collectors;


public class DocumentSchemaUtil {

    public static Document.Builder toBuilder(final Schema schema, final Document document) {
        return document.toBuilder();
    }

    public static Document convert(final Schema schema, final Document document) {
        return document;
    }

    public static String getAsString(final Object document, final String fieldName) {
        if(document == null) {
            return null;
        }
        return getAsString((Document) document, fieldName);
    }

    public static String getAsString(final Document document, final String fieldName) {
        if(document == null || fieldName == null) {
            return null;
        }
        if(!document.containsFields(fieldName)) {
            return null;
        }

        final Value value = document.getFieldsOrThrow(fieldName);
        switch(value.getValueTypeCase()) {
            case REFERENCE_VALUE:
            case STRING_VALUE:
                return value.getStringValue();
            case BYTES_VALUE:
                return Base64.getEncoder().encodeToString(value.getBytesValue().toByteArray());
            case INTEGER_VALUE:
                return Long.toString(value.getIntegerValue());
            case DOUBLE_VALUE:
                return Double.toString(value.getDoubleValue());
            case BOOLEAN_VALUE:
                return Boolean.toString(value.getBooleanValue());
            case TIMESTAMP_VALUE:
                return Instant.ofEpochMilli(Timestamps.toMillis(value.getTimestampValue())).toString();
            case GEO_POINT_VALUE:
            case MAP_VALUE:
            case ARRAY_VALUE:
            case VALUETYPE_NOT_SET:
            case NULL_VALUE:
            default:
                return null;
        }
    }

    public static byte[] getBytes(final Document document, final String fieldName) {
        if(document == null || fieldName == null) {
            return null;
        }
        if(!document.containsFields(fieldName)) {
            return null;
        }

        final Value value = document.getFieldsOrThrow(fieldName);
        switch(value.getValueTypeCase()) {
            case REFERENCE_VALUE:
            case STRING_VALUE:
                return Base64.getDecoder().decode(value.getStringValue());
            case BYTES_VALUE:
                return value.getBytesValue().toByteArray();
            case INTEGER_VALUE:
            case DOUBLE_VALUE:
            case BOOLEAN_VALUE:
            case TIMESTAMP_VALUE:
            case GEO_POINT_VALUE:
            case MAP_VALUE:
            case ARRAY_VALUE:
            case NULL_VALUE:
            case VALUETYPE_NOT_SET:
            default:
                return null;
        }

    }

    public static List<Float> getAsFloatList(final Document document, final String fieldName) {
        if(document == null || fieldName == null) {
            return new ArrayList<>();
        }
        if(!document.containsFields(fieldName)) {
            return new ArrayList<>();
        }

        final Value value = document.getFieldsOrThrow(fieldName);
        switch (value.getValueTypeCase()) {
            case ARRAY_VALUE: {
                return value.getArrayValue().getValuesList().stream().map(v -> {
                    switch (v.getValueTypeCase()) {
                        case DOUBLE_VALUE:
                            return Double.valueOf(v.getDoubleValue()).floatValue();
                        case INTEGER_VALUE:
                            return Long.valueOf(v.getIntegerValue()).floatValue();
                        case STRING_VALUE:
                            return Float.valueOf(v.getStringValue());
                        case BOOLEAN_VALUE:
                            return v.getBooleanValue() ? 1F :  0F;
                        default:
                            return null;
                    }
                }).collect(Collectors.toList());
            }
            default:
                return new ArrayList<>();
        }
    }

    public static Object getValue(final Document document, final String fieldName) {
        if(document == null || fieldName == null) {
            return null;
        }
        if(!document.containsFields(fieldName)) {
            return null;
        }
        final Value value = document.getFieldsOrThrow(fieldName);
        switch(value.getValueTypeCase()) {
            case STRING_VALUE: return value.getStringValue();
            case BYTES_VALUE: return value.getBytesValue().toByteArray();
            case INTEGER_VALUE: return value.getIntegerValue();
            case DOUBLE_VALUE: return value.getDoubleValue();
            case BOOLEAN_VALUE: return value.getBooleanValue();
            case TIMESTAMP_VALUE: return Instant.ofEpochMilli(Timestamps.toMillis(value.getTimestampValue()));
            case MAP_VALUE: return value.getMapValue();
            case ARRAY_VALUE: {
                return value.getArrayValue().getValuesList()
                        .stream()
                        .map(v -> {
                            if(v == null) {
                                return null;
                            }
                            switch (v.getValueTypeCase()) {
                                case BOOLEAN_VALUE:
                                    return v.getBooleanValue();
                                case INTEGER_VALUE:
                                    return v.getIntegerValue();
                                case BYTES_VALUE:
                                    return v.getBytesValue().toByteArray();
                                case STRING_VALUE:
                                    return v.getStringValue();
                                case DOUBLE_VALUE:
                                    return v.getDoubleValue();
                                case GEO_POINT_VALUE:
                                    return v.getGeoPointValue();
                                case TIMESTAMP_VALUE:
                                    return Instant.ofEpochMilli(Timestamps.toMillis(v.getTimestampValue()));
                                case MAP_VALUE:
                                    return v.getMapValue();
                                case ARRAY_VALUE:
                                case NULL_VALUE:
                                case VALUETYPE_NOT_SET:
                                default:
                                    return null;
                            }
                        })
                        .collect(Collectors.toList());
            }
            case GEO_POINT_VALUE:
                return value.getGeoPointValue();
            case REFERENCE_VALUE:
            case VALUETYPE_NOT_SET:
            case NULL_VALUE:
                return null;
            default:
                throw new IllegalArgumentException(String.format("%s is not supported!", value.getValueTypeCase().name()));
        }
    }

    public static Object getAsPrimitive(Object row, Schema.FieldType fieldType, String field) {
        return null;
    }

    public static Object convertPrimitive(Schema.FieldType fieldType, Object primitiveValue) {
        return null;
    }

    public static Document merge(final Schema schema, Document document, final Map<String, ? extends Object> values) {
        Document.Builder builder = Document.newBuilder(document);
        for(final Schema.Field field : schema.getFields()) {
            final Object object = values.get(field.getName());
            final Value value =  toValue(field.getType(), object);
            builder = builder.putFields(field.getName(), value);
        }
        return builder.build();
    }

    private static Value toValue(Schema.FieldType fieldType, Object object) {
        if(object == null) {
            return Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build();
        }

        switch (fieldType.getTypeName()) {
            case BOOLEAN:
                return Value.newBuilder().setBooleanValue((Boolean)object).build();
            case STRING:
                return Value.newBuilder().setStringValue(object.toString()).build();
            case BYTES:
                return Value.newBuilder().setBytesValue(ByteString.copyFrom((byte[]) object)).build();
            case BYTE:
                return Value.newBuilder().setIntegerValue((Byte) object).build();
            case INT16:
                return Value.newBuilder().setIntegerValue((Short) object).build();
            case INT32:
                return Value.newBuilder().setIntegerValue((Integer) object).build();
            case INT64:
                return Value.newBuilder().setIntegerValue((Long) object).build();
            case FLOAT:
                return Value.newBuilder().setDoubleValue((Float) object).build();
            case DOUBLE:
                return Value.newBuilder().setDoubleValue((Double) object).build();
            case DATETIME: {
                if(object instanceof Timestamp) {
                    return Value.newBuilder().setTimestampValue(((Timestamp) object)).build();
                } else if(object instanceof Long) {
                    return Value.newBuilder().setTimestampValue(DateTimeUtil.toProtoTimestamp((Long) object)).build();
                } else if(object instanceof Instant) {
                    return Value.newBuilder().setTimestampValue(DateTimeUtil.toProtoTimestamp((Instant) object)).build();
                } else if(object instanceof String) {
                    final Instant instant = DateTimeUtil.toJodaInstant((String) object);
                    return Value.newBuilder().setTimestampValue(DateTimeUtil.toProtoTimestamp(instant)).build();
                } else {
                    throw new IllegalStateException();
                }
            }
            case DECIMAL:
                return Value.newBuilder().setStringValue(object.toString()).build();
            case ROW: {
                final Map<String, Value> childValues = new HashMap<>();
                final Map<String, Object> child = (Map<String, Object>) object;
                final Schema childSchema = fieldType.getCollectionElementType().getRowSchema();
                for(final Map.Entry<String, Object> entry : child.entrySet()) {
                    final Schema.FieldType childFieldType = childSchema.getField(entry.getKey()).getType();
                    final Value fieldValue = toValue(childFieldType, entry.getValue());
                    childValues.put(entry.getKey(), fieldValue);
                }
                return Value.newBuilder().setMapValue(MapValue.newBuilder().putAllFields(childValues).build()).build();
            }
            case ITERABLE:
            case ARRAY:
            default: {
                throw new IllegalArgumentException("Not supported type: " + fieldType + ", value: " + object);
            }
        }
    }

}
