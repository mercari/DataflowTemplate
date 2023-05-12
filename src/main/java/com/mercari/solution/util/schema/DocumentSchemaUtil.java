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
        final Value value = document.getFieldsOrDefault(fieldName, null);
        if(value == null
                || Value.ValueTypeCase.NULL_VALUE.equals(value.getValueTypeCase())
                || Value.ValueTypeCase.VALUETYPE_NOT_SET.equals(value.getValueTypeCase())) {
            return null;
        }
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
            default:
                return null;
        }
    }

    public static byte[] getBytes(final Document document, final String fieldName) {
        final Value value = document.getFieldsOrDefault(fieldName, null);
        if(Value.ValueTypeCase.NULL_VALUE.equals(value.getValueTypeCase())
                || Value.ValueTypeCase.VALUETYPE_NOT_SET.equals(value.getValueTypeCase())) {
            return null;
        }
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
            default:
                return null;
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
