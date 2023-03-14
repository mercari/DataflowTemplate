package com.mercari.solution.util.schema;

import com.google.firestore.v1.Value;
import com.google.firestore.v1.Document;
import com.google.protobuf.util.Timestamps;
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

    public static Object getAsPrimitive(Object row, Schema.FieldType fieldType, String field) {
        return null;
    }

    public static Object convertPrimitive(Schema.FieldType fieldType, Object primitiveValue) {
        return null;
    }


}
