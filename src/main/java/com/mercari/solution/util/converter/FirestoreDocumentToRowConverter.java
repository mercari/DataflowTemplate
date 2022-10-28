package com.mercari.solution.util.converter;

import com.google.firestore.v1.Document;
import com.google.firestore.v1.MapValue;
import com.google.firestore.v1.Value;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.schema.RowSchemaUtil;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class FirestoreDocumentToRowConverter {

    public static Row convert(final Schema schema, final Document document) {
        if(schema == null) {
            throw new RuntimeException("schema must not be null for document: " + document);
        }
        final Map<String,Object> values = new HashMap<>();
        for(final Schema.Field field : schema.getFields()) {
            final Value value = document.getFieldsMap().get(field.getName());
            values.put(field.getName(), getValue(field.getType(), value));
        }
        return Row
                .withSchema(schema)
                .withFieldValues(values)
                .build();
    }

    private static Row convert(final Schema schema, final MapValue document) {
        if(schema == null) {
            throw new RuntimeException("schema must not be null for document: " + document);
        }
        final Map<String,Object> values = new HashMap<>();
        for(final Schema.Field field : schema.getFields()) {
            final Value value = document.getFieldsMap().get(field.getName());
            values.put(field.getName(), getValue(field.getType(), value));
        }
        return Row
                .withSchema(schema)
                .withFieldValues(values)
                .build();
    }

    private static Object getValue(final Schema.FieldType fieldType, final Value value) {
        if (value == null
                || value.getValueTypeCase().equals(Value.ValueTypeCase.VALUETYPE_NOT_SET)
                || value.getValueTypeCase().equals(Value.ValueTypeCase.NULL_VALUE)) {
            return null;
        }

        switch (fieldType.getTypeName()) {
            case STRING:
                return value.getStringValue();
            case BYTES:
                return value.getBytesValue().toByteArray();
            case BOOLEAN:
                return value.getBooleanValue();
            case BYTE:
            case INT16:
            case INT32:
            case INT64:
                return value.getIntegerValue();
            case FLOAT:
            case DOUBLE:
                return value.getDoubleValue();
            case DECIMAL:
                return new BigDecimal(value.getStringValue());
            case DATETIME:
                return value.getTimestampValue();
            case LOGICAL_TYPE:
                if(RowSchemaUtil.isLogicalTypeDate(fieldType)) {
                    return DateTimeUtil.toLocalDate(value.getStringValue());
                } else if(RowSchemaUtil.isLogicalTypeTime(fieldType)) {
                    return DateTimeUtil.toLocalTime(value.getStringValue());
                } else if(RowSchemaUtil.isLogicalTypeTimestamp(fieldType)) {
                    return DateTimeUtil.toJodaInstant(value.getTimestampValue());
                } else if(RowSchemaUtil.isLogicalTypeEnum(fieldType)) {
                    return value.getStringValue();
                } else {
                    throw new IllegalArgumentException(
                            "Unsupported Beam logical type: " + fieldType.getLogicalType());
                }
            case MAP: {
                final Map<String,Object> values = new HashMap<>();
                for(final Map.Entry<String, Value> entry : value.getMapValue().getFieldsMap().entrySet()) {
                    final Object object = getValue(fieldType.getMapValueType(), entry.getValue());
                    values.put(entry.getKey(), object);
                }
                return values;
            }
            case ROW:
                return convert(fieldType.getRowSchema(), value.getMapValue());
            case ITERABLE:
            case ARRAY:
                return value.getArrayValue().getValuesList().stream()
                        .map(v -> getValue(fieldType.getCollectionElementType(), v))
                        .collect(Collectors.toList());
            default:
                return null;
        }
    }

}
