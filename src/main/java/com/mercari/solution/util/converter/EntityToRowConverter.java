package com.mercari.solution.util.converter;

import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.Value;
import com.mercari.solution.util.schema.EntitySchemaUtil;
import com.mercari.solution.util.schema.RowSchemaUtil;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class EntityToRowConverter {


    public static Schema addKeyToSchema(final Schema schema) {
        List<Schema.Field> fields = new ArrayList<>();
        fields.add(Schema.Field.of("__key__", Schema.FieldType.STRING));
        return RowSchemaUtil.addSchema(schema, fields);
    }

    public static Row convert(final Schema schema, final Entity entity) {
        return convert(schema, entity, 0);
    }

    private static Row convert(final Schema schema, final Entity entity, final int depth) {
        if(schema == null) {
            throw new RuntimeException("schema must not be null! " + entity.getKey().toString());
        }
        final Row.Builder builder = Row.withSchema(schema);
        if(depth == 0) {
            builder.withFieldValue("__key__", convertKeyRecord(entity.getKey()));
        }

        for(final Schema.Field field : schema.getFields()) {
            if(field.getName().equals("__key__")) {
                continue;
            }
            final Value value = entity.getPropertiesOrDefault(field.getName(), null);
            builder.withFieldValue(field.getName(), convertEntityValue(value, field.getType(), depth + 1));
        }
        return builder.build();
    }

    private static Object convertEntityValue(final Value value, final Schema.FieldType fieldType, final int depth) {
        if (value == null || value.getValueTypeCase().equals(Value.ValueTypeCase.VALUETYPE_NOT_SET)
                || value.getValueTypeCase().equals(Value.ValueTypeCase.NULL_VALUE)) {
            return null;
        }

        switch (fieldType.getTypeName()) {
            case STRING:
                return value.getStringValue();
            case BYTES:
                return value.getBlobValue().toByteArray();
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
                return value.getStringValue();
            case DATETIME:
                return value.getTimestampValue();
            case LOGICAL_TYPE:
                if(RowSchemaUtil.isLogicalTypeDate(fieldType)) {
                    return EntitySchemaUtil.convertDate(value);
                } else if(RowSchemaUtil.isLogicalTypeTime(fieldType)) {
                    return value.getStringValue();
                } else if(RowSchemaUtil.isLogicalTypeTimestamp(fieldType)) {
                    return value.getTimestampValue();
                } else {
                    throw new IllegalArgumentException(
                            "Unsupported Beam logical type: " + fieldType.getLogicalType());
                }
            case ROW:
                return convert(fieldType.getRowSchema(), value.getEntityValue(), depth + 1);
            case ITERABLE:
            case ARRAY:
                return value.getArrayValue().getValuesList().stream()
                        .map(v -> convertEntityValue(v, fieldType.getCollectionElementType(), depth))
                        .collect(Collectors.toList());
            case MAP:
            default:
                return null;
        }
    }

    private static String convertKeyRecord(final Key key) {
        if(key.getPathCount() == 0) {
            throw new RuntimeException("PathList size must not be zero! " + key.toString() + " " + key.getPathList().size());
        }
        final Key.PathElement lastPath = key.getPath(key.getPathCount() - 1);
        return lastPath.getName() == null ? Long.toString(lastPath.getId()) : lastPath.getName();
    }

}
