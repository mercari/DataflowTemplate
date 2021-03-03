package com.mercari.solution.util.converter;

import com.google.cloud.ByteArray;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Type;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.Value;
import com.mercari.solution.util.schema.EntitySchemaUtil;
import com.mercari.solution.util.schema.StructSchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.stream.Collectors;

public class EntityToMutationConverter {

    private static final Logger LOG = LoggerFactory.getLogger(EntityToMutationConverter.class);

    private EntityToMutationConverter() {}

    public static Mutation convert(final Type type, final Entity entity, final String table, final String mutationOp,
                                    final Iterable<String> keyFields, final Set<String> excludeFields, final Set<String> hideFields) {
        if(entity == null) {
            throw new RuntimeException("entity must not be null! ");
        }
        if(type == null) {
            throw new RuntimeException("schema must not be null! " + entity.getKey().toString());
        }

        if(mutationOp != null && "DELETE".equals(mutationOp.trim().toUpperCase())) {
            return StructSchemaUtil.createDeleteMutation(entity, table, keyFields, EntitySchemaUtil::getKeyFieldValue);
        }

        final Mutation.WriteBuilder builder = StructSchemaUtil.createMutationWriteBuilder(table, mutationOp);

        final Key.PathElement pe = entity.getKey().getPath(entity.getKey().getPathCount()-1);
        if(pe.getName() == null) {
            builder.set("__key__").to(pe.getName());
        } else {
            builder.set("__key__").to(pe.getId());
        }

        for(final Type.StructField field : type.getStructFields()) {
            if(field.getName().equals("__key__") || (excludeFields != null && excludeFields.contains(field.getName()))) {
                continue;
            }
            final Value value = entity.getPropertiesOrDefault(field.getName(), null);
            setEntityValue(builder, field.getName(), value, field.getType());
        }
        return builder.build();
    }

    private static void setEntityValue(final Mutation.WriteBuilder builder, final String fieldName, final Value value, final Type type) {
        if (value == null || value.getValueTypeCase().equals(Value.ValueTypeCase.VALUETYPE_NOT_SET)
                || value.getValueTypeCase().equals(Value.ValueTypeCase.NULL_VALUE)) {
            return;
        }

        switch (type.getCode()) {
            case STRING:
                builder.set(fieldName).to(value.getStringValue());
                return;
            case BYTES:
                builder.set(fieldName).to(ByteArray.copyFrom(value.getBlobValue().toByteArray()));
                return;
            case BOOL:
                builder.set(fieldName).to(value.getBooleanValue());
                return;
            case INT64:
                builder.set(fieldName).to(value.getIntegerValue());
                return;
            case FLOAT64:
                builder.set(fieldName).to(value.getDoubleValue());
                return;
            case DATE:
                builder.set(fieldName).to(EntitySchemaUtil.convertDate(value));
                return;
            case TIMESTAMP:
                builder.set(fieldName).to(com.google.cloud.Timestamp.fromProto(value.getTimestampValue()));
                return;
            case ARRAY: {
                switch (type.getArrayElementType().getCode()) {
                    case BOOL:
                        builder.set(fieldName).toBoolArray(value.getArrayValue().getValuesList().stream()
                                .map(Value::getBooleanValue)
                                .collect(Collectors.toList()));
                        return;
                    case BYTES:
                        return;
                    case STRING:
                        builder.set(fieldName).toStringArray(value.getArrayValue().getValuesList().stream()
                                .map(Value::getStringValue)
                                .collect(Collectors.toList()));
                        return;
                    case INT64:
                        builder.set(fieldName).toInt64Array(value.getArrayValue().getValuesList().stream()
                                .map(Value::getIntegerValue)
                                .collect(Collectors.toList()));
                        return;
                    case FLOAT64:
                        builder.set(fieldName).toFloat64Array(value.getArrayValue().getValuesList().stream()
                                .map(Value::getDoubleValue)
                                .collect(Collectors.toList()));
                        return;
                    case DATE:
                        builder.set(fieldName).toDateArray(value.getArrayValue().getValuesList().stream()
                                .map(EntitySchemaUtil::convertDate)
                                .collect(Collectors.toList()));
                        return;
                    case TIMESTAMP:
                        builder.set(fieldName).toTimestampArray(value.getArrayValue().getValuesList().stream()
                                .map(Value::getTimestampValue)
                                .map(Timestamp::fromProto)
                                .collect(Collectors.toList()));
                        return;
                    case STRUCT:
                    case ARRAY:
                    default:
                        return;
                }
            }
            case STRUCT:
            default:
        }
    }



}
