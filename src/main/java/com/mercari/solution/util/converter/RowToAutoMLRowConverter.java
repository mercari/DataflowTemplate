package com.mercari.solution.util.converter;

import com.google.cloud.automl.v1beta1.ColumnSpec;
import com.google.protobuf.*;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class RowToAutoMLRowConverter {

    public static com.google.cloud.automl.v1beta1.Row convert(final List<ColumnSpec> columnSpecs, final Row row) {
        com.google.cloud.automl.v1beta1.Row.Builder builder = com.google.cloud.automl.v1beta1.Row.newBuilder();
        final Schema schema = row.getSchema();
        for(final ColumnSpec columnSpec : columnSpecs) {
            final Schema.Field field = schema.getField(columnSpec.getDisplayName());
            final Object object = row.getValue(columnSpec.getDisplayName());
            final Value value = convertValue(field.getType(), object);
            builder = builder.addValues(value).addColumnSpecIds(columnSpec.getName());
        }
        return builder.build();
    }

    private static Value convertValue(final Schema.FieldType fieldType, Object object) {
        if(object == null) {
            return Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build();
        }
        switch (fieldType.getTypeName()) {
            case BYTES:
                return Value.newBuilder().setStringValueBytes(ByteString.copyFrom((ByteBuffer) object)).build();
            case STRING:
                return Value.newBuilder().setStringValue((String) object).build();
            case INT16:
                return Value.newBuilder().setNumberValue((Short) object).build();
            case INT32:
                return Value.newBuilder().setNumberValue((Integer) object).build();
            case INT64:
                return Value.newBuilder().setNumberValue((Long) object).build();
            case FLOAT:
                return Value.newBuilder().setNumberValue((Float) object).build();
            case DOUBLE:
                return Value.newBuilder().setNumberValue((Double) object).build();
            case BOOLEAN:
                return Value.newBuilder().setBoolValue((Boolean) object).build();
            case ROW:
                return Value.newBuilder().setStructValue(convertStruct((Row) object)).build();
            case ARRAY:
                return Value.newBuilder().setListValue(
                        ListValue.newBuilder().addAllValues(
                                ((List<Object>) object).stream()
                                        .filter(Objects::nonNull)
                                        .map(o -> convertValue(fieldType.getCollectionElementType(), o))
                                        .collect(Collectors.toList())))
                        .build();
            case DATETIME:
            case DECIMAL:
            case MAP:
            case LOGICAL_TYPE:
            case ITERABLE:
            case BYTE:
            default:
                throw new IllegalStateException("");
        }
    }

    private static Struct convertStruct(final Row row) {
        Struct.Builder builder = Struct.newBuilder();
        if(row == null) {
            return builder.build();
        }
        for(final Schema.Field field : row.getSchema().getFields()) {
            builder = builder.putFields(field.getName(), convertValue(field.getType(), row.getValue(field.getName())));
        }
        return builder.build();
    }

}
