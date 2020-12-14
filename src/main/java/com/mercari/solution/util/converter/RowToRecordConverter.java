package com.mercari.solution.util.converter;

import com.mercari.solution.util.AvroSchemaUtil;
import com.mercari.solution.util.RowSchemaUtil;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.io.gcp.bigquery.AvroWriteRequest;
import org.apache.beam.sdk.values.Row;

import java.time.LocalDate;
import java.util.List;
import java.util.stream.Collectors;

public class RowToRecordConverter {

    public static GenericRecord convert(final AvroWriteRequest<Row> request) {
        return convert(request.getSchema(), request.getElement());
    }

    public static GenericRecord convert(final Schema schema, final Row row) {
        final GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        for(final Schema.Field field : schema.getFields()) {
            final Object value = convertRecordValue(field.schema(), row.getValue(field.name()));
            builder.set(field, value);
        }
        return builder.build();
    }

    public static Schema convertSchema(final org.apache.beam.sdk.schemas.Schema schema) {
        final SchemaBuilder.FieldAssembler<Schema> schemaFields = SchemaBuilder.record("root").fields();
        for(final org.apache.beam.sdk.schemas.Schema.Field field : schema.getFields()) {
            schemaFields.name(field.getName()).type(convertSchema(field.getType(), field.getName(), null)).noDefault();
        }
        return schemaFields.endRecord();
    }

    private static Schema convertSchema(org.apache.beam.sdk.schemas.Schema.FieldType fieldType, final String fieldName, final String parentNamespace) {
        switch (fieldType.getTypeName()) {
            case BOOLEAN:
                return fieldType.getNullable() ? AvroSchemaUtil.NULLABLE_BOOLEAN : AvroSchemaUtil.REQUIRED_BOOLEAN;
            case STRING:
                return fieldType.getNullable() ? AvroSchemaUtil.NULLABLE_STRING : AvroSchemaUtil.REQUIRED_STRING;
            case BYTES:
                return fieldType.getNullable() ? AvroSchemaUtil.NULLABLE_BYTES : AvroSchemaUtil.REQUIRED_BYTES;
            case DECIMAL:
                return fieldType.getNullable() ? AvroSchemaUtil.NULLABLE_LOGICAL_DECIMAL_TYPE : AvroSchemaUtil.REQUIRED_LOGICAL_DECIMAL_TYPE;
            case INT16:
            case INT32:
            case INT64:
                return fieldType.getNullable() ? AvroSchemaUtil.NULLABLE_LONG : AvroSchemaUtil.REQUIRED_LONG;
            case FLOAT:
            case DOUBLE:
                return fieldType.getNullable() ? AvroSchemaUtil.NULLABLE_DOUBLE : AvroSchemaUtil.REQUIRED_DOUBLE;
            case DATETIME:
                return fieldType.getNullable() ?
                        AvroSchemaUtil.NULLABLE_LOGICAL_TIMESTAMP_MICRO_TYPE :
                        AvroSchemaUtil.REQUIRED_LOGICAL_TIMESTAMP_MICRO_TYPE;
            case LOGICAL_TYPE:
                if(RowSchemaUtil.isLogicalTypeDate(fieldType)) {
                    return fieldType.getNullable() ?
                            AvroSchemaUtil.NULLABLE_LOGICAL_DATE_TYPE :
                            AvroSchemaUtil.REQUIRED_LOGICAL_DATE_TYPE;
                } else if(RowSchemaUtil.isLogicalTypeTime(fieldType)) {
                    return fieldType.getNullable() ?
                            AvroSchemaUtil.NULLABLE_LOGICAL_TIME_MICRO_TYPE :
                            AvroSchemaUtil.REQUIRED_LOGICAL_TIME_MICRO_TYPE;
                } else if(RowSchemaUtil.isLogicalTypeTimestamp(fieldType)) {
                    return fieldType.getNullable() ?
                            AvroSchemaUtil.NULLABLE_LOGICAL_TIMESTAMP_MICRO_TYPE :
                            AvroSchemaUtil.REQUIRED_LOGICAL_TIMESTAMP_MICRO_TYPE;
                } else {
                    throw new IllegalArgumentException(
                            "Unsupported Beam logical type: " + fieldType.getLogicalType().getIdentifier());
                }
            case ROW:
                final String namespace = (parentNamespace == null ? fieldName : parentNamespace + "." + fieldName).toLowerCase();
                final List<Schema.Field> fields = fieldType.getRowSchema().getFields().stream()
                        .map(f -> new Schema.Field(f.getName(), convertSchema(f.getType(), f.getName(), namespace), f.getDescription(), (Object)null, Schema.Field.Order.IGNORE))
                        .collect(Collectors.toList());
                final Schema rowSchema = Schema.createRecord(fieldName, fieldType.getTypeName().name(), namespace, false, fields);
                return fieldType.getNullable() ? Schema.createUnion(Schema.create(Schema.Type.NULL), rowSchema) : rowSchema;
            case ITERABLE:
            case ARRAY:
                final Schema arraySchema = Schema.createArray(convertSchema(fieldType.getCollectionElementType(), fieldName, parentNamespace));
                return fieldType.getNullable() ? Schema.createUnion(Schema.create(Schema.Type.NULL), arraySchema) : arraySchema;
            case MAP:
            case BYTE:
            default:
                throw new IllegalArgumentException(fieldType.getTypeName().name() + " is not supported for bigquery.");
        }
    }

    private static Object convertRecordValue(final Schema schema, final Object value) {
        if(value == null) {
            return null;
        }
        switch (schema.getType()) {
            case ENUM:
            case STRING:
            case FIXED:
            case BYTES:
            case BOOLEAN:
            case FLOAT:
            case DOUBLE:
                return value;
            case INT:
                if (LogicalTypes.date().equals(schema.getLogicalType())) {
                    if(value instanceof String) {
                        return AvroSchemaUtil.convertDateStringToInteger(value.toString());
                    } else if(value instanceof LocalDate) {
                        return ((Long)((LocalDate) value).toEpochDay()).intValue();
                    } else if(value instanceof Integer) {
                        return value;
                    } else {
                        throw new IllegalArgumentException("Class: " + value.getClass().getName() + " is illegal for date");
                    }
                } else if (LogicalTypes.timeMillis().equals(schema.getLogicalType())) {
                    if(value instanceof String) {
                        return AvroSchemaUtil.convertTimeStringToInteger(value.toString());
                    } else {
                        throw new IllegalArgumentException("Class: " + value.getClass().getName() + " is illegal for time");
                    }
                } else {
                    return Integer.valueOf(value.toString());
                }
            case LONG:
                if (LogicalTypes.timestampMillis().equals(schema.getLogicalType())) {
                    return java.time.Instant.parse(value.toString()).toEpochMilli();
                } else if (LogicalTypes.timestampMicros().equals(schema.getLogicalType())) {
                    return java.time.Instant.parse(value.toString()).toEpochMilli() * 1000;
                } else if (LogicalTypes.timeMicros().equals(schema.getLogicalType())) {
                    return Long.valueOf(value.toString());
                } else {
                    return Long.valueOf(value.toString());
                }
            case RECORD:
                return convert(schema, (Row)value);
            case ARRAY:
                return ((List<Object>)value).stream()
                        .map(v -> convertRecordValue(schema.getElementType(), v))
                        .collect(Collectors.toList());
            case UNION:
                final Schema childSchema = AvroSchemaUtil.unnestUnion(schema);
                return convertRecordValue(childSchema, value);
            case MAP:
            case NULL:
            default:
                return null;
        }
    }

}
