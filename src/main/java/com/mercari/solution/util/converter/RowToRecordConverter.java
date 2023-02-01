package com.mercari.solution.util.converter;

import com.mercari.solution.config.SourceConfig;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.RowSchemaUtil;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.io.gcp.bigquery.AvroWriteRequest;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.apache.beam.sdk.values.Row;

import java.time.LocalDate;
import java.time.LocalTime;
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
        final String schemaName;
        final org.apache.beam.sdk.schemas.Schema.Options options = schema.getOptions();
        if(options != null && options.hasOption("name")) {
            schemaName = options.getValue("name", String.class);
        } else {
            schemaName = "root";
        }
        final SchemaBuilder.FieldAssembler<Schema> schemaFields = SchemaBuilder.record(schemaName).fields();
        for(final org.apache.beam.sdk.schemas.Schema.Field field : schema.getFields()) {
            if(field.getOptions().hasOption(SourceConfig.OPTION_ORIGINAL_FIELD_NAME)) {
                schemaFields
                        .name(field.getName())
                        .prop(SourceConfig.OPTION_ORIGINAL_FIELD_NAME, field.getOptions().getValue(SourceConfig.OPTION_ORIGINAL_FIELD_NAME))
                        .type(convertFieldSchema(field.getType(), field.getOptions(), field.getName(), null))
                        .noDefault();
            } else {
                schemaFields
                        .name(field.getName())
                        .type(convertFieldSchema(field.getType(), field.getOptions(), field.getName(), null))
                        .noDefault();
            }
        }
        return schemaFields.endRecord();
    }

    public static Schema convertFieldSchema(final org.apache.beam.sdk.schemas.Schema.FieldType fieldType) {
        return convertFieldSchema(fieldType, null, "", null);
    }

    private static Schema convertFieldSchema(
            final org.apache.beam.sdk.schemas.Schema.FieldType fieldType,
            final org.apache.beam.sdk.schemas.Schema.Options fieldOptions,
            final String fieldName,
            final String parentNamespace) {

        final Schema fieldSchema;
        switch (fieldType.getTypeName()) {
            case BOOLEAN:
                fieldSchema = Schema.create(Schema.Type.BOOLEAN);
                break;
            case STRING:
                fieldSchema = Schema.create(Schema.Type.STRING);
                break;
            case BYTES:
                fieldSchema = Schema.create(Schema.Type.BYTES);
                break;
            case DECIMAL:
                fieldSchema = LogicalTypes.decimal(38, 9).addToSchema(Schema.create(Schema.Type.BYTES));
                break;
            case INT16:
            case INT32:
                fieldSchema = Schema.create(Schema.Type.INT);
                break;
            case INT64:
                fieldSchema = Schema.create(Schema.Type.LONG);
                break;
            case FLOAT:
                fieldSchema = Schema.create(Schema.Type.FLOAT);
                break;
            case DOUBLE:
                fieldSchema = Schema.create(Schema.Type.DOUBLE);
                break;
            case DATETIME:
                fieldSchema = LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
                break;
            case LOGICAL_TYPE:
                if(RowSchemaUtil.isLogicalTypeDate(fieldType)) {
                    fieldSchema = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
                    break;
                } else if(RowSchemaUtil.isLogicalTypeTime(fieldType)) {
                    fieldSchema = LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG));
                    break;
                } else if(RowSchemaUtil.isLogicalTypeTimestamp(fieldType)) {
                    fieldSchema = LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
                    break;
                } else if(RowSchemaUtil.isLogicalTypeDateTime(fieldType)) {
                    fieldSchema = (new LogicalType("local-timestamp-micros")).addToSchema(Schema.create(Schema.Type.LONG));
                    break;
                } else if(RowSchemaUtil.isLogicalTypeEnum(fieldType)) {
                    final String doc = fieldType.getLogicalType(EnumerationType.class).getValues().stream().collect(Collectors.joining(","));
                    fieldSchema = Schema.createEnum(fieldName, doc, parentNamespace, fieldType.getLogicalType(EnumerationType.class).getValues());
                    break;
                } else {
                    throw new IllegalArgumentException(
                            "Unsupported Beam logical type: " + fieldType.getLogicalType().getIdentifier());
                }
            case ROW: {
                final String namespace = (parentNamespace == null ? fieldName : parentNamespace + "." + fieldName).toLowerCase();
                final List<Schema.Field> fields = fieldType.getRowSchema().getFields().stream()
                        .map(f -> new Schema.Field(f.getName(), convertFieldSchema(f.getType(), f.getOptions(), f.getName(), namespace), f.getDescription(), (Object) null, Schema.Field.Order.IGNORE))
                        .collect(Collectors.toList());
                fieldSchema = Schema.createRecord(fieldName, fieldType.getTypeName().name(), namespace, false, fields);
                break;
            }
            case ITERABLE:
            case ARRAY: {
                fieldSchema = Schema.createArray(convertFieldSchema(fieldType.getCollectionElementType(), fieldOptions, fieldName, parentNamespace));
                break;
            }
            case MAP:
            case BYTE:
            default:
                throw new IllegalArgumentException(fieldType.getTypeName().name() + " is not supported for bigquery.");
        }

        if(fieldOptions != null
                && !org.apache.beam.sdk.schemas.Schema.TypeName.ARRAY.equals(fieldType.getTypeName())
                && !org.apache.beam.sdk.schemas.Schema.TypeName.ITERABLE.equals(fieldType.getTypeName())) {
            for(final String optionName : fieldOptions.getOptionNames()) {
                final Object optionValue = fieldOptions.getValue(optionName);
                fieldSchema.addProp(optionName, optionValue);
            }
        }

        if(fieldType.getNullable()) {
            return Schema.createUnion(Schema.create(Schema.Type.NULL), fieldSchema);
        } else {
            return fieldSchema;
        }
    }

    private static Object convertRecordValue(final Schema schema, final Object value) {
        if(value == null) {
            return null;
        }
        switch (schema.getType()) {
            case STRING:
            case FIXED:
            case BYTES:
            case BOOLEAN:
            case FLOAT:
            case DOUBLE:
                return value;
            case ENUM: {
                final EnumerationType.Value enumValue = (EnumerationType.Value) value;
                if(enumValue.getValue() >= schema.getEnumSymbols().size()) {
                    return schema.getEnumSymbols().get(0);
                }
                return schema.getEnumSymbols().get(enumValue.getValue());
            }
            case INT:
                if (LogicalTypes.date().equals(schema.getLogicalType())) {
                    if(value instanceof String) {
                        final LocalDate localDate = DateTimeUtil.toLocalDate(value.toString());
                        return localDate != null ? Long.valueOf(localDate.toEpochDay()).intValue() : null;
                    } else if(value instanceof LocalDate) {
                        return ((Long)((LocalDate) value).toEpochDay()).intValue();
                    } else if(value instanceof Integer) {
                        return value;
                    } else {
                        throw new IllegalArgumentException("Class: " + value.getClass().getName() + " is illegal for date");
                    }
                } else if (LogicalTypes.timeMillis().equals(schema.getLogicalType())) {
                    if(value instanceof String) {
                        final LocalTime localTime = DateTimeUtil.toLocalTime(value.toString());
                        return DateTimeUtil.toMilliOfDay(localTime);
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
