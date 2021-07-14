package com.mercari.solution.util.converter;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.mercari.solution.util.schema.RowSchemaUtil;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class RowToTableRowConverter {

    private static final Logger LOG = LoggerFactory.getLogger(RowToTableRowConverter.class);

    private static final DateTimeFormatter FORMATTER_YYYY_MM_DD = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter FORMATTER_HH_MM_SS   = DateTimeFormatter.ofPattern("HH:mm:ss");
    private static final org.joda.time.format.DateTimeFormatter FORMATTER_TIMESTAMP = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");

    public static TableRow convert(final Row row) {
        return convert(row.getSchema(), row);
    }

    public static TableRow convert(final Schema schema, final Row row) {
        if(row == null) {
            return null;
        }
        final TableRow tableRow = new TableRow();
        for(final Schema.Field field : schema.getFields()) {
            tableRow.set(field.getName(), convertValue(field.getType(), row.getValue(field.getName())));
        }
        return tableRow;
    }

    public static TableSchema convertTableSchema(final Schema schema) {
        final List<TableFieldSchema> tableFieldSchemas = new ArrayList<>();
        for(final Schema.Field field : schema.getFields()) {
            tableFieldSchemas.add(convertTableFieldSchema(field.getType(), field.getName()));
        }
        return new TableSchema().setFields(tableFieldSchemas);
    }

    private static Object convertValue(final Schema.FieldType fieldType, final Object value) {
        if(value == null) {
            return null;
        }
        switch (fieldType.getTypeName()) {
            case STRING:
            case BOOLEAN:
            case INT64:
            case DOUBLE:
            case DECIMAL:
                return value;
            case BYTES:
                return ByteBuffer.wrap((byte[]) value);
            case INT32:
                return ((Integer) value).longValue();
            case FLOAT:
                return ((Float) value).doubleValue();
            case DATETIME:
                return ((Instant) value).toString(FORMATTER_TIMESTAMP);
            case LOGICAL_TYPE:
                if(RowSchemaUtil.isLogicalTypeDate(fieldType)) {
                    return ((LocalDate) value).format(FORMATTER_YYYY_MM_DD);
                } else if(RowSchemaUtil.isLogicalTypeTime(fieldType)) {
                    return ((LocalTime) value).format(FORMATTER_HH_MM_SS);
                } else {
                    throw new IllegalArgumentException(
                            "Unsupported Beam logical type: " + fieldType.getLogicalType().getIdentifier());
                }
            case ROW:
                return convert(fieldType.getRowSchema(), (Row) value);
            case ITERABLE:
            case ARRAY:
                return ((List<?>) value).stream()
                        .map(v -> convertValue(fieldType.getCollectionElementType(), v))
                        .collect(Collectors.toList());
            case BYTE:
            case MAP:
            case INT16:
            default:
                return value;
        }
    }

    private static TableFieldSchema convertTableFieldSchema(Schema.Field field) {
        return convertTableFieldSchema(field.getType(), field.getName());
    }

    private static TableFieldSchema convertTableFieldSchema(Schema.FieldType fieldType, final String fieldName) {
        final String mode = fieldType.getNullable() ? "NULLABLE" : "REQUIRED";
        final TableFieldSchema tableFieldSchema = new TableFieldSchema()
                .setMode(mode);

        switch (fieldType.getTypeName()) {
            case BOOLEAN:
                return tableFieldSchema.setName(fieldName).setType("BOOLEAN");
            case STRING:
                return tableFieldSchema.setName(fieldName).setType("STRING");
            case BYTES:
                return tableFieldSchema.setName(fieldName).setType("BYTES");
            case DECIMAL:
                return tableFieldSchema.setName(fieldName).setType("NUMERIC");
            case INT16:
            case INT32:
            case INT64:
                return tableFieldSchema.setName(fieldName).setType("INTEGER");
            case FLOAT:
            case DOUBLE:
                return tableFieldSchema.setName(fieldName).setType("FLOAT");
            case DATETIME:
                return tableFieldSchema.setName(fieldName).setType("TIMESTAMP");
            case LOGICAL_TYPE:
                if(RowSchemaUtil.isLogicalTypeDate(fieldType)) {
                    return tableFieldSchema.setName(fieldName).setType("DATE");
                } else if(RowSchemaUtil.isLogicalTypeTime(fieldType)) {
                    return tableFieldSchema.setName(fieldName).setType("STRING");
                } else {
                    throw new IllegalArgumentException(
                            "Unsupported Beam logical type: " + fieldType.getLogicalType().getIdentifier());
                }
            case ROW: {
                final List<TableFieldSchema> childTableFieldSchemas = fieldType.getRowSchema().getFields().stream()
                        .map(RowToTableRowConverter::convertTableFieldSchema)
                        .collect(Collectors.toList());
                return tableFieldSchema.setName(fieldName).setType("RECORD").setFields(childTableFieldSchemas);
            }
            case ITERABLE:
            case ARRAY: {
                final List<TableFieldSchema> childTableFieldSchemas = fieldType
                        .getCollectionElementType()
                        .getRowSchema()
                        .getFields()
                        .stream()
                        .map(RowToTableRowConverter::convertTableFieldSchema)
                        .collect(Collectors.toList());
                if(Schema.TypeName.ROW.equals(fieldType.getCollectionElementType().getTypeName())) {
                    return tableFieldSchema
                            .setName(fieldName)
                            .setType("RECORD")
                            .setFields(childTableFieldSchemas)
                            .setMode("REPEATED");
                } else {
                    return tableFieldSchema
                            .setName(fieldName)
                            .setType(convertTableFieldSchema(fieldType.getCollectionElementType(), "").getType())
                            .setMode("REPEATED");
                }
            }
            case MAP:
            case BYTE:
            default:
                throw new IllegalArgumentException(fieldType.getTypeName().name() + " is not supported for bigquery.");
        }
    }

}
