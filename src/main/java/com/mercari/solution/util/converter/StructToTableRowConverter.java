package com.mercari.solution.util.converter;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.Date;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class StructToTableRowConverter {

    public static TableSchema convertSchema(final Type type) {
        final List<TableFieldSchema> tableFieldSchemas = new ArrayList<>();
        for(final Type.StructField field : type.getStructFields()) {
            tableFieldSchemas.add(convertTableFieldSchema(field));
        }
        return new TableSchema().setFields(tableFieldSchemas);
    }

    public static TableRow convert(final Struct struct) {
        TableRow row = new TableRow();
        for (final Type.StructField field : struct.getType().getStructFields()) {
            final String fieldName = field.getName();
            if ("f".equals(fieldName)) {
                throw new IllegalArgumentException("Struct must not have field name f because `f` is reserved tablerow field name.");
            }
            row = setFieldValue(row, fieldName, field.getType(), struct);
        }
        return row;
    }

    private static TableFieldSchema convertTableFieldSchema(Type.StructField field) {
        return convertTableFieldSchema(field.getType(), field.getName());
    }

    private static TableFieldSchema convertTableFieldSchema(Type fieldType, final String fieldName) {
        final String mode = "NULLABLE";//fieldType.getNullable() ? "NULLABLE" : "REQUIRED";
        final TableFieldSchema tableFieldSchema = new TableFieldSchema()
                .setName(fieldName)
                .setMode(mode);

        switch (fieldType.getCode()) {
            case BOOL:
                return tableFieldSchema.setType("BOOLEAN");
            case STRING:
                return tableFieldSchema.setType("STRING");
            case BYTES:
                return tableFieldSchema.setType("BYTES");
            case INT64:
                return tableFieldSchema.setType("INTEGER");
            case FLOAT64:
                return tableFieldSchema.setType("FLOAT");
            case NUMERIC:
                return tableFieldSchema.setType("NUMERIC");
            case DATE:
                return tableFieldSchema.setType("DATE");
            case TIMESTAMP:
                return tableFieldSchema.setType("TIMESTAMP");
            case STRUCT: {
                final List<TableFieldSchema> childTableFieldSchemas = fieldType.getStructFields().stream()
                        .map(StructToTableRowConverter::convertTableFieldSchema)
                        .collect(Collectors.toList());
                return tableFieldSchema.setType("RECORD").setFields(childTableFieldSchemas);
            }
            case ARRAY: {
                if(Type.Code.STRUCT.equals(fieldType.getArrayElementType().getCode())) {
                    final List<TableFieldSchema> childTableFieldSchemas = fieldType.getStructFields().stream()
                            .map(StructToTableRowConverter::convertTableFieldSchema)
                            .collect(Collectors.toList());
                    return tableFieldSchema
                            .setType("RECORD")
                            .setFields(childTableFieldSchemas)
                            .setMode("REPEATED");
                } else {
                    return tableFieldSchema
                            .setType(convertTableFieldSchema(fieldType.getArrayElementType(), fieldName).getType())
                            .setMode("REPEATED");
                }
            }
            default:
                throw new IllegalArgumentException(fieldType.toString() + " is not supported for bigquery.");
        }
    }

    private static TableRow setFieldValue(TableRow row, final String fieldName, final Type type,final Struct struct) {
        if(struct.isNull(fieldName)) {
            return row.set(fieldName, null);
        }
        switch (type.getCode()) {
            case STRING:
                return row.set(fieldName, struct.getString(fieldName));
            case BYTES:
                return row.set(fieldName, struct.getBytes(fieldName).toByteArray());
            case BOOL:
                return row.set(fieldName, struct.getBoolean(fieldName));
            case INT64:
                return row.set(fieldName, struct.getLong(fieldName));
            case FLOAT64:
                return row.set(fieldName, struct.getDouble(fieldName));
            case NUMERIC:
                return row.set(fieldName, struct.getBigDecimal(fieldName));
            case DATE:
                final Date date = struct.getDate(fieldName);
                final LocalDate localDate = LocalDate.of(date.getYear(), date.getMonth(), date.getDayOfMonth());
                return row.set(fieldName, localDate.format(DateTimeFormatter.ISO_LOCAL_DATE));
            case TIMESTAMP:
                return row.set(fieldName, struct.getTimestamp(fieldName).toString());
            case STRUCT:
                final Struct childStruct = struct.getStruct(fieldName);
                TableRow childRow = new TableRow();
                for(Type.StructField field : childStruct.getType().getStructFields()) {
                    childRow = setFieldValue(childRow, field.getName(), field.getType(), childStruct);
                }
                return row.set(fieldName, childRow);
            case ARRAY:
                return setArrayFieldValue(row, fieldName, type.getArrayElementType(), struct);
            default:
                return row;
        }
    }

    private static TableRow setArrayFieldValue(TableRow row, final String fieldName, final Type type, final Struct struct) {
        if (struct.isNull(fieldName)) {
            return row.set(fieldName, null);
        }
        switch (type.getCode()) {
            case STRING:
                return row.set(fieldName, struct.getStringList(fieldName).stream()
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList()));
            case BYTES:
                return row.set(fieldName, struct.getBytesList(fieldName).stream()
                        .filter(Objects::nonNull)
                        .map(bytes -> bytes.toByteArray())
                        .collect(Collectors.toList()));
            case BOOL:
                return row.set(fieldName, struct.getBooleanList(fieldName).stream()
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList()));
            case INT64:
                return row.set(fieldName, struct.getLongList(fieldName).stream()
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList()));
            case FLOAT64:
                return row.set(fieldName, struct.getDoubleList(fieldName).stream()
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList()));
            case NUMERIC:
                return row.set(fieldName, struct.getBigDecimalList(fieldName).stream()
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList()));
            case DATE:
                return row.set(fieldName, struct.getDateList(fieldName).stream()
                        .filter(Objects::nonNull)
                        .map(date -> LocalDate.of(date.getYear(), date.getMonth(), date.getDayOfMonth()))
                        .map(localDate -> localDate.format(DateTimeFormatter.ISO_LOCAL_DATE))
                        .collect(Collectors.toList()));
            case TIMESTAMP:
                final List<String> timestampList = struct.getTimestampList(fieldName).stream()
                        .filter(Objects::nonNull)
                        .map(timestamp -> timestamp.toString())
                        .collect(Collectors.toList());
                return row.set(fieldName, timestampList);
            case STRUCT:
                final List<TableRow> childRows = new ArrayList<>();
                for(Struct childStruct : struct.getStructList(fieldName)) {
                    if(childStruct == null) {
                        continue;
                    }
                    TableRow childRow = new TableRow();
                    for(final Type.StructField field : childStruct.getType().getStructFields()) {
                        childRow = setFieldValue(childRow, field.getName(), field.getType(), childStruct);
                    }
                    childRows.add(childRow);
                }
                return row.set(fieldName, childRows);
            case ARRAY:
                // Not support ARRAY in ARRAY
                return row;
            default:
                return row;
        }
    }


}
