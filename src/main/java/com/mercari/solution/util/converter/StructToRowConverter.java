package com.mercari.solution.util.converter;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.mercari.solution.util.schema.RowSchemaUtil;
import com.mercari.solution.util.gcp.SpannerUtil;
import com.mercari.solution.util.schema.StructSchemaUtil;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class StructToRowConverter {

    public static Schema convertSchema(final Type type) {
        final Schema.Builder builder = Schema.builder();
        for(final Type.StructField field : type.getStructFields()) {
            builder.addField(Schema.Field.of(
                    field.getName(),
                    convertFieldType(field.getType()))
                    .withNullable(true));
        }
        return builder.build();
    }

    public static Row convert(final Schema schema, final Struct struct) {
        if(struct == null) {
            return null;
        }
        final Row.Builder builder = Row.withSchema(schema);
        final List<Object> values = new ArrayList<>();
        for(final Schema.Field field : schema.getFields()) {
            values.add(getValue(field.getName(), field.getType(), struct));
        }
        return builder.addValues(values).build();
    }

    private static Object getValue(final String fieldName, final Schema.FieldType fieldType, final Struct struct) {
        if(!StructSchemaUtil.hasField(struct, fieldName)) {
            return null;
        }
        if(struct.isNull(fieldName)) {
            return null;
        }
        switch (fieldType.getTypeName()) {
            case BOOLEAN:
                return struct.getBoolean(fieldName);
            case STRING:
                return struct.getString(fieldName);
            case DECIMAL:
            case BYTES:
                return struct.getBytes(fieldName).toByteArray();
            case INT16:
            case INT32:
            case INT64:
                return struct.getLong(fieldName);
            case FLOAT:
            case DOUBLE:
                return struct.getDouble(fieldName);
            case DATETIME:
                return Instant.ofEpochMilli(struct.getTimestamp(fieldName).toSqlTimestamp().toInstant().toEpochMilli());
            case LOGICAL_TYPE:
                if(RowSchemaUtil.isLogicalTypeDate(fieldType)) {
                    final Date date = struct.getDate(fieldName);
                    //final DateTime dateTime = DateTime.parse(date.toString(), FORMATTER_YYYY_MM_DD);
                    return LocalDate.of(date.getYear(), date.getMonth(), date.getDayOfMonth());
                } else if(RowSchemaUtil.isLogicalTypeTime(fieldType)) {
                    return struct.getString(fieldName);
                //} else if(RowSchemaUtil.isLogicalTypeTimestamp(fieldType)) {

                } else {
                    throw new IllegalArgumentException(
                            "Unsupported Beam logical type: " + fieldType.getLogicalType().getIdentifier());
                }
            case ROW:
                return convert(fieldType.getRowSchema(), struct);
            case ARRAY:
            case ITERABLE:
                return getArray(fieldName, fieldType.getCollectionElementType(), struct);
            case BYTE:
            case MAP:
            default:
                return new IllegalArgumentException("");
        }
    }

    private static List<?> getArray(final String fieldName, final Schema.FieldType fieldType, final Struct struct) {
        if(struct.isNull(fieldName)) {
            return new ArrayList<>();
        }
        switch (fieldType.getTypeName()) {
            case BOOLEAN:
                return struct.getBooleanList(fieldName);
            case STRING:
                return struct.getStringList(fieldName);
            case DECIMAL:
            case BYTES:
                return struct.getBytesList(fieldName).stream()
                        .map(ByteArray::toByteArray)
                        .collect(Collectors.toList());
            case INT16:
            case INT32:
            case INT64:
                return struct.getLongList(fieldName);
            case FLOAT:
            case DOUBLE:
                return struct.getDoubleList(fieldName);
            case DATETIME:
                return struct.getTimestampList(fieldName).stream()
                        .filter(Objects::nonNull)
                        .map(Timestamp::toSqlTimestamp)
                        .map(java.sql.Timestamp::toInstant)
                        .map(java.time.Instant::toEpochMilli)
                        .map(Instant::ofEpochMilli)
                        .collect(Collectors.toList());
            case LOGICAL_TYPE:
                if(RowSchemaUtil.isLogicalTypeDate(fieldType)) {
                    return struct.getDateList(fieldName).stream()
                            //.map(Date::toString)
                            //.map(s -> DateTime.parse(s, FORMATTER_YYYY_MM_DD))
                            .map(d -> LocalDate.of(d.getYear(), d.getMonth(), d.getDayOfMonth()))
                            .collect(Collectors.toList());
                } else if(RowSchemaUtil.isLogicalTypeTime(fieldType)) {
                    return struct.getStringList(fieldName);
                } else if(RowSchemaUtil.isLogicalTypeTimestamp(fieldType)) {

                } else {
                    throw new IllegalArgumentException(
                            "Unsupported Beam logical type: " + fieldType.getLogicalType().getIdentifier());
                }
            case ROW:
                return struct.getStructList(fieldName).stream()
                        .map(s -> convert(fieldType.getRowSchema(), s))
                        .collect(Collectors.toList());
            case ARRAY:
            case ITERABLE:
                throw new IllegalStateException("Array in Array is not supported!");
            case BYTE:
            case MAP:
            default:
                return null;
        }
    }

    private static Schema.FieldType convertFieldType(final Type type) {
        switch (type.getCode()) {
            case BYTES:
                return Schema.FieldType.BYTES.withNullable(true);
            case STRING:
                return Schema.FieldType.STRING;
            case INT64:
                return Schema.FieldType.INT64.withNullable(true);
            case FLOAT64:
                return Schema.FieldType.DOUBLE;
            case BOOL:
                return Schema.FieldType.BOOLEAN;
            case DATE:
                return CalciteUtils.DATE;
            case TIMESTAMP:
                return Schema.FieldType.DATETIME;
            case STRUCT:
                return Schema.FieldType.row(convertSchema(type));
            case ARRAY:
                return Schema.FieldType.array(convertFieldType(type.getArrayElementType()));
            default:
                throw new IllegalArgumentException("Spanner type: " + type.toString() + " not supported!");
        }
    }

}
