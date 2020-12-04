package com.mercari.solution.util;


import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.Collection;
import java.util.List;

public class RowSchemaUtil {

    private static final DateTimeFormatter FORMATTER_YYYY_MM_DD = DateTimeFormat.forPattern("yyyy-MM-dd");
    private static final DateTimeFormatter FORMATTER_TIMESTAMP = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");

    public static Schema addSchema(final Schema schema, final List<Schema.Field> fields) {
        Schema.Builder builder = Schema.builder();
        for(final Schema.Field field : schema.getFields()) {
            builder.addField(field);
        }
        builder.addFields(fields);
        return builder.build();
    }

    public static boolean isLogicalTypeDate(final Schema.FieldType fieldType) {
        return CalciteUtils.DATE.typesEqual(fieldType) ||
                CalciteUtils.NULLABLE_DATE.typesEqual(fieldType) ||
                fieldType.getLogicalType().getIdentifier().equals(CalciteUtils.DATE.getLogicalType().getIdentifier()) ||
                fieldType.getLogicalType().getIdentifier().equals(CalciteUtils.NULLABLE_DATE.getLogicalType().getIdentifier());
    }

    public static boolean isLogicalTypeTime(final Schema.FieldType fieldType) {
        return CalciteUtils.TIME.typesEqual(fieldType) ||
                CalciteUtils.NULLABLE_TIME.typesEqual(fieldType) ||
                fieldType.getLogicalType().getIdentifier().equals(CalciteUtils.TIME.getLogicalType().getIdentifier()) ||
                fieldType.getLogicalType().getIdentifier().equals(CalciteUtils.NULLABLE_TIME.getLogicalType().getIdentifier());
    }

    public static boolean isLogicalTypeTimestamp(final Schema.FieldType fieldType) {
        return CalciteUtils.TIMESTAMP.typesEqual(fieldType) || CalciteUtils.NULLABLE_TIMESTAMP.typesEqual(fieldType);
    }

    public static Object getLogicalTypeValue(final Schema.FieldType fieldType, final Object value) {
        switch (fieldType.getLogicalType().getBaseType().getTypeName()) {
            case DATETIME:
                switch (fieldType.getLogicalType().getArgumentType().getTypeName()) {
                    case STRING:
                        return ((Instant) value).toString(FORMATTER_YYYY_MM_DD);
                }
        }
        return null;
    }

    public static Schema removeFields(final Schema schema, final Collection<String> excludeFields) {
        if(excludeFields == null || excludeFields.size() == 0) {
            return schema;
        }

        final Schema.Builder builder = Schema.builder();
        for(final Schema.Field field : schema.getFields()) {
            if(excludeFields.contains(field.getName())) {
                continue;
            }
            builder.addField(field);
        }

        final Schema.Options.Builder optionBuilder = Schema.Options.builder();
        for(final String optionName : schema.getOptions().getOptionNames()) {
            optionBuilder.setOption(optionName, schema.getOptions().getType(optionName), schema.getOptions().getValue(optionName));
        }
        builder.setOptions(optionBuilder);

        return builder.build();
    }

    public static String getAsString(final Row row, final String field) {
        if(row.getValue(field) == null) {
            return null;
        }
        return row.getValue(field).toString();
    }

}
