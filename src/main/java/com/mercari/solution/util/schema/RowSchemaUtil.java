package com.mercari.solution.util.schema;

import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RowSchemaUtil {

    private static final DateTimeFormatter FORMATTER_YYYY_MM_DD = DateTimeFormat.forPattern("yyyy-MM-dd");
    private static final DateTimeFormatter FORMATTER_TIMESTAMP = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");


    public static Schema.Builder toBuilder(final Schema schema) {
        return toBuilder(schema, null, false);
    }

    public static Schema.Builder toBuilder(final Schema schema, final List<String> filter) {
        return toBuilder(schema, filter, false);
    }

    public static Schema.Builder toBuilder(final Schema schema, final List<String> filter, final boolean exclude) {
        final Schema.Builder builder = Schema.builder();
        for(final Schema.Field field : schema.getFields()) {
            if(exclude) {
                if(filter == null || !filter.contains(field.getName())) {
                    builder.addField(field);
                }
            } else {
                if(filter == null || filter.contains(field.getName())) {
                    builder.addField(field);
                }
            }
        }
        return builder;
    }

    public static Row.FieldValueBuilder toBuilder(final Row row) {
        return toBuilder(row.getSchema(), row);
    }

    public static Row.FieldValueBuilder toBuilder(final Schema schema, final Row row) {
        final Row.FieldValueBuilder builder = Row.withSchema(schema).withFieldValues(new HashMap<>());
        for(final Schema.Field field : row.getSchema().getFields()) {
            builder.withFieldValue(field.getName(), row.getValue(field.getName()));
        }
        return builder;
    }

    public static Schema addSchema(final Schema schema, final List<Schema.Field> fields) {
        Schema.Builder builder = Schema.builder();
        for(final Schema.Field field : schema.getFields()) {
            builder.addField(field);
        }
        builder.addFields(fields);
        return builder.build();
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

    public static Row merge(final Row row, final Map<String, ? extends Object> values) {
        return merge(row.getSchema(), row, values);
    }

    public static Row merge(final Schema schema, final Row row, final Map<String, ? extends Object> values) {
        final Row.Builder builder = Row.withSchema(schema);
        for(Schema.Field field : schema.getFields()) {
            if(values.containsKey(field.getName())) {
                builder.addValue(values.get(field.getName()));
            } else {
                builder.addValue(row.getValue(field.getName()));
            }
        }
        return builder.build();
    }

    public static Schema flatten(final Schema schema, final String path, final boolean addPrefix) {
        final List<String> paths = Arrays.asList(path.split("\\."));
        final List<Schema.Field> fields = flattenFields(schema, paths, null, addPrefix);
        return Schema.builder().addFields(fields).build();
    }

    public static List<Row> flatten(final Schema schema, final Row row, final String path, final boolean addPrefix) {
        final List<String> paths = Arrays.asList(path.split("\\."));
        final List<Object> values = flattenValues(row, paths, null, addPrefix);
        return values.stream()
                .map(v -> Row.withSchema(schema).withFieldValues((Map<String, Object>)v).build())
                .collect(Collectors.toList());
    }

    private static List<Schema.Field> flattenFields(final Schema schema, final List<String> paths, final String prefix, final boolean addPrefix) {
        return schema.getFields().stream()
                .flatMap(f -> {
                    final String name;
                    if(addPrefix) {
                        name = (prefix == null ? "" : prefix + "_") + f.getName();
                    } else {
                        name = f.getName();
                    }

                    if(paths.size() == 0 || !f.getName().equals(paths.get(0))) {
                        return Stream.of(Schema.Field.of(name, f.getType()).withNullable(true));
                    }

                    if(Schema.TypeName.ARRAY.equals(f.getType().getTypeName())) {
                        final Schema.FieldType elementType = f.getType().getCollectionElementType();
                        if(Schema.TypeName.ROW.equals(elementType.getTypeName())) {
                            return flattenFields(
                                    elementType.getRowSchema(),
                                    paths.subList(1, paths.size()), name, addPrefix)
                                    .stream();
                        } else {
                            return Stream.of(Schema.Field.of(f.getName(), elementType).withNullable(true));
                        }
                    } else {
                        return Stream.of(Schema.Field.of(name + f.getName(), f.getType()).withNullable(true));
                    }
                })
                .collect(Collectors.toList());
    }

    private static List<Object> flattenValues(final Row row, final List<String> paths, final String prefix, final boolean addPrefix) {
        final Map<String, Object> values = new HashMap<>();
        Schema.Field pathField = null;
        String pathName = null;
        for(final Schema.Field field : row.getSchema().getFields()) {
            final String name;
            if(addPrefix) {
                name = (prefix == null ? "" : prefix + "_") + field.getName();
            } else {
                name = field.getName();
            }
            if(paths.size() == 0 || !field.getName().equals(paths.get(0))) {
                values.put(name, row.getValue(field.getName()));
            } else {
                pathName = name;
                pathField = field;
            }
        }

        if(pathField == null) {
            return Arrays.asList(values);
        }

        if(row.getValue(pathField.getName()) == null) {
            return Arrays.asList(values);
        }

        if(Schema.TypeName.ARRAY.equals(pathField.getType().getTypeName())) {
            final List<Object> arrayValues = new ArrayList<>();
            final Collection<Object> array = row.getArray(pathField.getName());
            for(final Object value : array) {
                if(Schema.TypeName.ROW.equals(pathField.getType().getCollectionElementType().getTypeName())) {
                    final List<Object> list = flattenValues(
                            (Row)value,
                            paths.subList(1, paths.size()), pathName, addPrefix);
                    for(Object obj : list) {
                        if(obj instanceof Map) {
                            ((Map<String, Object>)obj).putAll(values);
                        }
                        arrayValues.add(obj);
                    }
                } else {
                    arrayValues.add(value);
                }
            }
            return arrayValues;
        } else {
            values.put(pathName, row.getValue(pathField.getName()));
        }

        return Arrays.asList(values);
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

    public static String getAsString(final Row row, final String field) {
        if(row.getValue(field) == null) {
            return null;
        }
        return row.getValue(field).toString();
    }

    public static byte[] getBytes(final Row row, final String fieldName) {
        if(row == null) {
            return null;
        }
        if(!row.getSchema().hasField(fieldName)) {
            return null;
        }
        if(row.getValue(fieldName) == null) {
            return null;
        }
        switch (row.getSchema().getField(fieldName).getType().getTypeName()) {
            case STRING:
                return Base64.getDecoder().decode(row.getString(fieldName));
            case BYTES:
                return row.getBytes(fieldName);
            default:
                return null;
        }
    }

    public static Instant getTimestamp(final Row row, final String fieldName, final Instant defaultTimestamp) {
        final Schema.Field field = row.getSchema().getField(fieldName);
        if(field == null) {
            return defaultTimestamp;
        }
        final Object value = row.getValue(fieldName);
        if(value == null) {
            return defaultTimestamp;
        }
        switch (field.getType().getTypeName()) {
            case DATETIME: {
                return (Instant) value;
            }
            case LOGICAL_TYPE: {
                if(RowSchemaUtil.isLogicalTypeDate(field.getType())) {
                    return (Instant) value;
                } else if(RowSchemaUtil.isLogicalTypeTimestamp(field.getType())) {
                    return (Instant) value;
                }
                return defaultTimestamp;
            }
            case STRING: {
                final String stringValue = value.toString();
                try {
                    return Instant.parse(stringValue);
                } catch (Exception e) {
                    return defaultTimestamp;
                }
            }
            case INT32: {
                final LocalDate localDate = LocalDate.ofEpochDay((int) value);
                return new DateTime(localDate.getYear(), localDate.getMonthValue(), localDate.getDayOfMonth(),
                        0, 0, DateTimeZone.UTC).toInstant();
            }
            case INT64: {
                return Instant.ofEpochMilli((long) value);
            }
            case BYTES:
            case BOOLEAN:
            case FLOAT:
            case DOUBLE:
            case MAP:
            case INT16:
            case DECIMAL:
            case BYTE:
            case ARRAY:
            case ITERABLE:
            case ROW:
            default:
                return defaultTimestamp;
        }
    }

}
