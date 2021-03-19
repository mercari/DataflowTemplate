package com.mercari.solution.util.schema;

import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.apache.beam.sdk.values.Row;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;
import org.joda.time.ReadableDateTime;

import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RowSchemaUtil {

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
        for(final Schema.Field field : schema.getFields()) {
            if(row.getValue(field.getName()) == null) {
                builder.withFieldValue(field.getName(), null);
                continue;
            }
            if(row.getSchema().hasField(field.getName())) {
                switch (field.getType().getTypeName()) {
                    case ITERABLE:
                    case ARRAY: {
                        if(field.getType().getCollectionElementType().getTypeName().equals(Schema.TypeName.ROW)) {
                            final List<Row> children = new ArrayList<>();
                            for(final Row child : row.<Row>getArray(field.getName())) {
                                if(child == null) {
                                    children.add(null);
                                } else {
                                    children.add(toBuilder(field.getType().getCollectionElementType().getRowSchema(), child).build());
                                }
                            }
                            builder.withFieldValue(field.getName(), children);
                        } else {
                            builder.withFieldValue(field.getName(), row.getValue(field.getName()));
                        }
                        break;
                    }
                    case ROW: {
                        final Row child = toBuilder(field.getType().getRowSchema(), row.getRow(field.getName())).build();
                        builder.withFieldValue(field.getName(), child);
                        break;
                    }
                    default:
                        builder.withFieldValue(field.getName(), row.getValue(field.getName()));
                        break;
                }
            } else {
                builder.withFieldValue(field.getName(), null);
            }
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

    public static Schema selectFields(Schema schema, final List<String> fields) {
        final Schema.Builder builder = Schema.builder();
        final Map<String, List<String>> childFields = new HashMap<>();
        for(String field : fields) {
            if(field.contains(".")) {
                final String[] strs = field.split("\\.", 2);
                if(childFields.containsKey(strs[0])) {
                    childFields.get(strs[0]).add(strs[1]);
                } else {
                    childFields.put(strs[0], new ArrayList<>(Arrays.asList(strs[1])));
                }
            } else {
                builder.addField(schema.getField(field));
            }
        }

        if(childFields.size() > 0) {
            for(var entry : childFields.entrySet()) {
                final Schema.Field childField = schema.getField(entry.getKey());
                switch (childField.getType().getTypeName()) {
                    case ROW: {
                        final Schema childSchema = selectFields(childField.getType().getRowSchema(), entry.getValue());
                        builder.addField(entry.getKey(), Schema.FieldType.row(childSchema));
                        break;
                    }
                    case ITERABLE:
                    case ARRAY: {
                        if(!childField.getType().getCollectionElementType().getTypeName().equals(Schema.TypeName.ROW)) {
                            throw new IllegalStateException();
                        }
                        final Schema childSchema = selectFields(childField.getType().getCollectionElementType().getRowSchema(), entry.getValue());
                        builder.addField(entry.getKey(), Schema.FieldType.array(Schema.FieldType.row(childSchema).withNullable(true)).withNullable(true));
                        break;
                    }
                    default:
                        throw new IllegalStateException();
                }
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

    public static boolean isLogicalTypeEnum(final Schema.FieldType fieldType) {
        return fieldType.getLogicalType().getIdentifier().equals(EnumerationType.IDENTIFIER);
    }

    public static boolean isLogicalTypeTimestamp(final Schema.FieldType fieldType) {
        return CalciteUtils.TIMESTAMP.typesEqual(fieldType) || CalciteUtils.NULLABLE_TIMESTAMP.typesEqual(fieldType);
    }

    public static Object getValue(final Row row, final String fieldName) {
        if(row == null) {
            return null;
        }
        if(!row.getSchema().hasField(fieldName)) {
            return null;
        }
        if(row.getValue(fieldName) == null) {
            return null;
        }
        final Schema.Field field = row.getSchema().getField(fieldName);
        switch (field.getType().getTypeName()) {
            case BOOLEAN:
                return row.getBoolean(fieldName);
            case STRING:
                return row.getString(fieldName);
            case BYTES:
                return row.getBytes(fieldName);
            case BYTE:
                return row.getByte(fieldName);
            case INT16:
                return row.getInt16(fieldName);
            case INT32:
                return row.getInt32(fieldName);
            case INT64:
                return row.getInt64(fieldName);
            case FLOAT:
                return row.getFloat(fieldName);
            case DOUBLE:
                return row.getDouble(fieldName);
            case DECIMAL:
                return row.getDecimal(fieldName);
            case DATETIME:
                return row.getDateTime(fieldName).toInstant();
            case LOGICAL_TYPE:
                if(isLogicalTypeDate(field.getType())) {
                    return row.getValue(fieldName); //LocalDate
                } else if(isLogicalTypeTime(field.getType())) {
                    return row.getValue(fieldName); //LocalTime
                } else if(isLogicalTypeEnum(field.getType())) {
                    final EnumerationType.Value enumValue = row.getValue(fieldName);
                    return ((EnumerationType)field.getType().getLogicalType()).getValues().get(enumValue.getValue());
                }
                return row.getValue(fieldName);
            case ARRAY:
            case ITERABLE:
                if(field.getType().getCollectionElementType().getTypeName().equals(Schema.TypeName.DATETIME)) {
                    return row.getArray(fieldName).stream()
                            .map(v -> {
                                if(v == null) {
                                    return null;
                                }
                                final Schema.FieldType arrayType = field.getType().getCollectionElementType();
                                switch (arrayType.getTypeName()) {
                                    case DATETIME:
                                        return ((ReadableDateTime)v).toInstant();
                                    case LOGICAL_TYPE:
                                        if(isLogicalTypeEnum(field.getType().getCollectionElementType())) {
                                            final EnumerationType.Value ev = (EnumerationType.Value)v;
                                            return ((EnumerationType)arrayType.getLogicalType()).getValues().get(ev.getValue());
                                        }
                                    default:
                                        return v;
                                }
                            })
                            .collect(Collectors.toList());
                } else {
                    return row.getArray(fieldName);
                }
            case ROW:
                return row.getRow(fieldName);
            case MAP:
                return row.getMap(fieldName);
            default:
                return null;
        }
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
