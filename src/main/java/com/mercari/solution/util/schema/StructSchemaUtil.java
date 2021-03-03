package com.mercari.solution.util.schema;

import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.*;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.LocalDate;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StructSchemaUtil {

    private static final Logger LOG = LoggerFactory.getLogger(StructSchemaUtil.class);

    private static final Pattern PATTERN_DATE1 = Pattern.compile("[0-9]{8}");
    private static final Pattern PATTERN_DATE2 = Pattern.compile("[0-9]{4}-[0-9]{2}-[0-9]{2}");
    private static final Pattern PATTERN_DATE3 = Pattern.compile("[0-9]{4}/[0-9]{2}/[0-9]{2}");
    private static final Pattern PATTERN_ARRAY_ELEMENT = Pattern.compile("(?<=\\<).*?(?=\\>)");


    public static boolean hasField(final Struct struct, final String fieldName) {
        if(struct == null || fieldName == null) {
            return false;
        }
        return struct.getType().getStructFields().stream()
                .anyMatch(f -> f.getName().equals(fieldName));
    }

    public static Object getValue(final Struct struct, final String field) {
        if(struct.isNull(field)) {
            return null;
        }
        switch (struct.getColumnType(field).getCode()) {
            case BOOL:
                return struct.getBoolean(field);
            case BYTES:
                return struct.getBytes(field).toBase64();
            case STRING:
                return struct.getString(field);
            case INT64:
                return struct.getLong(field);
            case FLOAT64:
                return struct.getDouble(field);
            case DATE:
                return struct.getDate(field).toString();
            case TIMESTAMP:
                return struct.getTimestamp(field).toString();
            case STRUCT:
                return struct.getStruct(field);
            case ARRAY:
            default:
                throw new IllegalArgumentException("Not supported column type: " + struct.getColumnType(field).getCode().name());
        }
    }

    public static String getAsString(final Struct struct, final String field) {
        if(struct.isNull(field)) {
            return null;
        }
        switch (struct.getColumnType(field).getCode()) {
            case BOOL:
                return Boolean.toString(struct.getBoolean(field));
            case BYTES:
                return struct.getBytes(field).toBase64();
            case STRING:
                return struct.getString(field);
            case INT64:
                return Long.toString(struct.getLong(field));
            case FLOAT64:
                return Double.toString(struct.getDouble(field));
            case DATE:
                return struct.getDate(field).toString();
            case TIMESTAMP:
                return struct.getTimestamp(field).toString();
            default:
                throw new IllegalArgumentException("Not supported column type: " + struct.getColumnType(field).getCode().name());
        }
    }

    public static byte[] getBytes(final Struct struct, final String fieldName) {
        final Type.StructField field = struct.getType().getStructFields().stream()
                .filter(f -> f.getName().equals(fieldName))
                .findAny().orElse(null);
        if(field == null) {
            return null;
        }
        switch(field.getType().getCode()) {
            case BYTES:
                return struct.getBytes(field.getName()).toByteArray();
            case STRING:
                return Base64.getDecoder().decode(struct.getString(fieldName));
            default:
                return null;
            //throw new IllegalStateException();
        }
    }

    public static long getEpochDay(final Date date) {
        return LocalDate.of(date.getYear(), date.getMonth(), date.getDayOfMonth()).toEpochDay();
    }

    public static Instant getTimestamp(final Struct struct, final String field, final Instant timestampDefault) {
        if(struct.isNull(field)) {
            return timestampDefault;
        }
        switch (struct.getColumnType(field).getCode()) {
            case STRING: {
                final String stringValue = struct.getString(field);
                try {
                    return Instant.parse(stringValue);
                } catch (Exception e) {
                    if(PATTERN_DATE1.matcher(stringValue).find()) {
                        return new DateTime(
                                Integer.valueOf(stringValue.substring(0, 4)),
                                Integer.valueOf(stringValue.substring(4, 6)),
                                Integer.valueOf(stringValue.substring(6, 8)),
                                0, 0, DateTimeZone.UTC).toInstant();
                    }

                    Matcher matcher = PATTERN_DATE2.matcher(stringValue);
                    if(matcher.find()) {
                        final String[] values = matcher.group().split("-");
                        return new DateTime(
                                Integer.valueOf(values[0]),
                                Integer.valueOf(values[1]),
                                Integer.valueOf(values[2]),
                                0, 0, DateTimeZone.UTC).toInstant();
                    }
                    matcher = PATTERN_DATE3.matcher(stringValue);
                    if(matcher.find()) {
                        final String[] values = matcher.group().split("/");
                        return new DateTime(
                                Integer.valueOf(values[0]),
                                Integer.valueOf(values[1]),
                                Integer.valueOf(values[2]),
                                0, 0, DateTimeZone.UTC).toInstant();
                    }
                    return timestampDefault;
                }
            }
            case DATE: {
                final Date date = struct.getDate(field);
                return new DateTime(date.getYear(), date.getMonth(), date.getDayOfMonth(),
                        0, 0, DateTimeZone.UTC).toInstant();
            }
            case TIMESTAMP:
                return Instant.ofEpochMilli(struct.getTimestamp(field).toSqlTimestamp().getTime());
            case INT64:
            case FLOAT64:
            case BOOL:
            case BYTES:
            default:
                throw new IllegalArgumentException("Not supported column type: " + struct.getColumnType(field).getCode().name());
        }
    }

    public static Timestamp toCloudTimestamp(final Instant instant) {
        if(instant == null) {
            return null;
        }
        return Timestamp.ofTimeMicroseconds(instant.getMillis() * 1000);
    }

    public static Type addStructField(final Type type, final List<Type.StructField> fields) {
        final List<Type.StructField> allFields = type.getStructFields();
        allFields.addAll(fields);
        return Type.struct(allFields);
    }

    public static Type flatten(final Type type, final String path, final boolean addPrefix) {
        final List<String> paths = Arrays.asList(path.split("\\."));
        final List<Type.StructField> fields = flattenFields(type, paths, null, addPrefix);
        return Type.struct(fields);
    }

    public static List<Struct> flatten(final Type type, final Struct struct, final String path, final boolean addPrefix) {
        final List<String> paths = Arrays.asList(path.split("\\."));
        final List<Object> values = flattenValues(struct, paths, null, addPrefix);

        return null;
    }

    public static Struct.Builder toBuilder(final Struct struct) {
        return toBuilder(struct, null, null);
    }

    public static Struct.Builder toBuilder(final Struct struct,
                                           final Collection<String> includeFields,
                                           final Collection<String> excludeFields) {

        final Struct.Builder builder = Struct.newBuilder();
        for(final Type.StructField field : struct.getType().getStructFields()) {
            if(includeFields != null && !includeFields.contains(field.getName())) {
                continue;
            }
            if(excludeFields != null && excludeFields.contains(field.getName())) {
                continue;
            }
            switch (field.getType().getCode()) {
                case BOOL:
                    builder.set(field.getName()).to(struct.getBoolean(field.getName()));
                    break;
                case STRING:
                    builder.set(field.getName()).to(struct.getString(field.getName()));
                    break;
                case BYTES:
                    builder.set(field.getName()).to(struct.getBytes(field.getName()));
                    break;
                case INT64:
                    builder.set(field.getName()).to(struct.getLong(field.getName()));
                    break;
                case FLOAT64:
                    builder.set(field.getName()).to(struct.getDouble(field.getName()));
                    break;
                case NUMERIC:
                    builder.set(field.getName()).to(struct.getBigDecimal(field.getName()));
                    break;
                case TIMESTAMP:
                    builder.set(field.getName()).to(struct.getTimestamp(field.getName()));
                    break;
                case DATE:
                    builder.set(field.getName()).to(struct.getDate(field.getName()));
                    break;
                case STRUCT:
                    builder.set(field.getName()).to(struct.getStruct(field.getName()));
                    break;
                case ARRAY: {
                    switch (field.getType().getArrayElementType().getCode()) {
                        case FLOAT64:
                            builder.set(field.getName()).toFloat64Array(struct.getDoubleList(field.getName()));
                            break;
                        case BOOL:
                            builder.set(field.getName()).toBoolArray(struct.getBooleanList(field.getName()));
                            break;
                        case INT64:
                            builder.set(field.getName()).toInt64Array(struct.getLongList(field.getName()));
                            break;
                        case STRING:
                            builder.set(field.getName()).toStringArray(struct.getStringList(field.getName()));
                            break;
                        case BYTES:
                            builder.set(field.getName()).toBytesArray(struct.getBytesList(field.getName()));
                            break;
                        case DATE:
                            builder.set(field.getName()).toDateArray(struct.getDateList(field.getName()));
                            break;
                        case TIMESTAMP:
                            builder.set(field.getName()).toTimestampArray(struct.getTimestampList(field.getName()));
                            break;
                        case NUMERIC:
                            builder.set(field.getName()).toNumericArray(struct.getBigDecimalList(field.getName()));
                            break;
                        case STRUCT:
                            builder.set(field.getName()).toStructArray(field.getType().getArrayElementType(), struct.getStructList(field.getName()));
                            break;
                        case ARRAY:
                            throw new IllegalStateException("Array in Array not supported for spanner struct: " + struct.getType());
                        default:
                            break;
                    }
                    break;
                }
                default:
                    break;
            }
        }
        return builder;
    }

    public static Schema convertSchemaFromInformationSchema(final List<Struct> structs, final Collection<String> columnNames) {
        final Schema.Builder builder = Schema.builder();
        for(final Struct struct : structs) {
            if(columnNames != null && !columnNames.contains(struct.getString("COLUMN_NAME"))) {
                LOG.info("skipField: " + struct.getString("COLUMN_NAME"));
                continue;
            } else {
                LOG.info("includeField: " + struct.getString("COLUMN_NAME"));
            }
            builder.addField(Schema.Field.of(
                    struct.getString("COLUMN_NAME"),
                    convertFieldType(struct.getString("SPANNER_TYPE")))
                    .withNullable("YES".equals(struct.getString("IS_NULLABLE"))));
        }
        return builder.build();
    }

    public static Type convertTypeFromInformationSchema(final List<Struct> structs, final Collection<String> columnNames) {
        final List<Type.StructField> fields = new ArrayList<>();
        for(final Struct struct : structs) {
            if(columnNames != null && !columnNames.contains(struct.getString("COLUMN_NAME"))) {
                LOG.info("skipField: " + struct.getString("COLUMN_NAME"));
                continue;
            } else {
                LOG.info("includeField: " + struct.getString("COLUMN_NAME"));
            }
            fields.add(Type.StructField.of(
                    struct.getString("COLUMN_NAME"),
                    convertSchemaField(struct.getString("SPANNER_TYPE"))));
        }
        return Type.struct(fields);
    }



    private static Schema.FieldType convertFieldType(final String t) {
        final String type = t.trim().toUpperCase();
        switch (type) {
            case "INT64":
                return Schema.FieldType.INT64;
            case "FLOAT64":
                return Schema.FieldType.DOUBLE;
            case "BOOL":
                return Schema.FieldType.BOOLEAN;
            case "DATE":
                return CalciteUtils.DATE;
            case "TIMESTAMP":
                return Schema.FieldType.DATETIME;
            case "BYTES":
                return Schema.FieldType.BYTES;
            default:
                if(type.startsWith("STRING")) {
                    return Schema.FieldType.STRING;
                } else if(type.startsWith("ARRAY")) {
                    final Matcher m = PATTERN_ARRAY_ELEMENT.matcher(type);
                    if(m.find()) {
                        return Schema.FieldType.array(convertFieldType(m.group()).withNullable(true));
                    }
                }
                throw new IllegalStateException("DataType: " + type + " is not supported!");
        }
    }

    private static Type convertSchemaField(final String t) {
        final String type = t.trim().toUpperCase();
        switch (type) {
            case "INT64":
                return Type.int64();
            case "FLOAT64":
                return Type.float64();
            case "BOOL":
                return Type.bool();
            case "DATE":
                return Type.date();
            case "TIMESTAMP":
                return Type.timestamp();
            default:
                if(type.startsWith("STRING")) {
                    return Type.string();
                } else if(type.startsWith("BYTES")) {
                    return Type.bytes();
                } else if(type.startsWith("ARRAY")) {
                    final Matcher m = PATTERN_ARRAY_ELEMENT.matcher(type);
                    if(m.find()) {
                        return Type.array(convertSchemaField(m.group()));
                    }
                }
                throw new IllegalStateException("DataType: " + type + " is not supported!");
        }
    }

    public static Mutation.WriteBuilder createMutationWriteBuilder(final String table, final String mutationOp) {
        if(mutationOp == null) {
            return Mutation.newInsertOrUpdateBuilder(table);
        }
        switch(mutationOp.trim().toUpperCase()) {
            case "INSERT":
                return Mutation.newInsertBuilder(table);
            case "UPDATE":
                return Mutation.newUpdateBuilder(table);
            case "INSERT_OR_UPDATE":
                return Mutation.newInsertOrUpdateBuilder(table);
            case "REPLACE":
                return Mutation.newReplaceBuilder(table);
            case "DELETE":
                throw new IllegalArgumentException("MutationOP(for insert) must not be DELETE!");
            default:
                return Mutation.newInsertOrUpdateBuilder(table);
        }
    }


    private static Mutation.WriteBuilder createMutationWriteBuilder(final String table, final Mutation.Op mutationOp) {
        switch(mutationOp) {
            case INSERT:
                return Mutation.newInsertBuilder(table);
            case UPDATE:
                return Mutation.newUpdateBuilder(table);
            case INSERT_OR_UPDATE:
                return Mutation.newInsertOrUpdateBuilder(table);
            case REPLACE:
                return Mutation.newReplaceBuilder(table);
            case DELETE:
                throw new IllegalArgumentException("MutationOP(for insert) must not be DELETE!");
            default:
                return Mutation.newInsertOrUpdateBuilder(table);
        }
    }

    public static <InputT> Mutation createDeleteMutation(
            final InputT element,
            final String table, final Iterable<String> keyFields,
            final ValueGetter<InputT> function) {

        if(keyFields == null) {
            throw new IllegalArgumentException("keyFields is null. Set keyFields when using mutationOp:DELETE");
        }
        Key.Builder keyBuilder = Key.newBuilder();
        for(final String keyField : keyFields) {
            keyBuilder = keyBuilder.appendObject(function.convert(element, keyField));
        }
        return Mutation.delete(table, keyBuilder.build());
    }

    private static List<Type.StructField> flattenFields(final Type type, final List<String> paths, final String prefix, final boolean addPrefix) {
        return type.getStructFields().stream()
                .flatMap(f -> {
                    final String name;
                    if(addPrefix) {
                        name = (prefix == null ? "" : prefix + "_") + f.getName();
                    } else {
                        name = f.getName();
                    }

                    if(paths.size() == 0 || !f.getName().equals(paths.get(0))) {
                        return Stream.of(Type.StructField.of(name, f.getType()));
                    }

                    if(Type.Code.ARRAY.equals(f.getType().getCode())) {
                        final Type elementType = f.getType().getArrayElementType();
                        if(Type.Code.STRUCT.equals(elementType.getCode())) {
                            return flattenFields(
                                    elementType,
                                    paths.subList(1, paths.size()), name, addPrefix)
                                    .stream();
                        } else {
                            return Stream.of(Type.StructField.of(f.getName(), elementType));
                        }
                    } else {
                        return Stream.of(Type.StructField.of(name + f.getName(), f.getType()));
                    }
                })
                .collect(Collectors.toList());
    }
    private static List<Object> flattenValues(final Struct struct, final List<String> paths, final String prefix, final boolean addPrefix) {
        final Map<String, Object> values = new HashMap<>();
        Type.StructField pathField = null;
        String pathName = null;
        for(final Type.StructField field : struct.getType().getStructFields()) {
            final String name;
            if(addPrefix) {
                name = (prefix == null ? "" : prefix + "_") + field.getName();
            } else {
                name = field.getName();
            }
            if(paths.size() == 0 || !field.getName().equals(paths.get(0))) {
                values.put(name, getValue(struct, field.getName()));
            } else {
                pathName = name;
                pathField = field;
            }
        }

        if(pathField == null) {
            return Arrays.asList(values);
        }

        if(getValue(struct, pathField.getName()) == null) {
            return Arrays.asList(values);
        }

        if(Type.Code.ARRAY.equals(pathField.getType().getCode())) {
            final List<Object> arrayValues = new ArrayList<>();
            final List<Object> array = (List)getValue(struct, pathField.getName());
            for(final Object value : array) {
                if(Type.Code.STRUCT.equals(pathField.getType().getArrayElementType().getCode())) {
                    final List<Object> list = flattenValues(
                            (Struct)value,
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
            values.put(pathName, getValue(struct, pathField.getName()));
        }

        return Arrays.asList(values);
    }

    public interface ValueGetter<InputT> extends Serializable {
        Object convert(InputT element, String fieldName);
    }

}
