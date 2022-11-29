package com.mercari.solution.util.schema;

import com.google.protobuf.ByteString;
import com.mercari.solution.util.DateTimeUtil;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.apache.beam.sdk.values.Row;
import org.apache.hadoop.hbase.util.Bytes;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;
import org.joda.time.ReadableDateTime;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalTime;
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
        return toBuilder(schema, row, new HashMap<>());
    }

    public static Row.FieldValueBuilder toBuilder(final Schema schema, final Row row, final Map<String, String> renameFields) {
        final Row.FieldValueBuilder builder = Row.withSchema(schema).withFieldValues(new HashMap<>());
        for(final Schema.Field field : schema.getFields()) {
            final String getFieldName = renameFields.getOrDefault(field.getName(), field.getName());
            final String setFieldName = field.getName();
            if(row.getSchema().hasField(getFieldName)) {
                if(row.getValue(getFieldName) == null) {
                    builder.withFieldValue(setFieldName, null);
                    continue;
                }
                final Schema.Field rowField = row.getSchema().getField(getFieldName);
                if(!field.getType().getTypeName().equals(rowField.getType().getTypeName())) {
                    builder.withFieldValue(setFieldName, null);
                    continue;
                }

                switch (field.getType().getTypeName()) {
                    case ITERABLE:
                    case ARRAY: {
                        if(field.getType().getCollectionElementType().getTypeName().equals(Schema.TypeName.ROW)) {
                            final List<Row> children = new ArrayList<>();
                            for(final Row child : row.<Row>getArray(getFieldName)) {
                                if(child == null) {
                                    children.add(null);
                                } else {
                                    children.add(toBuilder(field.getType().getCollectionElementType().getRowSchema(), child).build());
                                }
                            }
                            builder.withFieldValue(setFieldName, children);
                        } else {
                            builder.withFieldValue(setFieldName, row.getValue(getFieldName));
                        }
                        break;
                    }
                    case ROW: {
                        final Row child = toBuilder(field.getType().getRowSchema(), row.getRow(getFieldName)).build();
                        builder.withFieldValue(setFieldName, child);
                        break;
                    }
                    default:
                        builder.withFieldValue(setFieldName, row.getValue(getFieldName));
                        break;
                }
            } else if(renameFields.containsValue(setFieldName)) {
                final String getOuterFieldName = renameFields.entrySet().stream()
                        .filter(e -> e.getValue().equals(setFieldName))
                        .map(Map.Entry::getKey)
                        .findAny()
                        .orElse(setFieldName);
                if(row.getValue(getOuterFieldName) == null) {
                    builder.withFieldValue(setFieldName, null);
                    continue;
                }
                final Schema.Field rowField = row.getSchema().getField(getOuterFieldName);
                if(!field.getType().getTypeName().equals(rowField.getType().getTypeName())) {
                    builder.withFieldValue(setFieldName, null);
                    continue;
                }

                switch (field.getType().getTypeName()) {
                    case ITERABLE:
                    case ARRAY: {
                        if(field.getType().getCollectionElementType().getTypeName().equals(Schema.TypeName.ROW)) {
                            final List<Row> children = new ArrayList<>();
                            for(final Row child : row.<Row>getArray(getOuterFieldName)) {
                                if(child == null) {
                                    children.add(null);
                                } else {
                                    children.add(toBuilder(field.getType().getCollectionElementType().getRowSchema(), child).build());
                                }
                            }
                            builder.withFieldValue(setFieldName, children);
                        } else {
                            builder.withFieldValue(setFieldName, row.getValue(getOuterFieldName));
                        }
                        break;
                    }
                    case ROW: {
                        final Row child = toBuilder(field.getType().getRowSchema(), row.getRow(getOuterFieldName)).build();
                        builder.withFieldValue(setFieldName, child);
                        break;
                    }
                    default:
                        builder.withFieldValue(setFieldName, row.getValue(getOuterFieldName));
                        break;
                }
            } else {
                builder.withFieldValue(setFieldName, null);
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
        final Row.FieldValueBuilder builder = Row.withSchema(schema).withFieldValues(new HashMap<>());
        for(Schema.Field field : schema.getFields()) {
            if(values.containsKey(field.getName())) {
                builder.withFieldValue(field.getName(), values.get(field.getName()));
            } else if(row.getSchema().hasField(field.getName())) {
                builder.withFieldValue(field.getName(), row.getValue(field.getName()));
            } else {
                builder.withFieldValue(field.getName(), null);
            }
        }
        return builder.build();
    }

    public static Row create(final Schema schema, Map<String, Object> values) {
        final Map<String, Object> v = new HashMap<>();
        for(final Schema.Field field : schema.getFields()) {
            v.put(field.getName(), values.getOrDefault(field.getName(), null));
        }
        return Row
                .withSchema(schema)
                .withFieldValues(v)
                .build();
    }

    public static Schema selectFields(Schema schema, final List<String> fields) {
        return selectFieldsBuilder(schema, fields).build();
    }

    public static Schema.Builder selectFieldsBuilder(Schema schema, final List<String> fields) {
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
        return builder;
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

    public static boolean isSqlTypeJson(final Schema.Options fieldOptions) {
        if(fieldOptions == null) {
            return false;
        }
        if(!fieldOptions.hasOption("sqlType")) {
            return false;
        }
        final String sqlType = fieldOptions.getValue("sqlType", String.class);
        if("json".equalsIgnoreCase(sqlType)) {
            return true;
        }
        return false;
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
        if(row == null) {
            return null;
        }
        if(!row.getSchema().hasField(field)) {
            return null;
        }
        if(row.getValue(field) == null) {
            return null;
        }
        return row.getValue(field).toString();
    }

    public static Long getAsLong(final Row row, final String fieldName) {
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
                return row.getBoolean(fieldName) ? 1L : 0L;
            case STRING: {
                try {
                    return Long.valueOf(row.getString(fieldName));
                } catch (Exception e) {
                    return null;
                }
            }
            case BYTE:
                return row.getByte(fieldName).longValue();
            case INT16:
                return row.getInt16(fieldName).longValue();
            case INT32:
                return row.getInt32(fieldName).longValue();
            case INT64:
                return row.getInt64(fieldName);
            case FLOAT:
                return row.getFloat(fieldName).longValue();
            case DOUBLE:
                return row.getDouble(fieldName).longValue();
            case DECIMAL:
                return row.getDecimal(fieldName).longValue();
            case LOGICAL_TYPE:
            case DATETIME:
            case BYTES:
            case ARRAY:
            case ITERABLE:
            case ROW:
            case MAP:
            default:
                return null;
        }
    }

    public static Double getAsDouble(final Row row, final String fieldName) {
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
                return row.getBoolean(fieldName) ? 1D : 0D;
            case STRING: {
                try {
                    return Double.valueOf(row.getString(fieldName));
                } catch (Exception e) {
                    return null;
                }
            }
            case BYTE:
                return row.getByte(fieldName).doubleValue();
            case INT16:
                return row.getInt16(fieldName).doubleValue();
            case INT32:
                return row.getInt32(fieldName).doubleValue();
            case INT64:
                return row.getInt64(fieldName).doubleValue();
            case FLOAT:
                return row.getFloat(fieldName).doubleValue();
            case DOUBLE:
                return row.getDouble(fieldName);
            case DECIMAL:
                return row.getDecimal(fieldName).doubleValue();
            case LOGICAL_TYPE:
            case DATETIME:
            case BYTES:
            case ARRAY:
            case ITERABLE:
            case ROW:
            case MAP:
            default:
                return null;
        }
    }

    public static BigDecimal getAsBigDecimal(final Row row, final String fieldName) {
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
                return BigDecimal.valueOf(row.getBoolean(fieldName) ? 1D : 0D);
            case STRING: {
                try {
                    return BigDecimal.valueOf(Double.valueOf(row.getString(fieldName)));
                } catch (Exception e) {
                    return null;
                }
            }
            case BYTE:
                return BigDecimal.valueOf(row.getByte(fieldName).longValue());
            case INT16:
                return BigDecimal.valueOf(row.getInt16(fieldName).longValue());
            case INT32:
                return BigDecimal.valueOf(row.getInt32(fieldName).longValue());
            case INT64:
                return BigDecimal.valueOf(row.getInt64(fieldName));
            case FLOAT:
                return BigDecimal.valueOf(row.getFloat(fieldName).doubleValue());
            case DOUBLE:
                return BigDecimal.valueOf(row.getDouble(fieldName));
            case DECIMAL:
                return row.getDecimal(fieldName);
            case LOGICAL_TYPE:
            case DATETIME:
            case BYTES:
            case ARRAY:
            case ITERABLE:
            case ROW:
            case MAP:
            default:
                return null;
        }
    }

    // for bigtable
    public static ByteString getAsByteString(final Row row, final String fieldName) {
        if(row == null || fieldName == null) {
            return null;
        }
        if(!row.getSchema().hasField(fieldName)) {
            return null;
        }
        final Schema.Field field = row.getSchema().getField(fieldName);
        final Object value = row.getValue(fieldName);
        if(value == null) {
            return null;
        }

        final byte[] bytes;
        switch (field.getType().getTypeName()) {
            case BOOLEAN:
                bytes = Bytes.toBytes(row.getBoolean(fieldName));
                break;
            case STRING:
                bytes = Bytes.toBytes(row.getString(fieldName));
                break;
            case BYTES:
                bytes = row.getBytes(fieldName);
                break;
            case BYTE:
                bytes = Bytes.toBytes(row.getByte(fieldName));
                break;
            case INT16:
                bytes = Bytes.toBytes(row.getInt16(fieldName));
                break;
            case INT32:
                bytes = Bytes.toBytes(row.getInt32(fieldName));
                break;
            case INT64:
                bytes = Bytes.toBytes(row.getInt64(fieldName));
                break;
            case FLOAT:
                bytes = Bytes.toBytes(row.getFloat(fieldName));
                break;
            case DOUBLE:
                bytes = Bytes.toBytes(row.getDouble(fieldName));
                break;
            case DECIMAL:
                bytes = Bytes.toBytes(row.getDecimal(fieldName));
                break;
            case DATETIME: {
                final Long epochMicroSecond = DateTimeUtil.toEpochMicroSecond(row.getDateTime(fieldName));
                bytes = Bytes.toBytes(epochMicroSecond);
                break;
            }
            case LOGICAL_TYPE: {
                if(isLogicalTypeDate(field.getType())) {
                    final LocalDate localDate = row.getValue(fieldName);
                    bytes = Bytes.toBytes(((Long)localDate.toEpochDay()).intValue());
                } else if(isLogicalTypeTime(field.getType())) {
                    final LocalTime localTime = row.getValue(fieldName);
                    bytes = Bytes.toBytes(DateTimeUtil.toMilliOfDay(localTime));
                } else if(isLogicalTypeEnum(field.getType())) {
                    final EnumerationType.Value enumValue = row.getValue(fieldName);
                    final String evalue = ((EnumerationType)field.getType().getLogicalType()).getValues().get(enumValue.getValue());
                    bytes = Bytes.toBytes(evalue);
                } else {
                    return null;
                }
                break;
            }
            default:
                return null;
        }
        return ByteString.copyFrom(bytes);
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

    public static Instant getAsInstant(final Row row, final String fieldName) {
        return getTimestamp(row, fieldName, null);
    }

    public static Instant getTimestamp(final Row row, final String fieldName, final Instant defaultTimestamp) {
        final Schema.Field field = row.getSchema().getField(fieldName);
        if(field == null) {
            return defaultTimestamp;
        }
        if(!row.getSchema().hasField(fieldName)) {
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
            case INT16: {
                return Instant.ofEpochMilli((short) value);
            }
            case INT32: {
                final LocalDate localDate = LocalDate.ofEpochDay((int) value);
                return new DateTime(localDate.getYear(), localDate.getMonthValue(), localDate.getDayOfMonth(),
                        0, 0, DateTimeZone.UTC).toInstant();
            }
            case INT64: {
                return Instant.ofEpochMilli((long) value);
            }
            case FLOAT: {
                return Instant.ofEpochMilli(((Float) value).longValue());
            }
            case DOUBLE: {
                return Instant.ofEpochMilli(((Double) value).longValue());
            }
            case BYTES:
            case BOOLEAN:
            case MAP:
            case DECIMAL:
            case BYTE:
            case ARRAY:
            case ITERABLE:
            case ROW:
            default:
                return defaultTimestamp;
        }
    }

    public static Instant toInstant(final Object value) {
        return (Instant) value;
    }

    public static EnumerationType.Value toEnumerationTypeValue(final Schema.FieldType fieldType, final String value) {
        final int typeCode = fieldType
                .getLogicalType(EnumerationType.class)
                .getArgument()
                .getOrDefault(value, 0);
        return new EnumerationType.Value(typeCode);
    }

    public static String toString(final Schema.FieldType fieldType, final EnumerationType.Value value) {
        return fieldType.getLogicalType(EnumerationType.class).toString(value);
    }

}
