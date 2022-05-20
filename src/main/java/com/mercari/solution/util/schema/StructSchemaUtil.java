package com.mercari.solution.util.schema;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.*;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.Timestamps;
import com.mercari.solution.util.DateTimeUtil;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.hadoop.hbase.util.Bytes;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
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

    public static Object getValue(final Struct struct, final String fieldName) {
        if(struct.isNull(fieldName)) {
            return null;
        }
        switch (struct.getColumnType(fieldName).getCode()) {
            case BOOL:
                return struct.getBoolean(fieldName);
            case BYTES:
                return struct.getBytes(fieldName).toByteArray();
            case STRING:
                return struct.getString(fieldName);
            case JSON:
                return struct.getJson(fieldName);
            case INT64:
                return struct.getLong(fieldName);
            case FLOAT64:
                return struct.getDouble(fieldName);
            case NUMERIC:
                return struct.getBigDecimal(fieldName);
            case DATE: {
                final Date date = struct.getDate(fieldName);
                return LocalDate.of(date.getYear(), date.getMonth(), date.getDayOfMonth());
            }
            case TIMESTAMP: {
                return Instant.ofEpochMilli(Timestamps.toMillis(struct.getTimestamp(fieldName).toProto()));
            }
            case STRUCT:
                return struct.getStruct(fieldName);
            case ARRAY:
            default:
                throw new IllegalArgumentException("Not supported column type: " + struct.getColumnType(fieldName).getCode().name());
        }
    }

    public static Object getCSVLineValue(final Struct struct, final String field) {
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
            case JSON:
                return struct.getJson(field);
            case INT64:
                return struct.getLong(field);
            case FLOAT64:
                return struct.getDouble(field);
            case NUMERIC:
                return struct.getBigDecimal(field);
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

    public static Value getStructValue(final Struct struct, final String field) {
        if(!hasField(struct, field)) {
            return null;
        }
        if(struct.isNull(field)) {
            return null;
        }
        switch (struct.getColumnType(field).getCode()) {
            case BOOL:
                return Value.bool(struct.getBoolean(field));
            case BYTES:
                return Value.bytes(struct.getBytes(field));
            case NUMERIC:
                return Value.numeric(struct.getBigDecimal(field));
            case STRING:
                return Value.string(struct.getString(field));
            case JSON:
                return Value.json(struct.getJson(field));
            case INT64:
                return Value.int64(struct.getLong(field));
            case FLOAT64:
                return Value.float64(struct.getDouble(field));
            case DATE:
                return Value.date(struct.getDate(field));
            case TIMESTAMP:
                return Value.timestamp(struct.getTimestamp(field));
            case STRUCT:
                return Value.struct(struct.getColumnType(field), struct.getStruct(field));
            case ARRAY: {
                switch (struct.getColumnType(field).getArrayElementType().getCode()) {
                    case BOOL:
                        return Value.boolArray(struct.getBooleanArray(field));
                    case BYTES:
                        return Value.bytesArray(struct.getBytesList(field));
                    case NUMERIC:
                        return Value.numericArray(struct.getBigDecimalList(field));
                    case STRING:
                        return Value.stringArray(struct.getStringList(field));
                    case JSON:
                        return Value.jsonArray(struct.getJsonList(field));
                    case INT64:
                        return Value.int64Array(struct.getLongArray(field));
                    case FLOAT64:
                        return Value.float64Array(struct.getDoubleArray(field));
                    case DATE:
                        return Value.dateArray(struct.getDateList(field));
                    case TIMESTAMP:
                        return Value.timestampArray(struct.getTimestampList(field));
                    case STRUCT:
                        return Value.structArray(struct.getColumnType(field).getArrayElementType(), struct.getStructList(field));
                }
            }
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
            case JSON:
                return struct.getJson(field);
            case INT64:
                return Long.toString(struct.getLong(field));
            case FLOAT64:
                return Double.toString(struct.getDouble(field));
            case NUMERIC:
                return struct.getBigDecimal(field).toString();
            case DATE:
                return struct.getDate(field).toString();
            case TIMESTAMP:
                return struct.getTimestamp(field).toString();
            default:
                throw new IllegalArgumentException("Not supported column type: " + struct.getColumnType(field).getCode().name());
        }
    }

    public static Long getAsLong(final Struct struct, final String field) {
        if(struct.isNull(field)) {
            return null;
        }
        switch (struct.getColumnType(field).getCode()) {
            case BOOL:
                return struct.getBoolean(field) ? 1L: 0L;
            case STRING: {
                try {
                    return Long.valueOf(struct.getString(field));
                } catch (Exception e) {
                    return null;
                }
            }
            case INT64:
                return struct.getLong(field);
            case FLOAT64:
                return Double.valueOf(struct.getDouble(field)).longValue();
            case NUMERIC:
                return struct.getBigDecimal(field).longValue();
            case DATE:
            case TIMESTAMP:
            case BYTES:
            case JSON:
            case STRUCT:
            case ARRAY:
            default:
                throw new IllegalArgumentException("Not supported column type: " + struct.getColumnType(field).getCode().name());
        }
    }

    public static Double getAsDouble(final Struct struct, final String field) {
        if(struct.isNull(field)) {
            return null;
        }
        switch (struct.getColumnType(field).getCode()) {
            case BOOL:
                return struct.getBoolean(field) ? 1D: 0D;
            case STRING: {
                try {
                    return Double.valueOf(struct.getString(field));
                } catch (Exception e) {
                    return null;
                }
            }
            case INT64:
                return Long.valueOf(struct.getLong(field)).doubleValue();
            case FLOAT64:
                return struct.getDouble(field);
            case NUMERIC:
                return struct.getBigDecimal(field).doubleValue();
            case DATE:
            case TIMESTAMP:
            case BYTES:
            case JSON:
            case STRUCT:
            case ARRAY:
            default:
                throw new IllegalArgumentException("Not supported column type: " + struct.getColumnType(field).getCode().name());
        }
    }

    public static BigDecimal getAsBigDecimal(final Struct struct, final String field) {
        if(struct.isNull(field)) {
            return null;
        }
        switch (struct.getColumnType(field).getCode()) {
            case BOOL:
                return BigDecimal.valueOf(struct.getBoolean(field) ? 1D: 0D);
            case STRING: {
                try {
                    return BigDecimal.valueOf(Double.valueOf(struct.getString(field)));
                } catch (Exception e) {
                    return null;
                }
            }
            case INT64:
                return BigDecimal.valueOf(struct.getLong(field));
            case FLOAT64:
                return BigDecimal.valueOf(struct.getDouble(field));
            case NUMERIC:
                return struct.getBigDecimal(field);
            case DATE:
            case TIMESTAMP:
            case BYTES:
            case JSON:
            case STRUCT:
            case ARRAY:
            default:
                throw new IllegalArgumentException("Not supported column type: " + struct.getColumnType(field).getCode().name());
        }
    }

    // for bigtable
    public static ByteString getAsByteString(final Struct struct, final String fieldName) {
        if(struct == null || fieldName == null) {
            return null;
        }
        if(!StructSchemaUtil.hasField(struct, fieldName)) {
            return null;
        }
        final Type.StructField field = struct.getType().getStructFields().stream().filter(f -> f.getName().equals(fieldName)).findAny().get();

        final byte[] bytes;
        switch (field.getType().getCode()) {
            case BOOL:
                bytes = Bytes.toBytes(struct.getBoolean(fieldName));
                break;
            case STRING:
                bytes = Bytes.toBytes(struct.getString(fieldName));
                break;
            case JSON:
                bytes = Bytes.toBytes(struct.getJson(fieldName));
                break;
            case BYTES:
                bytes = struct.getBytes(fieldName).toByteArray();
                break;
            case INT64:
                bytes = Bytes.toBytes(struct.getLong(fieldName));
                break;
            case FLOAT64:
                bytes = Bytes.toBytes(struct.getDouble(fieldName));
                break;
            case NUMERIC:
                bytes = Bytes.toBytes(struct.getBigDecimal(fieldName));
                break;
            case DATE: {
                final Date date = struct.getDate(fieldName);
                bytes = Bytes.toBytes(DateTimeUtil.toEpochDay(date));
                break;
            }
            case TIMESTAMP: {
                final Timestamp timestamp = struct.getTimestamp(fieldName);
                bytes = Bytes.toBytes(Timestamps.toMicros(timestamp.toProto()));
                break;
            }
            case STRUCT:
            case ARRAY:
            default:
                return null;
        }
        return ByteString.copyFrom(bytes);
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
            case JSON:
                return Base64.getDecoder().decode(struct.getJson(fieldName));
            default:
                return null;
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
            case JSON:
            case NUMERIC:
            case STRUCT:
            case ARRAY:
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
        final List<Type.StructField> allFields = new ArrayList<>(type.getStructFields());
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
        final List<Map<String, Value>> values = flattenValues(struct, paths, null, addPrefix);
        return values.stream()
                .map(map -> {
                    final Struct.Builder builder = Struct.newBuilder();
                    map.entrySet().forEach(e -> {
                        if(e.getValue() != null) {
                            builder.set(e.getKey()).to(e.getValue());
                        }
                    });
                    return builder.build();
                })
                .collect(Collectors.toList());
    }

    public static Struct.Builder toBuilder(final Type type, final Struct struct) {
        final Struct.Builder builder = Struct.newBuilder();
        for(final Type.StructField field : type.getStructFields()) {
            if(!hasField(struct, field.getName())) {
                continue;
            }
            final Value value = getStructValue(struct, field.getName());
            if(value != null) {
                switch (field.getType().getCode()) {
                    case ARRAY: {
                        if(field.getType().getArrayElementType().getCode().equals(Type.Code.STRUCT)) {
                            final List<Struct> children = new ArrayList<>();
                            for(final Struct child : struct.getStructList(field.getName())) {
                                if(child == null) {
                                    children.add(null);
                                } else {
                                    children.add(toBuilder(field.getType().getArrayElementType(), child).build());
                                }
                            }
                            builder.set(field.getName()).toStructArray(field.getType().getArrayElementType(), children);
                        } else {
                            builder.set(field.getName()).to(value);
                        }
                        break;
                    }
                    case STRUCT: {
                        final Struct child = toBuilder(field.getType(), struct.getStruct(field.getName())).build();
                        builder.set(field.getName()).to(child);
                        break;
                    }
                    default:
                        builder.set(field.getName()).to(value);
                        break;
                }
            } else {
                switch (field.getType().getCode()) {
                    case BOOL:
                        builder.set(field.getName()).to((Boolean)null);
                        break;
                    case JSON:
                    case STRING:
                        builder.set(field.getName()).to((String)null);
                        break;
                    case BYTES:
                        builder.set(field.getName()).to((ByteArray) null);
                        break;
                    case INT64:
                        builder.set(field.getName()).to((Long)null);
                        break;
                    case FLOAT64:
                        builder.set(field.getName()).to((Double)null);
                        break;
                    case NUMERIC:
                        builder.set(field.getName()).to((BigDecimal) null);
                        break;
                    case DATE:
                        builder.set(field.getName()).to((Date)null);
                        break;
                    case TIMESTAMP:
                        builder.set(field.getName()).to((Timestamp)null);
                        break;
                    case STRUCT:
                        builder.set(field.getName()).to(field.getType(), null);
                        break;
                    case ARRAY: {
                        switch (field.getType().getArrayElementType().getCode()) {
                            case BOOL:
                                builder.set(field.getName()).toBoolArray((Iterable<Boolean>)null);
                                break;
                            case BYTES:
                                builder.set(field.getName()).toBytesArray(null);
                                break;
                            case STRING:
                                builder.set(field.getName()).toStringArray(null);
                                break;
                            case JSON:
                                builder.set(field.getName()).toJsonArray(null);
                                break;
                            case INT64:
                                builder.set(field.getName()).toInt64Array((Iterable<Long>)null);
                                break;
                            case FLOAT64:
                                builder.set(field.getName()).toFloat64Array((Iterable<Double>)null);
                                break;
                            case NUMERIC:
                                builder.set(field.getName()).toNumericArray(null);
                                break;
                            case DATE:
                                builder.set(field.getName()).toDateArray(null);
                                break;
                            case TIMESTAMP:
                                builder.set(field.getName()).toTimestampArray(null);
                                break;
                            case STRUCT:
                                builder.set(field.getName()).toStructArray(field.getType().getArrayElementType(), null);
                                break;
                            case ARRAY:
                                throw new IllegalStateException();
                        }
                        break;
                    }
                }
            }
        }
        return builder;
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
                case JSON:
                    builder.set(field.getName()).to(struct.getJson(field.getName()));
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
                        case JSON:
                            builder.set(field.getName()).toJsonArray(struct.getJsonList(field.getName()));
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

    public static Struct.Builder toBuilder(final Type type,
                                           final Struct struct,
                                           final Map<String, String> renameFields) {

        final Struct.Builder builder = Struct.newBuilder();
        for(final Type.StructField field : type.getStructFields()) {
            final String getFieldName = renameFields.getOrDefault(field.getName(), field.getName());
            final String setFieldName = field.getName();
            if(!hasField(struct, getFieldName)) {
                if(renameFields.containsValue(setFieldName)) {
                    final String getOuterFieldName = renameFields.entrySet().stream()
                            .filter(e -> e.getValue().equals(setFieldName))
                            .map(Map.Entry::getKey)
                            .findAny()
                            .orElse(setFieldName);
                    if(struct.isNull(getOuterFieldName)) {
                        setNullValue(field, builder);
                        continue;
                    }
                    final Type.StructField rowField = struct.getType().getStructFields().stream().filter(f -> f.getName().equals(getOuterFieldName)).findAny().get();
                    if(!field.getType().getCode().equals(rowField.getType().getCode())) {
                        setNullValue(field, builder);
                        continue;
                    }

                    switch (field.getType().getCode()) {
                        case ARRAY: {
                            if(field.getType().getArrayElementType().getCode().equals(Type.Code.STRUCT)) {
                                final List<Struct> children = new ArrayList<>();
                                for(final Struct child : struct.getStructList(getOuterFieldName)) {
                                    if(child != null) {
                                        children.add(toBuilder(field.getType().getArrayElementType(), child).build());
                                    }
                                }
                                builder.set(setFieldName).toStructArray(field.getType().getArrayElementType(), children);
                            } else {
                                builder.set(setFieldName).to(struct.getValue(getOuterFieldName));
                            }
                            break;
                        }
                        case STRUCT: {
                            final Struct child = toBuilder(field.getType(), struct.getStruct(getOuterFieldName)).build();
                            builder.set(setFieldName).to(field.getType(), child);
                            break;
                        }
                        default: {
                            builder.set(setFieldName).to(struct.getValue(getOuterFieldName));
                            break;
                        }
                    }
                } else {
                    setNullValue(field, builder);
                }
            } else {
                final Value value = getStructValue(struct, getFieldName);
                if (value != null) {
                    switch (field.getType().getCode()) {
                        case ARRAY: {
                            if (field.getType().getArrayElementType().getCode().equals(Type.Code.STRUCT)) {
                                final List<Struct> children = new ArrayList<>();
                                for (final Struct child : struct.getStructList(getFieldName)) {
                                    if (child == null) {
                                        children.add(null);
                                    } else {
                                        children.add(toBuilder(field.getType().getArrayElementType(), child).build());
                                    }
                                }
                                builder.set(field.getName()).toStructArray(field.getType().getArrayElementType(), children);
                            } else {
                                builder.set(field.getName()).to(value);
                            }
                            break;
                        }
                        case STRUCT: {
                            final Struct child = toBuilder(field.getType(), struct.getStruct(getFieldName)).build();
                            builder.set(field.getName()).to(child);
                            break;
                        }
                        default:
                            builder.set(field.getName()).to(value);
                            break;
                    }
                } else {
                    setNullValue(field, builder);
                }
            }
        }

        return builder;
    }

    public static Type renameFields(final Type type, final Map<String, String> renameFields) {
        final List<Type.StructField> structFields = new ArrayList<>();
        for(Type.StructField field : type.getStructFields()) {
            if (renameFields.containsKey(field.getName())) {
                structFields.add(Type.StructField.of(renameFields.get(field.getName()), field.getType()));
            } else {
                structFields.add(field);
            }
        }
        return Type.struct(structFields);
    }

    public static Struct merge(final Type type, final Struct struct, Map<String, ? extends Object> values) {
        final Struct.Builder builder = Struct.newBuilder();
        for(Type.StructField field : type.getStructFields()) {
            if(values.containsKey(field.getName())) {
                builder.set(field.getName()).to(toValue(field.getType(), values.get(field.getName())));
            } else if(hasField(struct, field.getName())) {
                builder.set(field.getName()).to(struct.getValue(field.getName()));
            } else {
                builder.set(field.getName()).to(toValue(field.getType(), null));
            }
        }
        return builder.build();
    }

    public static Struct create(final Type type, Map<String, Object> values) {
        final Struct.Builder builder = Struct.newBuilder();
        for (final Type.StructField field : type.getStructFields()) {
            final Object value = values.get(field.getName());
            switch (field.getType().getCode()) {
                case BOOL:
                    builder.set(field.getName()).to((Boolean) value);
                    break;
                case JSON:
                case STRING:
                    builder.set(field.getName()).to((String) value);
                    break;
                case BYTES:
                    builder.set(field.getName()).to((ByteArray) value);
                    break;
                case INT64: {
                    if(value instanceof Integer) {
                        builder.set(field.getName()).to((Integer) value);
                    } else {
                        builder.set(field.getName()).to((Long) value);
                    }
                    break;
                }
                case FLOAT64: {
                    if(value instanceof Float) {
                        builder.set(field.getName()).to((Float) value);
                    } else {
                        builder.set(field.getName()).to((Double) value);
                    }
                    break;
                }
                case NUMERIC:
                    builder.set(field.getName()).to((BigDecimal) value);
                    break;
                case DATE:
                    builder.set(field.getName()).to((Date) value);
                    break;
                case TIMESTAMP:
                    builder.set(field.getName()).to((Timestamp) value);
                    break;
                case STRUCT:
                    builder.set(field.getName()).to((Struct) value);
                    break;
                case ARRAY: {
                    break;
                }
            }
        }
        return builder.build();
    }

    public static Type selectFields(Type type, final List<String> fields) {
        final List<Type.StructField> structFields = selectFieldsBuilder(type, fields);
        return Type.struct(structFields);
    }

    public static List<Type.StructField> selectFieldsBuilder(Type type, final List<String> fields) {
        final List<Type.StructField> structFields = new ArrayList<>();
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
                structFields.add(type.getStructFields().get(type.getFieldIndex(field)));
            }
        }

        if(childFields.size() > 0) {
            for(var entry : childFields.entrySet()) {
                final Type.StructField childField = type.getStructFields().get(type.getFieldIndex(entry.getKey()));
                switch (childField.getType().getCode()) {
                    case STRUCT: {
                        final Type childType = selectFields(childField.getType(), entry.getValue());
                        structFields.add(Type.StructField.of(entry.getKey(), childType));
                        break;
                    }
                    case ARRAY: {
                        if(!childField.getType().getArrayElementType().getCode().equals(Type.Code.STRUCT)) {
                            throw new IllegalStateException();
                        }
                        final Type childType = selectFields(childField.getType().getArrayElementType(), entry.getValue());
                        structFields.add(Type.StructField.of(entry.getKey(), Type.array(childType)));
                        break;
                    }
                    default:
                        throw new IllegalStateException();
                }
            }
        }
        return structFields;
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
            case "NUMERIC":
                return Schema.FieldType.DECIMAL;
            case "BOOL":
                return Schema.FieldType.BOOLEAN;
            case "JSON":
                return Schema.FieldType.STRING;
            case "DATE":
                return CalciteUtils.DATE;
            case "TIMESTAMP":
                return Schema.FieldType.DATETIME;
            case "BYTES":
                return Schema.FieldType.BYTES;
            default:
                if(type.startsWith("STRING")) {
                    return Schema.FieldType.STRING;
                } else if(type.startsWith("BYTES")) {
                    return Schema.FieldType.BYTES;
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
            case "NUMERIC":
                return Type.numeric();
            case "BOOL":
                return Type.bool();
            case "JSON":
                return Type.json();
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
                        return Stream.of(Type.StructField.of(name, f.getType()));
                    }
                })
                .collect(Collectors.toList());
    }

    private static Value toValue(final Type type, final Object value) {
        switch (type.getCode()) {
            case BOOL:
                return Value.bool((Boolean) value);
            case STRING:
                return Value.string((String) value);
            case BYTES:
                return Value.bytes((ByteArray) value);
            case JSON:
                return Value.json((String) value);
            case INT64: {
                if(value instanceof Integer) {
                    return Value.int64((Integer) value);
                } else {
                    return Value.int64((Long) value);
                }
            }
            case FLOAT64: {
                if(value instanceof Float) {
                    return Value.float64((Float) value);
                } else {
                    return Value.float64((Double) value);
                }
            }
            case NUMERIC:
                return Value.numeric((BigDecimal) value);
            case DATE:
                return Value.date((Date) value);
            case TIMESTAMP:
                return Value.timestamp((Timestamp) value);
            case STRUCT: {
                if(value == null) {
                    return Value.struct(type, null);
                }
                final Struct child = (Struct) value;
                final Struct mergedChild = merge(type, child, new HashMap<>());
                return Value.struct(type, mergedChild);
            }
            case ARRAY: {
                switch (type.getArrayElementType().getCode()) {
                    case BOOL:
                        return Value.boolArray((Iterable<Boolean>) value);
                    case STRING:
                        return Value.stringArray((Iterable<String>) value);
                    case BYTES:
                        return Value.bytesArray((Iterable<ByteArray>) value);
                    case JSON:
                        return Value.jsonArray((Iterable<String>) value);
                    case INT64:
                        return Value.int64Array((Iterable<Long>) value);
                    case FLOAT64:
                        return Value.float64Array((Iterable<Double>) value);
                    case NUMERIC:
                        return Value.numericArray((Iterable<BigDecimal>) value);
                    case DATE:
                        return Value.dateArray((Iterable<Date>) value);
                    case TIMESTAMP:
                        return Value.timestampArray((Iterable<Timestamp>) value);
                    case STRUCT: {
                        final List<Struct> children = new ArrayList<>();
                        for(final Struct child : (Iterable<Struct>) value) {
                            final Struct mergedChild = merge(type.getArrayElementType(), child, new HashMap<>());
                            children.add(mergedChild);
                        }
                        return Value.structArray(type.getArrayElementType(), children);
                    }
                    case ARRAY:
                        throw new IllegalArgumentException("Array in Array is not supported");
                }
            }
            default:
                throw new IllegalArgumentException("Not supported struct type: " + type);
        }
    }

    private static void setNullValue(final Type.StructField field, Struct.Builder builder) {
        switch (field.getType().getCode()) {
            case BOOL:
                builder.set(field.getName()).to((Boolean)null);
                break;
            case JSON:
            case STRING:
                builder.set(field.getName()).to((String)null);
                break;
            case BYTES:
                builder.set(field.getName()).to((ByteArray) null);
                break;
            case INT64:
                builder.set(field.getName()).to((Long)null);
                break;
            case FLOAT64:
                builder.set(field.getName()).to((Double)null);
                break;
            case NUMERIC:
                builder.set(field.getName()).to((BigDecimal) null);
                break;
            case DATE:
                builder.set(field.getName()).to((Date)null);
                break;
            case TIMESTAMP:
                builder.set(field.getName()).to((Timestamp)null);
                break;
            case STRUCT:
                builder.set(field.getName()).to(field.getType(), null);
                break;
            case ARRAY: {
                switch (field.getType().getArrayElementType().getCode()) {
                    case BOOL:
                        builder.set(field.getName()).toBoolArray((Iterable<Boolean>)null);
                        break;
                    case BYTES:
                        builder.set(field.getName()).toBytesArray(null);
                        break;
                    case STRING:
                        builder.set(field.getName()).toStringArray(null);
                        break;
                    case JSON:
                        builder.set(field.getName()).toJsonArray(null);
                        break;
                    case INT64:
                        builder.set(field.getName()).toInt64Array((Iterable<Long>)null);
                        break;
                    case FLOAT64:
                        builder.set(field.getName()).toFloat64Array((Iterable<Double>)null);
                        break;
                    case NUMERIC:
                        builder.set(field.getName()).toNumericArray(null);
                        break;
                    case DATE:
                        builder.set(field.getName()).toDateArray(null);
                        break;
                    case TIMESTAMP:
                        builder.set(field.getName()).toTimestampArray(null);
                        break;
                    case STRUCT:
                        builder.set(field.getName()).toStructArray(field.getType().getArrayElementType(), null);
                        break;
                    case ARRAY:
                        throw new IllegalStateException();
                }
                break;
            }
        }
    }

    private static List<Map<String, Value>> flattenValues(final Struct struct, final List<String> paths, final String prefix, final boolean addPrefix) {
        final Map<String, Value> properties = new HashMap<>();
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
                properties.put(name, getStructValue(struct, field.getName()));
            } else {
                pathName = name;
                pathField = field;
            }
        }

        if(pathField == null) {
            return Arrays.asList(properties);
        }

        if(getStructValue(struct, pathField.getName()) == null) {
            return Arrays.asList(properties);
        }

        if(Type.Code.ARRAY.equals(pathField.getType().getCode())) {
            final List<Map<String, Value>> arrayValues = new ArrayList<>();
            final Value array = getStructValue(struct, pathField.getName());
            if(Type.Code.STRUCT.equals(pathField.getType().getArrayElementType().getCode())) {
                try {
                    final Method getStructArray = array.getClass().getDeclaredMethod("getStructArray");
                    getStructArray.setAccessible(true);
                    final Object structArray = getStructArray.invoke(array);
                    for (final Struct child : (List<Struct>)structArray) {
                        final List<Map<String, Value>> list = flattenValues(
                                child, paths.subList(1, paths.size()), pathName, addPrefix);
                        for(Map<String, Value> unnestedChildProperties : list) {
                            unnestedChildProperties.putAll(properties);
                            arrayValues.add(unnestedChildProperties);
                        }
                    }
                    return arrayValues;
                } catch (NoSuchMethodException e) {
                    throw new RuntimeException(e);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                } catch (InvocationTargetException e) {
                    throw new RuntimeException(e);
                }
            } else {
                properties.put(pathName, getStructValue(struct, pathField.getName()));
            }
        } else {
            properties.put(pathName, getStructValue(struct, pathField.getName()));
        }

        final Struct.Builder builder = Struct.newBuilder();
        properties.entrySet().forEach(e -> builder.set(e.getKey()).to(e.getValue()));
        return Arrays.asList(Collections.singletonMap(pathName, Value.struct(builder.build())));
    }

    public interface ValueGetter<InputT> extends Serializable {
        Object convert(InputT element, String fieldName);
    }

}
