package com.mercari.solution.util.schema;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.*;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.Timestamps;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.converter.RecordToMutationConverter;
import org.apache.avro.LogicalTypes;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.util.Utf8;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.io.gcp.spanner.MutationGroup;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.*;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.hbase.util.Bytes;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StructSchemaUtil {

    private static final Logger LOG = LoggerFactory.getLogger(StructSchemaUtil.class);
    private static final Pattern PATTERN_ARRAY_ELEMENT = Pattern.compile("(?<=\\<).*?(?=\\>)");
    private static final Pattern PATTERN_CHANGE_RECORD_TYPE_CODE = Pattern.compile("\"code\":\"([A-Z][A-Z0-9]*)\"");

    public static boolean hasField(final Struct struct, final String fieldName) {
        if(struct == null || fieldName == null) {
            return false;
        }
        return hasField(struct.getType(), fieldName);
    }

    public static boolean hasField(final Type type, final String fieldName) {
        if(type == null || fieldName == null) {
            return false;
        }
        return type.getStructFields().stream()
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
            case PG_JSONB:
            case PG_NUMERIC:
                return struct.getString(fieldName);
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

    public static Object getRawValue(final Struct struct, final String fieldName) {
        if(struct == null) {
            return null;
        }

        if(!hasField(struct, fieldName)) {
            return null;
        }
        if(struct.isNull(fieldName)) {
            return null;
        }
        switch (struct.getColumnType(fieldName).getCode()) {
            case BOOL:
                return struct.getBoolean(fieldName);
            case BYTES:
                return struct.getBytes(fieldName);
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
            case PG_JSONB:
                return struct.getPgJsonb(fieldName);
            case PG_NUMERIC:
                return struct.getString(fieldName);
            case DATE: {
                return struct.getDate(fieldName);
            }
            case TIMESTAMP:
                return struct.getTimestamp(fieldName);
            case STRUCT:
                return struct.getStruct(fieldName);
            case ARRAY: {
                switch (struct.getColumnType(fieldName).getArrayElementType().getCode()) {
                    case BOOL:
                        return struct.getBooleanList(fieldName);
                    case BYTES:
                        return struct.getBytesList(fieldName);
                    case STRING:
                        return struct.getStringList(fieldName);
                    case JSON:
                        return struct.getJsonList(fieldName);
                    case INT64:
                        return struct.getLongList(fieldName);
                    case FLOAT64:
                        return struct.getDoubleList(fieldName);
                    case NUMERIC:
                        return struct.getBigDecimalList(fieldName);
                    case PG_JSONB:
                        return struct.getPgJsonbList(fieldName);
                    case PG_NUMERIC:
                        return struct.getStringList(fieldName);
                    case DATE: {
                        return struct.getDateList(fieldName);
                    }
                    case TIMESTAMP:
                        return struct.getTimestampList(fieldName);
                    case STRUCT:
                        return struct.getStructList(fieldName);
                    case ARRAY:
                        throw new IllegalArgumentException("Not supported array in array for field: " + fieldName);
                }
            }
            default:
                throw new IllegalArgumentException("Not supported column type: " + struct.getColumnType(fieldName).getCode().name());
        }
    }

    public static Instant toInstant(final Object value) {
        final Timestamp timestamp = (Timestamp) value;
        return DateTimeUtil.toJodaInstant(timestamp);
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
            case PG_JSONB:
            case PG_NUMERIC:
                return struct.getString(field);
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
            case PG_NUMERIC:
                return Value.pgNumeric(struct.getString(field));
            case PG_JSONB:
                return Value.pgJsonb(struct.getString(field));
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
                    case PG_NUMERIC:
                        return Value.pgNumericArray(struct.getStringList(field));
                    case PG_JSONB:
                        return Value.pgJsonbArray(struct.getStringList(field));
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

    public static String getAsString(final Object struct, final String field) {
        if(struct == null) {
            return null;
        }
        return getAsString((Struct) struct, field);
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
            case PG_JSONB:
            case PG_NUMERIC:
                return struct.getString(field);
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
            case PG_NUMERIC:
                return new BigDecimal(struct.getString(field)).longValue();
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
            case PG_NUMERIC:
                return new BigDecimal(struct.getString(field)).doubleValue();
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
            case PG_NUMERIC:
                return new BigDecimal(struct.getString(field));
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
            case PG_NUMERIC:
                bytes = Bytes.toBytes(struct.getString(fieldName));
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

    public static List<Float> getAsFloatList(final Struct struct, final String fieldName) {
        if(struct == null || fieldName == null) {
            return new ArrayList<>();
        }
        if(!StructSchemaUtil.hasField(struct, fieldName)) {
            return new ArrayList<>();
        }
        final Type.StructField field = struct.getType().getStructFields().stream()
                .filter(f -> f.getName().equals(fieldName))
                .findAny()
                .get();
        if(!Type.Code.ARRAY.equals(field.getType().getCode())) {
            return new ArrayList<>();
        }

        switch (field.getType().getArrayElementType().getCode()) {
            case BOOL:
                return struct.getBooleanList(fieldName).stream()
                        .map(b -> Optional.ofNullable(b).orElse(false) ? 1F : 0F)
                        .collect(Collectors.toList());
            case STRING:
            case PG_NUMERIC:
                return struct.getStringList(fieldName).stream()
                        .map(s -> Float.valueOf(Optional.ofNullable(s).orElse("0")))
                        .collect(Collectors.toList());
            case INT64:
                return struct.getLongList(fieldName).stream()
                        .map(s -> Float.valueOf(Optional.ofNullable(s).orElse(0L)))
                        .collect(Collectors.toList());
            case FLOAT64:
                return struct.getDoubleList(fieldName).stream()
                        .map(s -> Optional.ofNullable(s).orElse(0D).floatValue())
                        .collect(Collectors.toList());
            case NUMERIC:
                return struct.getBigDecimalList(fieldName).stream()
                        .map(s -> Optional.ofNullable(s).orElse(BigDecimal.ZERO).floatValue())
                        .collect(Collectors.toList());
            case DATE:
            case TIMESTAMP:
            case JSON:
            case BYTES:
            case STRUCT:
            case ARRAY:
            default:
                return new ArrayList<>();
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
                    return DateTimeUtil.toJodaInstant(stringValue);
                } catch (IllegalArgumentException e) {
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
            case PG_NUMERIC:
            case STRUCT:
            case ARRAY:
            default:
                throw new IllegalArgumentException("Not supported column type: " + struct.getColumnType(field).getCode().name());
        }
    }

    public static Object getAsPrimitive(Object row, Schema.FieldType fieldType, String field) {
        return null;
    }

    public static Object convertPrimitive(Schema.FieldType fieldType, Object primitiveValue) {
        return null;
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
                    case PG_NUMERIC:
                        builder.set(field.getName()).to((String) null);
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
                            case PG_NUMERIC:
                                builder.set(field.getName()).toPgNumericArray(null);
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
                case PG_NUMERIC:
                    builder.set(field.getName()).to(struct.getString(field.getName()));
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
                        case PG_NUMERIC:
                            builder.set(field.getName()).toPgNumericArray(struct.getStringList(field.getName()));
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
                case PG_NUMERIC:
                    builder.set(field.getName()).to((String) value);
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
            }
            fields.add(Type.StructField.of(
                    struct.getString("COLUMN_NAME"),
                    convertSchemaField(struct.getString("SPANNER_TYPE"))));
        }
        return Type.struct(fields);
    }

    public static Schema createDataChangeRecordRowSchema() {
        final Schema rowTypeSchema = Schema.builder()
                .addField(Schema.Field.of("name", Schema.FieldType.STRING))
                .addField(Schema.Field.of("Type", Schema.FieldType.logicalType(EnumerationType
                        .create("TYPE_CODE_UNSPECIFIED", "BOOL", "INT64", "FLOAT64",
                                "TIMESTAMP", "DATE", "STRING", "BYTES", "ARRAY", "STRUCT",
                                "NUMERIC", "JSON"))))
                .addField(Schema.Field.of("isPrimaryKey", Schema.FieldType.BOOLEAN))
                .addField(Schema.Field.of("ordinalPosition", Schema.FieldType.INT64))
                .build();
        final Schema.Options sqlTypeOption = Schema.Options.builder().setOption("sqlType", Schema.FieldType.STRING, "JSON").build();
        final Schema modSchema = Schema.builder()
                .addField(Schema.Field.of("keysJson", Schema.FieldType.STRING).withOptions(sqlTypeOption))
                .addField(Schema.Field.of("oldValuesJson", Schema.FieldType.STRING.withNullable(true)).withOptions(sqlTypeOption))
                .addField(Schema.Field.of("newValuesJson", Schema.FieldType.STRING.withNullable(true)).withOptions(sqlTypeOption))
                .build();
        final Schema metadataSchema = Schema.builder()
                .addField(Schema.Field.of("partitionToken", Schema.FieldType.STRING))
                .addField(Schema.Field.of("recordTimestamp", Schema.FieldType.DATETIME))
                .addField(Schema.Field.of("partitionStartTimestamp", Schema.FieldType.DATETIME))
                .addField(Schema.Field.of("partitionEndTimestamp", Schema.FieldType.DATETIME))
                .addField(Schema.Field.of("partitionCreatedAt", Schema.FieldType.DATETIME))
                .addField(Schema.Field.of("partitionScheduledAt", Schema.FieldType.DATETIME))
                .addField(Schema.Field.of("partitionRunningAt", Schema.FieldType.DATETIME))
                .addField(Schema.Field.of("queryStartedAt", Schema.FieldType.DATETIME))
                .addField(Schema.Field.of("recordStreamStartedAt", Schema.FieldType.DATETIME))
                .addField(Schema.Field.of("recordStreamEndedAt", Schema.FieldType.DATETIME))
                .addField(Schema.Field.of("recordReadAt", Schema.FieldType.DATETIME))
                .addField(Schema.Field.of("totalStreamTimeMillis", Schema.FieldType.INT64))
                .addField(Schema.Field.of("numberOfRecordsRead", Schema.FieldType.INT64))
                .build();
        return Schema.builder()
                .addField(Schema.Field.of("partitionToken", Schema.FieldType.STRING))
                .addField(Schema.Field.of("commitTimestamp", Schema.FieldType.DATETIME))
                .addField(Schema.Field.of("serverTransactionId", Schema.FieldType.STRING))
                .addField(Schema.Field.of("isLastRecordInTransactionInPartition", Schema.FieldType.BOOLEAN))
                .addField(Schema.Field.of("recordSequence", Schema.FieldType.STRING))
                .addField(Schema.Field.of("tableName", Schema.FieldType.STRING))
                .addField(Schema.Field.of("rowType", Schema.FieldType.array(Schema.FieldType.row(rowTypeSchema))))
                .addField(Schema.Field.of("mods", Schema.FieldType.array(Schema.FieldType.row(modSchema))))
                .addField(Schema.Field.of("modType", Schema.FieldType.logicalType(EnumerationType.create("INSERT","UPDATE","DELETE"))))
                .addField(Schema.Field.of("valueCaptureType", Schema.FieldType.logicalType(EnumerationType.create("OLD_AND_NEW_VALUES", "NEW_ROW", "NEW_VALUES"))))
                .addField(Schema.Field.of("numberOfRecordsInTransaction", Schema.FieldType.INT64))
                .addField(Schema.Field.of("numberOfPartitionsInTransaction", Schema.FieldType.INT64))
                .addField(Schema.Field.of("metadata", Schema.FieldType.row(metadataSchema).withNullable(true)))
                .build();
    }

    public static org.apache.avro.Schema createDataChangeRecordAvroSchema() {
        final org.apache.avro.Schema rowTypeSchema = org.apache.avro.SchemaBuilder.builder("com.google.cloud.teleport.v2")
                .record("ColumnType")
                .fields()
                .name("name").type(AvroSchemaUtil.REQUIRED_STRING).noDefault()
                .name("Type").type(org.apache.avro.Schema
                        .createEnum("TypeCode", "", "com.google.cloud.teleport.v2", Arrays
                                .asList("TYPE_CODE_UNSPECIFIED", "BOOL", "INT64", "FLOAT64",
                                        "TIMESTAMP", "DATE", "STRING", "BYTES", "ARRAY", "STRUCT",
                                        "NUMERIC", "JSON"))).noDefault()
                .name("isPrimaryKey").type(AvroSchemaUtil.REQUIRED_BOOLEAN).noDefault()
                .name("ordinalPosition").type(AvroSchemaUtil.REQUIRED_LONG).noDefault()
                .endRecord();

        final org.apache.avro.Schema jsonSchema = org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING);
        jsonSchema.addProp("sqlType", "JSON");
        final org.apache.avro.Schema jsonSchemaNullable = org.apache.avro.Schema.createUnion(
                jsonSchema,
                org.apache.avro.Schema.create(org.apache.avro.Schema.Type.NULL));
        final org.apache.avro.Schema modSchema = org.apache.avro.SchemaBuilder.builder("com.google.cloud.teleport.v2")
                .record("Mod")
                .fields()
                .name("keysJson").type(jsonSchema).noDefault()
                .name("oldValuesJson").type(jsonSchemaNullable).noDefault()
                .name("newValuesJson").type(jsonSchemaNullable).noDefault()
                .endRecord();

        final org.apache.avro.Schema metadataSchema = org.apache.avro.SchemaBuilder.builder("com.google.cloud.teleport.v2")
                .record("ChangeStreamRecordMetadata")
                .fields()
                .name("partitionToken").type(AvroSchemaUtil.REQUIRED_STRING).noDefault()
                .name("recordTimestamp").type(AvroSchemaUtil.REQUIRED_LOGICAL_TIMESTAMP_MICRO_TYPE).noDefault()
                .name("partitionStartTimestamp").type(AvroSchemaUtil.REQUIRED_LOGICAL_TIMESTAMP_MICRO_TYPE).noDefault()
                .name("partitionEndTimestamp").type(AvroSchemaUtil.REQUIRED_LOGICAL_TIMESTAMP_MICRO_TYPE).noDefault()
                .name("partitionCreatedAt").type(AvroSchemaUtil.REQUIRED_LOGICAL_TIMESTAMP_MICRO_TYPE).noDefault()
                .name("partitionScheduledAt").type(AvroSchemaUtil.REQUIRED_LOGICAL_TIMESTAMP_MICRO_TYPE).noDefault()
                .name("partitionRunningAt").type(AvroSchemaUtil.REQUIRED_LOGICAL_TIMESTAMP_MICRO_TYPE).noDefault()
                .name("queryStartedAt").type(AvroSchemaUtil.REQUIRED_LOGICAL_TIMESTAMP_MICRO_TYPE).noDefault()
                .name("recordStreamStartedAt").type(AvroSchemaUtil.REQUIRED_LOGICAL_TIMESTAMP_MICRO_TYPE).noDefault()
                .name("recordStreamEndedAt").type(AvroSchemaUtil.REQUIRED_LOGICAL_TIMESTAMP_MICRO_TYPE).noDefault()
                .name("recordReadAt").type(AvroSchemaUtil.REQUIRED_LOGICAL_TIMESTAMP_MICRO_TYPE).noDefault()
                .name("totalStreamTimeMillis").type(AvroSchemaUtil.REQUIRED_LONG).noDefault()
                .name("numberOfRecordsRead").type(AvroSchemaUtil.REQUIRED_LONG).noDefault()
                .endRecord();

        return org.apache.avro.SchemaBuilder.builder("com.google.cloud.teleport.v2")
                .record("DataChangeRecord")
                .fields()
                .name("partitionToken").type(AvroSchemaUtil.REQUIRED_STRING).noDefault()
                .name("commitTimestamp").type(AvroSchemaUtil.REQUIRED_LOGICAL_TIMESTAMP_MICRO_TYPE).noDefault()
                .name("serverTransactionId").type(AvroSchemaUtil.REQUIRED_STRING).noDefault()
                .name("isLastRecordInTransactionInPartition").type(AvroSchemaUtil.REQUIRED_BOOLEAN).noDefault()
                .name("recordSequence").type(AvroSchemaUtil.REQUIRED_STRING).noDefault()
                .name("tableName").type(AvroSchemaUtil.REQUIRED_STRING).noDefault()
                .name("rowType").type(org.apache.avro.Schema.createArray(rowTypeSchema)).noDefault()
                .name("mods").type(org.apache.avro.Schema.createArray(modSchema)).noDefault()
                .name("modType").type(org.apache.avro.Schema
                        .createEnum("ModType", "", "com.google.cloud.teleport.v2", Arrays.asList("INSERT","UPDATE","DELETE"))).noDefault()
                .name("valueCaptureType").type(org.apache.avro.Schema
                        .createEnum("ValueCaptureType", "", "com.google.cloud.teleport.v2", Arrays.asList("OLD_AND_NEW_VALUES", "NEW_ROW", "NEW_VALUES"))).noDefault()
                .name("numberOfRecordsInTransaction").type(AvroSchemaUtil.REQUIRED_LONG).noDefault()
                .name("numberOfPartitionsInTransaction").type(AvroSchemaUtil.REQUIRED_LONG).noDefault()
                .name("metadata").type(org.apache.avro.Schema.createUnion(
                        metadataSchema, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.NULL))).noDefault()
                .endRecord();
    }

    public static Schema createMutationSchema() {
        return Schema.builder()
                .addField("table", Schema.FieldType.STRING)
                .addField("op", Schema.FieldType.logicalType(EnumerationType.create(Arrays.asList(
                        Mutation.Op.DELETE.name(),
                        Mutation.Op.INSERT.name(),
                        Mutation.Op.UPDATE.name(),
                        Mutation.Op.REPLACE.name(),
                        Mutation.Op.INSERT_OR_UPDATE.name()))))
                .addField("timestamp", Schema.FieldType.DATETIME)
                .addField("keys", Schema.FieldType.array(Schema.FieldType.STRING))
                .addField(Schema.Field
                        .of("mutation", Schema.FieldType.STRING.withNullable(true))
                        .withOptions(Schema.Options.builder().setOption("sqlType", Schema.FieldType.STRING, "JSON").build()))
                .build();
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

    public static String getChangeRecordTableName(final GenericRecord record) {
        final Object value = record.get("tableName");
        if(value == null) {
            throw new IllegalArgumentException();
        }
        return value.toString();
    }

    public static List<KV<Key, GenericRecord>> createChangeRecordKey(
            final org.apache.avro.Schema tableSchema,
            final GenericRecord changeRecord) {

        final List<GenericRecord> mods = (List<GenericRecord>)changeRecord.get("mods");
        if(mods == null || mods.size() == 0) {
            throw new IllegalStateException("mods must not be null or zero size");
        }
        final Map<String, String> rowTypes = ((List<GenericRecord>) (changeRecord.get("rowType")))
                .stream()
                .collect(Collectors.toMap(
                        r -> r.get("name").toString(),
                        r -> r.get("Type").toString()));

        final List<KV<Key, GenericRecord>> keyAndChangeRecords = new ArrayList<>();
        for(final GenericRecord mod : mods) {
            Key.Builder keyBuilder = Key.newBuilder();
            final String keysJson = Optional.ofNullable(mod.get("keysJson")).orElse(new Utf8()).toString();
            final JsonObject keyObject = new Gson().fromJson(keysJson, JsonObject.class);
            for(final Map.Entry<String, JsonElement> entry : keyObject.entrySet()) {
                final String fieldName = entry.getKey();
                final JsonElement fieldValue = entry.getValue();
                final boolean isNull = fieldValue == null || fieldValue.isJsonNull();

                final String rowType = rowTypes.get(fieldName);
                if(rowType != null && !"TYPE_CODE_UNSPECIFIED".equalsIgnoreCase(rowType)) {
                    switch (rowType) {
                        case "BOOL":
                            keyBuilder = keyBuilder.append(isNull ? null : fieldValue.getAsBoolean());
                            break;
                        case "INT64":
                            keyBuilder = keyBuilder.append(isNull ? null : fieldValue.getAsLong());
                            break;
                        case "FLOAT64":
                            keyBuilder = keyBuilder.append(isNull ? null : fieldValue.getAsDouble());
                            break;
                        case "NUMERIC":
                            keyBuilder = keyBuilder.append(isNull ? null : fieldValue.getAsBigDecimal());
                            break;
                        case "DATE":
                            keyBuilder = keyBuilder.append(isNull ? null : Date.parseDate(fieldValue.getAsString()));
                            break;
                        case "TIMESTAMP":
                            keyBuilder = keyBuilder.append(isNull ? null : Timestamp.parseTimestamp(fieldValue.getAsString()));
                            break;
                        case "JSON":
                        case "STRING":
                            keyBuilder = keyBuilder.append(isNull ? null : fieldValue.getAsString());
                            break;
                        case "BYTES": {
                            if(isNull) {
                                keyBuilder = keyBuilder.append((ByteArray) null);
                            } else {
                                final byte[] bytes = Base64.getDecoder().decode(fieldValue.getAsString());
                                keyBuilder = keyBuilder.append(ByteArray.copyFrom(bytes));
                            }
                            break;
                        }
                        case "ARRAY":
                        case "STRUCT":
                        case "TYPE_CODE_UNSPECIFIED":
                        default:
                            throw new IllegalArgumentException(
                                    "Not supported modType: " + rowType + " for field: " + fieldName);
                    }
                } else {
                    final org.apache.avro.Schema.Field field = tableSchema == null ? null : tableSchema.getField(fieldName);
                    if(field == null) {
                        throw new IllegalStateException("");
                    }

                    final org.apache.avro.Schema fieldSchema = AvroSchemaUtil.unnestUnion(field.schema());
                    switch (fieldSchema.getType()) {
                        case BOOLEAN:
                            keyBuilder = keyBuilder.append(isNull ? null : fieldValue.getAsBoolean());
                            break;
                        case FIXED:
                        case BYTES: {
                            if (AvroSchemaUtil.isLogicalTypeDecimal(fieldSchema)) {
                                keyBuilder = keyBuilder.append(isNull ? null : fieldValue.getAsBigDecimal());
                            } else {
                                if(isNull) {
                                    keyBuilder = keyBuilder.append((ByteArray) null);
                                } else {
                                    final byte[] bytes = Base64.getDecoder().decode(fieldValue.getAsString());
                                    keyBuilder = keyBuilder.append(ByteArray.copyFrom(bytes));
                                }
                            }
                            break;
                        }
                        case ENUM:
                        case STRING:
                            keyBuilder = keyBuilder.append(isNull ? null : fieldValue.getAsString());
                            break;
                        case INT: {
                            if (LogicalTypes.date().equals(fieldSchema.getLogicalType())) {
                                keyBuilder = keyBuilder.append(isNull ? null : Date.parseDate(fieldValue.getAsString()));
                            } else if (LogicalTypes.timeMillis().equals(fieldSchema.getLogicalType())) {
                                keyBuilder = keyBuilder.append(isNull ? null : fieldValue.getAsString());
                            } else {
                                keyBuilder = keyBuilder.append(isNull ? null : fieldValue.getAsInt());
                            }
                            break;
                        }
                        case LONG: {
                            if (LogicalTypes.timestampMillis().equals(fieldSchema.getLogicalType())) {
                                keyBuilder = keyBuilder.append(isNull ? null : Timestamp.parseTimestamp(fieldValue.getAsString()));
                            } else if (LogicalTypes.timestampMicros().equals(fieldSchema.getLogicalType())) {
                                keyBuilder = keyBuilder.append(isNull ? null : Timestamp.parseTimestamp(fieldValue.getAsString()));
                            } else if (LogicalTypes.timeMicros().equals(fieldSchema.getLogicalType())) {
                                keyBuilder = keyBuilder.append(isNull ? null : fieldValue.getAsString());
                            } else {
                                keyBuilder = keyBuilder.append(isNull ? null : fieldValue.getAsLong());
                            }
                            break;
                        }
                        case FLOAT:
                            keyBuilder = keyBuilder.append(isNull ? null : fieldValue.getAsFloat());
                            break;
                        case DOUBLE:
                            keyBuilder = keyBuilder.append(isNull ? null : fieldValue.getAsDouble());
                            break;
                        case RECORD:
                        case ARRAY:
                        case MAP:
                        case UNION:
                        case NULL:
                        default: {
                            throw new IllegalStateException("Not supported fieldSchema: " + fieldSchema);
                        }
                    }
                }
            }

            final Key key = keyBuilder.build();
            final GenericRecord record = AvroSchemaUtil
                    .toBuilder(changeRecord)
                    .set("mods", Arrays.asList(mod))
                    .build();
            keyAndChangeRecords.add(KV.of(key, record));
        }

        return keyAndChangeRecords;
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

    public static List<Mutation> convertToMutation(final Type type, final DataChangeRecord record) {
        return convertToMutation(type, record, new HashMap<>(), false, false);
    }

    public static List<Mutation> convertToMutation(final Type type,
                                                   final DataChangeRecord record,
                                                   final Map<String,String> renameTables,
                                                   final Boolean applyUpsertForInsert,
                                                   final Boolean applyUpsertForUpdate) {

        final Map<String, TypeCode> columnTypeCodes = Optional.ofNullable(record.getRowType())
                .orElseGet(ArrayList::new)
                .stream()
                .collect(Collectors
                        .toMap(ColumnType::getName, ColumnType::getType));

        final String table = renameTables.getOrDefault(record.getTableName(), record.getTableName());
        if(type == null) {
            throw new IllegalStateException("Not found table: " + table + "'s type.");
        }

        final ModType modType = record.getModType();
        if(ModType.DELETE.equals(modType)) {
            return delete(record, table, type, columnTypeCodes);
        } else {
            return update(record, table, type, columnTypeCodes, applyUpsertForInsert, applyUpsertForUpdate);
        }
    }

    private static List<Mutation> delete(final DataChangeRecord record, final String table, final Type type, final Map<String, TypeCode> columnTypeCodes) {
        final List<Mutation> deletes = new ArrayList<>();
        for(final Mod mod : record.getMods()) {
            final JsonObject keys = new Gson().fromJson(mod.getKeysJson(), JsonObject.class);
            Key.Builder keyBuilder = Key.newBuilder();
            for(final Map.Entry<String, JsonElement> keyField : keys.entrySet()) {
                final String code = Optional.ofNullable(columnTypeCodes.get(keyField.getKey()))
                        .map(TypeCode::getCode)
                        .orElse(null);
                if(code != null && !"TYPE_CODE_UNSPECIFIED".equals(code) && false) { // TODO
                    switch (code) {
                        case "BOOL":
                            keyBuilder = keyBuilder.append(keyField.getValue().getAsBoolean());
                            break;
                        case "STRING":
                        case "JSON":
                            keyBuilder = keyBuilder.append(keyField.getValue().getAsString());
                            break;
                        case "INT64":
                            keyBuilder = keyBuilder.append(keyField.getValue().getAsLong());
                            break;
                        case "FLOAT64":
                            keyBuilder = keyBuilder.append(keyField.getValue().getAsDouble());
                            break;
                        case "NUMERIC":
                            keyBuilder = keyBuilder.append(keyField.getValue().getAsBigDecimal());
                            break;
                        case "PG_NUMERIC":
                            keyBuilder = keyBuilder.append(keyField.getValue().getAsString());
                        case "DATE":
                            keyBuilder = keyBuilder.append(Date.parseDate(keyField.getValue().getAsString()));
                            break;
                        case "TIMESTAMP":
                            keyBuilder = keyBuilder.append(Timestamp.parseTimestamp(keyField.getValue().getAsString()));
                            break;
                        case "BYTES":
                            keyBuilder = keyBuilder.append(ByteArray.copyFrom(keyField.getValue().getAsString()));
                            break;
                        case "ARRAY":
                        case "STRUCT":
                        case "PG_JSONB":
                        default:
                            throw new IllegalStateException("Not supported columnTypeCode: " + code + " for key column: " + keyField.getKey());
                    }
                } else {
                    final Type fieldType = type.getStructFields().get(type.getFieldIndex(keyField.getKey())).getType();
                    switch (fieldType.getCode()) {
                        case BOOL:
                            keyBuilder = keyBuilder.append(keyField.getValue().getAsBoolean());
                            break;
                        case JSON:
                        case STRING:
                            keyBuilder = keyBuilder.append(keyField.getValue().getAsString());
                            break;
                        case INT64:
                            keyBuilder = keyBuilder.append(keyField.getValue().getAsLong());
                            break;
                        case FLOAT64:
                            keyBuilder = keyBuilder.append(keyField.getValue().getAsDouble());
                            break;
                        case NUMERIC:
                            keyBuilder = keyBuilder.append(keyField.getValue().getAsBigDecimal());
                            break;
                        case PG_NUMERIC:
                            keyBuilder = keyBuilder.append(keyField.getValue().getAsString());
                            break;
                        case PG_JSONB:
                            keyBuilder = keyBuilder.append(keyField.getValue().getAsString());
                            break;
                        case DATE:
                            keyBuilder = keyBuilder.append(Date.parseDate(keyField.getValue().getAsString()));
                            break;
                        case TIMESTAMP:
                            keyBuilder = keyBuilder.append(Timestamp.parseTimestamp(keyField.getValue().getAsString()));
                            break;
                        case BYTES:
                            keyBuilder = keyBuilder.append(ByteArray.copyFrom(Base64.getDecoder().decode(keyField.getValue().getAsString())));
                            break;
                        case STRUCT:
                        case ARRAY:
                        default:
                            throw new IllegalStateException("Not supported spanner key field type: " + fieldType.getCode());
                    }
                }
            }
            deletes.add(Mutation.delete(table, keyBuilder.build()));
        }
        return deletes;
    }

    private static List<Mutation> update(final DataChangeRecord record,
                                         final String table,
                                         final Type type,
                                         final Map<String, TypeCode> columnTypeCodes,
                                         final Boolean applyUpsertForInsert,
                                         final Boolean applyUpsertForUpdate) {

        final List<Mutation> mutations = new ArrayList<>();
        for(final Mod mod : record.getMods()) {
            final Mutation.WriteBuilder builder;
            switch (record.getModType()) {
                case INSERT:
                    if(applyUpsertForInsert) {
                        builder = Mutation.newInsertOrUpdateBuilder(table);
                    } else {
                        builder = Mutation.newInsertBuilder(table);
                    }
                    break;
                case UPDATE:
                    if(applyUpsertForUpdate) {
                        builder = Mutation.newInsertOrUpdateBuilder(table);
                    } else {
                        builder = Mutation.newUpdateBuilder(table);
                    }
                    break;
                default:
                    throw new IllegalStateException("Not supported modType: " + record.getModType());
            }

            final JsonObject keys = new Gson().fromJson(mod.getKeysJson(), JsonObject.class);
            for(final Map.Entry<String, JsonElement> value : keys.entrySet()) {
                if(columnTypeCodes.containsKey(value.getKey())) {
                    final TypeCode typeCode = columnTypeCodes.get(value.getKey());
                    builder.set(value.getKey()).to(getStructValue(typeCode.getCode(), value.getValue()));
                } else {
                    final Type fieldType = type.getStructFields().get(type.getFieldIndex(value.getKey())).getType();
                    builder.set(value.getKey()).to(getStructValue(fieldType, value.getValue()));
                }
            }

            final JsonObject values = new Gson().fromJson(mod.getNewValuesJson(), JsonObject.class);
            for(final Map.Entry<String, JsonElement> value : values.entrySet()) {
                if(columnTypeCodes.containsKey(value.getKey())) {
                    final TypeCode typeCode = columnTypeCodes.get(value.getKey());
                    builder.set(value.getKey()).to(getStructValue(typeCode.getCode(), value.getValue()));
                } else {
                    final Type fieldType = type.getStructFields().get(type.getFieldIndex(value.getKey())).getType();
                    builder.set(value.getKey()).to(getStructValue(fieldType, value.getValue()));
                }
            }
            mutations.add(builder.build());
        }
        return mutations;
    }

    private static Struct convert(final Type type, final JsonObject object) {
        final Struct.Builder builder = Struct.newBuilder();
        for(final Type.StructField field : type.getStructFields()) {
            builder.set(field.getName()).to(getStructValue(field.getType(), object.get(field.getName())));
        }
        return builder.build();
    }

    private static Value getStructValue(String columnTypeCode, final JsonElement element) {
        final boolean isNull = element == null || element.isJsonNull();
        final JsonElement typeCodeElement = new Gson().fromJson(columnTypeCode, JsonElement.class);
        if(typeCodeElement.isJsonObject()) {
            columnTypeCode = typeCodeElement.getAsJsonObject().get("code").getAsString();
        } else if(typeCodeElement.isJsonPrimitive()) {
            columnTypeCode = typeCodeElement.getAsString();
        } else {
            throw new IllegalStateException();
        }
        switch (columnTypeCode) {
            case "BOOL":
                return Value.bool(isNull ? null : element.getAsBoolean());
            case "JSON":
                return Value.json(isNull ? null : element.getAsString());
            case "STRING":
                return Value.string(isNull ? null : element.getAsString());
            case "INT64":
                return Value.int64(isNull ? null : element.getAsLong());
            case "FLOAT64":
                return Value.float64(isNull ? null : element.getAsDouble());
            case "NUMERIC":
                return Value.numeric(isNull ? null : element.getAsBigDecimal());
            case "PG_NUMERIC":
                return Value.pgNumeric(isNull ? null : element.getAsString());
            case "PG_JSONB":
                return Value.pgJsonb(isNull ? null : element.getAsString());
            case "DATE":
                return Value.date(Date.parseDate(isNull ? null : element.getAsString()));
            case "TIMESTAMP":
                return Value.timestamp(isNull ? null : Timestamp.parseTimestamp(element.getAsString()));
            case "BYTES":
                return Value.bytes(isNull ? null : ByteArray.copyFrom(Base64.getDecoder().decode(element.getAsString())));
            case "STRUCT":
                throw new IllegalStateException("");
            default: {
                if(columnTypeCode.startsWith("ARRAY")) {
                    final List<JsonElement> elements = new ArrayList<>();
                    if(!isNull) {
                        for(final JsonElement child : element.getAsJsonArray()) {
                            elements.add(child);
                        }
                    }
                    final Matcher m = PATTERN_ARRAY_ELEMENT.matcher(columnTypeCode);
                    if(m.find()) {
                        final String elementColumnTypeCode = m.group();
                        switch (elementColumnTypeCode) {
                            case "BOOL":
                                return Value.boolArray(isNull ? new ArrayList<>() : elements.stream().map(JsonElement::getAsBoolean).collect(Collectors.toList()));
                            case "JSON":
                                return Value.jsonArray(isNull ? new ArrayList<>() : elements.stream().map(JsonElement::getAsString).collect(Collectors.toList()));
                            case "STRING":
                                return Value.stringArray(isNull ? new ArrayList<>() : elements.stream().map(JsonElement::getAsString).collect(Collectors.toList()));
                            case "INT64":
                                return Value.int64Array(isNull ? new ArrayList<>() : elements.stream().map(JsonElement::getAsLong).collect(Collectors.toList()));
                            case "FLOAT64":
                                return Value.float64Array(isNull ? new ArrayList<>() : elements.stream().map(JsonElement::getAsDouble).collect(Collectors.toList()));
                            case "NUMERIC":
                                return Value.numericArray(isNull ? new ArrayList<>() : elements.stream().map(JsonElement::getAsBigDecimal).collect(Collectors.toList()));
                            case "PG_NUMERIC":
                                return Value.pgNumericArray(isNull ? new ArrayList<>() : elements.stream().map(JsonElement::getAsString).collect(Collectors.toList()));
                            case "PG_JSONB":
                                return Value.pgJsonbArray(isNull ? new ArrayList<>() : elements.stream().map(JsonElement::getAsString).collect(Collectors.toList()));
                            case "DATE":
                                return Value.dateArray(isNull ? new ArrayList<>() : elements.stream().map(e -> Date.parseDate(e.getAsString())).collect(Collectors.toList()));
                            case "TIMESTAMP":
                                return Value.timestampArray(isNull ? new ArrayList<>() : elements.stream().map(e -> Timestamp.parseTimestamp(e.getAsString())).collect(Collectors.toList()));
                            case "BYTES":
                                return Value.bytesArray(isNull ? new ArrayList<>() : elements.stream().map(e -> ByteArray.copyFrom(Base64.getDecoder().decode(e.getAsString()))).collect(Collectors.toList()));
                            case "STRUCT":
                            case "ARRAY":
                            default: {
                                throw new IllegalStateException("Not supported spanner array element type: " + elementColumnTypeCode);
                            }
                        }
                    } else {
                        throw new IllegalStateException("Not found array element type: " + columnTypeCode);
                    }
                } else {
                    throw new IllegalStateException("Not supported spanner type: " + columnTypeCode);
                }
            }
        }
    }

    private static Value getStructValue(final Type fieldType, final JsonElement element) {
        final boolean isNull = element == null || element.isJsonNull();
        switch (fieldType.getCode()) {
            case BOOL:
                return Value.bool(isNull ? null : element.getAsBoolean());
            case JSON:
                return Value.json(isNull ? null : element.getAsString());
            case STRING:
                return Value.string(isNull ? null : element.getAsString());
            case INT64:
                return Value.int64(isNull ? null : element.getAsLong());
            case FLOAT64:
                return Value.float64(isNull ? null : element.getAsDouble());
            case NUMERIC:
                return Value.numeric(isNull ? null : element.getAsBigDecimal());
            case PG_NUMERIC:
                return Value.pgNumeric(isNull ? null : element.getAsString());
            case DATE:
                return Value.date(isNull ? null : Date.parseDate(element.getAsString()));
            case TIMESTAMP:
                return Value.timestamp(isNull ? null : Timestamp.parseTimestamp(element.getAsString()));
            case BYTES:
                return Value.bytes(isNull ? null : ByteArray.copyFrom(element.getAsString()));
            case STRUCT:
                return Value.struct(fieldType, isNull ? null : convert(fieldType, element.getAsJsonObject()));
            case ARRAY: {
                final List<JsonElement> elements = new ArrayList<>();
                if(!isNull) {
                    for (final JsonElement child : element.getAsJsonArray()) {
                        elements.add(child);
                    }
                }
                switch (fieldType.getArrayElementType().getCode()) {
                    case BOOL:
                        return Value.boolArray(isNull ? new ArrayList<>() : elements.stream().map(JsonElement::getAsBoolean).collect(Collectors.toList()));
                    case JSON:
                        return Value.jsonArray(isNull ? new ArrayList<>() : elements.stream().map(JsonElement::getAsString).collect(Collectors.toList()));
                    case STRING:
                        return Value.stringArray(isNull ? new ArrayList<>() : elements.stream().map(JsonElement::getAsString).collect(Collectors.toList()));
                    case INT64:
                        return Value.int64Array(isNull ? new ArrayList<>() : elements.stream().map(JsonElement::getAsLong).collect(Collectors.toList()));
                    case FLOAT64:
                        return Value.float64Array(isNull ? new ArrayList<>() : elements.stream().map(JsonElement::getAsDouble).collect(Collectors.toList()));
                    case NUMERIC:
                        return Value.numericArray(isNull ? new ArrayList<>() : elements.stream().map(JsonElement::getAsBigDecimal).collect(Collectors.toList()));
                    case PG_NUMERIC:
                        return Value.pgNumericArray(isNull ? new ArrayList<>() : elements.stream().map(JsonElement::getAsString).collect(Collectors.toList()));
                    case DATE:
                        return Value.dateArray(isNull ? new ArrayList<>() : elements.stream().map(e -> Date.parseDate(e.getAsString())).collect(Collectors.toList()));
                    case TIMESTAMP:
                        return Value.timestampArray(isNull ? new ArrayList<>() : elements.stream().map(e -> Timestamp.parseTimestamp(e.getAsString())).collect(Collectors.toList()));
                    case BYTES:
                        return Value.bytesArray(isNull ? new ArrayList<>() : elements.stream().map(e -> ByteArray.copyFrom(e.getAsString())).collect(Collectors.toList()));
                    case STRUCT:
                        return Value.structArray(fieldType.getArrayElementType(),
                                isNull ? new ArrayList<>() : elements.stream().map(e -> convert(fieldType.getArrayElementType(), e.getAsJsonObject())).collect(Collectors.toList()));
                    case ARRAY:
                    default: {
                        throw new IllegalStateException("Not supported spanner array element type: " + fieldType.getArrayElementType().getCode());
                    }
                }
            }
            default:
                throw new IllegalStateException("Not supported spanner field type: " + fieldType.getCode());
        }
    }

    public static MutationGroup convertToMutationGroup(final Map<String, Type> tableTypes, final Collection<DataChangeRecord> records) {
        final List<Mutation> mutations = records
                .stream()
                .sorted(Comparator.comparing(DataChangeRecord::getRecordSequence))
                .flatMap(v -> StructSchemaUtil.convertToMutation(tableTypes.get(v.getTableName()), v).stream())
                .collect(Collectors.toList());
        return MutationGroup.create(mutations.get(0), mutations.subList(1, mutations.size()));
    }

    public static Mutation accumulateChangeRecords(final String table,
                                                   final org.apache.avro.Schema tableSchema,
                                                   final GenericRecord snapshot,
                                                   final List<GenericRecord> changeRecords) {

        if(changeRecords.size() == 0) {
            final Mutation.WriteBuilder builder = Mutation.newInsertOrUpdateBuilder(table);
            if(snapshot == null) {
                throw new IllegalStateException("The size of changeRecords and snapshots are both zero.");
            }
            final Map<String, Value> values = RecordToMutationConverter.convertValues(tableSchema, snapshot);
            for(final Map.Entry<String, Value> entry : values.entrySet()) {
                builder.set(entry.getKey()).to(entry.getValue());
            }
            return builder.build();
        } else {
            final GenericRecord lastChangeRecord = changeRecords.get(changeRecords.size() - 1);
            if("DELETE".equalsIgnoreCase(lastChangeRecord.get("modType").toString())) {
                final List<KV<Key, GenericRecord>> keyAndRecords = StructSchemaUtil.createChangeRecordKey(tableSchema, lastChangeRecord);
                if(keyAndRecords.size() != 1) {
                    throw new IllegalStateException("illegal change record: " + lastChangeRecord + " contains multi mod");
                }
                return Mutation.delete(table, keyAndRecords.get(0).getKey());
            } else {
                final Map<String, Value> values = RecordToMutationConverter.convertValues(tableSchema, snapshot);
                for(final GenericRecord changeRecord : changeRecords) {
                    final Map<String, String> rowTypes = ((List<GenericRecord>) (changeRecord.get("rowType")))
                            .stream()
                            .collect(Collectors.toMap(
                                    r -> r.get("name").toString(),
                                    r -> r.get("Type").toString()));
                    switch (changeRecord.get("modType").toString().toUpperCase()) {
                        case "INSERT":
                        case "UPDATE": {
                            final List<GenericRecord> mods = (List<GenericRecord>) (changeRecord.get("mods"));
                            if(mods == null || mods.size() > 1) {
                                throw new IllegalStateException("illegal change record: " + changeRecord + " contains multi mod");
                            }
                            for(final GenericRecord mod : mods) {
                                final String keyValuesJson = mod.get("keysJson").toString();
                                final String newValuesJson = mod.get("newValuesJson").toString();
                                final JsonObject keyValues = new Gson().fromJson(keyValuesJson, JsonObject.class);
                                final JsonObject newValues = new Gson().fromJson(newValuesJson, JsonObject.class);
                                final Map<String, JsonElement> allValues = new HashMap<>();
                                allValues.putAll(keyValues.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
                                allValues.putAll(newValues.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
                                for(final Map.Entry<String, JsonElement> entry : allValues.entrySet()) {
                                    final boolean isNull = entry.getValue().isJsonNull();
                                    final org.apache.avro.Schema.Field field = tableSchema.getField(entry.getKey());
                                    if(field == null) {
                                        switch (rowTypes.get(entry.getKey())) {
                                            case "BOOL":
                                                values.put(entry.getKey(), Value.bool(isNull ? null : entry.getValue().getAsBoolean()));
                                                break;
                                            case "INT64":
                                                values.put(entry.getKey(), Value.int64(isNull ? null : entry.getValue().getAsLong()));
                                                break;
                                            case "FLOAT64":
                                                values.put(entry.getKey(), Value.float64(isNull ? null : entry.getValue().getAsDouble()));
                                                break;
                                            case "TIMESTAMP":
                                                values.put(entry.getKey(), Value.timestamp(isNull ? null : Timestamp.parseTimestamp(entry.getValue().getAsString())));
                                                break;
                                            case "DATE":
                                                values.put(entry.getKey(), Value.date(isNull ? null : Date.parseDate(entry.getValue().getAsString())));
                                                break;
                                            case "STRING":
                                                values.put(entry.getKey(), Value.string(isNull ? null : entry.getValue().getAsString()));
                                                break;
                                            case "BYTES":
                                                values.put(entry.getKey(), Value.bytes(isNull ? null : ByteArray.copyFrom(entry.getValue().getAsString().getBytes())));
                                                break;
                                            case "NUMERIC":
                                                values.put(entry.getKey(), Value.numeric(isNull ? null : entry.getValue().getAsBigDecimal()));
                                                break;
                                            case "JSON":
                                                values.put(entry.getKey(), Value.json(isNull ? null : entry.getValue().getAsString()));
                                                break;
                                            case "TYPE_CODE_UNSPECIFIED":
                                                throw new IllegalStateException();
                                            case "ARRAY":
                                            case "STRUCT":
                                            default:
                                                throw new IllegalArgumentException(
                                                        "Not supported modType: " + rowTypes.get(entry.getKey())
                                                                + " for field: " + entry.getKey());
                                        }
                                    } else {
                                        final org.apache.avro.Schema fieldSchema = AvroSchemaUtil.unnestUnion(field.schema());
                                        switch (fieldSchema.getType()) {
                                            case BOOLEAN:
                                                values.put(entry.getKey(), Value.bool(isNull ? null : entry.getValue().getAsBoolean()));
                                                break;
                                            case ENUM:
                                            case STRING: {
                                                if(AvroSchemaUtil.isSqlTypeJson(fieldSchema)) {
                                                    values.put(entry.getKey(), Value.json(isNull ? null : entry.getValue().getAsString()));
                                                } else {
                                                    values.put(entry.getKey(), Value.string(isNull ? null : entry.getValue().getAsString()));
                                                }
                                                break;
                                            }
                                            case FIXED:
                                            case BYTES: {
                                                if(AvroSchemaUtil.isLogicalTypeDecimal(fieldSchema)) {
                                                    values.put(entry.getKey(), Value.numeric(isNull ? null : entry.getValue().getAsBigDecimal()));
                                                } else {
                                                    values.put(entry.getKey(), Value.bytes(isNull ? null : ByteArray.copyFrom(entry.getValue().getAsString())));
                                                }
                                                break;
                                            }
                                            case INT: {
                                                if(LogicalTypes.date().equals(fieldSchema.getLogicalType())) {
                                                    values.put(entry.getKey(), Value.date(isNull ? null : Date.parseDate(entry.getValue().getAsString())));
                                                } else if(LogicalTypes.timeMillis().equals(fieldSchema.getLogicalType())) {
                                                    values.put(entry.getKey(), Value.string(isNull ? null : entry.getValue().getAsString()));
                                                } else {
                                                    values.put(entry.getKey(), Value.int64(isNull ? null : entry.getValue().getAsLong()));
                                                }
                                                break;
                                            }
                                            case LONG: {
                                                if(LogicalTypes.timestampMillis().equals(fieldSchema.getLogicalType())
                                                        || LogicalTypes.timestampMicros().equals(fieldSchema.getLogicalType())) {
                                                    if(isNull) {
                                                        values.put(entry.getKey(), Value.timestamp(null));
                                                    } else {
                                                        final Timestamp timestampValue = Timestamp.parseTimestamp(entry.getValue().getAsString());
                                                        values.put(entry.getKey(), Value.timestamp(timestampValue));
                                                    }
                                                } else if(LogicalTypes.timeMicros().equals(fieldSchema.getLogicalType())) {
                                                    values.put(entry.getKey(), Value.string(isNull ? null : entry.getValue().getAsString()));
                                                } else {
                                                    values.put(entry.getKey(), Value.int64(isNull ? null : entry.getValue().getAsLong()));
                                                }
                                                break;
                                            }
                                            case FLOAT:
                                            case DOUBLE:
                                                values.put(entry.getKey(), Value.float64(isNull ? null : entry.getValue().getAsDouble()));
                                                break;
                                            case ARRAY: {
                                                final org.apache.avro.Schema elementSchema = AvroSchemaUtil.unnestUnion(fieldSchema.getElementType());
                                                switch (elementSchema.getType()) {
                                                    case BOOLEAN: {
                                                        if(isNull) {
                                                            values.put(entry.getKey(), Value.boolArray(new ArrayList<>()));
                                                        }
                                                        final List<Boolean> elements = new ArrayList<>();
                                                        for(final JsonElement element : entry.getValue().getAsJsonArray()) {
                                                            elements.add(element.getAsBoolean());
                                                        }
                                                        values.put(entry.getKey(), Value.boolArray(elements));
                                                        break;
                                                    }
                                                    case ENUM:
                                                    case STRING: {
                                                        final List<String> strings = new ArrayList<>();
                                                        if(!isNull) {
                                                            for(final JsonElement element : entry.getValue().getAsJsonArray()) {
                                                                strings.add(element.getAsString());
                                                            }
                                                        }
                                                        if(AvroSchemaUtil.isSqlTypeJson(elementSchema)) {
                                                            values.put(entry.getKey(), Value.jsonArray(strings));
                                                        } else {
                                                            values.put(entry.getKey(), Value.stringArray(strings));
                                                        }
                                                        break;
                                                    }
                                                    case FIXED:
                                                    case BYTES: {
                                                        if(AvroSchemaUtil.isLogicalTypeDecimal(elementSchema)) {
                                                            if(isNull) {
                                                                values.put(entry.getKey(), Value.numericArray(new ArrayList<>()));
                                                            } else {
                                                                final List<BigDecimal> elements = new ArrayList<>();
                                                                for(final JsonElement element : entry.getValue().getAsJsonArray()) {
                                                                    elements.add(element.getAsBigDecimal());
                                                                }
                                                                values.put(entry.getKey(), Value.numericArray(elements));
                                                            }
                                                        } else {
                                                            if(isNull) {
                                                                values.put(entry.getKey(), Value.bytesArray(new ArrayList<>()));
                                                            } else {
                                                                final List<ByteArray> elements = new ArrayList<>();
                                                                for(final JsonElement element : entry.getValue().getAsJsonArray()) {
                                                                    elements.add(ByteArray.copyFrom(element.getAsString()));
                                                                }
                                                                values.put(entry.getKey(), Value.bytesArray(elements));
                                                            }
                                                            values.put(entry.getKey(), Value.bytes(isNull ? null : ByteArray.copyFrom(entry.getValue().getAsString())));
                                                        }
                                                        break;
                                                    }
                                                    case FLOAT:
                                                    case DOUBLE: {
                                                        if(isNull) {
                                                            values.put(entry.getKey(), Value.float64Array(new ArrayList<>()));
                                                        }
                                                        final List<Double> elements = new ArrayList<>();
                                                        for(final JsonElement element : entry.getValue().getAsJsonArray()) {
                                                            elements.add(element.getAsDouble());
                                                        }
                                                        values.put(entry.getKey(), Value.float64Array(elements));
                                                        break;
                                                    }
                                                    case INT: {
                                                        if(LogicalTypes.date().equals(fieldSchema.getLogicalType())) {
                                                            if(isNull) {
                                                                values.put(entry.getKey(), Value.dateArray(new ArrayList<>()));
                                                            } else {
                                                                final List<Date> elements = new ArrayList<>();
                                                                for(final JsonElement element : entry.getValue().getAsJsonArray()) {
                                                                    elements.add(Date.parseDate(element.getAsString()));
                                                                }
                                                                values.put(entry.getKey(), Value.dateArray(elements));
                                                            }
                                                        } else if(LogicalTypes.timeMillis().equals(fieldSchema.getLogicalType())) {
                                                            if(isNull) {
                                                                values.put(entry.getKey(), Value.stringArray(new ArrayList<>()));
                                                            } else {
                                                                final List<String> elements = new ArrayList<>();
                                                                for(final JsonElement element : entry.getValue().getAsJsonArray()) {
                                                                    elements.add(element.getAsString());
                                                                }
                                                                values.put(entry.getKey(), Value.stringArray(elements));
                                                            }
                                                        } else {
                                                            if(isNull) {
                                                                values.put(entry.getKey(), Value.int64Array(new ArrayList<>()));
                                                            } else {
                                                                final List<Long> elements = new ArrayList<>();
                                                                for(final JsonElement element : entry.getValue().getAsJsonArray()) {
                                                                    elements.add(element.getAsLong());
                                                                }
                                                                values.put(entry.getKey(), Value.int64Array(elements));
                                                            }
                                                        }
                                                        break;
                                                    }
                                                    case LONG: {
                                                        if(LogicalTypes.timestampMillis().equals(fieldSchema.getLogicalType())
                                                                || LogicalTypes.timestampMicros().equals(fieldSchema.getLogicalType())) {
                                                            if(isNull) {
                                                                values.put(entry.getKey(), Value.timestampArray(new ArrayList<>()));
                                                            } else {
                                                                final List<Timestamp> elements = new ArrayList<>();
                                                                for(final JsonElement element : entry.getValue().getAsJsonArray()) {
                                                                    final Timestamp timestampValue = Timestamp.parseTimestamp(element.getAsString());
                                                                    elements.add(timestampValue);
                                                                }
                                                                values.put(entry.getKey(), Value.timestampArray(elements));
                                                            }
                                                        } else if(LogicalTypes.timeMicros().equals(fieldSchema.getLogicalType())) {
                                                            if(isNull) {
                                                                values.put(entry.getKey(), Value.stringArray(new ArrayList<>()));
                                                            } else {
                                                                final List<String> elements = new ArrayList<>();
                                                                for(final JsonElement element : entry.getValue().getAsJsonArray()) {
                                                                    elements.add(element.getAsString());
                                                                }
                                                                values.put(entry.getKey(), Value.stringArray(elements));
                                                            }
                                                        } else {
                                                            if(isNull) {
                                                                values.put(entry.getKey(), Value.int64Array(new ArrayList<>()));
                                                            } else {
                                                                final List<Long> elements = new ArrayList<>();
                                                                for(final JsonElement element : entry.getValue().getAsJsonArray()) {
                                                                    elements.add(element.getAsLong());
                                                                }
                                                                values.put(entry.getKey(), Value.int64Array(elements));
                                                            }
                                                        }
                                                        break;
                                                    }
                                                    case RECORD:
                                                    case ARRAY:
                                                    case UNION:
                                                    case NULL:
                                                    case MAP:
                                                    default:
                                                        throw new IllegalArgumentException();
                                                }
                                                break;
                                            }
                                            case MAP:
                                            case RECORD:
                                            case NULL:
                                            case UNION:
                                            default:
                                                throw new IllegalStateException();
                                        }
                                    }
                                }
                            }
                            break;
                        }
                        case "DELETE":
                            values.clear();
                            break;
                        default:
                            throw new IllegalArgumentException("Not supported modType: " + changeRecord.get("modType").toString());
                    }
                }

                final Mutation.WriteBuilder builder = Mutation.newInsertOrUpdateBuilder(table);
                for(final Map.Entry<String, Value> value : values.entrySet()) {
                    builder.set(value.getKey()).to(value.getValue());
                }
                final Mutation mutation = builder.build();
                return mutation;
            }
        }
    }

    public static GenericRecord accumulateChangeRecords(final org.apache.avro.Schema tableSchema,
                                                   final GenericRecord snapshot,
                                                   final List<GenericRecord> changeRecords) {

        if(changeRecords.size() == 0) {
            if(snapshot == null) {
                throw new IllegalStateException("The size of changeRecords and snapshots are both zero.");
            }
            return snapshot;
        } else {
            final GenericRecord lastChangeRecord = changeRecords.get(changeRecords.size() - 1);
            if("DELETE".equalsIgnoreCase(lastChangeRecord.get("modType").toString())) {
                return null;
            } else {
                GenericRecordBuilder builder;
                if(snapshot == null) {
                    builder = new GenericRecordBuilder(tableSchema);
                } else {
                    builder = AvroSchemaUtil.toBuilder(tableSchema, snapshot);
                }
                for(final GenericRecord changeRecord : changeRecords) {
                    final Map<String, String> rowTypes = ((List<GenericRecord>) (changeRecord.get("rowType")))
                            .stream()
                            .collect(Collectors.toMap(
                                    r -> r.get("name").toString(),
                                    r -> r.get("Type").toString()));
                    switch (changeRecord.get("modType").toString().toUpperCase()) {
                        case "INSERT":
                        case "UPDATE": {
                            final List<GenericRecord> mods = (List<GenericRecord>) (changeRecord.get("mods"));
                            if(mods == null || mods.size() > 1) {
                                throw new IllegalStateException("illegal change record: " + changeRecord + " contains multi mod");
                            }
                            for(final GenericRecord mod : mods) {
                                final String keyValuesJson = mod.get("keysJson").toString();
                                final String newValuesJson = mod.get("newValuesJson").toString();
                                final JsonObject keyValues = new Gson().fromJson(keyValuesJson, JsonObject.class);
                                final JsonObject newValues = new Gson().fromJson(newValuesJson, JsonObject.class);
                                final Map<String, JsonElement> allValues = new HashMap<>();
                                allValues.putAll(keyValues.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
                                allValues.putAll(newValues.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
                                for(final Map.Entry<String, JsonElement> entry : allValues.entrySet()) {
                                    final boolean isNull = entry.getValue().isJsonNull();
                                    switch (rowTypes.get(entry.getKey())) {
                                        case "BOOL":
                                            builder.set(entry.getKey(), isNull ? null : entry.getValue().getAsBoolean());
                                            break;
                                        case "INT64":
                                            builder.set(entry.getKey(), isNull ? null : entry.getValue().getAsLong());
                                            break;
                                        case "FLOAT64":
                                            builder.set(entry.getKey(), isNull ? null : entry.getValue().getAsDouble());
                                            break;
                                        case "TIMESTAMP":
                                            builder.set(entry.getKey(), isNull ? null : DateTimeUtil.toEpochMicroSecond(entry.getValue().getAsString()));
                                            break;
                                        case "DATE":
                                            builder.set(entry.getKey(), isNull ? null : DateTimeUtil.toEpochDay(Date.parseDate(entry.getValue().getAsString())));
                                            break;
                                        case "JSON":
                                        case "STRING":
                                            builder.set(entry.getKey(), isNull ? null : entry.getValue().getAsString());
                                            break;
                                        case "BYTES":
                                            builder.set(entry.getKey(), isNull ? null : ByteBuffer.wrap(entry.getValue().getAsString().getBytes()));
                                            break;
                                        case "NUMERIC":
                                            builder.set(entry.getKey(), isNull ? null : ByteBuffer.wrap(entry.getValue().getAsBigDecimal().unscaledValue().toByteArray()));
                                            break;
                                        case "TYPE_CODE_UNSPECIFIED":
                                            throw new IllegalStateException();
                                        case "ARRAY":
                                        case "STRUCT":
                                        default:
                                            throw new IllegalArgumentException(
                                                    "Not supported modType: " + rowTypes.get(entry.getKey())
                                                            + " for field: " + entry.getKey());
                                    }
                                }
                            }
                            break;
                        }
                        case "DELETE":
                            builder = new GenericRecordBuilder(tableSchema);
                            break;
                        default:
                            throw new IllegalArgumentException("Not supported modType: " + changeRecord.get("modType").toString());
                    }
                }

                return builder.build();
            }
        }
    }

    public static Mutation convert(final Type type,
                                   final Mutation mutation,
                                   final String table,
                                   final String mutationOp,
                                   final Iterable<String> keyFields,
                                   final List<String> allowCommitTimestampFields,
                                   final Set<String> excludeFields,
                                   final Set<String> hideFields) {
        return mutation;
    }

    public static List<String> getKeys(final Mutation mutation) {
        final List<String> keys = new ArrayList<>();
        if(Mutation.Op.DELETE.equals(mutation.getOperation()) && mutation.getKeySet() != null) {
            for (final Key key : mutation.getKeySet().getKeys()) {
                keys.add(key.toString());
            }
        }
        return keys;
    }

    public static String convertChangeRecordTypeCode(String code) {
        final Matcher m = PATTERN_CHANGE_RECORD_TYPE_CODE.matcher(code);
        if(m.find()) {
            return m.group(1);
        }
        return null;
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

}
