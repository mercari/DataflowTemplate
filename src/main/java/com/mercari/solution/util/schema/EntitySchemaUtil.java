package com.mercari.solution.util.schema;

import com.google.cloud.Date;
import com.google.datastore.v1.ArrayValue;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.Value;
import com.google.protobuf.ByteString;
import com.google.protobuf.NullValue;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import com.mercari.solution.util.DateTimeUtil;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.hadoop.hbase.util.Bytes;
import org.joda.time.Instant;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class EntitySchemaUtil {

    private static final Pattern PATTERN_DATE1 = Pattern.compile("[0-9]{8}");
    private static final Pattern PATTERN_DATE2 = Pattern.compile("[0-9]{4}-[0-9]{2}-[0-9]{2}");
    private static final Pattern PATTERN_DATE3 = Pattern.compile("[0-9]{4}/[0-9]{2}/[0-9]{2}");


    public static Object getFieldValue(Entity entity, String fieldName) {
        return entity.getPropertiesMap().entrySet().stream()
                .filter(entry -> entry.getKey().equals(fieldName))
                .map(Map.Entry::getValue)
                .map(EntitySchemaUtil::getValue)
                .findAny()
                .orElse(null);
    }

    public static String getFieldValueAsString(Entity entity, String fieldName) {
        return entity.getPropertiesMap().entrySet().stream()
                .filter(entry -> entry.getKey().equals(fieldName))
                .map(Map.Entry::getValue)
                .map(EntitySchemaUtil::getValue)
                .map(v -> v == null ? null : v.toString())
                .findAny()
                .orElse(null);
    }

    public static Object getKeyFieldValue(final Entity entity, String fieldName) {
        final Key.PathElement pe = entity.getKey().getPath(entity.getKey().getPathCount()-1);
        return pe.getName() == null ? pe.getId() : pe.getName();
    }

    public static Object getValue(final Entity entity, final String fieldName) {
        if(entity == null) {
            return null;
        }
        final Value value = entity.getPropertiesOrDefault(fieldName, null);
        if(value == null) {
            return null;
        }
        switch(value.getValueTypeCase()) {
            case KEY_VALUE: return value.getKeyValue();
            case STRING_VALUE: return value.getStringValue();
            case BLOB_VALUE: return value.getBlobValue().toByteArray();
            case INTEGER_VALUE: return value.getIntegerValue();
            case DOUBLE_VALUE: return value.getDoubleValue();
            case BOOLEAN_VALUE: return value.getBooleanValue();
            case TIMESTAMP_VALUE: return Instant.ofEpochMilli(Timestamps.toMillis(value.getTimestampValue()));
            case ENTITY_VALUE: return value.getEntityValue();
            case ARRAY_VALUE: {
                return value.getArrayValue().getValuesList()
                        .stream()
                        .map(v -> {
                            if(v == null) {
                                return null;
                            }
                            switch (v.getValueTypeCase()) {
                                case KEY_VALUE:
                                    return v.getKeyValue();
                                case BOOLEAN_VALUE:
                                    return v.getBooleanValue();
                                case INTEGER_VALUE:
                                    return v.getIntegerValue();
                                case BLOB_VALUE:
                                    return v.getBlobValue().toByteArray();
                                case STRING_VALUE:
                                    return v.getStringValue();
                                case DOUBLE_VALUE:
                                    return v.getDoubleValue();
                                case GEO_POINT_VALUE:
                                    return v.getGeoPointValue();
                                case TIMESTAMP_VALUE:
                                    return Instant.ofEpochMilli(Timestamps.toMillis(v.getTimestampValue()));
                                case ENTITY_VALUE:
                                    return v.getEntityValue();
                                case ARRAY_VALUE:
                                case NULL_VALUE:
                                case VALUETYPE_NOT_SET:
                                default:
                                    return null;
                            }
                        })
                        .collect(Collectors.toList());
            }
            case GEO_POINT_VALUE:
                return value.getGeoPointValue();
            case VALUETYPE_NOT_SET:
            case NULL_VALUE:
                return null;
            default:
                throw new IllegalArgumentException(String.format("%s is not supported!", value.getValueTypeCase().name()));
        }
    }

    public static Object getValue(Value value) {
        if(value == null) {
            return null;
        }
        switch(value.getValueTypeCase()) {
            case KEY_VALUE: return value.getKeyValue();
            case STRING_VALUE: return value.getStringValue();
            case BLOB_VALUE: return value.getBlobValue();
            case INTEGER_VALUE: return value.getIntegerValue();
            case DOUBLE_VALUE: return value.getDoubleValue();
            case BOOLEAN_VALUE: return value.getBooleanValue();
            case TIMESTAMP_VALUE: return value.getTimestampValue();
            case ENTITY_VALUE: return value.getEntityValue();
            case ARRAY_VALUE: return value.getArrayValue();
            case VALUETYPE_NOT_SET:
            case NULL_VALUE:
                return null;
            case GEO_POINT_VALUE:
            default:
                throw new IllegalArgumentException(String.format("%s is not supported!", value.getValueTypeCase().name()));
        }
    }

    public static String getAsString(final Value value) {
        final Object object = getValue(value);
        if(object == null) {
            return null;
        }
        return object.toString();
    }

    public static String getAsString(final Entity entity, final String fieldName) {
        final Value value = entity.getPropertiesOrDefault(fieldName, null);
        if(value == null) {
            return null;
        }
        switch(value.getValueTypeCase()) {
            case KEY_VALUE:
                return toStringKey(value.getKeyValue());
            case STRING_VALUE:
                return value.getStringValue();
            case BLOB_VALUE:
                return Base64.getEncoder().encodeToString(value.getBlobValue().toByteArray());
            case INTEGER_VALUE:
                return Long.toString(value.getIntegerValue());
            case DOUBLE_VALUE:
                return Double.toString(value.getDoubleValue());
            case BOOLEAN_VALUE:
                return Boolean.toString(value.getBooleanValue());
            case TIMESTAMP_VALUE:
                return Instant.ofEpochMilli(Timestamps.toMillis(value.getTimestampValue())).toString();
            case GEO_POINT_VALUE:
            case ENTITY_VALUE:
            case ARRAY_VALUE:
            case VALUETYPE_NOT_SET:
            case NULL_VALUE:
            default:
                return null;
        }
    }

    public static Long getAsLong(final Entity entity, final String fieldName) {
        final Value value = entity.getPropertiesOrDefault(fieldName, null);
        if(value == null) {
            return null;
        }
        switch(value.getValueTypeCase()) {
            case BOOLEAN_VALUE:
                return value.getBooleanValue() ? 1L : 0L;
            case INTEGER_VALUE:
                return value.getIntegerValue();
            case DOUBLE_VALUE:
                return Double.valueOf(value.getDoubleValue()).longValue();
            case STRING_VALUE: {
                try {
                    return Long.valueOf(value.getStringValue());
                } catch (Exception e) {
                    return null;
                }
            }
            case KEY_VALUE:
            case BLOB_VALUE:
            case TIMESTAMP_VALUE:
            case GEO_POINT_VALUE:
            case ENTITY_VALUE:
            case ARRAY_VALUE:
            case VALUETYPE_NOT_SET:
            case NULL_VALUE:
            default:
                return null;
        }
    }

    public static Double getAsDouble(final Entity entity, final String fieldName) {
        final Value value = entity.getPropertiesOrDefault(fieldName, null);
        if(value == null) {
            return null;
        }
        switch(value.getValueTypeCase()) {
            case BOOLEAN_VALUE:
                return value.getBooleanValue() ? 1D : 0D;
            case INTEGER_VALUE:
                return Long.valueOf(value.getIntegerValue()).doubleValue();
            case DOUBLE_VALUE:
                return value.getDoubleValue();
            case STRING_VALUE: {
                try {
                    return Double.valueOf(value.getStringValue());
                } catch (Exception e) {
                    return null;
                }
            }
            case KEY_VALUE:
            case BLOB_VALUE:
            case TIMESTAMP_VALUE:
            case GEO_POINT_VALUE:
            case ENTITY_VALUE:
            case ARRAY_VALUE:
            case VALUETYPE_NOT_SET:
            case NULL_VALUE:
            default:
                return null;
        }
    }

    public static BigDecimal getAsBigDecimal(final Entity entity, final String fieldName) {
        final Value value = entity.getPropertiesOrDefault(fieldName, null);
        if(value == null) {
            return null;
        }
        switch(value.getValueTypeCase()) {
            case BOOLEAN_VALUE:
                return BigDecimal.valueOf(value.getBooleanValue() ? 1D : 0D);
            case INTEGER_VALUE:
                return BigDecimal.valueOf(value.getIntegerValue());
            case DOUBLE_VALUE:
                return BigDecimal.valueOf(value.getDoubleValue());
            case STRING_VALUE: {
                try {
                    return BigDecimal.valueOf(Double.valueOf(value.getStringValue()));
                } catch (Exception e) {
                    return null;
                }
            }
            case KEY_VALUE:
            case BLOB_VALUE:
            case TIMESTAMP_VALUE:
            case GEO_POINT_VALUE:
            case ENTITY_VALUE:
            case ARRAY_VALUE:
            case VALUETYPE_NOT_SET:
            case NULL_VALUE:
            default:
                return null;
        }
    }

    public static ByteString getAsByteString(final Entity entity, final String fieldName) {
        if(entity == null || fieldName == null) {
            return null;
        }

        if(!entity.getPropertiesMap().containsKey(fieldName)) {
            return null;
        }

        final Value value = entity.getPropertiesOrThrow(fieldName);
        final byte[] bytes;
        switch (value.getValueTypeCase()) {
            case BOOLEAN_VALUE:
                bytes = Bytes.toBytes(value.getBooleanValue());
                break;
            case STRING_VALUE:
                bytes = Bytes.toBytes(value.getStringValue());
                break;
            case BLOB_VALUE:
                bytes = value.getBlobValue().asReadOnlyByteBuffer().array();
                break;
            case INTEGER_VALUE:
                bytes = Bytes.toBytes(value.getIntegerValue());
                break;
            case DOUBLE_VALUE:
                bytes = Bytes.toBytes(value.getDoubleValue());
                break;
            case TIMESTAMP_VALUE: {
                final Timestamp timestamp = value.getTimestampValue();
                bytes = Bytes.toBytes(Timestamps.toMicros(timestamp));
                break;
            }
            case KEY_VALUE:
            case NULL_VALUE:
            case ARRAY_VALUE:
            case ENTITY_VALUE:
            case GEO_POINT_VALUE:
            case VALUETYPE_NOT_SET:
            default:
                return null;
        }
        return ByteString.copyFrom(bytes);
    }

    public static Date convertDate(final Value value) {
        if(Value.ValueTypeCase.STRING_VALUE.equals(value.getValueTypeCase())) {
            final String datestr = value.getStringValue();
            if(PATTERN_DATE1.matcher(datestr).find()) {
                return Date.fromYearMonthDay(
                        Integer.valueOf(datestr.substring(0, 4)),
                        Integer.valueOf(datestr.substring(4, 6)),
                        Integer.valueOf(datestr.substring(6, 8))
                );
            } else if(PATTERN_DATE2.matcher(datestr).find() || PATTERN_DATE3.matcher(datestr).find()) {
                return Date.fromYearMonthDay(
                        Integer.valueOf(datestr.substring(0, 4)),
                        Integer.valueOf(datestr.substring(5, 7)),
                        Integer.valueOf(datestr.substring(8, 10))
                );
            } else {
                throw new IllegalArgumentException("Illegal date string: " + datestr);
            }
        } else if(Value.ValueTypeCase.INTEGER_VALUE.equals(value.getValueTypeCase())) {
            final LocalDate localDate = LocalDate.ofEpochDay(value.getIntegerValue());
            return Date.fromYearMonthDay(localDate.getYear(), localDate.getMonthValue(), localDate.getDayOfMonth());
        } else {
            throw new IllegalArgumentException();
        }

    }

    public static Entity.Builder toBuilder(final Schema schema, final Entity entity) {
        return toBuilder(schema, entity, new HashMap<>());
    }

    public static Entity.Builder toBuilder(final Schema schema, final Entity entity, final Map<String, String> renameFields) {
        final Entity.Builder builder = Entity.newBuilder();
        builder.setKey(entity.getKey());
        final Map<String,Value> values = entity.getPropertiesMap();
        for(final Schema.Field field : schema.getFields()) {
            final String getFieldName = renameFields.getOrDefault(field.getName(), field.getName());
            final String setFieldName = field.getName();

            if(values.containsKey(getFieldName)) {
                switch (field.getType().getTypeName()) {
                    case ITERABLE:
                    case ARRAY: {
                        if(field.getType().getCollectionElementType().getTypeName().equals(Schema.TypeName.ROW)) {
                            final List<Entity> children = new ArrayList<>();
                            for(final Value child : values.get(getFieldName).getArrayValue().getValuesList()) {
                                if(child == null || child.getValueTypeCase().equals(Value.ValueTypeCase.NULL_VALUE)) {
                                    children.add(null);
                                } else {
                                    children.add(toBuilder(field.getType().getCollectionElementType().getRowSchema(), child.getEntityValue()).build());
                                }
                            }
                            final List<Value> entityValues = children.stream()
                                    .map(e -> {
                                        if(e == null) {
                                            return Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build();
                                        }
                                        return Value.newBuilder().setEntityValue(e).build();
                                    })
                                    .collect(Collectors.toList());
                            builder.putProperties(field.getName(), Value.newBuilder()
                                    .setArrayValue(ArrayValue.newBuilder().addAllValues(entityValues))
                                    .build());
                        } else {
                            builder.putProperties(field.getName(), values.get(getFieldName));
                        }
                        break;
                    }
                    case ROW: {
                        final Entity child = toBuilder(field.getType().getRowSchema(), values.get(getFieldName).getEntityValue()).build();
                        builder.putProperties(field.getName(), Value.newBuilder().setEntityValue(child).build());
                        break;
                    }
                    default:
                        builder.putProperties(field.getName(), values.get(getFieldName));
                        break;
                }
            } else if(renameFields.containsValue(setFieldName)) {
                final String getOuterFieldName = renameFields.entrySet().stream()
                        .filter(e -> e.getValue().equals(setFieldName))
                        .map(Map.Entry::getKey)
                        .findAny()
                        .orElse(setFieldName);
                if(!values.containsKey(getOuterFieldName) || values.get(getOuterFieldName) == null) {
                    builder.putProperties(field.getName(), Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build());
                    continue;
                }

                switch (field.getType().getTypeName()) {
                    case ITERABLE:
                    case ARRAY: {
                        if(field.getType().getCollectionElementType().getTypeName().equals(Schema.TypeName.ROW)) {
                            final List<Value> children = new ArrayList<>();
                            for(final Value child : values.get(getOuterFieldName).getArrayValue().getValuesList()) {
                                if(child != null && child.getNullValue() != null && child.getEntityValue() != null) {
                                    Entity.Builder childBuilder = toBuilder(field.getType().getCollectionElementType().getRowSchema(), child.getEntityValue());
                                    children.add(Value.newBuilder().setEntityValue(childBuilder).build());
                                }
                            }
                            builder.putProperties(setFieldName, Value.newBuilder().setArrayValue(ArrayValue.newBuilder().addAllValues(children).build()).build());
                        } else {
                            builder.putProperties(setFieldName, values.get(getOuterFieldName));
                        }
                        break;
                    }
                    case ROW: {
                        final Entity child = toBuilder(field.getType().getRowSchema(), values.get(getOuterFieldName).getEntityValue()).build();
                        builder.putProperties(setFieldName, Value.newBuilder().setEntityValue(child).build());
                        break;
                    }
                    default:
                        builder.putProperties(setFieldName, values.get(getOuterFieldName));
                        break;
                }
            } else {
                builder.putProperties(field.getName(), Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build());
            }
        }
        return builder;
    }

    public static Entity.Builder toBuilder(final Entity entity) {
        return Entity.newBuilder(entity);
    }

    public static Entity.Builder toBuilder(final Entity entity,
                                           final Collection<String> includeFields,
                                           final Collection<String> excludeFields) {

        final Entity.Builder builder = Entity.newBuilder();
        for(var entry : entity.getPropertiesMap().entrySet()) {
            if(includeFields != null && !includeFields.contains(entry.getKey())) {
                continue;
            }
            if(excludeFields != null && excludeFields.contains(entry.getKey())) {
                continue;
            }
            builder.putProperties(entry.getKey(), entry.getValue());
        }
        return builder;
    }

    public static Entity.Builder convertBuilder(final Schema schema, final Entity entity, final List<String> excludeFromIndexFields) {
        if (excludeFromIndexFields == null || excludeFromIndexFields.size() == 0) {
            return entity.toBuilder();
        }
        final Entity.Builder builder = Entity.newBuilder();
        builder.setKey(entity.getKey());
        for(Schema.Field field : schema.getFields()) {
            final Value value = entity.getPropertiesOrDefault(field.getName(),
                    Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build());
            final boolean excludeFromIndexes = excludeFromIndexFields.contains(field.getName());
            if (excludeFromIndexes) {
                builder.putProperties(field.getName(), value.toBuilder()
                        .setExcludeFromIndexes(true)
                        .build());
            } else {
                builder.putProperties(field.getName(), value);
            }
        }
        return builder;
    }

    public static byte[] getBytes(final Entity entity, final String fieldName) {
        if(entity == null) {
            return null;
        }
        final Value value = entity.getPropertiesMap().getOrDefault(fieldName, null);
        if(value == null) {
            return null;
        }
        switch (value.getValueTypeCase()) {
            case STRING_VALUE:
                return Base64.getDecoder().decode(value.getStringValue());
            case BLOB_VALUE:
                return value.getBlobValue().toByteArray();
            case NULL_VALUE:
            default:
                return null;
        }
    }

    public static Instant getTimestamp(final Entity entity, final String fieldName) {
        return getTimestamp(entity, fieldName, Instant.ofEpochSecond(0L));
    }

    public static Timestamp toProtoTimestamp(final Instant instant) {
        if(instant == null) {
            return null;
        }
        final java.time.Instant jinstant = java.time.Instant.ofEpochMilli(instant.getMillis());
        return Timestamp.newBuilder().setSeconds(jinstant.getEpochSecond()).setNanos(jinstant.getNano()).build();
    }

    public static Instant getTimestamp(final Entity entity, final String fieldName, final Instant timestampDefault) {
        final Value value = entity.getPropertiesMap().get(fieldName);
        if(value == null) {
            return timestampDefault;
        }
        switch (value.getValueTypeCase()) {
            case STRING_VALUE: {
                final String stringValue = value.getStringValue();
                try {
                    final java.time.Instant instant = DateTimeUtil.toInstant(stringValue);
                    if(instant == null) {
                        return timestampDefault;
                    }
                    return DateTimeUtil.toJodaInstant(instant);
                } catch (Exception e) {
                    return timestampDefault;
                }
            }
            case INTEGER_VALUE: {
                try {
                    return Instant.ofEpochMilli(value.getIntegerValue());
                } catch (Exception e){
                    return Instant.ofEpochMilli(value.getIntegerValue() / 1000);
                }
            }
            case TIMESTAMP_VALUE: {
                return Instant.ofEpochMilli(Timestamps.toMillis(value.getTimestampValue()));
            }
            case KEY_VALUE:
            case BOOLEAN_VALUE:
            case DOUBLE_VALUE:
            case BLOB_VALUE:
            case GEO_POINT_VALUE:
            case ENTITY_VALUE:
            case ARRAY_VALUE:
            case NULL_VALUE:
            case VALUETYPE_NOT_SET:
            default:
                return timestampDefault;
        }
    }

    public static Schema convertSchema(final String prefix, final List<Entity> entities) {
        final Schema.Builder builder = Schema.builder();
        final Map<String, List<Entity>> embeddedFields = new HashMap<>();
        for (final Entity entity : entities) {
            String fieldName = entity
                    .getPropertiesOrThrow("property_name")
                    .getStringValue();
            if(prefix.length() > 0 && fieldName.startsWith(prefix)) {
                fieldName = fieldName.replaceFirst(prefix + "\\.", "");
            }

            if(fieldName.contains(".")) {
                final String embeddedFieldName = fieldName.split("\\.", 2)[0];
                if(!embeddedFields.containsKey(embeddedFieldName)) {
                    embeddedFields.put(embeddedFieldName, new ArrayList<>());
                }
                embeddedFields.get(embeddedFieldName).add(entity);
                continue;
            }

            final String fieldType = entity.getPropertiesOrThrow("property_type").getStringValue();
            if("EmbeddedEntity".equals(fieldType) || "NULL".equals(fieldType)) {
                continue;
            }

            builder.addField(Schema.Field.of(
                    fieldName,
                    convertFieldType(fieldType))
                    .withNullable(true));
        }

        if(embeddedFields.size() > 0) {
            for(final Map.Entry<String, List<Entity>> child : embeddedFields.entrySet()) {
                final String childPrefix = (prefix.length() == 0 ? "" : prefix + ".") + child.getKey();
                final Schema childSchema = convertSchema(childPrefix, child.getValue());
                builder.addField(Schema.Field.of(
                        child.getKey(),
                        Schema.FieldType.row(childSchema).withNullable(true)));
            }
        }

        return builder.build();
    }

    private static Schema.FieldType convertFieldType(final String type) {
        switch (type) {
            case "Blob":
            case "ShortBlobg":
                return Schema.FieldType.BYTES.withNullable(true);
            case "IM":
            case "Link":
            case "Email":
            case "User":
            case "PhoneNumber":
            case "PostalAddress":
            case "Category":
            case "Text":
            case "String":
                return Schema.FieldType.STRING;
            case "Rating":
            case "Integer":
                return Schema.FieldType.INT64;
            case "Float":
                return Schema.FieldType.DOUBLE;
            case "Boolean":
                return Schema.FieldType.BOOLEAN;
            case "Date/Time":
                return Schema.FieldType.DATETIME;
            case "EmbeddedEntity":
                //return Schema.FieldType.row(convertSchema(type));
            case "Key":
                //return Schema.FieldType.array(convertFieldType(type.getArrayElementType()));
            case "NULL":
                return Schema.FieldType.STRING;
            default:
                throw new IllegalArgumentException("Spanner type: " + type + " not supported!");
        }
    }

    private static String toStringKey(Key key) {
        final List<String> names = new ArrayList<>();
        for(final Key.PathElement path : key.getPathList()) {
            if(path.getName() == null) {
                names.add(path.getKind() + "," + path.getId());
            } else {
                names.add(path.getKind() + "," + path.getName());
            }
        }
        return String.join(",", names);
    }

}
