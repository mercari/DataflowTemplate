package com.mercari.solution.util.schema;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Value;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.protobuf.ByteString;
import com.mercari.solution.util.DateTimeUtil;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.*;
import org.apache.avro.io.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.*;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class AvroSchemaUtil {

    private static final Logger LOG = LoggerFactory.getLogger(AvroSchemaUtil.class);

    private enum TableRowFieldType {
        STRING,
        BYTES,
        INT64,
        INTEGER,
        FLOAT64,
        FLOAT,
        NUMERIC,
        BOOLEAN,
        BOOL,
        DATE,
        TIME,
        DATETIME,
        TIMESTAMP,
        GEOGRAPHY,
        STRUCT,
        RECORD
    }

    private enum TableRowFieldMode {
        REQUIRED,
        NULLABLE,
        REPEATED
    }

    public static final Schema REQUIRED_STRING = Schema.create(Schema.Type.STRING);
    public static final Schema REQUIRED_BYTES = Schema.create(Schema.Type.BYTES);
    public static final Schema REQUIRED_BOOLEAN = Schema.create(Schema.Type.BOOLEAN);
    public static final Schema REQUIRED_INT = Schema.create(Schema.Type.INT);
    public static final Schema REQUIRED_LONG = Schema.create(Schema.Type.LONG);
    public static final Schema REQUIRED_FLOAT = Schema.create(Schema.Type.FLOAT);
    public static final Schema REQUIRED_DOUBLE = Schema.create(Schema.Type.DOUBLE);

    public static final Schema REQUIRED_LOGICAL_DATE_TYPE = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
    public static final Schema REQUIRED_LOGICAL_TIME_MILLI_TYPE = LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT));
    public static final Schema REQUIRED_LOGICAL_TIME_MICRO_TYPE = LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG));
    public static final Schema REQUIRED_LOGICAL_TIMESTAMP_MILLI_TYPE = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
    public static final Schema REQUIRED_LOGICAL_TIMESTAMP_MICRO_TYPE = LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
    public static final Schema REQUIRED_LOGICAL_DECIMAL_TYPE = LogicalTypes.decimal(38, 9).addToSchema(Schema.create(Schema.Type.BYTES));
    public static final Schema REQUIRED_SQL_DATETIME_TYPE = SchemaBuilder.builder().stringBuilder().prop("sqlType", "DATETIME").endString();
    public static final Schema REQUIRED_SQL_GEOGRAPHY_TYPE = SchemaBuilder.builder().stringBuilder().prop("sqlType", "GEOGRAPHY").endString();
    public static final Schema REQUIRED_ARRAY_DOUBLE_TYPE = Schema.createArray(Schema.create(Schema.Type.DOUBLE));

    public static final Schema NULLABLE_STRING = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING));
    public static final Schema NULLABLE_BYTES = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.BYTES));
    public static final Schema NULLABLE_BOOLEAN = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.BOOLEAN));
    public static final Schema NULLABLE_INT = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.INT));
    public static final Schema NULLABLE_LONG = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.LONG));
    public static final Schema NULLABLE_FLOAT = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.FLOAT));
    public static final Schema NULLABLE_DOUBLE = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.DOUBLE));

    public static final Schema NULLABLE_LOGICAL_DATE_TYPE = Schema.createUnion(Schema.create(Schema.Type.NULL), LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT)));
    public static final Schema NULLABLE_LOGICAL_TIME_MILLI_TYPE = Schema.createUnion(Schema.create(Schema.Type.NULL), LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT)));
    public static final Schema NULLABLE_LOGICAL_TIME_MICRO_TYPE = Schema.createUnion(Schema.create(Schema.Type.NULL), LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG)));
    public static final Schema NULLABLE_LOGICAL_TIMESTAMP_MILLI_TYPE = Schema.createUnion(Schema.create(Schema.Type.NULL), LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG)));
    public static final Schema NULLABLE_LOGICAL_TIMESTAMP_MICRO_TYPE = Schema.createUnion(Schema.create(Schema.Type.NULL), LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG)));
    public static final Schema NULLABLE_LOGICAL_DECIMAL_TYPE = Schema.createUnion(Schema.create(Schema.Type.NULL), LogicalTypes.decimal(38, 9).addToSchema(Schema.create(Schema.Type.BYTES)));
    public static final Schema NULLABLE_SQL_DATETIME_TYPE = SchemaBuilder.unionOf()
            .stringBuilder().prop("sqlType", "DATETIME").endString().and()
            .nullType()
            .endUnion();
    public static final Schema NULLABLE_SQL_GEOGRAPHY_TYPE = SchemaBuilder.unionOf()
            .stringBuilder().prop("sqlType", "GEOGRAPHY").endString().and()
            .nullType()
            .endUnion();
    public static final Schema NULLABLE_ARRAY_DOUBLE_TYPE = Schema.createUnion(
            Schema.createArray(Schema.create(Schema.Type.DOUBLE)),
            Schema.create(Schema.Type.NULL));
    public static final Schema NULLABLE_ARRAY_STRING_TYPE = Schema.createUnion(
            Schema.createArray(Schema.create(Schema.Type.STRING)),
            Schema.create(Schema.Type.NULL));

    public static final Schema NULLABLE_MAP_STRING = Schema.createUnion(
            Schema.create(Schema.Type.NULL),
            Schema.createMap(
                    Schema.createUnion(
                            Schema.create(Schema.Type.NULL),
                            Schema.create(Schema.Type.STRING))));

    private static final Pattern PATTERN_FIELD_NAME = Pattern.compile("^[A-Za-z_][A-Za-z0-9_]*$");

    /**
     * Convert BigQuery {@link TableSchema} object to Avro {@link Schema} object.
     *
     * @param tableSchema BigQuery TableSchema object.
     * @return Avro Schema object.
     */
    public static Schema convertSchema(TableSchema tableSchema) {
        return convertSchema(tableSchema, null);
    }

    public static Schema convertSchema(TableSchema tableSchema, final Collection<String> includedFields) {
        final SchemaBuilder.FieldAssembler<Schema> schemaFields = SchemaBuilder.record("Root").fields();
        for(final TableFieldSchema fieldSchema : tableSchema.getFields()) {
            if(includedFields != null && !includedFields.contains(fieldSchema.getName())) {
                continue;
            }
            schemaFields.name(fieldSchema.getName()).type(convertSchema(fieldSchema)).noDefault();
        }
        return schemaFields.endRecord();
    }

    public static Schema convertSchema(Entity entity) {
        final SchemaBuilder.FieldAssembler<Schema> schemaFields = SchemaBuilder.record("root").fields();
        for(final Map.Entry<String, Value> entry : entity.getPropertiesMap().entrySet()) {
            schemaFields.name(entry.getKey()).type(convertSchema(entry.getKey(), entry.getValue().getValueTypeCase(), entry.getValue())).noDefault();
        }
        return schemaFields.endRecord();
    }

    public static Schema convertSchema(final JsonArray sobjectDescribeFields, final List<String> fieldNames) {
        final SchemaBuilder.FieldAssembler<Schema> schemaFields = SchemaBuilder.record("root").fields();
        sobjectDescribeFields.forEach(element -> {
            final JsonObject field = element.getAsJsonObject();
            if(fieldNames == null || fieldNames.contains(field.get("name").getAsString())) {
                schemaFields
                        .name(field.get("name").getAsString())
                        .type(convertSchema(field))
                        .noDefault();
            }
        });
        return schemaFields.endRecord();
    }

    public static Schema convertSchema(final String jsonAvroSchema) {
        return new Schema.Parser().parse(jsonAvroSchema);
    }


    /**
     * Extract logical type decimal from {@link Schema}.
     *
     * @param schema Avro Schema object.
     * @return true if schema is logical decimal type.
     */
    public static LogicalTypes.Decimal getLogicalTypeDecimal(Schema schema) {
        if(schema.getType().equals(Schema.Type.UNION)) {
            Schema childSchema = unnestUnion(schema);
            return getLogicalTypeDecimal(childSchema);
        }
        final int precision = schema.getObjectProp("precision") != null ?
                Integer.valueOf(schema.getObjectProp("precision").toString()) : 0;
        final int scale = schema.getObjectProp("scale") != null ?
                Integer.valueOf(schema.getObjectProp("scale").toString()) : 0;
        return LogicalTypes.decimal(precision, scale);
    }

    /**
     * Check Avro {@link Schema} is logical type or not.
     *
     * @param schema Avro Schema object.
     * @return true if schema is logical type.
     */
    public static boolean isLogicalTypeDecimal(Schema schema) {
        if(schema.getType().equals(Schema.Type.UNION)) {
            Schema childSchema = unnestUnion(schema);
            return isLogicalTypeDecimal(childSchema);
        }
        final int precision = schema.getObjectProp("precision") != null ?
                Integer.valueOf(schema.getObjectProp("precision").toString()) : 0;
        final int scale = schema.getObjectProp("scale") != null ?
                Integer.valueOf(schema.getObjectProp("scale").toString()) : 0;
        return LogicalTypes.decimal(precision, scale).equals(schema.getLogicalType());
    }

    /**
     * Check Avro {@link Schema} is sql datetime type or not.
     *
     * @param schema Avro Schema object.
     * @return true if schema is sql datetime type.
     */
    public static boolean isSqlTypeDatetime(Schema schema) {
        if(schema.getType().equals(Schema.Type.UNION)) {
            Schema childSchema = unnestUnion(schema);
            return isSqlTypeDatetime(childSchema);
        }
        final String sqlType = schema.getProp("sqlType");
        return "DATETIME".equals(sqlType);
    }

    /**
     * Check Avro {@link Schema} is sql geography type or not.
     *
     * @param schema Avro Schema object.
     * @return true if schema is sql geography type.
     */
    public static boolean isSqlTypeGeography(Schema schema) {
        if(schema.getType().equals(Schema.Type.UNION)) {
            Schema childSchema = unnestUnion(schema);
            return isSqlTypeGeography(childSchema);
        }
        final String sqlType = schema.getProp("sqlType");
        return "GEOGRAPHY".equals(sqlType);
    }

    /**
     * Extract child Avro schema from nullable union Avro {@link Schema}.
     *
     * @param schema Avro Schema object.
     * @return Child Avro schema or input schema if not union schema.
     */
    public static Schema unnestUnion(Schema schema) {
        if(schema.getType().equals(Schema.Type.UNION)) {
            return schema.getTypes().stream()
                    .filter(s -> !s.getType().equals(Schema.Type.NULL))
                    .findAny()
                    .orElseThrow(() -> new IllegalArgumentException("UNION does not have another schema."));
        }
        return schema;
    }

    public static GenericRecordBuilder copy(final GenericRecord record, Schema targetSchema) {
        final Schema sourceSchema = record.getSchema();
        final GenericRecordBuilder builder = new GenericRecordBuilder(targetSchema);
        for(final Schema.Field field : targetSchema.getFields()) {
            if(sourceSchema.getField(field.name()) == null) {
                continue;
            }
            builder.set(field.name(), record.get(field.name()));
        }
        return builder;
    }

    public static SchemaBuilder.FieldAssembler<Schema> toBuilder(final Schema schema) {
        return toBuilder(schema, schema.getNamespace(), null);
    }

    public static SchemaBuilder.FieldAssembler<Schema> toBuilder(final Schema schema, final Collection<String> fieldNames) {
        return toBuilder(schema, schema.getNamespace(), fieldNames);
    }

    public static SchemaBuilder.FieldAssembler<Schema> toBuilder(final Schema schema,  final String namespace, final Collection<String> fieldNames) {
        final SchemaBuilder.FieldAssembler<Schema> schemaFields = SchemaBuilder.record("root").namespace(namespace).fields();
        schema.getFields().forEach(f -> {
            if(fieldNames == null || fieldNames.contains(f.name())) {
                schemaFields.name(f.name()).type(f.schema()).noDefault();
            }
        });
        return schemaFields;
    }

    public static GenericRecordBuilder toBuilder(final GenericRecord record) {
        return toBuilder(record.getSchema(), record);
    }

    public static GenericRecordBuilder toBuilder(final Schema schema, final GenericRecord record) {
        return toBuilder(schema, record, new HashMap<>());
    }

    public static GenericRecordBuilder toBuilder(final Schema schema, final GenericRecord record, final Map<String, String> renameFields) {
        final GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        for(final Schema.Field field : schema.getFields()) {
            final Schema.Field recordField = record.getSchema().getField(field.name());
            if(recordField != null) {
                final Schema fieldSchema = unnestUnion(field.schema());
                final Object fieldValue = record.get(field.name());
                if(fieldValue == null) {
                    builder.set(field.name(), null);
                    continue;
                }
                if(!unnestUnion(recordField.schema()).getType().equals(fieldSchema.getType())) {
                    builder.set(field.name(), null);
                    continue;
                }

                switch (fieldSchema.getType()) {
                    case ARRAY: {
                        final Schema elementSchema = unnestUnion(fieldSchema.getElementType());
                        if(elementSchema.getType().equals(Schema.Type.RECORD)) {
                            final List<GenericRecord> children = new ArrayList<>();
                            for(final GenericRecord child : (List<GenericRecord>)fieldValue) {
                                if(child == null) {
                                    children.add(null);
                                } else {
                                    children.add(toBuilder(elementSchema, child).build());
                                }
                            }
                            builder.set(field.name(), children);
                        } else {
                            builder.set(field.name(), fieldValue);
                        }
                        break;
                    }
                    case RECORD: {
                        final GenericRecord child = toBuilder(fieldSchema, (GenericRecord)fieldValue).build();
                        builder.set(field.name(), child);
                        break;
                    }
                    default:
                        builder.set(field.name(), fieldValue);
                        break;
                }
            } else if(renameFields.containsKey(field.name())) {
                final String oldFieldName = renameFields.get(field.name());
                builder.set(field, record.get(oldFieldName));
            } else {
                if(isNullable(field.schema())) {
                    builder.set(field, null);
                }
            }
        }
        return builder;
    }

    public static SchemaBuilder.RecordBuilder<Schema> toSchemaBuilder(
            final Schema schema,
            final Collection<String> includeFields,
            final Collection<String> excludeFields) {

        SchemaBuilder.RecordBuilder<Schema> builder = SchemaBuilder.record(schema.getName());
        final SchemaBuilder.FieldAssembler<Schema> schemaFields = builder.fields();
        for(final Schema.Field field : schema.getFields()) {
            if(includeFields != null && !includeFields.contains(field.name())) {
                continue;
            }
            if(excludeFields != null && excludeFields.contains(field.name())) {
                continue;
            }
            schemaFields.name(field.name()).type(field.schema()).noDefault();
        }
        return builder;
    }

    public static Schema renameFields(final Schema schema, final Map<String, String> renameFields) {

        SchemaBuilder.RecordBuilder<Schema> builder = SchemaBuilder.record(schema.getName());
        final SchemaBuilder.FieldAssembler<Schema> schemaFields = builder.fields();
        for(final Schema.Field field : schema.getFields()) {
            if(renameFields.containsKey(field.name())) {
                schemaFields.name(renameFields.get(field.name())).type(field.schema()).noDefault();
            } else {
                schemaFields.name(field.name()).type(field.schema()).noDefault();
            }
        }
        return schemaFields.endRecord();
    }

    public static GenericRecord merge(final Schema schema, final GenericRecord record, final Map<String, ? extends Object> values) {
        final GenericRecordBuilder builder = toBuilder(schema, record);
        for(var entry : values.entrySet()) {
            builder.set(entry.getKey(), entry.getValue());
        }
        return builder.build();
    }

    public static Schema selectFields(final Schema schema, final List<String> fields) {
        return selectFieldsBuilder(schema, fields, "root").endRecord();
    }

    public static SchemaBuilder.FieldAssembler<Schema> selectFieldsBuilder(final Schema schema, final List<String> fields, final String name) {
        final SchemaBuilder.FieldAssembler<Schema> builder = SchemaBuilder.builder().record(name).fields();
        final Map<String, List<String>> childFields = new HashMap<>();
        for(String field : fields) {
            if(field.contains(".")) {
                final String[] strs = field.split("\\.", 2);
                if(childFields.containsKey(strs[0])) {
                    childFields.get(strs[0]).add(strs[1]);
                } else {
                    childFields.put(strs[0], new ArrayList<>(Arrays.asList(strs[1])));
                }
            } else if(schema.getField(field) != null) {
                builder.name(field).type(schema.getField(field).schema()).noDefault();
            } else {
                throw new IllegalStateException("Field not found: " + field);
            }
        }

        if(childFields.size() > 0) {
            for(var entry : childFields.entrySet()) {
                final Schema.Field childField = schema.getField(entry.getKey());
                Schema unnestedChildSchema = unnestUnion(childField.schema());
                final String childName = name + "." + entry.getKey();
                switch (unnestedChildSchema.getType()) {
                    case RECORD: {
                        final Schema selectedChildSchema = selectFieldsBuilder(unnestedChildSchema, entry.getValue(), childName).endRecord();
                        builder.name(entry.getKey()).type(toNullable(selectedChildSchema)).noDefault();
                        break;
                    }
                    case ARRAY: {
                        if(!unnestUnion(unnestedChildSchema.getElementType()).getType().equals(Schema.Type.RECORD)) {
                            throw new IllegalStateException();
                        }
                        final Schema childSchema = selectFieldsBuilder(unnestUnion(unnestedChildSchema.getElementType()), entry.getValue(), childName).endRecord();
                        builder.name(entry.getKey()).type(toNullable(Schema.createArray(toNullable(childSchema)))).noDefault();
                        break;
                    }
                    default:
                        throw new IllegalStateException();
                }
            }
        }

        return builder;
    }

    public static Schema createMapRecordSchema(final String name, final Schema keySchema, final Schema valueSchema) {

        final Schema.Field keyField = new Schema.Field("key", keySchema, null, (Object)null);
        final Schema.Field valueField = new Schema.Field("value", valueSchema, null, (Object)null);
        return Schema.createRecord(name, null, null, false, List.of(keyField, valueField));
    }

    public static Schema createMapRecordsSchema(final String name, final Schema keySchema, final Schema valueSchema) {
        return SchemaBuilder.builder().array().items(
                SchemaBuilder.builder().record("map")
                        .fields()
                        .name("key").type(Schema.create(Schema.Type.STRING)).noDefault()
                        .endRecord());
    }

    public static Object getValue(final GenericRecord record, final String fieldName) {
        if(record == null) {
            return null;
        }
        Schema.Field field = record.getSchema().getField(fieldName);
        if(field == null) {
            return null;
        }

        final Object value = record.get(fieldName);
        if(value == null) {
            return null;
        }
        final Schema fieldSchema = unnestUnion(field.schema());
        switch(fieldSchema.getType()) {
            case BOOLEAN:
            case FLOAT:
            case DOUBLE:
                return value;
            case ENUM:
            case STRING:
                return value.toString();
            case BYTES:
            case FIXED:
                final byte[] bytes = ((ByteBuffer)value).array();
                return Arrays.copyOf(bytes, bytes.length);
            case INT: {
                final Integer intValue = (Integer) value;
                if(LogicalTypes.date().equals(fieldSchema.getLogicalType())) {
                    return LocalDate.ofEpochDay(intValue.longValue());
                } else if(LogicalTypes.timeMillis().equals(fieldSchema.getLogicalType())) {
                    return LocalTime.ofSecondOfDay(intValue * 1000);
                }
                return intValue;
            }
            case LONG: {
                final Long longValue = (Long) value;
                if(LogicalTypes.timestampMillis().equals(fieldSchema.getLogicalType())) {
                    return Instant.ofEpochMilli(longValue);
                } else if(LogicalTypes.timestampMicros().equals(fieldSchema.getLogicalType())) {
                    return Instant.ofEpochMilli(longValue * 1000);
                } else if(LogicalTypes.timeMicros().equals(fieldSchema.getLogicalType())) {
                    return LocalTime.ofSecondOfDay(longValue * 1000_000);
                }
                return longValue;
            }
            case RECORD:
            case MAP:
                return value;
            case ARRAY:
                return ((List<Object>)value).stream()
                        .map(v -> {
                            if(v == null) {
                                return null;
                            }
                            final Schema arraySchema = unnestUnion(fieldSchema.getElementType());
                            switch (arraySchema.getType()) {
                                case BOOLEAN:
                                case FLOAT:
                                case DOUBLE:
                                    return v;
                                case ENUM:
                                case STRING:
                                    return v.toString();
                                case FIXED:
                                case BYTES:
                                    final byte[] b = ((ByteBuffer)v).array();
                                    return Arrays.copyOf(b, b.length);
                                case INT: {
                                    final Integer intValue = (Integer) v;
                                    if(LogicalTypes.date().equals(arraySchema.getLogicalType())) {
                                        return LocalDate.ofEpochDay(intValue.longValue());
                                    } else if(LogicalTypes.timeMillis().equals(arraySchema.getLogicalType())) {
                                        return LocalTime.ofSecondOfDay(intValue * 1000);
                                    }
                                    return intValue;
                                }
                                case LONG: {
                                    final Long longValue = (Long) v;
                                    if(LogicalTypes.timestampMillis().equals(arraySchema.getLogicalType())) {
                                        return Instant.ofEpochMilli(longValue);
                                    } else if(LogicalTypes.timestampMicros().equals(arraySchema.getLogicalType())) {
                                        return Instant.ofEpochMilli(longValue * 1000);
                                    } else if(LogicalTypes.timeMicros().equals(arraySchema.getLogicalType())) {
                                        return LocalTime.ofSecondOfDay(longValue * 1000_000);
                                    }
                                    return longValue;
                                }
                                case MAP:
                                case RECORD:
                                    return v;
                                case ARRAY:
                                case NULL:
                                case UNION:
                                default:
                                    return null;
                            }
                        })
                        .collect(Collectors.toList());
            case UNION:
            case NULL:
            default:
                return null;
        }
    }

    public static byte[] getBytes(final GenericRecord record, final String fieldName) {
        final Schema.Field field = record.getSchema().getField(fieldName);
        if(field == null) {
            return null;
        }
        final Object value = record.get(fieldName);
        if(value == null) {
            return null;
        }
        final Schema schema = unnestUnion(field.schema());
        switch (schema.getType()) {
            case BYTES:
                return ((ByteBuffer)value).array();
            default:
                return null;
        }
    }

    public static org.joda.time.Instant getTimestamp(final GenericRecord record, final String fieldName) {
        return getTimestamp(record, fieldName, Instant.ofEpochSecond(0L));
    }

    public static org.joda.time.Instant getTimestamp(final GenericRecord record, final String fieldName, final Instant defaultTimestamp) {
        final Schema.Field field = record.getSchema().getField(fieldName);
        if(field == null) {
            return defaultTimestamp;
        }
        final Object value = record.get(fieldName);
        if(value == null) {
            return defaultTimestamp;
        }
        final Schema schema = unnestUnion(field.schema());
        switch (schema.getType()) {
            case ENUM:
            case STRING: {
                final String stringValue = value.toString().trim();
                try {
                    java.time.Instant instant = DateTimeUtil.toInstant(stringValue);
                    return DateTimeUtil.toJodaInstant(instant);
                } catch (Exception e) {
                    return defaultTimestamp;
                }
            }
            case INT: {
                if (LogicalTypes.date().equals(schema.getLogicalType())) {
                    final LocalDate localDate = LocalDate.ofEpochDay((int) value);
                    return new DateTime(localDate.getYear(), localDate.getMonthValue(), localDate.getDayOfMonth(),
                            0, 0, DateTimeZone.UTC).toInstant();
                }
                return defaultTimestamp;
            }
            case LONG: {
                final Long longValue = (Long)value;
                if (LogicalTypes.timestampMillis().equals(schema.getLogicalType())) {
                    return Instant.ofEpochMilli(longValue);
                } else if (LogicalTypes.timestampMicros().equals(schema.getLogicalType())) {
                    return Instant.ofEpochMilli(longValue / 1000);
                }
                return defaultTimestamp;
            }
            case FIXED:
            case BYTES:
            case BOOLEAN:
            case FLOAT:
            case DOUBLE:
            case UNION:
            case MAP:
            case RECORD:
            case ARRAY:
            case NULL:
            default:
                return defaultTimestamp;
        }
    }

    public static String getAsString(final GenericRecord record, final String fieldName) {
        final Object value = record.get(fieldName);
        if(value == null) {
            return null;
        }
        final Schema.Field field = record.getSchema().getField(fieldName);
        if(field == null) {
            return null;
        }

        final Schema fieldSchema = unnestUnion(field.schema());
        switch (fieldSchema.getType()) {
            case BOOLEAN:
            case FLOAT:
            case DOUBLE:
            case ENUM:
            case STRING:
                return value.toString();
            case FIXED:
            case BYTES:
                return Base64.getEncoder().encodeToString(((ByteBuffer) value).array());
            case INT: {
                if (LogicalTypes.date().equals(fieldSchema.getLogicalType())) {
                    return LocalDate.ofEpochDay((int) value).format(DateTimeFormatter.ISO_LOCAL_DATE);
                } else if (LogicalTypes.timeMillis().equals(fieldSchema.getLogicalType())) {
                    return LocalTime.ofNanoOfDay(new Long((Integer) value) * 1000 * 1000).format(DateTimeFormatter.ISO_LOCAL_TIME);
                } else {
                    return value.toString();
                }
            }
            case LONG: {
                final Long longValue = (Long) value;
                if (LogicalTypes.timestampMillis().equals(fieldSchema.getLogicalType())) {
                    return DateTimeFormatter.ISO_INSTANT.format(java.time.Instant.ofEpochMilli(longValue));
                } else if (LogicalTypes.timestampMicros().equals(fieldSchema.getLogicalType())) {
                    return DateTimeFormatter.ISO_INSTANT.format(java.time.Instant.ofEpochMilli(longValue / 1000));
                } else if (LogicalTypes.timeMicros().equals(fieldSchema.getLogicalType())) {
                    return LocalTime.ofNanoOfDay(longValue * 1000).format(DateTimeFormatter.ISO_LOCAL_TIME);
                } else {
                    return longValue.toString();
                }
            }
            case RECORD:
            case MAP:
            case ARRAY:
                return value.toString();
            case UNION:
            case NULL:
            default:
                return null;
        }
    }

    public static Long getAsLong(final GenericRecord record, final String fieldName) {
        final Object value = record.get(fieldName);
        if(value == null) {
            return null;
        }
        final Schema.Field field = record.getSchema().getField(fieldName);
        if(field == null) {
            return null;
        }

        final Schema fieldSchema = unnestUnion(field.schema());
        switch (fieldSchema.getType()) {
            case BOOLEAN:
                return (Boolean) value ? 1L : 0L;
            case FLOAT:
                return ((Float) value).longValue();
            case DOUBLE:
                return ((Double) value).longValue();
            case ENUM:
            case STRING: {
                try {
                    return Long.valueOf(value.toString());
                } catch (Exception e) {
                    return null;
                }
            }
            case FIXED:
            case BYTES: {
                final ByteBuffer byteBuffer = (ByteBuffer) value;
                if(isLogicalTypeDecimal(fieldSchema)) {
                    final int scale = fieldSchema.getObjectProp("scale") != null ?
                            Integer.valueOf(fieldSchema.getObjectProp("scale").toString()) : 0;
                    return BigDecimal.valueOf(new BigInteger(byteBuffer.array()).longValue(), scale).longValue();
                }
                return null;
            }
            case INT:
                return ((Integer) value).longValue();
            case LONG:
                return (Long) value;
            case RECORD:
            case MAP:
            case ARRAY:
            case UNION:
            case NULL:
            default:
                return null;
        }
    }

    public static Double getAsDouble(final GenericRecord record, final String fieldName) {
        final Object value = record.get(fieldName);
        if(value == null) {
            return null;
        }
        final Schema.Field field = record.getSchema().getField(fieldName);
        if(field == null) {
            return null;
        }

        final Schema fieldSchema = unnestUnion(field.schema());
        switch (fieldSchema.getType()) {
            case BOOLEAN:
                return (Boolean) value ? 1D : 0D;
            case FLOAT:
                return ((Float) value).doubleValue();
            case DOUBLE:
                return ((Double) value);
            case ENUM:
            case STRING: {
                try {
                    return Double.valueOf(value.toString());
                } catch (Exception e) {
                    return null;
                }
            }
            case FIXED:
            case BYTES: {
                final ByteBuffer byteBuffer = (ByteBuffer) value;
                if(isLogicalTypeDecimal(fieldSchema)) {
                    final int scale = fieldSchema.getObjectProp("scale") != null ?
                            Integer.valueOf(fieldSchema.getObjectProp("scale").toString()) : 0;
                    return BigDecimal.valueOf(new BigInteger(byteBuffer.array()).longValue(), scale).doubleValue();
                }
                return null;
            }
            case INT:
                return ((Integer) value).doubleValue();
            case LONG:
                return ((Long) value).doubleValue();
            case RECORD:
            case MAP:
            case ARRAY:
            case UNION:
            case NULL:
            default:
                return null;
        }
    }

    public static BigDecimal getAsBigDecimal(final GenericRecord record, final String fieldName) {
        final Object value = record.get(fieldName);
        if(value == null) {
            return null;
        }
        final Schema.Field field = record.getSchema().getField(fieldName);
        if(field == null) {
            return null;
        }

        final Schema fieldSchema = unnestUnion(field.schema());
        switch (fieldSchema.getType()) {
            case BOOLEAN:
                return BigDecimal.valueOf((Boolean) value ? 1D : 0D);
            case FLOAT:
                return BigDecimal.valueOf(((Float) value).doubleValue());
            case DOUBLE:
                return BigDecimal.valueOf((Double) value);
            case ENUM:
            case STRING: {
                try {
                    return BigDecimal.valueOf(Double.valueOf(value.toString()));
                } catch (Exception e) {
                    return null;
                }
            }
            case FIXED:
            case BYTES: {
                final ByteBuffer byteBuffer = (ByteBuffer) value;
                if(isLogicalTypeDecimal(fieldSchema)) {
                    final int scale = fieldSchema.getObjectProp("scale") != null ?
                            Integer.valueOf(fieldSchema.getObjectProp("scale").toString()) : 0;
                    return BigDecimal.valueOf(new BigInteger(byteBuffer.array()).longValue(), scale);
                }
                return null;
            }
            case INT:
                return BigDecimal.valueOf(((Integer) value).longValue());
            case LONG:
                return BigDecimal.valueOf((Long) value);
            case RECORD:
            case MAP:
            case ARRAY:
            case UNION:
            case NULL:
            default:
                return null;
        }
    }

    // for bigtable
    public static ByteString getAsByteString(final GenericRecord record, final String fieldName) {
        if(record == null || fieldName == null) {
            return null;
        }
        final Object value = record.get(fieldName);
        if(value == null) {
            return null;
        }
        final Schema.Field field = record.getSchema().getField(fieldName);
        if(field == null) {
            return null;
        }

        final Schema fieldSchema = unnestUnion(field.schema());
        final byte[] bytes;
        switch (fieldSchema.getType()) {
            case BOOLEAN:
                bytes = Bytes.toBytes((boolean)value);
                break;
            case FIXED:
            case BYTES:
                bytes = ((ByteBuffer)value).array();
                break;
            case ENUM:
            case STRING:
                bytes = Bytes.toBytes(value.toString());
                break;
            case INT:
                bytes = Bytes.toBytes((Integer)value);
                break;
            case LONG:
                bytes = Bytes.toBytes((Long) value);
                break;
            case FLOAT:
                bytes = Bytes.toBytes((Float)value);
                break;
            case DOUBLE:
                bytes = Bytes.toBytes((Double)value);
                break;
            case ARRAY: {
                final List values = (List)value;
                final Schema elementSchema = unnestUnion(fieldSchema.getElementType());
                final Writable[] array = new Writable[values.size()];
                for(int i=0; i<values.size(); i++) {
                    switch (elementSchema.getType()) {
                        case BOOLEAN:
                            array[i] = new BooleanWritable((Boolean)values.get(i));
                            break;
                        case ENUM:
                        case STRING:
                            array[i] = new Text(values.get(i).toString());
                            break;
                        case FIXED:
                        case BYTES:
                            array[i] = new BytesWritable(((ByteBuffer)values.get(i)).array());
                            break;
                        case INT:
                            array[i] = new IntWritable(((Integer)values.get(i)));
                            break;
                        case LONG:
                            array[i] = new LongWritable(((Long)values.get(i)));
                            break;
                        case FLOAT:
                            array[i] = new FloatWritable(((Float)values.get(i)));
                            break;
                        case DOUBLE:
                            array[i] = new DoubleWritable(((Double)values.get(i)));
                            break;
                        default:
                            break;
                    }
                }
                final ArrayWritable arrayWritable;
                switch (elementSchema.getType()) {
                    case BOOLEAN:
                        arrayWritable = new ArrayWritable(BooleanWritable.class, array);
                        break;
                    case ENUM:
                    case STRING:
                        arrayWritable = new ArrayWritable(Text.class, array);
                        break;
                    case FIXED:
                    case BYTES:
                        arrayWritable = new ArrayWritable(BytesWritable.class, array);
                        break;
                    case INT:
                        arrayWritable = new ArrayWritable(IntWritable.class, array);
                        break;
                    case LONG:
                        arrayWritable = new ArrayWritable(LongWritable.class, array);
                        break;
                    case FLOAT:
                        arrayWritable = new ArrayWritable(FloatWritable.class, array);
                        break;
                    case DOUBLE:
                        arrayWritable = new ArrayWritable(DoubleWritable.class, array);
                        break;
                    case MAP:
                    case NULL:
                    case ARRAY:
                    case UNION:
                    case RECORD:
                    default: {
                        throw new IllegalStateException();
                    }
                }
                bytes = WritableUtils.toByteArray(arrayWritable);
                break;
            }
            case MAP:
            case NULL:
            case RECORD:
            case UNION:
            default:
                bytes = Bytes.toBytes("");
                break;
        }
        return ByteString.copyFrom(bytes);
    }

    public static String convertNumericBytesToString(final byte[] bytes, final int scale) {
        if(bytes == null) {
            return null;
        }
        final BigDecimal bigDecimal = BigDecimal.valueOf(new BigInteger(bytes).longValue(), scale);
        if(scale == 0) {
            return bigDecimal.toPlainString();
        }
        final StringBuilder sb = new StringBuilder(bigDecimal.toPlainString());
        while(sb.lastIndexOf("0") == sb.length() - 1) {
            sb.deleteCharAt(sb.length() - 1);
        }
        if(sb.lastIndexOf(".") == sb.length() - 1) {
            sb.deleteCharAt(sb.length() - 1);
        }
        return sb.toString();
    }

    public static boolean isNullable(final Schema schema) {
        if(schema.getType().equals(Schema.Type.UNION)) {
            return schema.getTypes().stream().map(Schema::getType).anyMatch(Schema.Type.NULL::equals);
        }
        return false;
    }

    public static boolean isNestedSchema(final Schema schema) {
        if(schema == null) {
            return false;
        }
        return schema.getFields().stream()
                .map(f -> unnestUnion(f.schema()))
                .anyMatch(s -> {
                    if(Schema.Type.RECORD.equals(s.getType())) {
                        return true;
                    } else if(Schema.Type.ARRAY.equals(s.getType())) {
                        return Schema.Type.RECORD.equals(unnestUnion(s.getElementType()).getType());
                    }
                    return false;
                });
    }

    public static byte[] encode(final GenericRecord record) throws IOException {
        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(record.getSchema());
        try(final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
            final BinaryEncoder binaryEncoder = EncoderFactory.get().binaryEncoder(byteArrayOutputStream, null);
            datumWriter.write(record, binaryEncoder);
            binaryEncoder.flush();
            return byteArrayOutputStream.toByteArray();
        }
    }

    public static byte[] encode(final Schema schema, final List<GenericRecord> records) throws IOException {
        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        try(final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
            final BinaryEncoder binaryEncoder = EncoderFactory.get().binaryEncoder(byteArrayOutputStream, null);
            for(final GenericRecord record : records) {
                datumWriter.write(record, binaryEncoder);
                binaryEncoder.flush();
            }
            return byteArrayOutputStream.toByteArray();
        }
    }

    public static GenericRecord decode(final Schema schema, final byte[] bytes) throws IOException {
        final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        final BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
        GenericRecord record = new GenericData.Record(schema);
        return datumReader.read(record, decoder);
    }

    public static Schema toNullable(final Schema schema) {
        if(isNullable(schema)) {
            return schema;
        }
        return Schema.createUnion(schema, Schema.create(Schema.Type.NULL));
    }

    public static boolean isValidFieldName(final String name) {
        if(name == null) {
            return false;
        }
        if(name.length() == 0) {
            return false;
        }
        return PATTERN_FIELD_NAME.matcher(name).find();
    }

    private static Schema convertSchema(final TableFieldSchema fieldSchema) {
        return convertSchema(
                fieldSchema,
                TableRowFieldMode.valueOf(fieldSchema.getMode() == null ? TableRowFieldMode.NULLABLE.name() : fieldSchema.getMode()),
                null,
                false);
    }

    private static Schema convertSchema(final TableFieldSchema fieldSchema, final TableRowFieldMode mode, final String parentNamespace, final boolean inArray) {
        if(mode.equals(TableRowFieldMode.REPEATED)) {
            //return Schema.createUnion(
            //        Schema.create(Schema.Type.NULL),
            //        Schema.createArray(convertSchema(fieldSchema, TableRowFieldMode.NULLABLE, parentNamespace)));
            return Schema.createArray(convertSchema(fieldSchema, TableRowFieldMode.REQUIRED, parentNamespace, true));
        }
        switch(TableRowFieldType.valueOf(fieldSchema.getType())) {
            case DATETIME:
                final Schema datetimeSchema = Schema.create(Schema.Type.STRING);
                datetimeSchema.addProp("sqlType", "DATETIME");
                return TableRowFieldMode.NULLABLE.equals(mode) ? Schema.createUnion(Schema.create(Schema.Type.NULL), datetimeSchema) : datetimeSchema;
            case GEOGRAPHY:
                final Schema geoSchema = Schema.create(Schema.Type.STRING);
                geoSchema.addProp("sqlType", "GEOGRAPHY");
                return TableRowFieldMode.NULLABLE.equals(mode) ?
                        Schema.createUnion(Schema.create(Schema.Type.NULL), geoSchema) :
                        geoSchema;
            case STRING: return TableRowFieldMode.NULLABLE.equals(mode) ? NULLABLE_STRING : REQUIRED_STRING;
            case BYTES: return TableRowFieldMode.NULLABLE.equals(mode) ? NULLABLE_BYTES : REQUIRED_BYTES;
            case INT64:
            case INTEGER: return TableRowFieldMode.NULLABLE.equals(mode) ? NULLABLE_LONG : REQUIRED_LONG;
            case FLOAT64:
            case FLOAT: return TableRowFieldMode.NULLABLE.equals(mode) ? NULLABLE_DOUBLE : REQUIRED_DOUBLE;
            case BOOL:
            case BOOLEAN: return TableRowFieldMode.NULLABLE.equals(mode) ? NULLABLE_BOOLEAN : REQUIRED_BOOLEAN;
            case DATE: return TableRowFieldMode.NULLABLE.equals(mode) ? NULLABLE_LOGICAL_DATE_TYPE : REQUIRED_LOGICAL_DATE_TYPE;
            case TIME: return TableRowFieldMode.NULLABLE.equals(mode) ? NULLABLE_LOGICAL_TIME_MICRO_TYPE : REQUIRED_LOGICAL_TIME_MICRO_TYPE;
            case TIMESTAMP: return TableRowFieldMode.NULLABLE.equals(mode) ? NULLABLE_LOGICAL_TIMESTAMP_MICRO_TYPE : REQUIRED_LOGICAL_TIMESTAMP_MICRO_TYPE;
            case NUMERIC: return TableRowFieldMode.NULLABLE.equals(mode) ? NULLABLE_LOGICAL_DECIMAL_TYPE : REQUIRED_LOGICAL_DECIMAL_TYPE;
            case STRUCT:
            case RECORD:
                final String namespace = parentNamespace == null ? "root" : parentNamespace + "." + fieldSchema.getName().toLowerCase();
                final List<Schema.Field> fields = fieldSchema.getFields().stream()
                        .map(f -> new Schema.Field(f.getName(), convertSchema(f, TableRowFieldMode.valueOf(f.getMode()), namespace, false), null, (Object)null))
                        .collect(Collectors.toList());
                final String capitalName = fieldSchema.getName().substring(0, 1).toUpperCase()
                        + fieldSchema.getName().substring(1).toLowerCase();
                if(TableRowFieldMode.NULLABLE.equals(mode)) {
                    return Schema.createUnion(
                            Schema.create(Schema.Type.NULL),
                            Schema.createRecord(capitalName, fieldSchema.getDescription(), namespace, false, fields));
                } else {
                    return Schema.createRecord(inArray ? capitalName : fieldSchema.getName(), fieldSchema.getDescription(), namespace, false, fields);
                }
                //final Schema recordSchema = Schema.createRecord(fieldSchema.getName(), fieldSchema.getDescription(), namespace, false, fields);
                //return TableRowFieldMode.NULLABLE.equals(mode) ? Schema.createUnion(Schema.create(Schema.Type.NULL), recordSchema) : recordSchema;
                //return recordSchema;
            default: throw new IllegalArgumentException();
        }
    }

    private static Schema convertSchema(final String name, final Value.ValueTypeCase valueTypeCase, final Value value) {
        switch(valueTypeCase) {
            case STRING_VALUE: return NULLABLE_STRING;
            case BLOB_VALUE: return NULLABLE_BYTES;
            case INTEGER_VALUE: return NULLABLE_LONG;
            case DOUBLE_VALUE: return NULLABLE_DOUBLE;
            case BOOLEAN_VALUE: return NULLABLE_BOOLEAN;
            case TIMESTAMP_VALUE: return NULLABLE_LOGICAL_TIMESTAMP_MICRO_TYPE;
            case ENTITY_VALUE:
                final List<Schema.Field> fields = value.getEntityValue().getPropertiesMap().entrySet().stream()
                        .map(s -> new Schema.Field(s.getKey(), convertSchema(s.getKey(), s.getValue().getValueTypeCase(), s.getValue()), null, (Object)null, Schema.Field.Order.IGNORE))
                        .collect(Collectors.toList());
                return Schema.createUnion(
                        Schema.create(Schema.Type.NULL),
                        Schema.createRecord(name, null, null, false, fields));
            case ARRAY_VALUE:
                final Value av = value.getArrayValue().getValues(0);
                return Schema.createUnion(
                        Schema.create(Schema.Type.NULL),
                        Schema.createArray(convertSchema(name, av.getValueTypeCase(), av)));
            case VALUETYPE_NOT_SET:
            case NULL_VALUE:
                return convertSchema(name, value.getDefaultInstanceForType().getValueTypeCase(), value);
            default:
                throw new IllegalArgumentException(String.format("%s %s is not supported!", valueTypeCase.name(), name));
        }
    }

    private static Schema convertSchema(final JsonObject jsonObject) {
        final boolean nillable;
        if(!jsonObject.has("nillable") || jsonObject.get("nillable").isJsonNull()) {
            nillable = true;
        } else {
            nillable = jsonObject.get("nillable").getAsBoolean();
        }

        switch(jsonObject.get("type").getAsString().toLowerCase()) {
            case "id": return REQUIRED_STRING;
            case "reference":
            case "string":
            case "base64":
            case "textarea":
            case "url":
            case "phone":
            case "email":
            case "address":
            case "location":
            case "picklist": return nillable ? NULLABLE_STRING : REQUIRED_STRING;
            case "byte": return nillable ? NULLABLE_BYTES : REQUIRED_BYTES;
            case "boolean": return nillable ? NULLABLE_BOOLEAN : REQUIRED_BOOLEAN;
            case "int": return nillable ? NULLABLE_LONG : REQUIRED_LONG;
            case "currency":
            case "percent":
            case "double": return nillable ? NULLABLE_DOUBLE : REQUIRED_DOUBLE;
            case "date": return nillable ? NULLABLE_LOGICAL_DATE_TYPE : REQUIRED_LOGICAL_DATE_TYPE;
            case "time": return nillable ? NULLABLE_LOGICAL_TIME_MILLI_TYPE : REQUIRED_LOGICAL_TIME_MILLI_TYPE;
            case "datetime": return nillable ? NULLABLE_LOGICAL_TIMESTAMP_MICRO_TYPE : REQUIRED_LOGICAL_TIMESTAMP_MICRO_TYPE;
            case "junctionidlist":
            case "multipicklist":
                return nillable ? Schema.createUnion(
                        Schema.create(Schema.Type.NULL),
                        Schema.createArray(REQUIRED_STRING)) : Schema.createArray(REQUIRED_STRING);
            default:
                return nillable ? NULLABLE_STRING : REQUIRED_STRING;
        }
    }

}