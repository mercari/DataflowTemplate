package com.mercari.solution.util.schema;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Value;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.joda.time.DateTimeZone;
import org.joda.time.Days;
import org.joda.time.Instant;
import org.joda.time.MutableDateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.DateTime;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class AvroSchemaUtil {

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

    public static final Schema NULLABLE_MAP_STRING = Schema.createUnion(
            Schema.create(Schema.Type.NULL),
            Schema.createMap(
                    Schema.createUnion(
                            Schema.create(Schema.Type.NULL),
                            Schema.create(Schema.Type.STRING))));

    private static final MutableDateTime EPOCH_DATETIME = new MutableDateTime(0, DateTimeZone.UTC);
    private static final org.joda.time.format.DateTimeFormatter FORMAT_TIME1 = DateTimeFormat.forPattern("HHmm");
    private static final org.joda.time.format.DateTimeFormatter FORMAT_TIME2 = DateTimeFormat.forPattern("HH:mm");
    private static final org.joda.time.format.DateTimeFormatter FORMAT_TIME3 = DateTimeFormat.forPattern("HH:mm:ss");
    private static final Pattern PATTERN_DATE1 = Pattern.compile("[0-9]{8}");
    private static final Pattern PATTERN_DATE2 = Pattern.compile("[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}");
    private static final Pattern PATTERN_DATE3 = Pattern.compile("[0-9]{4}/[0-9]{1,2}/[0-9]{1,2}");
    private static final Pattern PATTERN_TIME1 = Pattern.compile("[0-9]{4}");
    private static final Pattern PATTERN_TIME2 = Pattern.compile("[0-9]{2}:[0-9]{2}");
    private static final Pattern PATTERN_TIME3 = Pattern.compile("[0-9]{4}:[0-9]{2}:[0-9]{2}");

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
        return toBuilder(record, record.getSchema());
    }

    public static GenericRecordBuilder toBuilder(final GenericRecord record, final Schema schema) {
        final GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        for(final Schema.Field field : record.getSchema().getFields()) {
            builder.set(field, record.get(field.name()));
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
                }
                return defaultTimestamp;
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

    public static String getAsString(final GenericRecord record, final String field) {
        if(record.get(field) == null) {
            return null;
        }
        return record.get(field).toString();
    }

    public static Date convertEpochDaysToGDate(final Integer epochDays) {
        if(epochDays == null) {
            return null;
        }
        final LocalDate ld = LocalDate.ofEpochDay(epochDays);
        return Date.fromYearMonthDay(ld.getYear(), ld.getMonth().getValue(), ld.getDayOfMonth());
    }

    public static java.util.Date convertEpochDaysToDate(final Integer epochDays) {
        if(epochDays == null) {
            return null;
        }
        final LocalDate ld = LocalDate.ofEpochDay(epochDays);
        return java.util.Date.from(ld.atStartOfDay().toInstant(ZoneOffset.UTC));
    }

    public static String convertNanosecToTimeString(final Long nanos) {
        if(nanos == null) {
            return null;
        }
        final LocalTime localTime = LocalTime.ofNanoOfDay(nanos);
        return localTime.format(DateTimeFormatter.ISO_LOCAL_TIME);
    }

    public static Timestamp convertMicrosecToTimestamp(final Long micros) {
        if(micros == null) {
            return null;
        }
        return Timestamp.ofTimeMicroseconds(micros);
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

    public static int convertDateStringToInteger(final String dateString) {
        if(PATTERN_DATE1.matcher(dateString).find()) {
            return Days.daysBetween(EPOCH_DATETIME, new DateTime(
                    Integer.valueOf(dateString.substring(0, 4)),
                    Integer.valueOf(dateString.substring(4, 6)),
                    Integer.valueOf(dateString.substring(6, 8)),
                    0, 0, DateTimeZone.UTC)).getDays();
        }

        Matcher matcher = PATTERN_DATE2.matcher(dateString);
        if(matcher.find()) {
            final String[] values = matcher.group().split("-");
            return Days.daysBetween(EPOCH_DATETIME, new DateTime(
                    Integer.valueOf(values[0]),
                    Integer.valueOf(values[1]),
                    Integer.valueOf(values[2]),
                    0, 0, DateTimeZone.UTC)).getDays();
        }

        matcher = PATTERN_DATE3.matcher(dateString);
        if(matcher.find()) {
            final String[] values = matcher.group().split("/");
            return Days.daysBetween(EPOCH_DATETIME, new DateTime(
                    Integer.valueOf(values[0]),
                    Integer.valueOf(values[1]),
                    Integer.valueOf(values[2]),
                    0, 0, DateTimeZone.UTC)).getDays();
        }

        throw new IllegalArgumentException("Illegal date string: " + dateString);
    }

    public static int convertTimeStringToInteger(final String timeString) {
        if(PATTERN_TIME1.matcher(timeString).find()) {
            return FORMAT_TIME1.parseLocalTime(timeString).getMillisOfDay();
        } else if(PATTERN_TIME2.matcher(timeString).find()) {
            return FORMAT_TIME2.parseLocalTime(timeString).getMillisOfDay();
        } else if(PATTERN_TIME3.matcher(timeString).find()) {
            return FORMAT_TIME3.parseLocalTime(timeString).getMillisOfDay();
        } else {
            throw new IllegalArgumentException("Illegal time string: " + timeString);
        }
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

    public static byte[] toBytes(final GenericRecord record) throws IOException {
        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(record.getSchema());
        try(final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
            final BinaryEncoder binaryEncoder = EncoderFactory.get().binaryEncoder(byteArrayOutputStream, null);
            datumWriter.write(record, binaryEncoder);
            binaryEncoder.flush();
            return byteArrayOutputStream.toByteArray();
        }
    }

    public static byte[] toBytes(final Schema schema, final List<GenericRecord> records) throws IOException {
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