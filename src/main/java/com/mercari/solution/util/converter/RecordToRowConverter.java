package com.mercari.solution.util.converter;

import com.mercari.solution.util.AvroSchemaUtil;
import org.apache.avro.JsonProperties;
import org.apache.avro.LogicalTypes;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.apache.beam.sdk.values.Row;
import org.joda.time.DateTime;
import org.joda.time.Instant;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

public class RecordToRowConverter {

    private static final Logger LOG = LoggerFactory.getLogger(RecordToRowConverter.class);

    public static Row convert(final GenericRecord record) {
        return convert(record.getSchema(), record);
    }

    public static Row convert(final org.apache.avro.Schema avroSchema, final GenericRecord record) {
        final Schema schema = convertSchema(avroSchema);
        return convert(schema, record);
    }

    public static Row convert(final SchemaAndRecord schemaAndRecord) {
        return convert(schemaAndRecord.getRecord());
    }

    public static Row convert(final Schema schema, final GenericRecord record) {
        final Row.Builder builder = Row.withSchema(schema);
        final List<Object> values = new ArrayList<>();
        for(final Schema.Field field : schema.getFields()) {
            values.add(convertValue(field.getType(), record.get(field.getName())));
        }
        return builder.addValues(values).build();
    }

    public static Schema convertSchema(final String avroSchema) {
        return convertSchema(AvroSchemaUtil.convertSchema(avroSchema));
    }

    public static Schema convertSchema(final org.apache.avro.Schema avroSchema) {
        final Schema.Builder builder = convertSchemaBuilder(avroSchema);
        final Schema.Options.Builder optionBuilder = convertOptionBuilder(avroSchema);
        builder.setOptions(optionBuilder);
        return builder.build();
    }

    public static Schema.Builder convertSchemaBuilder(final org.apache.avro.Schema avroSchema) {
        final Schema.Builder schemaBuilder = Schema.builder();
        avroSchema.getFields().stream()
                .map(avroField -> Schema.Field
                        .of(avroField.name(), convertFieldType(avroField.schema()))
                        .withOptions(convertFieldOption(avroField))
                        .withDescription(avroField.doc() == null ? "" : avroField.doc()))
                .forEach(schemaBuilder::addField);
        return schemaBuilder;
    }

    public static Schema.Options.Builder convertOptionBuilder(final org.apache.avro.Schema avroSchema) {
        final Schema.Options.Builder optionBuilder = Schema.Options.builder();
        avroSchema.getObjectProps().keySet()
                .forEach(propName -> optionBuilder.setOption(propName, Schema.FieldType.STRING, avroSchema.getProp(propName)));
        return optionBuilder;
    }


    private static Object convertValue(final Schema.FieldType fieldType, final Object value) {
        if(value == null) {
            return null;
        }
        switch (fieldType.getTypeName()) {
            case BOOLEAN:
                return value;
            case STRING:
                return value.toString();
            case BYTES:
                final byte[] bytes = ((ByteBuffer)value).array();
                return Arrays.copyOf(bytes, bytes.length);
            case DECIMAL:
                //fieldType.getLogicalType().
                //final int scale = AvroSchemaUtil.getLogicalTypeDecimal(schema).getScale();
                return BigDecimal.valueOf(0, 0);
            case INT16:
            case INT32:
            case INT64:
            case FLOAT:
            case DOUBLE:
                return value;
            case DATETIME: {
                if(value instanceof org.joda.time.DateTime) {
                    return ((DateTime) value).toInstant();
                }
                switch (fieldType.getMetadataString("type")) {
                    case "millis":
                        return Instant.ofEpochMilli((Long) value);
                    case "micros":
                        return Instant.ofEpochMilli(1000 * (Long) value);
                    default:
                        LOG.error("DATETIME_ERROR: " + (Long) value);
                        return Instant.ofEpochMilli((Long) value / 1000);
                }
            }
            case ROW:
                return convert(fieldType.getRowSchema(), (GenericRecord) value);
            case ARRAY:
                return ((List<Object>) value).stream()
                        .map(o -> convertValue(fieldType.getCollectionElementType(), o))
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());
            case ITERABLE:
                return Collections.singletonList(value).stream()
                        .map(o -> convertValue(fieldType.getCollectionElementType(), o))
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());
            case LOGICAL_TYPE:
                return value;
            case BYTE:
            case MAP:
            default:
                return null;
        }
    }

    private static Schema.FieldType convertFieldType(final org.apache.avro.Schema avroSchema) {
        switch (avroSchema.getType()) {
            case BOOLEAN:
                return Schema.FieldType.BOOLEAN;
            case ENUM:
                return Schema.FieldType.logicalType(EnumerationType.create(avroSchema.getEnumSymbols()));
            case STRING: {
                final String sqlType = avroSchema.getProp("sqlType");
                if ("DATETIME".equals(sqlType)) {
                    return Schema.FieldType.DATETIME;
                } else if("GEOGRAPHY".equals(sqlType)) {
                    return Schema.FieldType.STRING;
                }
                return Schema.FieldType.STRING;
            }
            case FIXED:
            case BYTES:
                if(AvroSchemaUtil.isLogicalTypeDecimal(avroSchema)) {
                    return Schema.FieldType.DECIMAL;
                }
                return Schema.FieldType.BYTES;
            case INT:
                if(LogicalTypes.date().equals(avroSchema.getLogicalType())) {
                    return CalciteUtils.DATE;
                } else if(LogicalTypes.timeMillis().equals(avroSchema.getLogicalType())) {
                    return CalciteUtils.TIME;
                }
                return Schema.FieldType.INT32;
            case LONG:
                if(LogicalTypes.timestampMillis().equals(avroSchema.getLogicalType())) {
                    return Schema.FieldType.DATETIME;
                } else if(LogicalTypes.timestampMicros().equals(avroSchema.getLogicalType())) {
                    return Schema.FieldType.DATETIME;
                } else if(LogicalTypes.timeMicros().equals(avroSchema.getLogicalType())) {
                    return CalciteUtils.TIME;
                }
                return Schema.FieldType.INT64;
            case FLOAT:
                return Schema.FieldType.FLOAT;
            case DOUBLE:
                return Schema.FieldType.DOUBLE;
            case RECORD:
                return Schema.FieldType.row(convertSchema(avroSchema));
            case ARRAY:
                return Schema.FieldType.array(convertFieldType(avroSchema.getElementType()));
            case MAP:
                return Schema.FieldType.map(Schema.FieldType.STRING, convertFieldType(avroSchema.getValueType()));
            case UNION:
                final boolean nullable = avroSchema.getTypes().stream()
                        .anyMatch(s -> s.getType().equals(org.apache.avro.Schema.Type.NULL));
                final org.apache.avro.Schema unnested = avroSchema.getTypes().stream()
                        .filter(s -> !s.getType().equals(org.apache.avro.Schema.Type.NULL))
                        .findAny()
                        .orElseThrow(() -> new IllegalArgumentException(""));
                return convertFieldType(unnested).withNullable(nullable);
            case NULL:
            default:
                return Schema.FieldType.STRING;
        }

    }

    private static Schema.Options convertFieldOption(final org.apache.avro.Schema.Field field) {
        Schema.Options.Builder builder = Schema.Options.builder();
        if(field.defaultVal() != null && !JsonProperties.NULL_VALUE.equals(field.defaultVal())) {
            try {
                builder.setOption("defaultVal", convertFieldType(field.schema()), field.defaultVal());
            } catch (ClassCastException e) {
                LOG.warn("Failed to convert defaultVal cause: " + e.getMessage());
            }
        }
        if(field.order() != null) {
            builder.setOption("order", Schema.FieldType.STRING, field.order().name());
        }
        builder.setOption("pos", Schema.FieldType.INT32, field.pos());
        field.getObjectProps().entrySet()
                .forEach(e -> builder.setOption(e.getKey(), Schema.FieldType.STRING, e.getValue().toString()));
        return builder.build();
    }

}
