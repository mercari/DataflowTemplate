package com.mercari.solution.util.converter;

import com.mercari.solution.util.schema.AvroSchemaUtil;
import org.apache.avro.JsonProperties;
import org.apache.avro.LogicalTypes;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.*;
import java.util.stream.Collectors;

public class RecordToRowConverter {

    private static final Logger LOG = LoggerFactory.getLogger(RecordToRowConverter.class);

    public static Row convert(final Schema schema, final GenericRecord record) {
        final Row.Builder builder = Row.withSchema(schema);
        Row.FieldValueBuilder fieldBuilder = builder.withFieldValues(new HashMap<>());
        for(final org.apache.avro.Schema.Field field : record.getSchema().getFields()) {
            fieldBuilder = fieldBuilder.withFieldValue(field.name(),
                        convertValue(field.schema(), schema.getField(field.name()).getType(), record.get(field.name())));
        }
        return fieldBuilder.build();
    }

    public static Row convert(final org.apache.avro.Schema avroSchema, final Schema schema, final GenericRecord record) {
        final Row.Builder builder = Row.withSchema(schema);
        Row.FieldValueBuilder fieldBuilder = null;
        for(final org.apache.avro.Schema.Field field : avroSchema.getFields()) {
            if(fieldBuilder == null) {
                fieldBuilder = builder.withFieldValue(field.name(),
                        convertValue(field.schema(), schema.getField(field.name()).getType(), record.get(field.name())));
            } else {
                fieldBuilder = fieldBuilder.withFieldValue(field.name(),
                        convertValue(field.schema(), schema.getField(field.name()).getType(), record.get(field.name())));
            }
        }
        return fieldBuilder.build();
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

    private static Object convertValue(final org.apache.avro.Schema schema, final Schema.FieldType fieldType, final Object value) {
        if(value == null) {
            return null;
        }
        switch (schema.getType()) {
            case ENUM:
            case STRING:
                return value.toString();
            case FIXED:
            case BYTES: {
                final byte[] bytes = ((ByteBuffer) value).array();
                if(AvroSchemaUtil.isLogicalTypeDecimal(schema)) {
                    final int scale = schema.getObjectProp("scale") != null ?
                            Integer.valueOf(schema.getObjectProp("scale").toString()) : 0;
                    return BigDecimal.valueOf(new BigInteger(bytes).longValue(), scale);
                }
                return Arrays.copyOf(bytes, bytes.length);
            }
            case INT: {
                final Integer intValue = (Integer) value;
                if(LogicalTypes.date().equals(schema.getLogicalType())) {
                    return LocalDate.ofEpochDay(intValue.longValue());
                } else if(LogicalTypes.timeMillis().equals(schema.getLogicalType())) {
                    return LocalTime.ofNanoOfDay(intValue * 1000_1000L);
                }
                return intValue;
            }
            case LONG: {
                final Long longValue = (Long) value;
                if(LogicalTypes.timestampMillis().equals(schema.getLogicalType())) {
                    return Instant.ofEpochMilli(longValue);
                } else if(LogicalTypes.timestampMicros().equals(schema.getLogicalType())) {
                    return Instant.ofEpochMilli(longValue / 1000);
                } else if(LogicalTypes.timeMicros().equals(schema.getLogicalType())) {
                    return LocalTime.ofNanoOfDay(longValue * 1000L);
                }
                return longValue;
            }
            case BOOLEAN:
            case FLOAT:
            case DOUBLE:
                return value;
            case RECORD:
                return convert(schema, fieldType.getRowSchema(), (GenericRecord) value);
            case ARRAY:
                return ((List<Object>) value).stream()
                        .map(o -> convertValue(schema.getElementType(), fieldType.getCollectionElementType(), o))
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());
            case UNION:
                return convertValue(AvroSchemaUtil.unnestUnion(schema), fieldType, value);
            case MAP:
            case NULL:
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
                if(sqlType == null) {
                    return Schema.FieldType.STRING;
                }
                if ("DATETIME".equalsIgnoreCase(sqlType)) {
                    return Schema.FieldType.DATETIME;
                } else if("JSON".equalsIgnoreCase(sqlType)) {
                    return Schema.FieldType.STRING;
                } else if("GEOGRAPHY".equalsIgnoreCase(sqlType)) {
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

        final org.apache.avro.Schema unnestFieldSchema = AvroSchemaUtil.unnestUnion(field.schema());
        if(org.apache.avro.Schema.Type.ARRAY.equals(unnestFieldSchema.getType())) {
            AvroSchemaUtil.unnestUnion(unnestFieldSchema.getElementType()).getObjectProps()
                    .forEach((key, value) -> {
                        switch (key) {
                            case "scale":
                            case "precision":
                                builder.setOption(key, Schema.FieldType.INT32, value);
                                break;
                            default:
                                builder.setOption(key, Schema.FieldType.STRING, value.toString());
                                break;
                        }
                    });
        } else {
            unnestFieldSchema.getObjectProps()
                    .forEach((key, value) -> {
                        switch (key) {
                            case "scale":
                            case "precision":
                                builder.setOption(key, Schema.FieldType.INT32, value);
                                break;
                            default:
                                builder.setOption(key, Schema.FieldType.STRING, value.toString());
                                break;
                        }
                    });
        }
        return builder.build();
    }

}
