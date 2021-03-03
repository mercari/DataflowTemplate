package com.mercari.solution.util.converter;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.joda.time.DateTime;
import org.joda.time.Instant;
import org.joda.time.format.ISODateTimeFormat;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RecordToTableRowConverter {

    public static TableSchema convertSchema(final Schema schema) {
        final List<TableFieldSchema> tableFieldSchemas = new ArrayList<>();
        for(final Schema.Field field : schema.getFields()) {
            tableFieldSchemas.add(convertTableFieldSchema(field.name(), field.schema(), false));
        }
        return new TableSchema().setFields(tableFieldSchemas);
    }

    public static TableRow convert(final GenericRecord record) {
        final TableRow row = new TableRow();
        for(final Schema.Field field : record.getSchema().getFields()) {
            row.set(field.name(), convertTableRowValue(field.schema(), record.get(field.name())));
        }
        return row;
    }

    private static Object convertTableRowValue(final Schema schema, final Object value) {
        return convertTableRowValue(schema, value, false);
    }

    private static Object convertTableRowValues(final Schema schema, final Object value) {
        return convertTableRowValue(schema, value, true);
    }

    private static Object convertTableRowValue(final Schema schema, final Object value, final boolean isArray) {
        if(schema == null) {
            throw new IllegalArgumentException(String.format("Schema of fieldValue: %v must not be null!", value));
        }
        if(value == null) {
            return null;
        }
        if(isArray) {
            return ((List<Object>)value).stream()
                    .map(v -> convertTableRowValue(schema, v))
                    .filter(v -> v != null)
                    .collect(Collectors.toList());
        }
        switch(schema.getType()) {
            case ENUM:
            case STRING:
                return value.toString();
            case FIXED:
            case BYTES:
                if(AvroSchemaUtil.isLogicalTypeDecimal(schema)) {
                    final byte[] bytes;
                    if(Schema.Type.FIXED.equals(schema.getType())) {
                        bytes = ((GenericData.Fixed)value).bytes();
                    } else {
                        bytes = ((ByteBuffer)value).array();
                    }
                    if(bytes.length == 0) {
                        return BigDecimal.valueOf(0, 0);
                    }
                    final int scale = AvroSchemaUtil.getLogicalTypeDecimal(schema).getScale();
                    return BigDecimal.valueOf(new BigInteger(bytes).longValue(), scale);
                }
                if(Schema.Type.FIXED.equals(schema.getType())) {
                    return ByteBuffer.wrap(((GenericData.Fixed)value).bytes());
                }
                return value;
            case INT:
                if(LogicalTypes.date().equals(schema.getLogicalType())) {
                    return LocalDate
                            .ofEpochDay((Integer)value)
                            .format(DateTimeFormatter.ISO_LOCAL_DATE);
                } else if(LogicalTypes.timeMillis().equals(schema.getLogicalType())) {
                    final Long intValue = new Long((Integer)value);
                    return LocalTime
                            .ofNanoOfDay(intValue * 1000 * 1000)
                            .format(DateTimeFormatter.ISO_LOCAL_TIME);
                }
                return value;
            case LONG: {
                if(value instanceof org.joda.time.DateTime) {
                    return ((DateTime) value).toString(ISODateTimeFormat.dateTime());
                }
                final Long longValue = (Long) value;
                if (LogicalTypes.timestampMillis().equals(schema.getLogicalType())) {
                    return Instant
                            .ofEpochMilli(longValue)
                            .toString(ISODateTimeFormat.dateTime());
                } else if (LogicalTypes.timestampMicros().equals(schema.getLogicalType())) {
                    return Instant
                            .ofEpochMilli(longValue/1000)
                            .toString(ISODateTimeFormat.dateTime());
                } else if (LogicalTypes.timeMicros().equals(schema.getLogicalType())) {
                    return LocalTime
                            .ofNanoOfDay(longValue * 1000)
                            .format(DateTimeFormatter.ISO_LOCAL_TIME);
                }
                return value;
            }
            case FLOAT:
            case DOUBLE:
            case BOOLEAN:
                return value;
            case RECORD:
                return convert((GenericRecord) value);
            case MAP:
                final Map<Object, Object> map = (Map)value;
                return map.entrySet().stream()
                        .map(entry -> new TableRow()
                                .set("key", entry.getKey() == null ? "" : entry.getKey().toString())
                                .set("value", convertTableRowValue(schema.getValueType(), entry.getValue())))
                        .collect(Collectors.toList());
            case UNION:
                final Schema childSchema = schema.getTypes().stream()
                        .filter(s -> !Schema.Type.NULL.equals(s.getType()))
                        .findAny().orElse(null);
                return convertTableRowValue(childSchema, value);
            case ARRAY:
                return convertTableRowValues(schema.getElementType(), value);
            default:
                return value;
        }
    }

    private static TableFieldSchema convertTableFieldSchema(final Schema.Field field) {
        return convertTableFieldSchema(field.name(), field.schema(), AvroSchemaUtil.isNullable(field.schema()));
    }


    private static TableFieldSchema convertTableFieldSchema(final String name, final Schema schema, final boolean nullable) {
        final String mode = nullable ? "NULLABLE" : "REQUIRED";
        final TableFieldSchema tableFieldSchema = new TableFieldSchema()
                .setMode(mode);
        switch (schema.getType()) {
            case BOOLEAN:
                return tableFieldSchema.setName(name).setType("BOOLEAN");
            case ENUM:
            case STRING:
                final String sqlType = schema.getProp("sqlType");
                if ("DATETIME".equals(sqlType)) {
                    return tableFieldSchema.setName(name).setType("DATETIME");
                } else if("GEOGRAPHY".equals(sqlType)) {
                    return tableFieldSchema.setName(name).setType("GEOGRAPHY");
                }
                return tableFieldSchema.setName(name).setType("STRING");
            case FIXED:
            case BYTES:
                if(AvroSchemaUtil.isLogicalTypeDecimal(schema)) {
                    final LogicalTypes.Decimal decimal = (LogicalTypes.Decimal)schema.getLogicalType();
                    return tableFieldSchema.setName(name).setType("NUMERIC")
                            .set("scale", decimal.getScale())
                            .set("precision", decimal.getPrecision());
                }
                return tableFieldSchema.setName(name).setType("BYTES");
            case INT:
                if(LogicalTypes.date().equals(schema.getLogicalType())) {
                    return tableFieldSchema.setName(name).setType("DATE");
                } else if(LogicalTypes.timeMillis().equals(schema.getLogicalType())) {
                    return tableFieldSchema.setName(name).setType("TIME");
                }
                return tableFieldSchema.setName(name).setType("INTEGER").set("avroSchema", "INT");
            case LONG:
                if(LogicalTypes.timestampMillis().equals(schema.getLogicalType())) {
                    return tableFieldSchema.setName(name).setType("TIMESTAMP");
                } else if(LogicalTypes.timestampMicros().equals(schema.getLogicalType())) {
                    return tableFieldSchema.setName(name).setType("TIMESTAMP");
                } else if(LogicalTypes.timeMicros().equals(schema.getLogicalType())) {
                    return tableFieldSchema.setName(name).setType("TIME");
                }
                return tableFieldSchema.setName(name).setType("INTEGER");
            case FLOAT:
                return tableFieldSchema.setName(name).setType("FLOAT").set("avroSchema", "FLOAT");
            case DOUBLE:
                return tableFieldSchema.setName(name).setType("FLOAT");
            case RECORD:
                final List<TableFieldSchema> childTableFieldSchemas = schema.getFields().stream()
                        .map(RecordToTableRowConverter::convertTableFieldSchema)
                        .collect(Collectors.toList());
                return tableFieldSchema.setName(name).setType("RECORD").setFields(childTableFieldSchemas);
            case ARRAY:
                return tableFieldSchema
                        .setName(name)
                        .setType(convertTableFieldSchema(name, schema.getElementType(), AvroSchemaUtil.isNullable(schema.getElementType())).getType())
                        .setMode("REPEATED");
            case UNION:
                return convertTableFieldSchema(name, AvroSchemaUtil.unnestUnion(schema), AvroSchemaUtil.isNullable(schema));
            case MAP:
            case NULL:
            default:
                throw new IllegalArgumentException(schema.getType().getName() + " is not supported for bigquery.");
        }
    }

}
