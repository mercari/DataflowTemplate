package com.mercari.solution.util.converter;

/*
 * Copyright (c) Mercari, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.protobuf.util.Timestamps;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.io.gcp.bigquery.AvroWriteRequest;
import org.apache.beam.sdk.values.KV;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Days;
import org.joda.time.MutableDateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Collectors;

/**
 * Converter converts Cloud Spanner Struct to Avro GenericRecord
 */
public class StructToRecordConverter {

    private static final Logger LOG = LoggerFactory.getLogger(StructToRecordConverter.class);

    private static final MutableDateTime EPOCH_DATETIME = new MutableDateTime(0, DateTimeZone.UTC);

    private StructToRecordConverter() {}

    public static GenericRecord convert(final AvroWriteRequest<Struct> request) {
        return convert(request.getSchema(), request.getElement());
    }

    /**
     * Convert Spanner {@link Struct} object to Avro {@link GenericRecord} object.
     *
     * @param schema Avro schema object.
     * @param struct Spanner Struct to be converted to GenericRecord object.
     * @return Avro GenericRecord object.
     */
    public static GenericRecord convert(final Schema schema, Struct struct) {
        final GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        for(final Schema.Field field : schema.getFields()) {
            setFieldValue(builder, field, struct);
        }
        return builder.build();
    }

    public static GenericRecord convert(KV<String, Struct> kv, Schema schema) {
        return convert(schema, kv.getValue());
    }

    public static Schema convertSchema(final Type type) {
        return convertSchema(type, "root");
    }

    public static Schema convertSchema(final Type type, final String recordName) {
        try {
            final SchemaBuilder.FieldAssembler<Schema> schemaFields = SchemaBuilder.record(recordName).fields();
            type.getStructFields().forEach(field -> schemaFields
                    .name(field.getName())
                    .type(convertFieldType(field.getType(), field.getName(), true))
                    .noDefault());
            return schemaFields.endRecord();
        } catch (RuntimeException e) {
            final String message = "Failed to convert schema from type: " + type;
            LOG.error(message);
            throw new IllegalStateException(message, e);
        }
    }

    //
    private static void setFieldValue(GenericRecordBuilder builder, Schema.Field field, Struct struct) {
        setFieldValue(builder, field.name(), field.schema(), struct);
    }

    private static void setFieldValue(final GenericRecordBuilder builder,
                                      final String fieldName,
                                      final Schema schema,
                                      final Struct struct) {

        final Type type = struct.getType().getStructFields().stream()
                .filter(f -> f.getName().equals(fieldName))
                .map(Type.StructField::getType)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(String.format("Missing field %s", fieldName)));

        if(struct.isNull(fieldName)) {
            builder.set(fieldName, null);
            return;
        }

        switch (schema.getType()) {
            case FIXED:
            case BYTES:
                builder.set(fieldName, struct.getBytes(fieldName).asReadOnlyByteBuffer());
                break;
            case ENUM:
            case STRING:
                builder.set(fieldName, struct.getString(fieldName));
                break;
            case INT:
                if(Type.date().equals(type)) {
                    final Date date = struct.getDate(fieldName);
                    final DateTime datetime = new DateTime(date.getYear(), date.getMonth(), date.getDayOfMonth(), 0, 0, DateTimeZone.UTC);
                    final Days days = Days.daysBetween(EPOCH_DATETIME, datetime);
                    builder.set(fieldName, days.getDays());
                } else {
                    builder.set(fieldName, struct.getLong(fieldName));
                }
                break;
            case LONG:
                if(Type.timestamp().equals(type)) {
                    builder.set(fieldName, Timestamps.toMicros(struct.getTimestamp(fieldName).toProto()));
                } else {
                    builder.set(fieldName, struct.getLong(fieldName));
                }
                break;
            case DOUBLE:
                builder.set(fieldName, struct.getDouble(fieldName));
                break;
            case BOOLEAN:
                builder.set(fieldName, struct.getBoolean(fieldName));
                break;
            case RECORD:
                final GenericRecord chileRecord = convert(schema, struct.getStruct(fieldName));
                builder.set(fieldName, chileRecord);
                break;
            case UNION:
                for(final Schema childSchema : schema.getTypes()) {
                    if(Schema.Type.NULL.equals(childSchema.getType())) {
                        continue;
                    }
                    setFieldValue(builder, fieldName, childSchema, struct);
                }
                break;
            case ARRAY:
                setArrayFieldValue(builder, fieldName, schema.getElementType(), struct);
                break;
            case NULL:
                builder.set(fieldName, null);
                break;
            case FLOAT:
            case MAP:
                break;
            default:
                break;
        }
    }

    private static void setArrayFieldValue(final GenericRecordBuilder builder,
                                           final String fieldName,
                                           final Schema schema,
                                           final Struct struct) {

        final Type type = struct.getType().getStructFields().stream()
                .filter(f -> f.getName().equals(fieldName))
                .map(Type.StructField::getType)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(String.format("Missing field %s", fieldName)));

        if(struct.isNull(fieldName)) {
            builder.set(fieldName, null);
            return;
        }
        switch (schema.getType()) {
            case BOOLEAN:
                builder.set(fieldName, struct.getBooleanList(fieldName));
                return;
            case FIXED:
            case BYTES:
                builder.set(fieldName, struct.getBytesList(fieldName)
                        .stream()
                        .map(ByteArray::asReadOnlyByteBuffer)
                        .collect(Collectors.toList()));
                return;
            case ENUM:
            case STRING:
                builder.set(fieldName, struct.getStringList(fieldName));
                return;
            case INT:
                if(Type.array(Type.date()).equals(type)) {
                    builder.set(fieldName, struct.getDateList(fieldName).stream()
                            .map(date -> new DateTime(date.getYear(), date.getMonth(), date.getDayOfMonth(), 0, 0, DateTimeZone.UTC))
                            .map(datetime -> Days.daysBetween(EPOCH_DATETIME, datetime).getDays())
                            .collect(Collectors.toList()));
                } else {
                    builder.set(fieldName, struct.getLongList(fieldName));
                }
                return;
            case LONG:
                if(Type.array(Type.timestamp()).equals(type)) {
                    builder.set(fieldName, struct.getTimestampList(fieldName).stream()
                            .map(Timestamp::toProto)
                            .map(Timestamps::toMicros)
                            .collect(Collectors.toList()));
                } else {
                    builder.set(fieldName, struct.getLongList(fieldName));
                }
                return;
            case DOUBLE:
                builder.set(fieldName, struct.getDoubleList(fieldName));
                return;
            case RECORD:
                builder.set(fieldName, struct.getStructList(fieldName).stream()
                        .map(childStruct -> convert(schema, childStruct))
                        .collect(Collectors.toList()));
                return;
            case UNION:
                for(final Schema childSchema : schema.getTypes()) {
                    if(Schema.Type.NULL.equals(childSchema.getType())) {
                        continue;
                    }
                    setArrayFieldValue(builder, fieldName, childSchema, struct);
                }
                return;
            case ARRAY:
                setArrayFieldValue(builder, fieldName, schema.getElementType(), struct);
                return;
            case NULL:
                builder.set(fieldName, null);
                return;
            case FLOAT:
            case MAP:
                return;
            default:
                break;
        }
    }

    private static Schema convertFieldType(final Type type, final String name, final boolean nullable) {
        switch (type.getCode()) {
            case BYTES:
                return nullable ? AvroSchemaUtil.NULLABLE_BYTES : AvroSchemaUtil.REQUIRED_BYTES;
            case STRING:
                return nullable ? AvroSchemaUtil.NULLABLE_STRING : AvroSchemaUtil.REQUIRED_STRING;
            case INT64:
                return nullable ? AvroSchemaUtil.NULLABLE_LONG : AvroSchemaUtil.REQUIRED_LONG;
            case FLOAT64:
                return nullable ? AvroSchemaUtil.NULLABLE_DOUBLE : AvroSchemaUtil.REQUIRED_DOUBLE;
            case BOOL:
                return nullable ? AvroSchemaUtil.NULLABLE_BOOLEAN : AvroSchemaUtil.REQUIRED_BOOLEAN;
            case DATE:
                return nullable ? AvroSchemaUtil.NULLABLE_LOGICAL_DATE_TYPE : AvroSchemaUtil.REQUIRED_LOGICAL_DATE_TYPE;
            case TIMESTAMP:
                return nullable ? AvroSchemaUtil.NULLABLE_LOGICAL_TIMESTAMP_MICRO_TYPE : AvroSchemaUtil.REQUIRED_LOGICAL_TIMESTAMP_MICRO_TYPE;
            case STRUCT:
                final Schema structSchema = convertSchema(type, name);
                return nullable ? Schema.createUnion(Schema.create(Schema.Type.NULL), structSchema) : structSchema;
            case ARRAY:
                final Schema arraySchema = Schema.createArray(convertFieldType(type.getArrayElementType(), name, false));
                //return nullable ? Schema.createUnion(Schema.create(Schema.Type.NULL), arraySchema) : arraySchema;
                return arraySchema;
            default:
                throw new IllegalArgumentException("Spanner type: " + type.toString() + " not supported!");
        }
    }

}