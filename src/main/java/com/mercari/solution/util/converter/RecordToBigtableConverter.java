package com.mercari.solution.util.converter;

import com.google.bigtable.v2.Mutation;
import com.google.common.collect.Lists;
import com.google.common.io.BaseEncoding;
import com.google.protobuf.*;
import com.mercari.solution.util.OptionUtil;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.hbase.util.Bytes;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class RecordToBigtableConverter {

    //private static DateTimeFormatter FORMATTER_YYYYMMDD = DateTimeFormatter.ofPattern("YYYYMMDD");
    private static org.joda.time.format.DateTimeFormatter FORMATTER_YYYYMMDD = DateTimeFormat.forPattern("yyyyMMdd");

    public static KV<ByteString, Iterable<Mutation>> convert(
            final Schema schema, final GenericRecord record, final org.joda.time.Instant timestamp,
            final String columnFamily, final List<String> keyFields,
            final Set<String> excludeFields, final String separator,
            final boolean body) {

        final ByteString key = createKey(schema, record, timestamp, keyFields, separator);

        final List<Mutation> mutations;
        if(body) {
            mutations = convertBody(record, columnFamily);
        } else {
            mutations = convertFields(schema, record, columnFamily, excludeFields);
        }

        return KV.of(key, mutations);
    }

    private static List<Mutation> convertFields(
            final Schema schema, final GenericRecord record,
            final String columnFamily, final Set<String> excludeFields) {

        final List<Mutation> mutations = new ArrayList<>();
        for(final Schema.Field field : schema.getFields()) {
            if(excludeFields != null && excludeFields.contains(field.name())) {
                continue;
            }
            final Object value = record.get(field.name());
            if(value == null) {
                continue;
            }
            setFieldValueAsMutations(mutations, columnFamily, field, value, true);
        }
        return mutations;
    }

    private static List<Mutation> convertBody(
            final GenericRecord record, final String columnFamily) {

        final Mutation.SetCell.Builder cellBuilder = Mutation.SetCell.newBuilder()
                .setFamilyName(columnFamily)
                .setColumnQualifier(ByteString.copyFrom("body", StandardCharsets.UTF_8));

        final ByteString body = ByteString.copyFrom(toBytes(record));
        final Mutation mutation = Mutation.newBuilder().setSetCell(cellBuilder.setValue(body)).build();
        return Lists.newArrayList(mutation);
    }

    private static ByteString createKey(
            final Schema schema, final GenericRecord record, final org.joda.time.Instant timestamp,
            final List<String> keyFields, final String separator) {

        final String keyString = keyFields.stream()
                .map(String::trim)
                .map(keyField -> {
                    if(keyField.startsWith("_EVENTTIME")) {
                        if(timestamp == null) {
                            return "";
                        }
                        return OptionUtil.parseDatetime(timestamp, keyField);
                    } else {
                        return convertToString(schema.getField(keyField).schema(), record.get(keyField));
                    }
                })
                .collect(Collectors.joining(separator));

        return ByteString.copyFrom(keyString, StandardCharsets.UTF_8);
    }

    private static String convertToString(final Schema schema, final Object value) {
        if(schema == null || value == null) {
            return "";
        }
        switch (schema.getType()) {
            case FIXED:
            case BYTES:
                return BaseEncoding.base64().encode((byte[]) value);
            case ENUM:
            case STRING:
            case BOOLEAN:
            case FLOAT:
            case DOUBLE:
                return value.toString();
            case INT: {
                if (LogicalTypes.date().equals(schema.getLogicalType())) {
                    return LocalDate.ofEpochDay((int) value).format(DateTimeFormatter.ISO_LOCAL_DATE);
                } else if (LogicalTypes.timeMillis().equals(schema.getLogicalType())) {
                    return LocalTime.ofNanoOfDay(new Long((Integer) value) * 1000 * 1000).format(DateTimeFormatter.ISO_LOCAL_TIME);
                }
                return value.toString();
            }
            case LONG: {
                final Long longValue = (Long)value;
                if (LogicalTypes.timestampMillis().equals(schema.getLogicalType())) {
                    //return DateTimeFormatter.ISO_INSTANT.format(Instant.ofEpochMilli(longValue));
                    return FORMATTER_YYYYMMDD.print(Instant.ofEpochMilli(longValue));
                } else if (LogicalTypes.timestampMicros().equals(schema.getLogicalType())) {
                    //return DateTimeFormatter.ISO_INSTANT.format(Instant.ofEpochMilli(longValue/1000));
                    return FORMATTER_YYYYMMDD.print(Instant.ofEpochMilli(longValue/1000));
                } else if (LogicalTypes.timeMicros().equals(schema.getLogicalType())) {
                    return LocalTime.ofNanoOfDay(new Long((Integer) value) * 1000).format(DateTimeFormatter.ISO_LOCAL_TIME);
                }
                return longValue.toString();
            }
            case UNION:
                return schema.getTypes().stream()
                        .filter(s -> !s.getType().equals(Schema.Type.NULL))
                        .map(s -> convertToString(s, value))
                        .findAny()
                        .orElse("");
            case ARRAY:
            case MAP:
            case RECORD:
            case NULL:
            default:
                return "";
        }
    }

    private static void setFieldValueAsMutations(
            final List<Mutation> mutations,
            final String familyName,
            final Schema.Field field,
            final Object value,
            final boolean structAsFamily) {

        if(value == null) {
            return;
        }

        final Mutation.SetCell.Builder cellBuilder = Mutation.SetCell.newBuilder()
                .setFamilyName(familyName)
                .setColumnQualifier(ByteString.copyFrom(field.name(), StandardCharsets.UTF_8));

        if(field.schema().getType().equals(Schema.Type.RECORD) && structAsFamily) {
            final GenericRecord child = (GenericRecord)value;
            for(Schema.Field childField : child.getSchema().getFields()) {
                setFieldValueAsMutations(mutations, field.name(), childField, child.get(childField.name()), false);
            }
        } else {
            mutations.add(Mutation.newBuilder().setSetCell(cellBuilder.setValue(convertToByteString(field.schema(), value))).build());
        }
    }

    private static ByteString convertToByteString(final Schema schema, final Object value) {
        final byte[] bytes;
        switch (schema.getType()) {
            case BOOLEAN:
                bytes = Bytes.toBytes((boolean)value);
                break;
            case FIXED:
            case BYTES:
                bytes = (byte[])value;
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
            case RECORD:
                final GenericRecord child = (GenericRecord)value;
                final Struct.Builder childBuilder = Struct.newBuilder();
                for(Schema.Field childField : schema.getFields()) {
                    childBuilder.putFields(childField.name(), convertProtobufValue(childField.schema(), child.get(childField.name())));
                }
                return childBuilder.build().toByteString();
            case ARRAY:
                final ListValue.Builder listBuilder = ListValue.newBuilder();
                ((List<Object>) value).forEach(v -> listBuilder.addValues(convertProtobufValue(schema.getElementType(), v)));
                return listBuilder.build().toByteString();
            case UNION:
                return schema.getTypes().stream()
                        .filter(s -> !s.getType().equals(Schema.Type.NULL))
                        .map(s -> RecordToBigtableConverter.convertToByteString(s, value))
                        .findAny()
                        .orElseThrow(() -> new IllegalArgumentException("UNION does not have another schema."));
            case MAP:
            case NULL:
            default:
                return ByteString.copyFrom("", StandardCharsets.UTF_8);
        }
        return ByteString.copyFrom(bytes);
    }

    private static byte[] toBytes(final GenericRecord record) {
        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(record.getSchema());
        try(final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
            final BinaryEncoder binaryEncoder = EncoderFactory.get().binaryEncoder(byteArrayOutputStream, null);
            datumWriter.write(record, binaryEncoder);
            binaryEncoder.flush();
            return byteArrayOutputStream.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static Value convertProtobufValue(final Schema schema, final Object value) {
        if(value == null) {
            return Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build();
        }
        switch (schema.getType()) {
            case BOOLEAN:
                return Value.newBuilder().setBoolValue((boolean)value).build();
            case FIXED:
            case BYTES:
                return Value.newBuilder().setStringValueBytes(ByteString.copyFrom((byte[])value)).build();
            case ENUM:
            case STRING:
                return Value.newBuilder().setStringValue(value.toString()).build();
            case INT:
                return Value.newBuilder().setNumberValue((int)value).build();
            case LONG:
                return Value.newBuilder().setNumberValue((long)value).build();
            case FLOAT:
                return Value.newBuilder().setNumberValue((float)value).build();
            case DOUBLE:
                return Value.newBuilder().setNumberValue((double)value).build();
            case RECORD:
                final GenericRecord record = (GenericRecord)value;
                final Struct.Builder structBuilder = Struct.newBuilder();
                for(final Schema.Field field : schema.getFields()) {
                    structBuilder.putFields(field.name(), convertProtobufValue(field.schema(), record.get(field.name())));
                }
                return Value.newBuilder().setStructValue(structBuilder).build();
            case ARRAY:
                final ListValue.Builder listBuilder = ListValue.newBuilder();
                ((List<Object>) value).forEach(v -> listBuilder.addValues(convertProtobufValue(schema.getElementType(), v)));
                return Value.newBuilder().setListValue(listBuilder).build();
            case UNION:
                return schema.getTypes().stream()
                        .filter(s -> !s.getType().equals(Schema.Type.NULL))
                        .map(s -> convertProtobufValue(s, value))
                        .findAny()
                        .orElseThrow(() -> new IllegalArgumentException("UNION does not have another schema."));
            case NULL:
            case MAP:
            default:
                return Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build();
        }
    }

}
