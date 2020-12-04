package com.mercari.solution.util.converter;

import com.google.bigtable.v2.Mutation;
import com.google.common.io.BaseEncoding;
import com.google.protobuf.*;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.apache.hadoop.hbase.util.Bytes;
import org.joda.time.Instant;
import org.joda.time.ReadableInstant;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class RowToBigtableConverter {

    public static KV<ByteString, Iterable<Mutation>> convert(final Row row, final List<String> keyFields, final String columnFamily, final String separator) {
        final List<Mutation> mutations = new ArrayList<>();
        for(Schema.Field field : row.getSchema().getFields()) {
            setMutations(mutations, columnFamily, field, row.getValue(field.getName()), true);
        }

        final ByteString key = createKey(row, keyFields, separator);
        return KV.of(key, mutations);
    }

    private static ByteString createKey(final Row row, final List<String> keyFields, final String separator) {

        for(final String keyField : keyFields) {
            if(!row.getSchema().hasField(keyField) || row.getValue(keyField) == null) {

            }
        }

        final String keyString = keyFields.stream()
                .map(keyField -> {
                    if(!row.getSchema().hasField(keyField)) {
                        return "";
                    }
                    final Object value = row.getValue(keyField);
                    if(value == null) {
                        return "";
                    }
                    switch (row.getSchema().getField(keyField).getType().getTypeName()) {
                        case BYTE:
                            return BaseEncoding.base64().encode(new byte[]{(byte) value});
                        case BYTES:
                            return BaseEncoding.base64().encode((byte[]) value);
                        case BOOLEAN:
                        case STRING:
                        case INT16:
                        case INT32:
                        case INT64:
                        case FLOAT:
                        case DOUBLE:
                            return value.toString();
                        case DATETIME:
                            final ReadableInstant instant = (ReadableInstant) value;
                            return instant.toString();
                        case ARRAY:
                        case DECIMAL:
                        case ITERABLE:
                        case LOGICAL_TYPE:
                        case MAP:
                        case ROW:
                        default:
                            return "";
                    }
                })
                .collect(Collectors.joining(separator));

        return ByteString.copyFrom(keyString, StandardCharsets.UTF_8);
        /*
        int bufferSize = 0;
        final List<ByteString> keyByteStrings = new ArrayList<>();
        for(final String keyField : keyFields) {
            final ByteString bs = convertToByteString(row.getSchema().getField(keyField).getType(), row.getValue(keyField));
            bufferSize += (bs.size() + splitter.size());
            keyByteStrings.add(bs);
            keyByteStrings.add(splitter);
        }
        final ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
        keyByteStrings.forEach(bs -> buffer.put(bs.toByteArray()));
        return ByteString.copyFrom(buffer.array());
        */
    }

    public static void setMutations(final List<Mutation> mutations,
                                    final String familyName,
                                    final Schema.Field field,
                                    final Object value,
                                    final boolean structAsFamily) {

        if(value == null) {
            return;
        }

        final Mutation.SetCell.Builder cellBuilder = Mutation.SetCell.newBuilder()
                .setFamilyName(familyName)
                .setColumnQualifier(ByteString.copyFrom(field.getName(), StandardCharsets.UTF_8));


        if(field.getType().getTypeName().equals(Schema.TypeName.ROW) && structAsFamily) {
            final Row child = (Row)value;
            for(Schema.Field childField : child.getSchema().getFields()) {
                setMutations(mutations, field.getName(), childField, child.getValue(childField.getName()), false);
            }
        } else {
            mutations.add(toMutation(cellBuilder.setValue(convertToByteString(field.getType(), value))));
        }
    }

    private static ByteString convertToByteString(final Schema.FieldType fieldType, final Object value) {
        final byte[] bytes;
        switch (fieldType.getTypeName()) {
            case BOOLEAN:
                bytes = Bytes.toBytes((boolean)value);
                break;
            case BYTES:
                bytes = (byte[])value;
                break;
            case STRING:
                bytes = Bytes.toBytes((String)value);
                break;
            case INT16:
                bytes = Bytes.toBytes((Short)value);
                break;
            case INT32:
                bytes = Bytes.toBytes((Integer)value);
                break;
            case INT64:
                bytes = Bytes.toBytes((Long) value);
                break;
            case FLOAT:
                bytes = Bytes.toBytes((Float)value);
                break;
            case DOUBLE:
                bytes = Bytes.toBytes((Double)value);
                break;
            case DATETIME:
                bytes = Bytes.toBytes(((ReadableInstant) value).toString());
                break;
                /*
                final java.time.Instant datetime = java.time.Instant.ofEpochMilli(((ReadableInstant) value).getMillis());
                return Timestamp.newBuilder()
                        .setSeconds(datetime.getEpochSecond()).setNanos(datetime.getNano())
                        .build().toByteString();
                */
            case ROW:
                final Row childRow = (Row)value;
                final Struct.Builder childBuilder = Struct.newBuilder();
                for(Schema.Field childField : childRow.getSchema().getFields()) {
                    childBuilder.putFields(childField.getName(), convertProtobufValue(childField.getType(), childRow.getValue(childField.getName())));
                }
                return childBuilder.build().toByteString();
            case ITERABLE:
                final ListValue.Builder iterBuilder = ListValue.newBuilder();
                ((Iterable<Object>) value).forEach(v -> iterBuilder.addValues(convertProtobufValue(fieldType.getCollectionElementType(), v)));
                return iterBuilder.build().toByteString();
            case ARRAY:
                final ListValue.Builder listBuilder = ListValue.newBuilder();
                ((List<Object>) value).forEach(v -> listBuilder.addValues(convertProtobufValue(fieldType.getCollectionElementType(), v)));
                return listBuilder.build().toByteString();
            case DECIMAL:
            case BYTE:
            case LOGICAL_TYPE:
            case MAP:
            default:
                return ByteString.copyFrom("", StandardCharsets.UTF_8);
        }
        return ByteString.copyFrom(bytes);
    }

    private static Value convertProtobufValue(final Schema.FieldType fieldType, final Object value) {
        if(value == null) {
            return Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build();
        }

        switch (fieldType.getTypeName()) {
            case BOOLEAN:
                return Value.newBuilder().setBoolValue((boolean)value).build();
            case BYTES:
                return Value.newBuilder().setStringValueBytes(ByteString.copyFrom((byte[])value)).build();
            case STRING:
                return Value.newBuilder().setStringValue((String) value).build();
            case INT16:
            case INT32:
                return Value.newBuilder().setNumberValue((int)value).build();
            case INT64:
                return Value.newBuilder().setNumberValue((long)value).build();
            case FLOAT:
                return Value.newBuilder().setNumberValue((float)value).build();
            case DOUBLE:
                return Value.newBuilder().setNumberValue((double)value).build();
            case DATETIME:
                return Value.newBuilder().setNumberValue(((Instant)value).getMillis()).build();
            case ROW:
                final Row row = (Row)value;
                final Struct.Builder structBuilder = Struct.newBuilder();
                for(final Schema.Field field : row.getSchema().getFields()) {
                    structBuilder.putFields(field.getName(), convertProtobufValue(field.getType(), row.getValue(field.getName())));
                }
                return Value.newBuilder().setStructValue(structBuilder).build();
            case ITERABLE:
                final ListValue.Builder iterBuilder = ListValue.newBuilder();
                ((Iterable<Object>) value).forEach(v -> iterBuilder.addValues(convertProtobufValue(fieldType.getCollectionElementType(), v)));
                return Value.newBuilder().setListValue(iterBuilder).build();
            case ARRAY:
                final ListValue.Builder listBuilder = ListValue.newBuilder();
                ((List<Object>) value).forEach(v -> listBuilder.addValues(convertProtobufValue(fieldType.getCollectionElementType(), v)));
                return Value.newBuilder().setListValue(listBuilder).build();
            case MAP:
            case LOGICAL_TYPE:
            case BYTE:
            case DECIMAL:
            default:
                return Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build();
        }
    }

    private static Mutation toMutation(final Mutation.SetCell.Builder cell) {
        return Mutation.newBuilder().setSetCell(cell).build();
    }

}
