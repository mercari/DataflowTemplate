package com.mercari.solution.util.converter;

import com.google.cloud.ByteArray;
import com.google.protobuf.*;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.Timestamps;
import com.google.type.Date;
import com.google.type.DateTime;
import com.google.type.TimeOfDay;
import com.mercari.solution.util.AvroSchemaUtil;
import com.mercari.solution.util.ProtoUtil;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import java.nio.ByteBuffer;
import java.time.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ProtoToRecordConverter {

    public static Schema convertSchema(final Descriptors.Descriptor messageType) {
        final SchemaBuilder.FieldAssembler<Schema> schemaFields = SchemaBuilder
                .record(messageType.getName())
                .fields();
        for(final Descriptors.FieldDescriptor field : messageType.getFields()) {
            schemaFields.name(field.getName()).type(convertSchema(field)).noDefault();
        }
        return schemaFields.endRecord();
    }

    public static GenericRecord convert(final Schema schema,
                                        final Descriptors.Descriptor messageDescriptor,
                                        final byte[] bytes,
                                        final JsonFormat.Printer printer) {

        try {
            final DynamicMessage message = DynamicMessage
                    .newBuilder(messageDescriptor)
                    .mergeFrom(bytes)
                    .build();
            return convert(schema, messageDescriptor, message, printer);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException(e);
        }
    }

    public static GenericRecord convert(final Schema schema,
                                        final Descriptors.Descriptor messageDescriptor,
                                        final DynamicMessage message,
                                        final JsonFormat.Printer printer) {

        final GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        for(final Schema.Field field : schema.getFields()) {
            final Descriptors.FieldDescriptor fieldDescriptor = messageDescriptor.findFieldByName(field.name());
            boolean isNull = fieldDescriptor == null;
            builder.set(field.name(), convertValue(field.schema(), fieldDescriptor, message.getField(fieldDescriptor), printer, isNull));
        }
        return builder.build();
    }

    private static Schema convertSchema(Descriptors.FieldDescriptor field) {
        //final boolean nullable = !field.isRequired() && (field.hasDefaultValue() || field.isOptional());
        final Schema elementSchema;
        switch (field.getJavaType()) {
            case BOOLEAN:
                elementSchema = AvroSchemaUtil.REQUIRED_BOOLEAN;
                break;
            case ENUM: {
                final List<String> enumNames = field.getEnumType().getValues().stream()
                        .map(Descriptors.EnumValueDescriptor::getName)
                        .collect(Collectors.toList());
                final Schema enumSchema = Schema.createEnum(field.getName(), "", "", enumNames);
                elementSchema = enumSchema;
                break;
            }
            case STRING:
                // TODO Fix when handle NULL schema for all avro converter
                elementSchema = field.isRepeated() ? AvroSchemaUtil.REQUIRED_STRING : AvroSchemaUtil.NULLABLE_STRING;
                break;
            case BYTE_STRING:
                elementSchema = AvroSchemaUtil.REQUIRED_BYTES;
                break;
            case INT:
                elementSchema = AvroSchemaUtil.REQUIRED_INT;
                break;
            case LONG:
                elementSchema = AvroSchemaUtil.REQUIRED_LONG;
                break;
            case FLOAT:
                elementSchema = AvroSchemaUtil.REQUIRED_FLOAT;
                break;
            case DOUBLE:
                elementSchema = AvroSchemaUtil.REQUIRED_DOUBLE;
                break;
            case MESSAGE: {
                if(field.isMapField()) {
                    Descriptors.FieldDescriptor keyField = null;
                    Descriptors.FieldDescriptor valueField = null;
                    for(Descriptors.FieldDescriptor mapField : field.getMessageType().getFields()) {
                        if("key".equals(mapField.getName())) {
                            keyField = mapField;
                        } else if("value".equals(mapField.getName())) {
                            valueField = mapField;
                        }
                    }
                    elementSchema = AvroSchemaUtil.createMapRecordSchema(
                            field.getName(),
                            convertSchema(keyField),
                            convertSchema(valueField));
                    break;
                }
                switch (ProtoUtil.ProtoType.of(field.getMessageType().getFullName())) {
                    case BOOL_VALUE:
                        elementSchema = AvroSchemaUtil.REQUIRED_BOOLEAN;
                        break;
                    case STRING_VALUE:
                        elementSchema = AvroSchemaUtil.REQUIRED_STRING;
                        break;
                    case BYTES_VALUE:
                        elementSchema = AvroSchemaUtil.REQUIRED_BYTES;
                        break;
                    case INT32_VALUE:
                        elementSchema = AvroSchemaUtil.REQUIRED_INT;
                        break;
                    case INT64_VALUE:
                        elementSchema = AvroSchemaUtil.REQUIRED_LONG;
                        break;
                    case FLOAT_VALUE:
                        elementSchema = AvroSchemaUtil.REQUIRED_FLOAT;
                        break;
                    case DOUBLE_VALUE:
                        elementSchema = AvroSchemaUtil.REQUIRED_DOUBLE;
                        break;
                    case UINT32_VALUE:
                        elementSchema = AvroSchemaUtil.REQUIRED_INT;
                        break;
                    case UINT64_VALUE:
                        elementSchema = AvroSchemaUtil.REQUIRED_LONG;
                        break;
                    case DATE:
                        elementSchema = AvroSchemaUtil.REQUIRED_LOGICAL_DATE_TYPE;
                        break;
                    case TIME:
                        elementSchema = AvroSchemaUtil.REQUIRED_LOGICAL_TIME_MICRO_TYPE;
                        break;
                    case DATETIME:
                    case TIMESTAMP:
                        elementSchema = AvroSchemaUtil.REQUIRED_LOGICAL_TIMESTAMP_MICRO_TYPE;
                        break;
                    case ANY:
                        elementSchema = AvroSchemaUtil.REQUIRED_STRING;
                        break;
                    case NULL_VALUE:
                    case EMPTY:
                        elementSchema = AvroSchemaUtil.NULLABLE_STRING;
                        break;
                    case CUSTOM:
                    default:
                        final Schema customSchema = convertSchema(field.getMessageType());
                        elementSchema = Schema.createUnion(customSchema, Schema.create(Schema.Type.NULL));
                        break;
                }
                break;
            }
            default:
                throw new IllegalArgumentException(field.getName() + " is not supported for bigquery.");
        }

        if(field.isRepeated()) {
            return Schema.createArray(elementSchema);
        } else {
            return elementSchema;
        }

    }

    private static Object convertValue(final Schema schema,
                                       final Descriptors.FieldDescriptor field,
                                       final Object value,
                                       final JsonFormat.Printer printer,
                                       final boolean isNull) {

        if(field.isRepeated()) {
            if(value == null) {
                return new ArrayList<>();
            }
            if(field.isMapField()) {
                return ((List<DynamicMessage>) value).stream()
                        .map(e -> {
                            final GenericRecordBuilder builder = new GenericRecordBuilder(schema.getElementType());
                            builder.set("key", ProtoUtil.getValue(e, "key", printer));
                            builder.set("value", ProtoUtil.getValue(e, "value", printer));
                            return builder.build();
                        })
                        .collect(Collectors.toList());
            }
            return ((List<Object>) value).stream()
                    .map(v -> getValue(schema.getElementType(), field, v, printer, isNull))
                    .collect(Collectors.toList());
        }
        return getValue(schema, field, value, printer, isNull);
    }

    private static Object getValue(final Schema schema,
                                   final Descriptors.FieldDescriptor field,
                                   final Object value,
                                   final JsonFormat.Printer printer,
                                   boolean isNull) {

        switch (field.getJavaType()) {
            case BOOLEAN:
                return !isNull && (Boolean)value;
            case LONG:
            case INT:
                return isNull ? 0 : value;
            case FLOAT:
                return isNull ? 0f : value;
            case DOUBLE:
                return isNull ? 0d : value;
            case STRING:
                return isNull ? "" : value;
            case ENUM: {
                final Schema enumSchema = AvroSchemaUtil.unnestUnion(schema);
                if(isNull) {
                    return new GenericData.EnumSymbol(
                            enumSchema,
                            enumSchema.getEnumSymbols().get(0));
                } else {
                    return new GenericData.EnumSymbol(
                            AvroSchemaUtil.unnestUnion(schema),
                            ((Descriptors.EnumValueDescriptor) value).getName());
                }
            }
            case BYTE_STRING:
                return ByteBuffer.wrap(isNull ? ByteArray.copyFrom("").toByteArray() : ((ByteString) value).toByteArray());
            case MESSAGE: {
                final Object object = ProtoUtil
                        .convertBuildInValue(field.getMessageType().getFullName(), (DynamicMessage) value);
                isNull = object == null;
                switch (ProtoUtil.ProtoType.of(field.getMessageType().getFullName())) {
                    case BOOL_VALUE:
                        return !isNull && ((BoolValue) object).getValue();
                    case BYTES_VALUE:
                        return ByteBuffer.wrap(isNull ? ByteArray.copyFrom ("").toByteArray() : ((BytesValue) object).getValue().toByteArray());
                    case STRING_VALUE:
                        return isNull ? "" : ((StringValue) object).getValue();
                    case INT32_VALUE:
                        return isNull ? 0 : ((Int32Value) object).getValue();
                    case INT64_VALUE:
                        return isNull ? 0 : ((Int64Value) object).getValue();
                    case UINT32_VALUE:
                        return isNull ? 0 : ((UInt32Value) object).getValue();
                    case UINT64_VALUE:
                        return isNull ? 0 : ((UInt64Value) object).getValue();
                    case FLOAT_VALUE:
                        return isNull ? 0f : ((FloatValue) object).getValue();
                    case DOUBLE_VALUE:
                        return isNull ? 0d : ((DoubleValue) object).getValue();
                    case DATE: {
                        if(isNull) {
                            return (int)(LocalDate.of(1, 1, 1).toEpochDay());
                        }
                        final Date date = (Date) object;
                        return (int)(LocalDate.of(date.getYear(), date.getMonth(), date.getDay()).toEpochDay());
                    }
                    case TIME: {
                        if(isNull) {
                            return LocalTime.of(0, 0, 0, 0).toSecondOfDay() * 1000_000L;
                        }
                        final TimeOfDay timeOfDay = (TimeOfDay) object;
                        return LocalTime.of(
                                timeOfDay.getHours(), timeOfDay.getMinutes(), timeOfDay.getSeconds(), timeOfDay.getNanos())
                                .toSecondOfDay() * 1000_000L;
                    }
                    case DATETIME: {
                        if(isNull) {
                            return LocalDateTime.of(
                                    1, 1, 1,
                                    0, 0, 0, 0)
                                    .atOffset(ZoneOffset.UTC)
                                    .toInstant()
                                    .toEpochMilli() * 1000L;
                        }
                        final DateTime dt = (DateTime) object;
                        return LocalDateTime.of(
                                dt.getYear(), dt.getMonth(), dt.getDay(),
                                dt.getHours(), dt.getMinutes(), dt.getSeconds(), dt.getNanos())
                                .atOffset(ZoneOffset.ofTotalSeconds((int)dt.getUtcOffset().getSeconds()))
                                .toInstant()
                                .toEpochMilli() * 1000L;
                    }
                    case TIMESTAMP:
                        if(isNull) {
                            return LocalDateTime.of(
                                    1, 1, 1,
                                    0, 0, 0, 0)
                                    .atOffset(ZoneOffset.UTC)
                                    .toInstant()
                                    .toEpochMilli() * 1000;
                        }
                        return Timestamps.toMicros((Timestamp) object);
                    case ANY: {
                        if(isNull) {
                            return "";
                        }
                        final Any any = (Any) object;
                        try {
                            return printer.print(any);
                        } catch (InvalidProtocolBufferException e) {
                            return any.getValue().toStringUtf8();
                        }
                    }
                    case EMPTY:
                    case NULL_VALUE:
                        return null;
                    case CUSTOM:
                    default: {
                        return convert(AvroSchemaUtil.unnestUnion(schema), field.getMessageType(), (DynamicMessage) value, printer);
                    }
                }
            }
            default:
                throw new IllegalStateException("Not support data type: " + field);
        }
    }

}
