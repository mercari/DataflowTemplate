package com.mercari.solution.util.converter;

import com.google.protobuf.*;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.Timestamps;
import com.google.type.Date;
import com.google.type.DateTime;
import com.google.type.LatLng;
import com.google.type.TimeOfDay;
import com.mercari.solution.util.AvroSchemaUtil;
import com.mercari.solution.util.ProtoUtil;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
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
        for (final Descriptors.FieldDescriptor field : messageDescriptor.getFields()) {
            if (!field.isRepeated() && !message.hasField(field)) {
                builder.set(field.getName(), null);
            } else {
                builder.set(field.getName(), convertValue(field, message.getField(field), printer));
            }
        }
        return builder.build();
    }

    private static Schema convertSchema(Descriptors.FieldDescriptor field) {
        final boolean nullable = !field.isRequired() && (field.hasDefaultValue() || field.isOptional());
        final Schema elementSchema;
        switch (field.getJavaType()) {
            case BOOLEAN:
                elementSchema = nullable ? AvroSchemaUtil.NULLABLE_BOOLEAN : AvroSchemaUtil.REQUIRED_BOOLEAN;
                break;
            case ENUM: {
                final List<String> enumNames = field.getEnumType().getValues().stream()
                        .map(Descriptors.EnumValueDescriptor::getName)
                        .collect(Collectors.toList());
                final Schema enumSchema = Schema.createEnum(field.getName(), "", "", enumNames);
                elementSchema = nullable ? Schema.createUnion(Schema.create(Schema.Type.NULL), enumSchema) : enumSchema;
                break;
            }
            case STRING:
                elementSchema =  nullable ? AvroSchemaUtil.NULLABLE_STRING : AvroSchemaUtil.REQUIRED_STRING;
                break;
            case BYTE_STRING:
                elementSchema =  nullable ? AvroSchemaUtil.NULLABLE_BYTES : AvroSchemaUtil.REQUIRED_BYTES;
                break;
            case INT:
                elementSchema =  nullable ? AvroSchemaUtil.NULLABLE_INT : AvroSchemaUtil.REQUIRED_INT;
                break;
            case LONG:
                elementSchema =  nullable ? AvroSchemaUtil.NULLABLE_LONG : AvroSchemaUtil.REQUIRED_LONG;
                break;
            case FLOAT:
                elementSchema =  nullable ? AvroSchemaUtil.NULLABLE_FLOAT : AvroSchemaUtil.REQUIRED_FLOAT;
                break;
            case DOUBLE:
                elementSchema =  nullable ? AvroSchemaUtil.NULLABLE_DOUBLE : AvroSchemaUtil.REQUIRED_DOUBLE;
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
                        elementSchema =  nullable ? AvroSchemaUtil.NULLABLE_BOOLEAN : AvroSchemaUtil.REQUIRED_BOOLEAN;
                        break;
                    case STRING_VALUE:
                        elementSchema =  nullable ? AvroSchemaUtil.NULLABLE_STRING : AvroSchemaUtil.REQUIRED_STRING;
                        break;
                    case BYTES_VALUE:
                        elementSchema =  nullable ? AvroSchemaUtil.NULLABLE_BYTES : AvroSchemaUtil.REQUIRED_BYTES;
                        break;
                    case INT32_VALUE:
                        elementSchema =  nullable ? AvroSchemaUtil.NULLABLE_INT : AvroSchemaUtil.REQUIRED_INT;
                        break;
                    case INT64_VALUE:
                        elementSchema =  nullable ? AvroSchemaUtil.NULLABLE_LONG : AvroSchemaUtil.REQUIRED_LONG;
                        break;
                    case FLOAT_VALUE:
                        elementSchema =  nullable ? AvroSchemaUtil.NULLABLE_FLOAT : AvroSchemaUtil.REQUIRED_FLOAT;
                        break;
                    case DOUBLE_VALUE:
                        elementSchema =  nullable ? AvroSchemaUtil.NULLABLE_DOUBLE : AvroSchemaUtil.REQUIRED_DOUBLE;
                        break;
                    case UINT32_VALUE:
                        elementSchema =  nullable ? AvroSchemaUtil.NULLABLE_INT : AvroSchemaUtil.REQUIRED_INT;
                        break;
                    case UINT64_VALUE:
                        elementSchema =  nullable ? AvroSchemaUtil.NULLABLE_LONG : AvroSchemaUtil.REQUIRED_LONG;
                        break;
                    case DATE:
                        elementSchema =  nullable ? AvroSchemaUtil.NULLABLE_LOGICAL_DATE_TYPE : AvroSchemaUtil.REQUIRED_LOGICAL_DATE_TYPE;
                        break;
                    case TIME:
                        elementSchema =  nullable ? AvroSchemaUtil.NULLABLE_LOGICAL_TIME_MICRO_TYPE : AvroSchemaUtil.REQUIRED_LOGICAL_TIME_MICRO_TYPE;
                        break;
                    case DATETIME:
                    case TIMESTAMP:
                        elementSchema =  nullable ? AvroSchemaUtil.NULLABLE_LOGICAL_TIME_MICRO_TYPE : AvroSchemaUtil.REQUIRED_LOGICAL_TIME_MICRO_TYPE;
                        break;
                    case LATLNG:
                        elementSchema =  nullable ? AvroSchemaUtil.NULLABLE_STRING : AvroSchemaUtil.REQUIRED_STRING;
                        break;
                    case ANY:
                        elementSchema =  nullable ? AvroSchemaUtil.NULLABLE_STRING : AvroSchemaUtil.REQUIRED_STRING;
                        break;
                    case NULL_VALUE:
                    case EMPTY:
                        elementSchema =  nullable ? AvroSchemaUtil.NULLABLE_STRING : AvroSchemaUtil.REQUIRED_STRING;
                        break;
                    case CUSTOM:
                    default:
                        final Schema customSchema = convertSchema(field.getMessageType());
                        elementSchema =  nullable ? Schema.createUnion(customSchema, Schema.create(Schema.Type.NULL)) : customSchema;
                        break;
                }
                break;
            }
            default:
                throw new IllegalArgumentException(field.getName() + " is not supported for bigquery.");
        }

        if(field.isRepeated() && !field.isMapField()) {
            final Schema arraySchema = Schema.createArray(elementSchema);
            return nullable ? Schema.createUnion(arraySchema, Schema.create(Schema.Type.NULL)) : arraySchema;
        } else {
            return elementSchema;
        }

    }

    private static Object convertValue(final Descriptors.FieldDescriptor field,
                                       final Object value,
                                       final JsonFormat.Printer printer) {

        if(field.isRepeated()) {
            if(field.isMapField()) {
                if(value == null) {
                    return new HashMap<>();
                }
                return ((List<DynamicMessage>) value).stream()
                        .collect(Collectors.toMap(
                                e -> e.getField(field.getMessageType().findFieldByName("key")),
                                e -> e.getField(field.getMessageType().findFieldByName("value"))));
            }
            if(value == null) {
                return new ArrayList<>();
            }
            return ((List<Object>) value).stream()
                    .map(v -> getValue(field, v, printer))
                    .collect(Collectors.toList());
        }
        return getValue(field, value, printer);
    }

    private static Object getValue(final Descriptors.FieldDescriptor field,
                                   final Object value,
                                   final JsonFormat.Printer printer) {

        switch (field.getJavaType()) {
            case BOOLEAN:
            case LONG:
            case INT:
            case FLOAT:
            case DOUBLE:
            case STRING:
                return value;
            case ENUM: {
                return ((Descriptors.EnumValueDescriptor)value).getName();
            }
            case BYTE_STRING:
                return ((ByteString) value).asReadOnlyByteBuffer();
            case MESSAGE: {
                final Object object = ProtoUtil
                        .convertBuildInValue(field.getMessageType().getFullName(), (DynamicMessage) value);
                switch (ProtoUtil.ProtoType.of(field.getMessageType().getFullName())) {
                    case BOOL_VALUE:
                        return ((BoolValue) object).getValue();
                    case BYTES_VALUE:
                        return ((BytesValue) object).getValue().asReadOnlyByteBuffer();
                    case STRING_VALUE:
                        return ((StringValue) object).getValue();
                    case INT32_VALUE:
                        return ((Int32Value) object).getValue();
                    case INT64_VALUE:
                        return ((Int64Value) object).getValue();
                    case UINT32_VALUE:
                        return ((UInt32Value) object).getValue();
                    case UINT64_VALUE:
                        return ((UInt64Value) object).getValue();
                    case FLOAT_VALUE:
                        return ((FloatValue) object).getValue();
                    case DOUBLE_VALUE:
                        return ((DoubleValue) object).getValue();
                    case DATE: {
                        final Date date = (Date) object;
                        return (int) (LocalDate.of(date.getYear(), date.getMonth(), date.getDay()).toEpochDay());
                    }
                    case TIME: {
                        final TimeOfDay timeOfDay = (TimeOfDay) object;
                        return timeOfDay.getHours() * 60 * 60 + timeOfDay.getMinutes() * 60 + timeOfDay.getSeconds();
                    }
                    case DATETIME: {
                        final DateTime dt = (DateTime) object;
                        return LocalDateTime.of(
                                dt.getYear(), dt.getMonth(), dt.getDay(),
                                dt.getHours(), dt.getMinutes(), dt.getSeconds(), dt.getNanos())
                                .atOffset(ZoneOffset.ofTotalSeconds((int)dt.getUtcOffset().getSeconds()))
                                .toInstant()
                                .toEpochMilli() * 1000;
                    }
                    case TIMESTAMP:
                        return Timestamps.toMicros((Timestamp) object);
                    case ANY: {
                        final Any any = (Any) object;
                        try {
                            return printer.print(any);
                        } catch (InvalidProtocolBufferException e) {
                            return any.getValue().toStringUtf8();
                        }
                    }
                    case LATLNG: {
                        final LatLng ll = (LatLng) object;
                        return String.format("%f,%f", ll.getLatitude(), ll.getLongitude());
                    }
                    case EMPTY:
                    case NULL_VALUE:
                        return null;
                    case CUSTOM:
                    default: {
                        final Schema schema = convertSchema(field.getMessageType());
                        return convert(schema, field.getMessageType(), (DynamicMessage) value, printer);
                    }
                }
            }
            default:
                return null;
        }
    }

}
