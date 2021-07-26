package com.mercari.solution.module.transform;

import com.google.cloud.ByteArray;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.datastore.v1.ArrayValue;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Value;
import com.google.gson.Gson;
import com.google.protobuf.NullValue;
import com.mercari.solution.config.TransformConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.module.TransformModule;
import com.mercari.solution.util.OptionUtil;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.EntitySchemaUtil;
import com.mercari.solution.util.schema.RowSchemaUtil;
import com.mercari.solution.util.schema.StructSchemaUtil;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;


public class BufferTransform implements TransformModule {

    private static final Logger LOG = LoggerFactory.getLogger(BufferTransform.class);

    private class BufferTransformParameters implements Serializable {

        private BufferType type;
        private Integer size;
        private List<String> keyFields;
        private List<String> remainFields;
        private List<String> bufferFields;
        private String bufferedField;
        private Boolean ascending;

        public BufferType getType() {
            return type;
        }

        public void setType(BufferType type) {
            this.type = type;
        }

        public Integer getSize() {
            return size;
        }

        public void setSize(Integer size) {
            this.size = size;
        }

        public List<String> getKeyFields() {
            return keyFields;
        }

        public void setKeyFields(List<String> keyFields) {
            this.keyFields = keyFields;
        }

        public List<String> getRemainFields() {
            return remainFields;
        }

        public void setRemainFields(List<String> remainFields) {
            this.remainFields = remainFields;
        }

        public List<String> getBufferFields() {
            return bufferFields;
        }

        public void setBufferFields(List<String> bufferFields) {
            this.bufferFields = bufferFields;
        }

        public String getBufferedField() {
            return bufferedField;
        }

        public void setBufferedField(String bufferedField) {
            this.bufferedField = bufferedField;
        }

        public Boolean getAscending() {
            return ascending;
        }

        public void setAscending(Boolean ascending) {
            this.ascending = ascending;
        }
    }

    private enum BufferType implements Serializable {
        structs,
        arrays,
        fields
    }

    public String getName() {
        return "buffer";
    }

    public Map<String, FCollection<?>> expand(List<FCollection<?>> inputs, TransformConfig config) {

        final BufferTransformParameters parameters = new Gson().fromJson(config.getParameters(), BufferTransformParameters.class);
        validateParameters(parameters);
        setDefaultParameters(parameters);

        final Map<String, FCollection<?>> results = new HashMap<>();
        for (final FCollection<?> input : inputs) {
            final String name = config.getName() + (config.getInputs().size() == 1 ? "" : "." + input.getName());
            final PCollection<?> output;
            switch (input.getDataType()) {
                case ROW: {
                    final Schema inputSchema;
                    if(parameters.getBufferFields() != null && parameters.getBufferFields().size() > 0) {
                        inputSchema = RowSchemaUtil.selectFields(input.getSchema(), parameters.getBufferFields());
                    } else {
                        inputSchema = input.getSchema();
                    }

                    final Schema outputSchema;
                    final Buffering<Row, Schema> buffering;
                    switch (parameters.getType()) {
                        case structs: {
                            outputSchema = RowSchemaUtil
                                    .toBuilder(input.getSchema(), parameters.getRemainFields())
                                    .addField(
                                            parameters.getBufferedField(),
                                            Schema.FieldType
                                                    .array(Schema.FieldType.row(inputSchema))
                                                    .withNullable(true))
                                    .build();

                            buffering = (Schema s, Row r, int size, List<Row> buffer, List<String> bufferFields, String f) -> RowSchemaUtil
                                    .toBuilder(s, r)
                                    .withFieldValue(f, buffer).build();
                            break;
                        }
                        case arrays: {
                            final List<String> remainFields = input.getSchema().getFieldNames();
                            remainFields.removeAll(parameters.getBufferFields());

                            final Schema.Builder builder = RowSchemaUtil
                                    .toBuilder(input.getSchema(), remainFields);
                            for(final String fieldName : parameters.getBufferFields()) {
                                final Schema.Field field = input.getSchema().getField(fieldName);
                                builder.addField(Schema.Field.of(fieldName, Schema.FieldType.array(field.getType()).withNullable(true)));
                            }
                            outputSchema = builder.build();

                            buffering = (Schema s, Row r, int size, List<Row> buffer, List<String> bufferFields, String f) -> {
                                final Row.FieldValueBuilder bufferBuilder = RowSchemaUtil
                                        .toBuilder(s, r);
                                for(final String bufferField : bufferFields) {
                                    bufferBuilder.withFieldValue(
                                            bufferField,
                                            buffer.stream()
                                                    .map(rr -> rr.getValue(bufferField))
                                                    .filter(Objects::nonNull)
                                                    .collect(Collectors.toList()));
                                }
                                return bufferBuilder.build();
                            };
                            break;
                        }
                        case fields: {
                            final Schema.Builder builder = RowSchemaUtil
                                    .toBuilder(input.getSchema());
                            for(final String fieldName : parameters.getBufferFields()) {
                                final Schema.Field field = input.getSchema().getField(fieldName);
                                for(int i=1; i<parameters.getSize(); i++) {
                                    final String newFieldName = fieldName + "_bf" + i;
                                    builder.addField(Schema.Field.of(newFieldName, field.getType()).withNullable(true));
                                }
                            }
                            outputSchema = builder.build();

                            buffering = (Schema s, Row r, int size, List<Row> buffer, List<String> bufferFields, String f) -> {
                                final Row.FieldValueBuilder bufferBuilder = RowSchemaUtil
                                        .toBuilder(s, r);
                                for(final String bufferField : bufferFields) {
                                    for(int i=1; i<size; i++) {
                                        final String newFieldName = bufferField + "_bf" + i;
                                        if(i < buffer.size()) {
                                            bufferBuilder.withFieldValue(
                                                    newFieldName,
                                                    buffer.get(i).getValue(bufferField));
                                        } else {
                                            bufferBuilder.withFieldValue(newFieldName, null);
                                        }
                                    }

                                }
                                return bufferBuilder.build();
                            };
                            break;
                        }
                        default: {
                            throw new IllegalStateException("BufferTransform: " + config.getName() + ", type: " + parameters.getType() + " is not supported!");
                        }
                    }

                    final FCollection<Row> inputCollection = (FCollection<Row>) input;
                    final Transform<Row,Schema,Schema> transform = new Transform<>(
                            parameters,
                            inputSchema,
                            outputSchema,
                            s -> s,
                            (Schema s, Row r) -> RowSchemaUtil.toBuilder(s, r).build(),
                            buffering,
                            RowSchemaUtil::getAsString,
                            RowCoder.of(inputSchema));
                    output = inputCollection.getCollection().apply(name, transform).setCoder(RowCoder.of(outputSchema));
                    results.put(name, FCollection.of(name, output, DataType.ROW, outputSchema));
                    break;
                }
                case AVRO: {
                    final org.apache.avro.Schema inputSchema;
                    if(parameters.getBufferFields() != null && parameters.getBufferFields().size() > 0) {
                        inputSchema = AvroSchemaUtil.selectFields(input.getAvroSchema(), parameters.getBufferFields());
                    } else {
                        inputSchema = input.getAvroSchema();
                    }

                    final org.apache.avro.Schema outputSchema;
                    final Buffering<GenericRecord, org.apache.avro.Schema> buffering;
                    switch (parameters.getType()) {
                        case structs: {
                            outputSchema = AvroSchemaUtil
                                    .selectFieldsBuilder(input.getAvroSchema(), parameters.getRemainFields(), parameters.getBufferedField())
                                    .name(parameters.getBufferedField())
                                    .type(org.apache.avro.Schema.createUnion(
                                            org.apache.avro.Schema.createArray(inputSchema),
                                            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.NULL)))
                                    .noDefault()
                                    .endRecord();

                            buffering = (org.apache.avro.Schema s, GenericRecord r, int size, List<GenericRecord> buffer, List<String> bufferFields, String f) -> AvroSchemaUtil
                                    .toBuilder(s, r)
                                    .set(f, buffer)
                                    .build();
                            break;
                        }
                        case arrays: {
                            final List<String> remainFields = input.getAvroSchema().getFields().stream()
                                    .map(org.apache.avro.Schema.Field::name)
                                    .collect(Collectors.toList());
                            remainFields.removeAll(parameters.getBufferFields());

                            final SchemaBuilder.FieldAssembler<org.apache.avro.Schema> builder = AvroSchemaUtil
                                    .toBuilder(input.getAvroSchema(), remainFields);
                            for(final String fieldName : parameters.getBufferFields()) {
                                final org.apache.avro.Schema.Field field = input.getAvroSchema().getField(fieldName);
                                builder
                                        .name(fieldName)
                                        .type(org.apache.avro.Schema.createUnion(
                                                org.apache.avro.Schema.create(org.apache.avro.Schema.Type.NULL),
                                                org.apache.avro.Schema.createArray(field.schema())))
                                        .withDefault(null);
                            }
                            outputSchema = builder.endRecord();

                            buffering = (org.apache.avro.Schema s, GenericRecord r, int size, List<GenericRecord> buffer, List<String> bufferFields, String f) -> {
                                final GenericRecordBuilder bufferBuilder = AvroSchemaUtil.toBuilder(s, r);
                                for(final String bufferField : bufferFields) {
                                    bufferBuilder.set(
                                            bufferField,
                                            buffer.stream()
                                                    .map(rr -> rr.get(bufferField))
                                                    .filter(Objects::nonNull)
                                                    .collect(Collectors.toList()));
                                }
                                return bufferBuilder.build();
                            };
                            break;
                        }
                        case fields: {
                            final SchemaBuilder.FieldAssembler<org.apache.avro.Schema> builder = AvroSchemaUtil
                                    .toBuilder(input.getAvroSchema());
                            for(final String fieldName : parameters.getBufferFields()) {
                                final org.apache.avro.Schema.Field field = input.getAvroSchema().getField(fieldName);
                                for(int i=1; i<parameters.getSize(); i++) {
                                    final String newFieldName = fieldName + "_bf" + i;
                                    builder
                                            .name(newFieldName)
                                            .type(field.schema())
                                            .withDefault(null);
                                }
                            }
                            outputSchema = builder.endRecord();

                            buffering = (org.apache.avro.Schema s, GenericRecord r, int size, List<GenericRecord> buffer, List<String> bufferFields, String f) -> {
                                final GenericRecordBuilder bufferBuilder = AvroSchemaUtil.toBuilder(s, r);
                                for(final String bufferField : bufferFields) {
                                    for(int i=1; i<size; i++) {
                                        final String newFieldName = bufferField + "_bf" + i;
                                        if(i < buffer.size()) {
                                            bufferBuilder.set(
                                                    newFieldName,
                                                    buffer.get(i).get(bufferField));
                                        } else {
                                            bufferBuilder.set(newFieldName, null);
                                        }
                                    }
                                }
                                return bufferBuilder.build();
                            };
                            break;
                        }
                        default: {
                            throw new IllegalStateException("BufferTransform: " + config.getName() + ", type: " + parameters.getType() + " is not supported!");
                        }
                    }
                    final FCollection<GenericRecord> inputCollection = (FCollection<GenericRecord>) input;
                    final Transform<GenericRecord, String, org.apache.avro.Schema> transform = new Transform<>(
                            parameters,
                            inputSchema.toString(),
                            outputSchema.toString(),
                            AvroSchemaUtil::convertSchema,
                            (org.apache.avro.Schema s, GenericRecord r) -> AvroSchemaUtil.toBuilder(s, r).build(),
                            buffering,
                            AvroSchemaUtil::getAsString,
                            AvroCoder.of(inputSchema));
                    output = inputCollection.getCollection().apply(name, transform).setCoder(AvroCoder.of(outputSchema));
                    results.put(name, FCollection.of(name, output, DataType.AVRO, outputSchema));
                    break;
                }
                case STRUCT: {
                    final Type inputType;
                    if(parameters.getBufferFields() != null && parameters.getBufferFields().size() > 0) {
                        inputType = StructSchemaUtil.selectFields(input.getSpannerType(), parameters.getBufferFields());
                    } else {
                        inputType = input.getSpannerType();
                    }

                    final Type outputType;
                    final Buffering<Struct, Type> buffering;
                    switch (parameters.getType()) {
                        case structs: {
                            final List<Type.StructField> structFields = StructSchemaUtil
                                    .selectFieldsBuilder(input.getSpannerType(), parameters.getRemainFields());
                            structFields.add(Type.StructField.of(parameters.getBufferedField(), inputType));
                            outputType = Type.struct(structFields);

                            buffering = (Type t, Struct s, int size, List<Struct> b, List<String> fs, String f) -> StructSchemaUtil
                                    .toBuilder(t, s)
                                    .set(f)
                                    .toStructArray(t.getStructFields().get(t.getFieldIndex(f)).getType(), b)
                                    .build();
                            break;
                        }
                        case arrays: {
                            final List<String> remainFields = input.getSpannerType().getStructFields().stream()
                                    .map(Type.StructField::getName)
                                    .collect(Collectors.toList());
                            remainFields.removeAll(parameters.getBufferFields());

                            final List<Type.StructField> structFields = StructSchemaUtil
                                    .selectFieldsBuilder(input.getSpannerType(), remainFields);
                            for(final String fieldName : parameters.getBufferFields()) {
                                for(final Type.StructField structField : input.getSpannerType().getStructFields()) {
                                    if(structField.getName().equals(fieldName)) {
                                        structFields.add(Type.StructField.of(fieldName, Type.array(structField.getType())));
                                    }
                                }
                            }
                            outputType = Type.struct(structFields);

                            buffering = (Type t, Struct s, int size, List<Struct> b, List<String> fs, String f) -> {
                                Struct.Builder bufferBuilder = StructSchemaUtil.toBuilder(t, s);
                                for(final String bufferField : fs) {
                                    var vb = bufferBuilder.set(bufferField);
                                    final Type.StructField structField = t.getStructFields().stream()
                                            .filter(sf -> sf.getName().equals(bufferField))
                                            .findAny()
                                            .orElseThrow();
                                    switch (structField.getType().getArrayElementType().getCode()) {
                                        case BOOL: {
                                            bufferBuilder = vb.toBoolArray(b.stream()
                                                    .map(ss -> ss.getBoolean(bufferField))
                                                    .filter(Objects::nonNull)
                                                    .collect(Collectors.toList()));
                                            break;
                                        }
                                        case STRING: {
                                            bufferBuilder = vb.toStringArray(b.stream()
                                                    .map(ss -> ss.getString(bufferField))
                                                    .filter(Objects::nonNull)
                                                    .collect(Collectors.toList()));
                                            break;
                                        }
                                        case BYTES: {
                                            bufferBuilder = vb.toBytesArray(b.stream()
                                                    .map(ss -> ss.getBytes(bufferField))
                                                    .filter(Objects::nonNull)
                                                    .collect(Collectors.toList()));
                                            break;
                                        }
                                        case INT64: {
                                            bufferBuilder = vb.toInt64Array(b.stream()
                                                    .map(ss -> ss.getLong(bufferField))
                                                    .filter(Objects::nonNull)
                                                    .collect(Collectors.toList()));
                                            break;
                                        }
                                        case FLOAT64: {
                                            bufferBuilder = vb.toFloat64Array(b.stream()
                                                    .map(ss -> ss.getDouble(bufferField))
                                                    .filter(Objects::nonNull)
                                                    .collect(Collectors.toList()));
                                            break;
                                        }
                                        case NUMERIC: {
                                            bufferBuilder = vb.toNumericArray(b.stream()
                                                    .map(ss -> ss.getBigDecimal(bufferField))
                                                    .filter(Objects::nonNull)
                                                    .collect(Collectors.toList()));
                                            break;
                                        }
                                        case DATE: {
                                            bufferBuilder = vb.toDateArray(b.stream()
                                                    .map(ss -> ss.getDate(bufferField))
                                                    .filter(Objects::nonNull)
                                                    .collect(Collectors.toList()));
                                            break;
                                        }
                                        case TIMESTAMP: {
                                            bufferBuilder = vb.toTimestampArray(b.stream()
                                                    .map(ss -> ss.getTimestamp(bufferField))
                                                    .filter(Objects::nonNull)
                                                    .collect(Collectors.toList()));
                                            break;
                                        }
                                        case STRUCT: {
                                            bufferBuilder = vb.toStructArray(structField.getType().getArrayElementType(), b.stream()
                                                    .map(ss -> ss.getStruct(bufferField))
                                                    .filter(Objects::nonNull)
                                                    .collect(Collectors.toList()));
                                            break;
                                        }
                                        case ARRAY:
                                        default: {
                                            throw new IllegalStateException("");
                                        }
                                    }
                                }
                                return bufferBuilder.build();
                            };
                            break;
                        }
                        case fields: {
                            final List<Type.StructField> structFields = input.getSpannerType().getStructFields();
                            for(final String fieldName : parameters.getBufferFields()) {
                                for(final Type.StructField structField : input.getSpannerType().getStructFields()) {
                                    if(structField.getName().equals(fieldName)) {
                                        for(int i=1; i<parameters.getSize(); i++) {
                                            final String newFieldName = fieldName + "_bf" + i;
                                            structFields.add(Type.StructField.of(newFieldName, structField.getType()));
                                        }
                                    }
                                }
                            }
                            outputType = Type.struct(structFields);

                            buffering = (Type s, Struct e, int size, List<Struct> buffer, List<String> fs, String f) -> {
                                Struct.Builder bufferBuilder = StructSchemaUtil.toBuilder(s, e);
                                for(final String bufferField : fs) {
                                    final Type type = buffer.get(0).getColumnType(bufferField);
                                    for(int i=1; i<size; i++) {
                                        final String newFieldName = bufferField + "_bf" + i;
                                        switch (type.getCode()) {
                                            case BOOL: {
                                                if(i < buffer.size()) {
                                                    bufferBuilder = bufferBuilder.set(newFieldName).to(buffer.get(i).getBoolean(bufferField));
                                                } else {
                                                    bufferBuilder = bufferBuilder.set(newFieldName).to((Boolean)null);
                                                }
                                                break;
                                            }
                                            case STRING: {
                                                if(i < buffer.size()) {
                                                    bufferBuilder = bufferBuilder.set(newFieldName).to(buffer.get(i).getString(bufferField));
                                                } else {
                                                    bufferBuilder = bufferBuilder.set(newFieldName).to((String)null);
                                                }
                                                break;
                                            }
                                            case BYTES: {
                                                if(i < buffer.size()) {
                                                    bufferBuilder = bufferBuilder.set(newFieldName).to(buffer.get(i).getBytes(bufferField));
                                                } else {
                                                    bufferBuilder = bufferBuilder.set(newFieldName).to((ByteArray) null);
                                                }
                                                break;
                                            }
                                            case INT64: {
                                                if(i < buffer.size()) {
                                                    bufferBuilder = bufferBuilder.set(newFieldName).to(buffer.get(i).getLong(bufferField));
                                                } else {
                                                    bufferBuilder = bufferBuilder.set(newFieldName).to((Long)null);
                                                }
                                                break;
                                            }
                                            case FLOAT64: {
                                                if(i < buffer.size()) {
                                                    bufferBuilder = bufferBuilder.set(newFieldName).to(buffer.get(i).getDouble(bufferField));
                                                } else {
                                                    bufferBuilder = bufferBuilder.set(newFieldName).to((Double)null);
                                                }
                                                break;
                                            }
                                            case NUMERIC: {
                                                if(i < buffer.size()) {
                                                    bufferBuilder = bufferBuilder.set(newFieldName).to(buffer.get(i).getBigDecimal(bufferField));
                                                } else {
                                                    bufferBuilder = bufferBuilder.set(newFieldName).to((BigDecimal) null);
                                                }
                                                break;
                                            }
                                            case DATE: {
                                                if(i < buffer.size()) {
                                                    bufferBuilder = bufferBuilder.set(newFieldName).to(buffer.get(i).getDate(bufferField));
                                                } else {
                                                    bufferBuilder = bufferBuilder.set(newFieldName).to((com.google.cloud.Date)null);
                                                }
                                                break;
                                            }
                                            case TIMESTAMP: {
                                                if(i < buffer.size()) {
                                                    bufferBuilder = bufferBuilder.set(newFieldName).to(buffer.get(i).getTimestamp(bufferField));
                                                } else {
                                                    bufferBuilder = bufferBuilder.set(newFieldName).to((com.google.cloud.Timestamp)null);
                                                }
                                                break;
                                            }
                                            case STRUCT: {
                                                if(i < buffer.size()) {
                                                    bufferBuilder = bufferBuilder.set(newFieldName).to(buffer.get(i).getStruct(bufferField));
                                                } else {
                                                    bufferBuilder = bufferBuilder.set(newFieldName).to((Struct)null);
                                                }
                                                break;
                                            }
                                            case ARRAY: {
                                                throw new IllegalArgumentException();
                                            }
                                        }
                                    }
                                }
                                return bufferBuilder.build();
                            };

                            break;
                        }
                        default: {
                            throw new IllegalStateException("BufferTransform: " + config.getName() + ", type: " + parameters.getType() + " is not supported!");
                        }
                    }

                    final FCollection<Struct> inputCollection = (FCollection<Struct>) input;
                    final Transform<Struct, Type, Type> transform = new Transform<>(
                            parameters,
                            inputType,
                            outputType,
                            t -> t,
                            (Type t, Struct s) -> StructSchemaUtil.toBuilder(t, s).build(),
                            buffering,
                            StructSchemaUtil::getAsString,
                            SerializableCoder.of(Struct.class));
                    output = inputCollection.getCollection().apply(name, transform);
                    results.put(name, FCollection.of(name, output, DataType.STRUCT, outputType));
                    break;
                }
                case ENTITY: {
                    final Schema inputSchema;
                    if(parameters.getBufferFields() != null && parameters.getBufferFields().size() > 0) {
                        inputSchema = RowSchemaUtil.selectFields(input.getSchema(), parameters.getBufferFields());
                    } else {
                        inputSchema = input.getSchema();
                    }

                    final Schema outputSchema;
                    final Buffering<Entity, Schema> buffering;
                    switch (parameters.getType()) {
                        case structs: {
                            outputSchema = RowSchemaUtil.selectFieldsBuilder(input.getSchema(), parameters.getRemainFields())
                                    .addField(
                                            parameters.getBufferedField(),
                                            Schema.FieldType
                                                    .array(Schema.FieldType.row(inputSchema))
                                                    .withNullable(true))
                                    .build();

                            buffering = (Schema s, Entity e, int size, List<Entity> b, List<String> fs, String f) -> EntitySchemaUtil
                                    .toBuilder(s, e)
                                    .putProperties(f, Value.newBuilder()
                                            .setArrayValue(ArrayValue.newBuilder()
                                                    .addAllValues(b.stream()
                                                            .map(ee -> Value.newBuilder().setEntityValue(ee).build())
                                                            .collect(Collectors.toList()))
                                                    .build())
                                            .build())
                                    .build();
                            break;
                        }
                        case arrays: {
                            final List<String> remainFields = input.getSchema().getFieldNames();
                            remainFields.removeAll(parameters.getBufferFields());

                            final Schema.Builder builder = RowSchemaUtil.toBuilder(input.getSchema(), remainFields);
                            for(final String fieldName : parameters.getBufferFields()) {
                                final Schema.Field field = input.getSchema().getField(fieldName);
                                builder.addField(Schema.Field.of(fieldName, Schema.FieldType.array(field.getType()).withNullable(true)));
                            }
                            outputSchema = builder.build();

                            buffering = (Schema s, Entity e, int size, List<Entity> b, List<String> fs, String f) -> {
                                final Map<String, Value> properties = new HashMap<>();
                                for(final String bufferField : fs) {
                                    properties.put(bufferField, Value.newBuilder()
                                            .setArrayValue(
                                                    ArrayValue.newBuilder()
                                                            .addAllValues(b.stream()
                                                                    .map(ee -> ee.getPropertiesOrDefault(bufferField, Value
                                                                            .newBuilder()
                                                                            .setNullValue(NullValue.NULL_VALUE)
                                                                            .build()))
                                                                    .collect(Collectors.toList()))
                                                            .build())
                                            .build());
                                }
                                return EntitySchemaUtil
                                        .toBuilder(s, e)
                                        .putAllProperties(properties)
                                        .build();
                            };
                            break;
                        }
                        case fields: {
                            final Schema.Builder builder = RowSchemaUtil.toBuilder(input.getSchema());
                            for(final String fieldName : parameters.getBufferFields()) {
                                final Schema.Field field = input.getSchema().getField(fieldName);
                                for(int i=1; i<parameters.getSize(); i++) {
                                    final String newFieldName = fieldName + "_bf" + i;
                                    builder.addField(Schema.Field.of(newFieldName, field.getType()).withNullable(true));
                                }
                            }
                            outputSchema = builder.build();

                            buffering = (Schema s, Entity e, int size, List<Entity> buffer, List<String> fs, String f) -> {
                                final Map<String, Value> properties = new HashMap<>();
                                for(final String bufferField : fs) {
                                    for(int i=1; i<size; i++) {
                                        final String newFieldName = bufferField + "_bf" + i;
                                        if(i < buffer.size()) {
                                            properties.put(newFieldName, buffer.get(i).getPropertiesOrDefault(bufferField, Value
                                                    .newBuilder()
                                                    .setNullValue(NullValue.NULL_VALUE)
                                                    .build()));
                                        } else {
                                            properties.put(newFieldName, Value
                                                    .newBuilder()
                                                    .setNullValue(NullValue.NULL_VALUE)
                                                    .build());
                                        }
                                    }
                                }
                                return EntitySchemaUtil
                                        .toBuilder(s, e)
                                        .putAllProperties(properties)
                                        .build();
                            };
                            break;
                        }
                        default: {
                            throw new IllegalStateException("BufferTransform: " + config.getName() + ", type: " + parameters.getType() + " is not supported!");
                        }
                    }

                    final FCollection<Entity> inputCollection = (FCollection<Entity>) input;
                    final Transform<Entity,Schema,Schema> transform = new Transform<>(
                            parameters,
                            inputSchema,
                            outputSchema,
                            s -> s,
                            (Schema s, Entity e) -> EntitySchemaUtil.toBuilder(s, e).build(),
                            buffering,
                            EntitySchemaUtil::getAsString,
                            SerializableCoder.of(Entity.class));
                    output = inputCollection.getCollection().apply(name, transform);
                    results.put(name, FCollection.of(name, output, DataType.ENTITY, outputSchema));
                    break;
                }
                default:
                    throw new IllegalStateException("BufferTransform not support input type: " + input.getDataType());
            }
        }

        return results;
    }

    private static void validateParameters(final BufferTransformParameters parameters) {
        if(parameters == null) {
            throw new IllegalArgumentException("BufferTransform config parameters must not be empty!");
        }

        final List<String> errorMessages = new ArrayList<>();
        if(parameters.getSize() == null) {
            errorMessages.add("BufferTransform config parameters must contain size parameter.");
        } else if(parameters.getSize() == 0) {
            errorMessages.add("BufferTransform size parameter must be greater than zero.");
        }

        if(errorMessages.size() > 0) {
            throw new IllegalArgumentException(String.join("\n", errorMessages));
        }
    }

    private static void setDefaultParameters(BufferTransformParameters parameters) {
        if(parameters.getType() == null) {
            parameters.setType(BufferType.structs);
        }
        if(parameters.getBufferedField() == null) {
            parameters.setBufferedField("buffer");
        }
        if(parameters.getBufferFields() == null) {
            parameters.setBufferFields(new ArrayList<>());
        }
        if(parameters.getAscending() == null) {
            parameters.setAscending(true);
        }

        if(parameters.getType().equals(BufferType.fields)) {
            parameters.setSize(parameters.getSize() + 1);
        }
    }

    public static class Transform<T,InputSchemaT,RuntimeSchemaT> extends PTransform<PCollection<T>, PCollection<T>> {

        private final BufferTransformParameters parameters;
        private final InputSchemaT inputSchema;
        private final InputSchemaT outputSchema;
        private final SchemaConverter<InputSchemaT,RuntimeSchemaT> schemaConverter;
        private final Selector<T,RuntimeSchemaT> selector;
        private final Buffering<T,RuntimeSchemaT> buffering;
        private final StringGetter<T> stringGetter;
        private final Coder<T> coder;

        private Transform(final BufferTransformParameters parameters,
                          final InputSchemaT inputSchema,
                          final InputSchemaT outputSchema,
                          final SchemaConverter<InputSchemaT,RuntimeSchemaT> schemaConverter,
                          final Selector<T,RuntimeSchemaT> selector,
                          final Buffering<T,RuntimeSchemaT> buffering,
                          final StringGetter<T> stringGetter,
                          final Coder<T> coder) {

            this.parameters = parameters;
            this.inputSchema = inputSchema;
            this.outputSchema = outputSchema;
            this.schemaConverter = schemaConverter;
            this.selector = selector;
            this.buffering = buffering;
            this.stringGetter = stringGetter;
            this.coder = coder;
        }

        @Override
        public PCollection<T> expand(final PCollection<T> input) {

            final PCollection<KV<String, T>> withKey;
            if (parameters.getKeyFields().size() > 0) {
                final List<String> symbolFields = parameters.getKeyFields();
                withKey = input.apply("WithKey", WithKeys.of((T t) -> {
                    final StringBuilder sb = new StringBuilder();
                    for(String field : symbolFields) {
                        final String key = stringGetter.getAsString(t, field);
                        sb.append(key == null ? "" : key);
                        sb.append("#");
                    }
                    if(sb.length() > 0) {
                        sb.deleteCharAt(sb.length() - 1);
                    }
                    return sb.toString();
                }).withKeyType(TypeDescriptors.strings()));
            } else {
                withKey = input
                        .apply("WithFixedKey", WithKeys.of(""))
                        .setCoder(KvCoder.of(StringUtf8Coder.of(), input.getCoder()));
            }

            final PCollection<T> output;
            if(OptionUtil.isStreaming(input.getPipeline().getOptions())) {
                output = withKey.apply("StreamingTransform", ParDo
                        .of(new BufferStreamingDoFn<>(
                                parameters.getSize(), parameters.getBufferFields(), parameters.getBufferedField(), parameters.getAscending(),
                                inputSchema, outputSchema, schemaConverter, selector, buffering, coder)));
            } else {
                output = withKey.apply("BatchTransform", ParDo
                        .of(new BufferBatchDoFn<>(
                                parameters.getSize(), parameters.getBufferFields(), parameters.getBufferedField(), parameters.getAscending(),
                                inputSchema, outputSchema, schemaConverter, selector, buffering, coder)));
            }

            return output;
        }

        private static abstract class BufferDoFn<T, InputSchemaT, RuntimeSchemaT> extends DoFn<KV<String, T>, T> {

            static final String STATEID_VALUES = "values";

            private final Integer size;
            private final List<String> bufferFields;
            private final String bufferedField;
            private final InputSchemaT inputSchema;
            private final InputSchemaT outputSchema;
            private final SchemaConverter<InputSchemaT, RuntimeSchemaT> schemaConverter;
            private final Selector<T, RuntimeSchemaT> selector;
            private final Buffering<T, RuntimeSchemaT> buffering;
            private final Boolean ascending;

            private transient RuntimeSchemaT runtimeInputSchema;
            private transient RuntimeSchemaT runtimeOutputSchema;

            BufferDoFn(final Integer size,
                       final List<String> bufferFields,
                       final String bufferedField,
                       final Boolean ascending,
                       final InputSchemaT inputSchema,
                       final InputSchemaT outputSchema,
                       final SchemaConverter<InputSchemaT,RuntimeSchemaT> schemaConverter,
                       final Selector<T,RuntimeSchemaT> selector,
                       final Buffering<T,RuntimeSchemaT> buffering) {

                this.size = size;
                this.bufferFields = bufferFields;
                this.bufferedField = bufferedField;
                this.ascending = ascending;
                this.inputSchema = inputSchema;
                this.outputSchema = outputSchema;
                this.schemaConverter = schemaConverter;
                this.selector = selector;
                this.buffering = buffering;
            }

            @Setup
            public void setup() {
                this.runtimeInputSchema = schemaConverter.convert(inputSchema);
                this.runtimeOutputSchema = schemaConverter.convert(outputSchema);
            }

            void process(final ProcessContext c,
                         final ValueState<List<T>> valuesState) {

                final T element = c.element().getValue();
                final T selected = selector.convert(runtimeInputSchema, element);
                final T base = selector.convert(runtimeOutputSchema, element);
                final List<T> buffer = Optional.ofNullable(valuesState.read()).orElse(new ArrayList<>());
                if(ascending) {
                    buffer.add(selected);
                } else {
                    buffer.add(0, selected);
                }

                final T output = buffering.convert(runtimeOutputSchema, base, size, buffer, bufferFields, bufferedField);
                c.output(output);

                while(buffer.size() > size) {
                    if(ascending) {
                        buffer.remove(0);
                    } else {
                        buffer.remove(buffer.size() - 1);
                    }
                }
                valuesState.write(buffer);
            }
        }

        private static class BufferBatchDoFn<T, InputSchemaT, RuntimeSchemaT> extends BufferDoFn<T, InputSchemaT, RuntimeSchemaT> {

            @StateId(STATEID_VALUES)
            private final StateSpec<ValueState<List<T>>> values;

            BufferBatchDoFn(final Integer size,
                            final List<String> bufferFields,
                            final String bufferedField,
                            final Boolean ascending,
                            final InputSchemaT inputSchema,
                            final InputSchemaT outputSchema,
                            final SchemaConverter<InputSchemaT, RuntimeSchemaT> schemaConverter,
                            final Selector<T, RuntimeSchemaT> selector,
                            final Buffering<T,RuntimeSchemaT> buffering,
                            final Coder<T> coder) {

                super(size, bufferFields, bufferedField, ascending, inputSchema, outputSchema, schemaConverter, selector, buffering);
                this.values = StateSpecs.value(ListCoder.of(coder));
            }

            @Setup
            public void setup() {
                super.setup();
            }

            @ProcessElement
            @RequiresTimeSortedInput
            public void processElement(final ProcessContext c,
                                       final @StateId(STATEID_VALUES) ValueState<List<T>> valuesState) {

                process(c, valuesState);
            }
        }

        private static class BufferStreamingDoFn<T, InputSchemaT, RuntimeSchemaT> extends BufferDoFn<T, InputSchemaT, RuntimeSchemaT> {

            @StateId(STATEID_VALUES)
            private final StateSpec<ValueState<List<T>>> values;

            BufferStreamingDoFn(final Integer size,
                                final List<String> bufferFields,
                                final String bufferedField,
                                final Boolean ascending,
                                final InputSchemaT inputSchema,
                                final InputSchemaT outputSchema,
                                final SchemaConverter<InputSchemaT, RuntimeSchemaT> schemaConverter,
                                final Selector<T, RuntimeSchemaT> selector,
                                final Buffering<T,RuntimeSchemaT> buffering,
                                final Coder<T> coder) {

                super(size, bufferFields, bufferedField, ascending, inputSchema, outputSchema, schemaConverter, selector, buffering);
                this.values = StateSpecs.value(ListCoder.of(coder));
            }

            @Setup
            public void setup() {
                super.setup();
            }

            @ProcessElement
            public void processElement(final ProcessContext c,
                                       final @StateId(STATEID_VALUES) ValueState<List<T>> valuesState) {

                process(c, valuesState);
            }
        }

    }

    private interface StringGetter<T> extends Serializable {
        String getAsString(final T value, final String field);
    }

    private interface SchemaConverter<InputSchemaT, RuntimeSchemaT> extends Serializable {
        RuntimeSchemaT convert(final InputSchemaT schema);
    }

    private interface Selector<T, SchemaT> extends Serializable {
        T convert(final SchemaT schema, final T element);
    }

    private interface Buffering<T, SchemaT> extends Serializable {
        T convert(final SchemaT schema, final T base,
                  final int size,
                  final List<T> buffer,
                  final List<String> bufferFields,
                  final String bufferedField);
    }

}
