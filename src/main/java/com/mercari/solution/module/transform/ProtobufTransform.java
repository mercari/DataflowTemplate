package com.mercari.solution.module.transform;

import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Value;
import com.google.gson.Gson;
import com.google.protobuf.Descriptors;
import com.google.protobuf.util.JsonFormat;
import com.mercari.solution.config.TransformConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.util.AvroSchemaUtil;
import com.mercari.solution.util.ProtoUtil;
import com.mercari.solution.util.RowSchemaUtil;
import com.mercari.solution.util.converter.*;
import com.mercari.solution.util.gcp.DatastoreUtil;
import com.mercari.solution.util.gcp.SpannerUtil;
import com.mercari.solution.util.gcp.StorageUtil;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public class ProtobufTransform {

    private static final Logger LOG = LoggerFactory.getLogger(ProtobufTransform.class);
    private static final String OUTPUT_SUFFIX_FAILURES = ".failures";

    private class ProtobufTransformParameters implements Serializable {

        private String descriptorFilePath;
        private List<ProtoParameter> fields;
        private Boolean failFast;

        public String getDescriptorFilePath() {
            return descriptorFilePath;
        }

        public void setDescriptorFilePath(String descriptorFilePath) {
            this.descriptorFilePath = descriptorFilePath;
        }

        public List<ProtoParameter> getFields() {
            return fields;
        }

        public void setFields(List<ProtoParameter> fields) {
            this.fields = fields;
        }

        public Boolean getFailFast() {
            return failFast;
        }

        public void setFailFast(Boolean failFast) {
            this.failFast = failFast;
        }

        private class ProtoParameter implements Serializable {

            private String field;
            private String messageName;
            private String outputField;

            public String getField() {
                return field;
            }

            public void setField(String field) {
                this.field = field;
            }

            public String getMessageName() {
                return messageName;
            }

            public void setMessageName(String messageName) {
                this.messageName = messageName;
            }

            public String getOutputField() {
                return outputField;
            }

            public void setOutputField(String outputField) {
                this.outputField = outputField;
            }
        }

    }

    public static Map<String, FCollection<?>> transform(final List<FCollection<?>> inputs, final TransformConfig config) {

        final ProtobufTransformParameters parameters = new Gson().fromJson(config.getParameters(), ProtobufTransformParameters.class);
        validateParameters(parameters);

        final byte[] descriptorContentBytes = StorageUtil.readBytes(parameters.descriptorFilePath);
        final Map<String, Descriptors.Descriptor> descriptors = ProtoUtil.getDescriptors(descriptorContentBytes);
        validateDescriptors(parameters, descriptors);

        setDefaultParameters(parameters);

        final List<String> excludeFields = parameters.getFields().stream()
                .filter(f -> f.getOutputField() == null || f.getField().equals(f.getOutputField()))
                .map(ProtobufTransformParameters.ProtoParameter::getField)
                .collect(Collectors.toList());

        final Map<String, FCollection<?>> results = new HashMap<>();
        for(final FCollection<?> input : inputs){
            final String name = config.getName() + (config.getInputs().size() == 1 ? "" : "." + input.getName());
            switch (input.getDataType()) {
                case AVRO: {
                    final FCollection<GenericRecord> inputCollection = (FCollection<GenericRecord>) input;
                    final Schema inputSchema = inputCollection.getAvroSchema();
                    final SchemaBuilder.RecordBuilder<Schema> avroSchemaBuilder = AvroSchemaUtil
                            .toSchemaBuilder(inputSchema, null, excludeFields);
                    final Map<String, String> messageSchemas = new HashMap<>();
                    final SchemaBuilder.FieldAssembler<Schema> fieldAssembler = avroSchemaBuilder.fields();
                    for(var field : parameters.getFields()) {
                        final Schema messageSchema = ProtoToRecordConverter.convertSchema(descriptors.get(field.getMessageName()));
                        messageSchemas.put(field.getField(), messageSchema.toString());
                        fieldAssembler
                                .name(field.getOutputField() == null ? field.getField() : field.getOutputField())
                                .type(Schema.createUnion(messageSchema, Schema.create(Schema.Type.NULL)))
                                .noDefault();
                    }
                    final Schema outputSchema = fieldAssembler.endRecord();
                    final Transform<GenericRecord, String, Schema> transform = new Transform<>(
                            parameters,
                            messageSchemas,
                            outputSchema.toString(),
                            AvroSchemaUtil::convertSchema,
                            AvroSchemaUtil::getBytes,
                            (Schema s, GenericRecord r, Map<String, GenericRecord> messages) -> {
                                GenericRecordBuilder builder = AvroSchemaUtil.copy(r, s);
                                for(var entry : messages.entrySet()) {
                                    builder.set(entry.getKey(), entry.getValue());
                                }
                                return builder.build();
                            },
                            ProtoToRecordConverter::convert);
                    final PCollectionTuple outputs = inputCollection.getCollection().apply(name, transform);
                    final PCollection<?> output = outputs.get(transform.outputTag).setCoder(AvroCoder.of(outputSchema));
                    final PCollection<?> failures = outputs.get(transform.failuresTag).setCoder(inputCollection.getCollection().getCoder());
                    results.put(name, FCollection.of(config.getName(), output, DataType.AVRO, outputSchema));
                    results.put(name + OUTPUT_SUFFIX_FAILURES, FCollection.of(config.getName(), failures, DataType.AVRO, inputSchema));
                    break;
                }
                case ROW: {
                    final FCollection<Row> inputCollection = (FCollection<Row>) input;
                    final org.apache.beam.sdk.schemas.Schema inputSchema = inputCollection.getSchema();
                    final Map<String, org.apache.beam.sdk.schemas.Schema> messageTypes = new HashMap<>();
                    final List<org.apache.beam.sdk.schemas.Schema.Field> fields = inputSchema.getFields().stream()
                            .filter(f -> !excludeFields.contains(f.getName()))
                            .collect(Collectors.toList());
                    for(var field : parameters.getFields()) {
                        final org.apache.beam.sdk.schemas.Schema messageSchema = ProtoToRowConverter.convertSchema(descriptors.get(field.getMessageName()));
                        messageTypes.put(field.getField(), messageSchema);
                        fields.add(org.apache.beam.sdk.schemas.Schema.Field.of(
                                field.getOutputField() == null ? field.getField() : field.getOutputField(),
                                org.apache.beam.sdk.schemas.Schema.FieldType.row(messageSchema)));
                    }
                    final org.apache.beam.sdk.schemas.Schema outputSchema = org.apache.beam.sdk.schemas.Schema.builder()
                            .addFields(fields)
                            .build();

                    final Transform<Row, org.apache.beam.sdk.schemas.Schema, org.apache.beam.sdk.schemas.Schema> transform = new Transform<>(
                            parameters,
                            messageTypes,
                            outputSchema,
                            s -> s,
                            RowSchemaUtil::getBytes,
                            RowSchemaUtil::merge,
                            ProtoToRowConverter::convert);
                    final PCollectionTuple outputs = inputCollection.getCollection().apply(name, transform);
                    final PCollection<?> output = outputs.get(transform.outputTag).setCoder(RowCoder.of(outputSchema));
                    final PCollection<?> failures = outputs.get(transform.failuresTag).setCoder(inputCollection.getCollection().getCoder());
                    results.put(name, FCollection.of(config.getName(), output, DataType.ROW, outputSchema));
                    results.put(name + OUTPUT_SUFFIX_FAILURES, FCollection.of(config.getName(), failures, DataType.ROW, inputSchema));
                    break;
                }
                case STRUCT: {
                    final FCollection<Struct> inputCollection = (FCollection<Struct>) input;
                    final Type inputSchema = inputCollection.getSpannerType();
                    final Map<String, Type> messageTypes = new HashMap<>();
                    final List<Type.StructField> fields = inputSchema.getStructFields().stream()
                            .filter(f -> !excludeFields.contains(f.getName()))
                            .collect(Collectors.toList());
                    for(var field : parameters.getFields()) {
                        final Type messageType = ProtoToStructConverter
                                .convertSchema(descriptors.get(field.getMessageName()));
                        messageTypes.put(field.getField(), messageType);
                        fields.add(Type.StructField.of(
                                field.getOutputField() == null ? field.getField() : field.getOutputField(),
                                messageType));
                    }
                    final Type outputType = Type.struct(fields);
                    final Transform<Struct, Type, Type> transform = new Transform<>(
                            parameters,
                            messageTypes,
                            outputType,
                            s -> s,
                            SpannerUtil::getBytes,
                            (Type t, Struct struct, Map<String, Struct> messages) -> {
                                    Struct.Builder builder = SpannerUtil.toBuilder(struct, null, messages.keySet());
                                    for(var entry : messages.entrySet()) {
                                        builder.set(entry.getKey()).to(entry.getValue()).build();
                                    }
                                    return builder.build();
                            },
                            ProtoToStructConverter::convert);

                    final PCollectionTuple outputs = inputCollection.getCollection().apply(name, transform);
                    final PCollection<?> output = outputs.get(transform.outputTag);
                    final PCollection<?> failures = outputs.get(transform.failuresTag).setCoder(inputCollection.getCollection().getCoder());
                    results.put(name, FCollection.of(config.getName(), output, DataType.STRUCT, outputType));
                    results.put(name + OUTPUT_SUFFIX_FAILURES, FCollection.of(config.getName(), failures, DataType.STRUCT, inputSchema));
                    break;
                }
                case ENTITY: {
                    final FCollection<Entity> inputCollection = (FCollection<Entity>) input;
                    final org.apache.beam.sdk.schemas.Schema inputSchema = inputCollection.getSchema();
                    final Map<String, org.apache.beam.sdk.schemas.Schema> messageTypes = new HashMap<>();
                    final List<org.apache.beam.sdk.schemas.Schema.Field> fields = inputSchema.getFields();
                    for(var field : parameters.getFields()) {
                        final org.apache.beam.sdk.schemas.Schema messageSchema = ProtoToRowConverter.convertSchema(descriptors.get(field.getMessageName()));
                        messageTypes.put(field.getField(), messageSchema);
                        fields.add(org.apache.beam.sdk.schemas.Schema.Field.of(
                                field.getOutputField() == null ? field.getField() : field.getOutputField(),
                                org.apache.beam.sdk.schemas.Schema.FieldType.row(messageSchema)));
                    }
                    final org.apache.beam.sdk.schemas.Schema outputSchema = org.apache.beam.sdk.schemas.Schema.builder()
                            .addFields(fields)
                            .build();

                    final Transform<Entity, org.apache.beam.sdk.schemas.Schema, org.apache.beam.sdk.schemas.Schema> transform = new Transform<>(
                            parameters,
                            messageTypes,
                            outputSchema,
                            s -> s,
                            DatastoreUtil::getBytes,
                            (org.apache.beam.sdk.schemas.Schema s, Entity e, Map<String, Entity> messages) -> {
                                Entity.Builder builder = Entity.newBuilder(e);
                                for(var entry : messages.entrySet()) {
                                    builder.putProperties(entry.getKey(), Value.newBuilder().setEntityValue(entry.getValue()).build());
                                }
                                return builder.build();
                            },
                            ProtoToEntityConverter::convert);

                    final PCollectionTuple outputs = inputCollection.getCollection().apply(name, transform);
                    final PCollection<?> output = outputs.get(transform.outputTag);
                    final PCollection<?> failures = outputs.get(transform.failuresTag).setCoder(inputCollection.getCollection().getCoder());
                    results.put(name, FCollection.of(config.getName(), output, DataType.ENTITY, outputSchema));
                    results.put(name + OUTPUT_SUFFIX_FAILURES, FCollection.of(config.getName(), failures, DataType.ENTITY, inputSchema));
                    break;
                }
                default:
                    throw new IllegalArgumentException("ProtobufTransform dows not support input type: " + input.getDataType().name());
            }
        }

        return results;
    }

    private static void validateParameters(final ProtobufTransformParameters parameters) {
        if(parameters == null) {
            throw new IllegalArgumentException("ProtobufTransform config parameters must not be empty!");
        }

        final List<String> errorMessages = new ArrayList<>();
        if(parameters.getDescriptorFilePath() == null) {
            errorMessages.add("ProtobufTransform config parameters must contain descriptorFilePath parameter.");
        }
        if(parameters.getFields() == null) {
            errorMessages.add("ProtobufTransform config parameters must contain fields parameter.");
        }
        for(var field : parameters.getFields()) {
            if(field.getField() == null || field.getMessageName() == null) {
                errorMessages.add("ProtobufTransform config parameters.fields must contain both field and messageName: "
                        + field.getField() + ", " + field.getMessageName());
            }
        }

        if(errorMessages.size() > 0) {
            throw new IllegalArgumentException(String.join("\n", errorMessages));
        }
    }

    private static void validateDescriptors(final ProtobufTransformParameters parameters,
                                            final Map<String, Descriptors.Descriptor> descriptors) {

        final List<String> errorMessages = new ArrayList<>();
        for(var field : parameters.getFields()) {
            if(!descriptors.containsKey(field.getMessageName())) {
                errorMessages.add("Descriptor file does not contain messageName: " + field.getMessageName());
            }
        }

        if(errorMessages.size() > 0) {
            errorMessages.add("Descriptor file only contains: " + String.join(",", descriptors.keySet()));
            throw new IllegalArgumentException(String.join("\n", errorMessages));
        }
    }

    private static void setDefaultParameters(ProtobufTransformParameters parameters) {
        if(parameters.getFailFast() == null) {
            parameters.setFailFast(true);
        }
        for(final ProtobufTransformParameters.ProtoParameter protoParameter : parameters.getFields()) {
            if(protoParameter.getOutputField() == null) {
                protoParameter.setOutputField(protoParameter.getField());
            }
        }
    }

    public static class Transform<T, InputSchemaT, RuntimeSchemaT> extends PTransform<PCollection<T>, PCollectionTuple> {

        private final TupleTag<T> outputTag;
        private final TupleTag<T> failuresTag;

        private final ProtobufTransformParameters parameters;

        private final Map<String, InputSchemaT> inputMessageSchemas;
        private final InputSchemaT inputResultSchema;
        private final SerializableFunction<InputSchemaT, RuntimeSchemaT> schemaConverter;
        private final FieldGetter<T> getter;
        private final FieldSetter<T, RuntimeSchemaT> setter;
        private final ProtoContenter<T, RuntimeSchemaT> converter;

        private Transform(final ProtobufTransformParameters parameters,
                          final Map<String, InputSchemaT> inputMessageSchemas,
                          final InputSchemaT inputResultSchema,
                          final SerializableFunction<InputSchemaT, RuntimeSchemaT> schemaConverter,
                          final FieldGetter<T> getter,
                          final FieldSetter<T, RuntimeSchemaT> setter,
                          final ProtoContenter<T, RuntimeSchemaT> converter) {

            this.parameters = parameters;

            this.inputMessageSchemas = inputMessageSchemas;
            this.inputResultSchema = inputResultSchema;
            this.schemaConverter = schemaConverter;
            this.getter = getter;
            this.setter = setter;
            this.converter = converter;

            this.outputTag = new TupleTag<>(){};
            this.failuresTag = new TupleTag<>(){};
        }

        @Override
        public PCollectionTuple expand(final PCollection<T> input) {

            final Map<String, String> messageNames = parameters.getFields().stream()
                    .collect(Collectors.toMap(
                            ProtobufTransformParameters.ProtoParameter::getField,
                            ProtobufTransformParameters.ProtoParameter::getMessageName));

            final ProtobufDoFn dofn = new ProtobufDoFn(
                    parameters.getDescriptorFilePath(),
                    messageNames,
                    inputMessageSchemas,
                    inputResultSchema,
                    schemaConverter,
                    getter,
                    setter,
                    converter,
                    parameters.getFailFast());

            return input.apply("Deserialize", ParDo
                    .of(dofn)
                    .withOutputTags(outputTag, TupleTagList.of(failuresTag)));
        }

        private class ProtobufDoFn extends DoFn<T, T> {

            private final String descriptorPath;
            private final Map<String, String> messageNames;
            private final Map<String, InputSchemaT> inputMessageSchemas;
            private final InputSchemaT inputResultSchema;

            private final FieldGetter<T> getter;
            private final FieldSetter<T, RuntimeSchemaT> setter;
            private final SerializableFunction<InputSchemaT, RuntimeSchemaT> schemaConverter;
            private final ProtoContenter<T, RuntimeSchemaT> converter;

            private final boolean failFast;

            private transient Map<String, RuntimeSchemaT> messageSchemas;
            private transient RuntimeSchemaT resultSchema;
            private transient Map<String, Descriptors.Descriptor> messageDescriptors;
            private transient Map<String, Descriptors.Descriptor> descriptors;

            private transient JsonFormat.Printer printer;

            ProtobufDoFn(final String descriptorPath,
                         final Map<String, String> messageNames,
                         final Map<String, InputSchemaT> inputMessageSchemas,
                         final InputSchemaT inputResultSchema,
                         final SerializableFunction<InputSchemaT, RuntimeSchemaT> schemaConverter,
                         final FieldGetter<T> getter,
                         final FieldSetter<T, RuntimeSchemaT> setter,
                         final ProtoContenter<T, RuntimeSchemaT> converter,
                         final boolean failFast) {

                this.descriptorPath = descriptorPath;
                this.messageNames = messageNames;
                this.inputMessageSchemas = inputMessageSchemas;
                this.inputResultSchema = inputResultSchema;
                this.schemaConverter = schemaConverter;
                this.getter = getter;
                this.setter = setter;
                this.converter = converter;
                this.failFast = failFast;
            }

            @Setup
            public void setup() {
                final byte[] bytes = StorageUtil.readBytes(descriptorPath);
                this.descriptors = ProtoUtil.getDescriptors(bytes);
                this.messageDescriptors = messageNames.entrySet().stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, e -> this.descriptors.get(e.getValue())));
                this.messageSchemas = inputMessageSchemas.entrySet().stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, e -> schemaConverter.apply(e.getValue())));
                this.resultSchema = schemaConverter.apply(inputResultSchema);

                final JsonFormat.TypeRegistry.Builder builder = JsonFormat.TypeRegistry.newBuilder();
                this.descriptors.forEach((k, v) -> builder.add(v));
                this.printer = JsonFormat.printer().usingTypeRegistry(builder.build());
            }

            @ProcessElement
            public void processElement(ProcessContext c) {
                final T input = c.element();
                try {
                    final Map<String, T> messages = new HashMap<>();
                    for (var entry : messageDescriptors.entrySet()) {
                        final byte[] protoBytes = getter.getBytes(input, entry.getKey());
                        final T message = converter.convert(
                                messageSchemas.get(entry.getKey()),
                                messageDescriptors.get(entry.getKey()), protoBytes, printer);
                        messages.put(entry.getKey(), message);
                    }
                    final T output = setter.setValue(resultSchema, input, messages);
                    c.output(output);
                } catch (final Error e) {
                    final String message = "Error with record: " + input + ", cause: " + e.toString();
                    if(failFast) {
                        throw new IllegalStateException(message, e);
                    } else {
                        LOG.error(message);
                        c.output(failuresTag, input);
                    }
                } catch (final RuntimeException e) {
                    final String message = "Failed to deserialize record: " + input + ", cause: " + e.toString();
                    if(failFast) {
                        throw new IllegalStateException(message, e);
                    } else {
                        LOG.error(message);
                        c.output(failuresTag, input);
                    }
                }
            }
        }
    }

    private interface ProtoContenter<T, SchemaT> extends Serializable {
        T convert(final SchemaT schema,
                  final Descriptors.Descriptor messageDescriptor,
                  final byte[] bytes,
                  final JsonFormat.Printer printer);
    }

    private interface FieldGetter<T> extends Serializable {
        byte[] getBytes(final T value, final String field);
    }

    private interface FieldSetter<T, SchemaT> extends Serializable {
        T setValue(final SchemaT schema, final T parent, final Map<String, T> children);
    }

}
