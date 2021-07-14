package com.mercari.solution.module.transform;

import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.datastore.v1.ArrayValue;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Value;
import com.google.gson.Gson;
import com.mercari.solution.config.TransformConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.module.TransformModule;
import com.mercari.solution.util.OptionUtil;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.EntitySchemaUtil;
import com.mercari.solution.util.schema.RowSchemaUtil;
import com.mercari.solution.util.schema.StructSchemaUtil;
import org.apache.avro.generic.GenericRecord;
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
import java.util.*;
import java.util.stream.Collectors;


public class BufferTransform implements TransformModule {

    private static final Logger LOG = LoggerFactory.getLogger(BufferTransform.class);

    private class BufferTransformParameters implements Serializable {

        private Integer size;
        private List<String> keyFields;
        private List<String> remainFields;
        private List<String> bufferFields;
        private String bufferedField;
        private Boolean ascending;

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
                    if(parameters.getBufferFields().size() > 0) {
                        inputSchema = RowSchemaUtil.selectFields(input.getSchema(), parameters.getBufferFields());
                    } else {
                        inputSchema = input.getSchema();
                    }
                    final Schema outputSchema = RowSchemaUtil.selectFieldsBuilder(input.getSchema(), parameters.getRemainFields())
                            .addField(
                                    parameters.getBufferedField(),
                                    Schema.FieldType
                                            .array(Schema.FieldType.row(inputSchema))
                                            .withNullable(true))
                            .build();
                    final FCollection<Row> inputCollection = (FCollection<Row>) input;
                    final Transform<Row,Schema,Schema> transform = new Transform<>(
                            parameters,
                            inputSchema,
                            outputSchema,
                            s -> s,
                            (Schema s, Row r) -> RowSchemaUtil.toBuilder(s, r).build(),
                            (Schema s, Row r, String f, List<Row> b) -> RowSchemaUtil
                                    .toBuilder(s, r)
                                    .withFieldValue(f, b).build(),
                            RowSchemaUtil::getAsString,
                            RowCoder.of(inputSchema));
                    output = inputCollection.getCollection().apply(name, transform).setCoder(RowCoder.of(outputSchema));
                    results.put(name, FCollection.of(name, output, DataType.ROW, outputSchema));
                    break;
                }
                case AVRO: {
                    final org.apache.avro.Schema inputSchema;
                    if(parameters.getBufferFields().size() > 0) {
                        inputSchema = AvroSchemaUtil.selectFields(input.getAvroSchema(), parameters.getBufferFields());
                    } else {
                        inputSchema = input.getAvroSchema();
                    }
                    final org.apache.avro.Schema outputSchema = AvroSchemaUtil
                            .selectFieldsBuilder(input.getAvroSchema(), parameters.getRemainFields(), parameters.getBufferedField())
                            .name(parameters.getBufferedField())
                            .type(org.apache.avro.Schema.createUnion(
                                    org.apache.avro.Schema.createArray(inputSchema),
                                    org.apache.avro.Schema.create(org.apache.avro.Schema.Type.NULL)))
                            .noDefault()
                            .endRecord();
                    final FCollection<GenericRecord> inputCollection = (FCollection<GenericRecord>) input;
                    final Transform<GenericRecord, String, org.apache.avro.Schema> transform = new Transform<>(
                            parameters,
                            inputSchema.toString(),
                            outputSchema.toString(),
                            AvroSchemaUtil::convertSchema,
                            (org.apache.avro.Schema s, GenericRecord r) -> AvroSchemaUtil.toBuilder(s, r).build(),
                            (org.apache.avro.Schema s, GenericRecord r, String f, List<GenericRecord> b) -> AvroSchemaUtil
                                    .toBuilder(s, r)
                                    .set(f, b)
                                    .build(),
                            AvroSchemaUtil::getAsString,
                            AvroCoder.of(inputSchema));
                    output = inputCollection.getCollection().apply(name, transform).setCoder(AvroCoder.of(outputSchema));
                    results.put(name, FCollection.of(name, output, DataType.AVRO, outputSchema));
                    break;
                }
                case STRUCT: {
                    final Type inputType;
                    if(parameters.getBufferFields().size() > 0) {
                        inputType = StructSchemaUtil.selectFields(input.getSpannerType(), parameters.getBufferFields());
                    } else {
                        inputType = input.getSpannerType();
                    }
                    final List<Type.StructField> structFields = StructSchemaUtil
                            .selectFieldsBuilder(input.getSpannerType(), parameters.getRemainFields());
                    structFields.add(Type.StructField.of(parameters.getBufferedField(), inputType));
                    final Type outputType = Type.struct(structFields);
                    final FCollection<Struct> inputCollection = (FCollection<Struct>) input;
                    final Transform<Struct, Type, Type> transform = new Transform<>(
                            parameters,
                            inputType,
                            outputType,
                            t -> t,
                            (Type t, Struct s) -> StructSchemaUtil.toBuilder(t, s).build(),
                            (Type t, Struct s, String f, List<Struct> b) -> StructSchemaUtil
                                    .toBuilder(t, s)
                                    .set(f)
                                    .toStructArray(t.getStructFields().get(t.getFieldIndex(f)).getType(), b)
                                    .build(),
                            StructSchemaUtil::getAsString,
                            SerializableCoder.of(Struct.class));
                    output = inputCollection.getCollection().apply(name, transform);
                    results.put(name, FCollection.of(name, output, DataType.STRUCT, outputType));
                    break;
                }
                case ENTITY: {
                    final Schema inputSchema;
                    if(parameters.getBufferFields().size() > 0) {
                        inputSchema = RowSchemaUtil.selectFields(input.getSchema(), parameters.getBufferFields());
                    } else {
                        inputSchema = input.getSchema();
                    }
                    final Schema outputSchema = RowSchemaUtil.selectFieldsBuilder(input.getSchema(), parameters.getRemainFields())
                            .addField(
                                    parameters.getBufferedField(),
                                    Schema.FieldType
                                            .array(Schema.FieldType.row(inputSchema))
                                            .withNullable(true))
                            .build();
                    final FCollection<Entity> inputCollection = (FCollection<Entity>) input;
                    final Transform<Entity,Schema,Schema> transform = new Transform<>(
                            parameters,
                            inputSchema,
                            outputSchema,
                            s -> s,
                            (Schema s, Entity e) -> EntitySchemaUtil.toBuilder(s, e).build(),
                            (Schema s, Entity e, String f, List<Entity> b) -> EntitySchemaUtil
                                    .toBuilder(s, e)
                                    .putProperties(f, Value.newBuilder()
                                            .setArrayValue(ArrayValue.newBuilder()
                                                    .addAllValues(b.stream()
                                                            .map(ee -> Value.newBuilder().setEntityValue(ee).build())
                                                            .collect(Collectors.toList()))
                                                    .build())
                                            .build())
                                    .build(),
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
            errorMessages.add("BufferTransform size parameter must be greater than 0.");
        }

        if(errorMessages.size() > 0) {
            throw new IllegalArgumentException(String.join("\n", errorMessages));
        }
    }

    private static void setDefaultParameters(BufferTransformParameters parameters) {
        if(parameters.getBufferedField() == null) {
            parameters.setBufferedField("buffer");
        }
        if(parameters.getRemainFields() == null) {
            parameters.setRemainFields(new ArrayList<>());
        }
        if(parameters.getBufferFields() == null) {
            parameters.setBufferFields(new ArrayList<>());
        }
        if(parameters.getAscending() == null) {
            parameters.setAscending(true);
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
                                parameters.getSize(), parameters.getBufferedField(), parameters.getAscending(),
                                inputSchema, outputSchema, schemaConverter, selector, buffering, coder)));
            } else {
                output = withKey.apply("BatchTransform", ParDo
                        .of(new BufferBatchDoFn<>(
                                parameters.getSize(), parameters.getBufferedField(), parameters.getAscending(),
                                inputSchema, outputSchema, schemaConverter, selector, buffering, coder)));
            }

            return output;
        }

        private static abstract class BufferDoFn<T, InputSchemaT, RuntimeSchemaT> extends DoFn<KV<String, T>, T> {

            static final String STATEID_VALUES = "values";

            private final Integer size;
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
                       final String bufferedField,
                       final Boolean ascending,
                       final InputSchemaT inputSchema,
                       final InputSchemaT outputSchema,
                       final SchemaConverter<InputSchemaT,RuntimeSchemaT> schemaConverter,
                       final Selector<T,RuntimeSchemaT> selector,
                       final Buffering<T,RuntimeSchemaT> buffering) {

                this.size = size;
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
                final T output = buffering.convert(runtimeOutputSchema, base, bufferedField, buffer);
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
                            final String bufferedField,
                            final Boolean ascending,
                            final InputSchemaT inputSchema,
                            final InputSchemaT outputSchema,
                            final SchemaConverter<InputSchemaT, RuntimeSchemaT> schemaConverter,
                            final Selector<T, RuntimeSchemaT> selector,
                            final Buffering<T,RuntimeSchemaT> buffering,
                            final Coder<T> coder) {

                super(size, bufferedField, ascending, inputSchema, outputSchema, schemaConverter, selector, buffering);
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
                                final String bufferedField,
                                final Boolean ascending,
                                final InputSchemaT inputSchema,
                                final InputSchemaT outputSchema,
                                final SchemaConverter<InputSchemaT, RuntimeSchemaT> schemaConverter,
                                final Selector<T, RuntimeSchemaT> selector,
                                final Buffering<T,RuntimeSchemaT> buffering,
                                final Coder<T> coder) {

                super(size, bufferedField, ascending, inputSchema, outputSchema, schemaConverter, selector, buffering);
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
        T convert(final SchemaT schema, final T element, final String bufferedField, final List<T> buffer);
    }

}
