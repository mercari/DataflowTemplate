package com.mercari.solution.module.transform;

import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.mercari.solution.config.TransformConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.module.TransformModule;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.OptionUtil;
import com.mercari.solution.util.converter.RowToMutationConverter;
import com.mercari.solution.util.converter.RowToRecordConverter;
import com.mercari.solution.util.domain.sequence.Sequencer;
import com.mercari.solution.util.pipeline.Merge;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.RowSchemaUtil;
import com.mercari.solution.util.schema.SchemaUtil;
import com.mercari.solution.util.schema.StructSchemaUtil;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.*;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;


public class SequenceTransform implements TransformModule {

    private static final Logger LOG = LoggerFactory.getLogger(SequenceTransform.class);

    public static class SequenceTransformParameters implements Serializable {

        private List<String> groupFields;
        private String inputName;
        private String stateName;
        private JsonElement sequence;

        public List<String> getGroupFields() {
            return groupFields;
        }

        public String getInputName() {
            return inputName;
        }

        public String getStateName() {
            return stateName;
        }

        public JsonElement getSequence() {
            return sequence;
        }

        public void validate() {
            final List<String> errorMessages = new ArrayList<>();
            if(this.sequence == null || this.sequence.isJsonNull()) {
                errorMessages.add("sequence must not be null or size zero.");
            }
            if(errorMessages.size() > 0) {
                throw new IllegalArgumentException(String.join("\n", errorMessages));
            }
        }

        public void setDefaults() {
            if(this.groupFields == null) {
                this.groupFields = new ArrayList<>();
            }
        }

    }

    @Override
    public String getName() {
        return "sequence";
    }

    @Override
    public Map<String, FCollection<?>> expand(List<FCollection<?>> inputs, TransformConfig config) {
        final SequenceTransformParameters parameters = new Gson().fromJson(config.getParameters(), SequenceTransformParameters.class);
        if (parameters == null) {
            throw new IllegalArgumentException("sequence transform module parameters must not be empty!");
        }
        parameters.validate();
        parameters.setDefaults();

        if(inputs.size() == 0) {
            throw new IllegalArgumentException("sequence module: " + config.getName() + " has no inputs");
        }

        final Map<String, FCollection<?>> outputs = new HashMap<>();

        final FCollection<?> input = inputs.stream().filter(s -> s.getName().equals(parameters.getInputName())).findFirst().orElse(inputs.get(0));
        final FCollection<?> state = inputs.stream().filter(s -> s.getName().equals(parameters.getStateName())).findFirst().orElse(null);

        final String name = config.getName();
        outputs.put(name, sequence(parameters, input, state, name));

        return outputs;
    }

    public static FCollection<?> sequence(
            final SequenceTransformParameters parameters,
            final FCollection<?> input, final FCollection<?> state, final String name) {

        final List<Sequencer> sequencers = new ArrayList<>();
        for(final JsonElement element : parameters.getSequence().getAsJsonArray()) {
            final Sequencer sequencer = Sequencer.convert(element);
            sequencers.add(sequencer);
        }

        final Schema stateRowSchema = createSequenceStateSchema(sequencers, input.getSchema(), parameters.getGroupFields());
        final Schema outputRowSchema = createSequenceOutputSchema(sequencers, input.getSchema());

        final List<String> stateFields = stateRowSchema.getFieldNames();

        switch (input.getDataType()) {
            case ROW: {
                final TupleTag<Row> inputTag = new TupleTag<>(){};
                final PCollection<Row> inputCollection = (PCollection<Row>) input.getCollection();

                PCollectionTuple inputs = PCollectionTuple
                        .of(inputTag, inputCollection);

                final PCollection<Row> output;
                if(state == null) {
                    final TupleTag<Row> stateTag = new TupleTag<>(){};
                    final SystemTransform<Schema, Schema, Row, Schema, Schema, Row> transform = new SystemTransform<>(
                            name,
                            parameters.getInputName(),
                            null,
                            parameters.getGroupFields(),
                            sequencers,
                            input.getSchema(),
                            stateRowSchema,
                            outputRowSchema,
                            null,
                            null,
                            stateFields,
                            s -> s,
                            null,
                            RowSchemaUtil::getAsString,
                            null,
                            RowSchemaUtil::getRawValue,
                            null,
                            RowSchemaUtil::create,
                            null,
                            RowSchemaUtil::merge,
                            null,
                            RowSchemaUtil::toInstant,
                            null,
                            DataType.ROW,
                            null,
                            RowCoder.of(stateRowSchema),
                            RowCoder.of(outputRowSchema),
                            null,
                            null,
                            null,
                            inputTag,
                            stateTag);
                    output = inputs.apply(name, transform);
                } else {
                    switch (state.getDataType()) {
                        case ROW -> {
                            final TupleTag<Row> stateTag = new TupleTag<>() {
                            };
                            final SystemTransform<Schema, Schema, Row, Schema, Schema, Row> transform = new SystemTransform<>(
                                    name,
                                    parameters.getInputName(),
                                    parameters.getStateName(),
                                    parameters.getGroupFields(),
                                    sequencers,
                                    input.getSchema(),
                                    stateRowSchema,
                                    outputRowSchema,
                                    stateRowSchema,
                                    outputRowSchema,
                                    stateFields,
                                    s -> s,
                                    s -> s,
                                    RowSchemaUtil::getAsString,
                                    RowSchemaUtil::getAsString,
                                    RowSchemaUtil::getRawValue,
                                    RowSchemaUtil::getRawValue,
                                    RowSchemaUtil::create,
                                    RowSchemaUtil::create,
                                    RowSchemaUtil::merge,
                                    RowSchemaUtil::merge,
                                    RowSchemaUtil::toInstant,
                                    RowSchemaUtil::toInstant,
                                    DataType.ROW,
                                    DataType.ROW,
                                    RowCoder.of(stateRowSchema),
                                    RowCoder.of(outputRowSchema),
                                    RowCoder.of(state.getSchema()),
                                    RowCoder.of(stateRowSchema),
                                    RowCoder.of(outputRowSchema),
                                    inputTag,
                                    stateTag);

                            final PCollection<Row> stateCollection = (PCollection<Row>) state.getCollection();
                            inputs = inputs.and(stateTag, stateCollection);
                            output = inputs.apply(name, transform);
                        }
                        case AVRO -> {
                            final TupleTag<GenericRecord> stateTag = new TupleTag<>() {
                            };
                            final org.apache.avro.Schema stateStateAvroSchema = RowToRecordConverter.convertSchema(stateRowSchema);
                            final org.apache.avro.Schema stateOutputAvroSchema = RowToRecordConverter.convertSchema(outputRowSchema);
                            final SystemTransform<Schema, Schema, Row, String, org.apache.avro.Schema, GenericRecord> transform = new SystemTransform<>(
                                    name,
                                    parameters.getInputName(),
                                    parameters.getStateName(),
                                    parameters.getGroupFields(),
                                    sequencers,
                                    input.getSchema(),
                                    stateRowSchema,
                                    outputRowSchema,
                                    stateStateAvroSchema.toString(),
                                    stateOutputAvroSchema.toString(),
                                    stateFields,
                                    s -> s,
                                    AvroSchemaUtil::convertSchema,
                                    RowSchemaUtil::getAsString,
                                    AvroSchemaUtil::getAsString,
                                    RowSchemaUtil::getRawValue,
                                    AvroSchemaUtil::getRawValue,
                                    RowSchemaUtil::create,
                                    AvroSchemaUtil::create,
                                    RowSchemaUtil::merge,
                                    AvroSchemaUtil::merge,
                                    RowSchemaUtil::toInstant,
                                    AvroSchemaUtil::toInstant,
                                    DataType.ROW,
                                    DataType.AVRO,
                                    RowCoder.of(stateRowSchema),
                                    RowCoder.of(outputRowSchema),
                                    AvroCoder.of(state.getAvroSchema()),
                                    AvroCoder.of(stateStateAvroSchema),
                                    AvroCoder.of(stateOutputAvroSchema),
                                    inputTag,
                                    stateTag);

                            final PCollection<GenericRecord> stateCollection = (PCollection<GenericRecord>) state.getCollection();
                            inputs = inputs.and(stateTag, stateCollection);
                            output = inputs.apply(name, transform);
                        }
                        default -> throw new IllegalArgumentException();
                    }
                }
                return FCollection.of(name, output, DataType.ROW, outputRowSchema);
            }
            case AVRO: {
                final TupleTag<GenericRecord> inputTag = new TupleTag<>(){};
                final PCollection<GenericRecord> inputCollection = (PCollection<GenericRecord>) input.getCollection();

                PCollectionTuple inputs = PCollectionTuple
                        .of(inputTag, inputCollection);

                final org.apache.avro.Schema stateAvroSchema = RowToRecordConverter.convertSchema(stateRowSchema);
                final org.apache.avro.Schema outputAvroSchema = RowToRecordConverter.convertSchema(outputRowSchema);

                final PCollection<GenericRecord> output;
                if(state == null) {
                    final TupleTag<GenericRecord> stateTag = new TupleTag<>(){};
                    final SystemTransform<String, org.apache.avro.Schema, GenericRecord, String, org.apache.avro.Schema, GenericRecord> transform = new SystemTransform<>(
                            name,
                            parameters.getInputName(),
                            null,
                            parameters.getGroupFields(),
                            sequencers,
                            input.getAvroSchema().toString(),
                            stateAvroSchema.toString(),
                            outputAvroSchema.toString(),
                            null,
                            null,
                            stateFields,
                            AvroSchemaUtil::convertSchema,
                            null,
                            AvroSchemaUtil::getAsString,
                            null,
                            AvroSchemaUtil::getRawValue,
                            null,
                            AvroSchemaUtil::create,
                            null,
                            AvroSchemaUtil::merge,
                            null,
                            AvroSchemaUtil::toInstant,
                            null,
                            DataType.AVRO,
                            null,
                            AvroCoder.of(stateAvroSchema),
                            AvroCoder.of(outputAvroSchema),
                            null,
                            AvroCoder.of(stateAvroSchema),
                            AvroCoder.of(outputAvroSchema),
                            inputTag,
                            stateTag);
                    output = inputs.apply(name, transform);
                } else {
                    switch (state.getDataType()) {
                        case ROW -> {
                            final TupleTag<Row> stateTag = new TupleTag<>() {
                            };
                            final SystemTransform<String, org.apache.avro.Schema, GenericRecord, Schema, Schema, Row> transform = new SystemTransform<>(
                                    name,
                                    parameters.getInputName(),
                                    parameters.getStateName(),
                                    parameters.getGroupFields(),
                                    sequencers,
                                    input.getAvroSchema().toString(),
                                    stateAvroSchema.toString(),
                                    outputAvroSchema.toString(),
                                    stateRowSchema,
                                    outputRowSchema,
                                    stateFields,
                                    AvroSchemaUtil::convertSchema,
                                    s -> s,
                                    AvroSchemaUtil::getAsString,
                                    RowSchemaUtil::getAsString,
                                    AvroSchemaUtil::getRawValue,
                                    RowSchemaUtil::getRawValue,
                                    AvroSchemaUtil::create,
                                    RowSchemaUtil::create,
                                    AvroSchemaUtil::merge,
                                    RowSchemaUtil::merge,
                                    AvroSchemaUtil::toInstant,
                                    RowSchemaUtil::toInstant,
                                    DataType.AVRO,
                                    DataType.ROW,
                                    AvroCoder.of(stateAvroSchema),
                                    AvroCoder.of(outputAvroSchema),
                                    RowCoder.of(state.getSchema()),
                                    RowCoder.of(stateRowSchema),
                                    RowCoder.of(outputRowSchema),
                                    inputTag,
                                    stateTag);


                            final PCollection<Row> stateCollection = (PCollection<Row>) state.getCollection();
                            inputs = inputs.and(stateTag, stateCollection);
                            output = inputs.apply(name, transform);
                        }
                        case AVRO -> {
                            final TupleTag<GenericRecord> stateTag = new TupleTag<>() {
                            };
                            final SystemTransform<String, org.apache.avro.Schema, GenericRecord, String, org.apache.avro.Schema, GenericRecord> transform = new SystemTransform<>(
                                    name,
                                    parameters.getInputName(),
                                    parameters.getStateName(),
                                    parameters.getGroupFields(),
                                    sequencers,
                                    input.getAvroSchema().toString(),
                                    stateAvroSchema.toString(),
                                    outputAvroSchema.toString(),
                                    stateAvroSchema.toString(),
                                    outputAvroSchema.toString(),
                                    stateFields,
                                    AvroSchemaUtil::convertSchema,
                                    AvroSchemaUtil::convertSchema,
                                    AvroSchemaUtil::getAsString,
                                    AvroSchemaUtil::getAsString,
                                    AvroSchemaUtil::getRawValue,
                                    AvroSchemaUtil::getRawValue,
                                    AvroSchemaUtil::create,
                                    AvroSchemaUtil::create,
                                    AvroSchemaUtil::merge,
                                    AvroSchemaUtil::merge,
                                    AvroSchemaUtil::toInstant,
                                    AvroSchemaUtil::toInstant,
                                    DataType.AVRO,
                                    DataType.AVRO,
                                    AvroCoder.of(stateAvroSchema),
                                    AvroCoder.of(outputAvroSchema),
                                    AvroCoder.of(state.getAvroSchema()),
                                    AvroCoder.of(stateAvroSchema),
                                    AvroCoder.of(outputAvroSchema),
                                    inputTag,
                                    stateTag);

                            final PCollection<GenericRecord> stateCollection = (PCollection<GenericRecord>) state.getCollection();
                            inputs = inputs.and(stateTag, stateCollection);
                            output = inputs.apply(name, transform);
                        }
                        default -> throw new IllegalArgumentException();
                    }
                }
                return FCollection.of(name, output, DataType.AVRO, outputAvroSchema);
            }
            case STRUCT: {
                final TupleTag<Struct> inputTag = new TupleTag<>(){};
                final PCollection<Struct> inputCollection = (PCollection<Struct>) input.getCollection();

                PCollectionTuple inputs = PCollectionTuple
                        .of(inputTag, inputCollection);

                final Type stateType = RowToMutationConverter.convertSchema(stateRowSchema);
                final Type outputType = RowToMutationConverter.convertSchema(outputRowSchema);

                final PCollection<Struct> output;
                if(state == null) {
                    final TupleTag<Struct> stateTag = new TupleTag<>(){};
                    final SystemTransform<Type, Type, Struct, Type, Type, Struct> transform = new SystemTransform<>(
                            name,
                            parameters.getInputName(),
                            null,
                            parameters.getGroupFields(),
                            sequencers,
                            input.getSpannerType(),
                            stateType,
                            outputType,
                            null,
                            null,
                            stateFields,
                            s -> s,
                            null,
                            StructSchemaUtil::getAsString,
                            null,
                            StructSchemaUtil::getRawValue,
                            null,
                            StructSchemaUtil::create,
                            null,
                            StructSchemaUtil::merge,
                            null,
                            StructSchemaUtil::toInstant,
                            null,
                            DataType.STRUCT,
                            null,
                            SerializableCoder.of(Struct.class),
                            SerializableCoder.of(Struct.class),
                            null,
                            null,
                            null,
                            inputTag,
                            stateTag);
                    output = inputs.apply(name, transform);
                } else {
                    switch (state.getDataType()) {
                        case AVRO: {
                            final TupleTag<GenericRecord> stateTag = new TupleTag<>(){};
                            final org.apache.avro.Schema stateStateAvroSchema = RowToRecordConverter.convertSchema(stateRowSchema);
                            final org.apache.avro.Schema stateOutputAvroSchema = RowToRecordConverter.convertSchema(outputRowSchema);
                            final SystemTransform<Type, Type, Struct, String, org.apache.avro.Schema, GenericRecord> transform = new SystemTransform<>(
                                    name,
                                    parameters.getInputName(),
                                    parameters.getStateName(),
                                    parameters.getGroupFields(),
                                    sequencers,
                                    input.getSpannerType(),
                                    stateType,
                                    outputType,
                                    stateStateAvroSchema.toString(),
                                    stateOutputAvroSchema.toString(),
                                    stateFields,
                                    s -> s,
                                    AvroSchemaUtil::convertSchema,
                                    StructSchemaUtil::getAsString,
                                    AvroSchemaUtil::getAsString,
                                    StructSchemaUtil::getRawValue,
                                    AvroSchemaUtil::getRawValue,
                                    StructSchemaUtil::create,
                                    AvroSchemaUtil::create,
                                    StructSchemaUtil::merge,
                                    AvroSchemaUtil::merge,
                                    StructSchemaUtil::toInstant,
                                    AvroSchemaUtil::toInstant,
                                    DataType.STRUCT,
                                    DataType.AVRO,
                                    SerializableCoder.of(Struct.class),
                                    SerializableCoder.of(Struct.class),
                                    AvroCoder.of(state.getAvroSchema()),
                                    AvroCoder.of(stateStateAvroSchema),
                                    AvroCoder.of(stateOutputAvroSchema),
                                    inputTag,
                                    stateTag);

                            final PCollection<GenericRecord> stateCollection = (PCollection<GenericRecord>) state.getCollection();
                            inputs = inputs.and(stateTag, stateCollection);
                            output = inputs.apply(name, transform);
                            break;
                        }
                        default:
                            throw new IllegalArgumentException("Not supported state data type: " + state.getDataType());
                    }
                }
                return FCollection.of(name, output, DataType.STRUCT, outputType);
            }
            case DOCUMENT: {

            }
            case ENTITY: {

            }
            default:
                throw new IllegalArgumentException("Not supported input data type: " + input.getDataType());
        }
    }

    public static class SystemTransform<InputSchemaT,RuntimeSchemaT,T,StateInputSchemaT,StateRuntimeSchemaT,StateT> extends PTransform<PCollectionTuple, PCollection<T>> {

        private final String name;
        private final String inputName;
        private final String stateName;
        private final TupleTag<T> inputMainTag;
        private final TupleTag<StateT> inputStateTag;

        private final List<String> groupFields;
        private final List<Sequencer> sequencers;

        private final InputSchemaT inputSchema;
        private final InputSchemaT stateSchema;
        private final InputSchemaT outputSchema;
        private final StateInputSchemaT stateStateSchema;
        private final StateInputSchemaT stateOutputSchema;
        private final List<String> stateFields;
        private final SchemaUtil.SchemaConverter<InputSchemaT,RuntimeSchemaT> schemaConverter;
        private final SchemaUtil.SchemaConverter<StateInputSchemaT,StateRuntimeSchemaT> stateSchemaConverter;
        private final SchemaUtil.StringGetter<T> stringGetter;
        private final SchemaUtil.StringGetter<StateT> stateStringGetter;
        private final SchemaUtil.ValueGetter<T> valueGetter;
        private final SchemaUtil.ValueGetter<StateT> stateValueGetter;
        private final SchemaUtil.ValueCreator<RuntimeSchemaT,T> valueCreator;
        private final SchemaUtil.ValueCreator<StateRuntimeSchemaT,StateT> stateValueCreator;
        private final SchemaUtil.ValuesSetter<RuntimeSchemaT,T> valuesSetter;
        private final SchemaUtil.ValuesSetter<StateRuntimeSchemaT,StateT> stateValuesSetter;
        private final SchemaUtil.TimestampConverter timestampConverter;
        private final SchemaUtil.TimestampConverter stateTimestampConverter;

        private final DataType inputDataType;
        private final DataType stateDataType;

        private final Coder<T> stateCoder;
        private final Coder<T> outputCoder;
        private final Coder<StateT> stateInputCoder;
        private final Coder<StateT> stateStateCoder;
        private final Coder<StateT> stateOutputCoder;


        SystemTransform(final String name,
                        final String inputName,
                        final String stateName,
                        final List<String> groupFields,
                        final List<Sequencer> sequencers,
                        final InputSchemaT inputSchema,
                        final InputSchemaT stateSchema,
                        final InputSchemaT outputSchema,
                        final StateInputSchemaT stateStateSchema,
                        final StateInputSchemaT stateOutputSchema,
                        final List<String> stateFields,
                        final SchemaUtil.SchemaConverter<InputSchemaT,RuntimeSchemaT> schemaConverter,
                        final SchemaUtil.SchemaConverter<StateInputSchemaT,StateRuntimeSchemaT> stateSchemaConverter,
                        final SchemaUtil.StringGetter<T> stringGetter,
                        final SchemaUtil.StringGetter<StateT> stateStringGetter,
                        final SchemaUtil.ValueGetter<T> valueGetter,
                        final SchemaUtil.ValueGetter<StateT> stateValueGetter,
                        final SchemaUtil.ValueCreator<RuntimeSchemaT, T> valueCreator,
                        final SchemaUtil.ValueCreator<StateRuntimeSchemaT, StateT> stateValueCreator,
                        final SchemaUtil.ValuesSetter<RuntimeSchemaT, T> valuesSetter,
                        final SchemaUtil.ValuesSetter<StateRuntimeSchemaT, StateT> stateValuesSetter,
                        final SchemaUtil.TimestampConverter timestampConverter,
                        final SchemaUtil.TimestampConverter stateTimestampConverter,
                        final DataType inputDataType,
                        final DataType stateDataType,
                        final Coder<T> stateCoder,
                        final Coder<T> outputCoder,
                        final Coder<StateT> stateInputCoder,
                        final Coder<StateT> stateStateCoder,
                        final Coder<StateT> stateOutputCoder,
                        final TupleTag<T> inputMainTag,
                        final TupleTag<StateT> inputStateTag) {

            this.name = name;
            this.inputName = inputName;
            this.stateName = stateName;
            this.groupFields = groupFields;
            this.sequencers = sequencers;

            this.inputSchema = inputSchema;
            this.stateSchema = stateSchema;
            this.outputSchema = outputSchema;
            this.stateStateSchema = stateStateSchema;
            this.stateOutputSchema = stateOutputSchema;
            this.stateFields = stateFields;

            this.schemaConverter = schemaConverter;
            this.stateSchemaConverter = stateSchemaConverter;
            this.stringGetter = stringGetter;
            this.stateStringGetter = stateStringGetter;
            this.valueGetter = valueGetter;
            this.stateValueGetter = stateValueGetter;
            this.valueCreator = valueCreator;
            this.stateValueCreator = stateValueCreator;
            this.valuesSetter = valuesSetter;
            this.stateValuesSetter = stateValuesSetter;
            this.timestampConverter = timestampConverter;
            this.stateTimestampConverter = stateTimestampConverter;

            this.inputDataType = inputDataType;
            this.stateDataType = stateDataType;

            this.stateCoder = stateCoder;
            this.outputCoder = outputCoder;
            this.stateInputCoder = stateInputCoder;
            this.stateStateCoder = stateStateCoder;
            this.stateOutputCoder = stateOutputCoder;

            this.inputMainTag = inputMainTag;
            this.inputStateTag = inputStateTag;
        }

        @Override
        public PCollection<T> expand(final PCollectionTuple inputs) {

            final PCollection<T> state;
            if(inputs.has(inputStateTag)) {
                final Transform<StateInputSchemaT, StateRuntimeSchemaT, StateT> stateTransform = new Transform<>(
                        false,
                        inputName,
                        stateName,
                        groupFields,
                        sequencers,
                        stateStateSchema,
                        stateOutputSchema,
                        stateFields,
                        stateSchemaConverter,
                        stateStringGetter,
                        stateTimestampConverter,
                        stateValueGetter,
                        stateValueCreator,
                        stateValuesSetter,
                        stateInputCoder,
                        stateStateCoder,
                        stateOutputCoder);

                final PCollectionTuple stateOutputs = inputs.get(inputStateTag)
                        .apply("StateSequence", stateTransform);

                if(inputDataType.equals(stateDataType)) {
                    state = (PCollection<T>) (stateOutputs.get(stateTransform.outputStateTag));
                } else {
                    final RuntimeSchemaT runtimeStateSchema = schemaConverter.convert(stateSchema);
                    state = ((PCollection<T>) (stateOutputs
                            .get(stateTransform.outputStateTag).apply(
                                    "TypeTransform", SchemaUtil.transform(runtimeStateSchema, stateDataType, inputDataType))))
                            .setCoder(stateCoder);
                }

            } else {
                state = null;
            }

            final Transform<InputSchemaT, RuntimeSchemaT, T> mainTransform = new Transform<>(
                    state != null,
                    inputName,
                    stateName,
                    groupFields,
                    sequencers,
                    stateSchema,
                    outputSchema,
                    stateFields,
                    schemaConverter,
                    stringGetter,
                    timestampConverter,
                    valueGetter,
                    valueCreator,
                    valuesSetter,
                    stateCoder,
                    stateCoder,
                    outputCoder);

            if(state == null) {
                return inputs.get(inputMainTag)
                        .apply("Sequence", mainTransform)
                        .get(mainTransform.outputMainTag)
                        .setCoder(outputCoder);
            }

            final TupleTag<T> inputTag = new TupleTag<>(){};
            final TupleTag<T> stateTag = new TupleTag<>(){};
            final Map<TupleTag<?>, String> inputNames = Map.of(inputTag, inputName, stateTag, stateName);
            final Map<TupleTag<?>, DataType> inputTypes = Map.of(inputTag, inputDataType, stateTag, inputDataType);
            final Map<TupleTag<?>, InputSchemaT> inputSchemas = Map.of(inputTag, inputSchema, stateTag, stateSchema);

            final PTransform<PCollectionTuple, PCollection<T>> merge = new Merge<>(name, groupFields,
                    inputNames, inputTypes, inputSchemas, inputDataType, schemaConverter, valueGetter, valueCreator);

            final PCollection<T> waitOn = inputs.get(inputMainTag)
                    .apply("WaitOnState", Wait.on(state))
                    .setCoder(inputs.get(inputMainTag).getCoder());

            final PCollection<T> input = waitOn
                    .apply("WithCountTrigger", Window
                            .<T>into(new GlobalWindows())
                            .triggering(Repeatedly
                                    .forever(AfterPane.elementCountAtLeast(1)))
                            .withTimestampCombiner(TimestampCombiner.LATEST)
                            .discardingFiredPanes()
                            .withAllowedLateness(Duration.ZERO));

            final PCollection<T> mainInputs = PCollectionTuple
                    .of(inputTag, input)
                    .and(stateTag, state)
                    .apply("MergeMain", merge);

            final PCollectionTuple mainOutputs = mainInputs
                    .apply("Sequence", mainTransform);

            final PCollection<T> output = mainOutputs.get(mainTransform.outputMainTag)
                    .setCoder(outputCoder);
            return output;
        }

    }

    public static class Transform<InputSchemaT,RuntimeSchemaT,T> extends PTransform<PCollection<T>, PCollectionTuple> {

        private final Boolean nested;
        private final String inputName;
        private final String stateName;
        public final TupleTag<T> outputMainTag;
        public final TupleTag<T> outputStateTag;

        private final List<String> groupFields;
        private final List<Sequencer> sequencers;

        private final InputSchemaT stateSchema;
        private final InputSchemaT outputSchema;
        private final List<String> stateFields;
        private final SchemaUtil.SchemaConverter<InputSchemaT,RuntimeSchemaT> schemaConverter;
        private final SchemaUtil.StringGetter<T> stringGetter;
        private final SchemaUtil.TimestampConverter timestampConverter;
        private final SchemaUtil.ValueGetter<T> valueGetter;
        private final SchemaUtil.ValueCreator<RuntimeSchemaT,T> valueCreator;
        private final SchemaUtil.ValuesSetter<RuntimeSchemaT,T> valuesSetter;
        private final Coder<T> inputCoder;
        private final Coder<T> stateCoder;
        private final Coder<T> outputCoder;


        Transform(final Boolean nested,
                  final String inputName,
                  final String stateName,
                  final List<String> groupFields,
                  final List<Sequencer> sequencers,
                  final InputSchemaT stateSchema,
                  final InputSchemaT outputSchema,
                  final List<String> stateFields,
                  final SchemaUtil.SchemaConverter<InputSchemaT,RuntimeSchemaT> schemaConverter,
                  final SchemaUtil.StringGetter<T> stringGetter,
                  final SchemaUtil.TimestampConverter timestampConverter,
                  final SchemaUtil.ValueGetter<T> valueGetter,
                  final SchemaUtil.ValueCreator<RuntimeSchemaT, T> valueCreator,
                  final SchemaUtil.ValuesSetter<RuntimeSchemaT, T> valuesSetter,
                  final Coder<T> inputCoder,
                  final Coder<T> stateCoder,
                  final Coder<T> outputCoder) {

            this.nested = nested;
            this.inputName = inputName;
            this.stateName = stateName;

            this.groupFields = groupFields;
            this.sequencers = sequencers;

            this.stateSchema = stateSchema;
            this.outputSchema = outputSchema;
            this.stateFields = stateFields;

            this.schemaConverter = schemaConverter;
            this.stringGetter = stringGetter;
            this.timestampConverter = timestampConverter;
            this.valueGetter = valueGetter;
            this.valueCreator = valueCreator;
            this.valuesSetter = valuesSetter;

            this.inputCoder = inputCoder;
            this.stateCoder = stateCoder;
            this.outputCoder = outputCoder;

            this.outputMainTag = new TupleTag<>(){};
            this.outputStateTag = new TupleTag<>(){};
        }

        @Override
        public PCollectionTuple expand(final PCollection<T> input) {

            final String processName;
            final SequenceDoFn<InputSchemaT,RuntimeSchemaT,T> dofn;
            if(PCollection.IsBounded.BOUNDED.equals(input.isBounded())) {
                if(OptionUtil.isStreaming(input)) {
                    processName = "ProcessSequenceStreamingBatch";
                    dofn = new SequenceStreamingBatchDoFn(nested, inputName, stateName, groupFields, sequencers,
                            stateSchema, outputSchema, stateFields, schemaConverter,
                            valueGetter, valueCreator, valuesSetter, stringGetter, timestampConverter,
                            inputCoder, stateCoder, outputMainTag, outputStateTag);
                } else {
                    processName = "ProcessSequenceBatch";
                    dofn = new SequenceBatchDoFn(nested, inputName, stateName, groupFields, sequencers,
                            stateSchema, outputSchema, stateFields, schemaConverter,
                            valueGetter, valueCreator, valuesSetter, stringGetter, timestampConverter,
                            stateCoder, outputMainTag, outputStateTag);
                }
            } else {
                processName = "ProcessSequenceStreaming";
                dofn = new SequenceStreamingDoFn(nested, inputName, stateName, groupFields, sequencers,
                        stateSchema, outputSchema, stateFields, schemaConverter,
                        valueGetter, valueCreator, valuesSetter, stringGetter, timestampConverter,
                        stateCoder, outputMainTag, outputStateTag);
            }

            final PCollectionTuple output = input
                    .apply("WithCountTrigger", Window
                            .<T>into(new GlobalWindows())
                            .triggering(Repeatedly
                                    .forever(AfterPane.elementCountAtLeast(1)))
                            .withTimestampCombiner(TimestampCombiner.LATEST)
                            .discardingFiredPanes()
                            .withAllowedLateness(Duration.ZERO))
                    .apply("WithKeys", WithKeys
                            .of(SchemaUtil.createGroupKeysFunction(stringGetter, groupFields))
                            .withKeyType(TypeDescriptors.strings()))
                    .apply(processName, ParDo.of(dofn)
                            .withOutputTags(outputMainTag, TupleTagList.of(outputStateTag)));

            output.get(outputMainTag).setCoder(outputCoder);
            output.get(outputStateTag).setCoder(stateCoder);
            return output;
        }

        private static class SequenceDoFn<InputSchemaT,RuntimeSchemaT,T> extends DoFn<KV<String, T>, T> {

            static final String STATE_ID_STATE = "sequenceTransformState";

            public final TupleTag<T> outputMainTag;
            public final TupleTag<T> outputStateTag;

            private final Boolean nested;
            private final String inputName;
            private final String stateName;
            private final List<String> groupFields;

            private final List<Sequencer> sequencers;
            private final SchemaUtil.SchemaConverter<InputSchemaT,RuntimeSchemaT> schemaConverter;

            private final SchemaUtil.ValueGetter<T> valueGetter;
            private final SchemaUtil.ValueCreator<RuntimeSchemaT, T> valueCreator;
            private final SchemaUtil.ValuesSetter<RuntimeSchemaT,T> valuesSetter;
            private final SchemaUtil.StringGetter<T> stringGetter;
            private final SchemaUtil.TimestampConverter timestampConverter;

            private final InputSchemaT inputOutputSchema;
            private final InputSchemaT inputStateSchema;
            private final List<String> stateFields;

            private transient RuntimeSchemaT runtimeOutputSchema;
            private transient RuntimeSchemaT runtimeStateSchema;

            SequenceDoFn(final Boolean nested,
                         final String inputName,
                         final String stateName,
                         final List<String> groupFields,
                         final List<Sequencer> sequencers,
                         final InputSchemaT inputStateSchema,
                         final InputSchemaT inputOutputSchema,
                         final List<String> stateFields,
                         final SchemaUtil.SchemaConverter<InputSchemaT, RuntimeSchemaT> schemaConverter,
                         final SchemaUtil.ValueGetter<T> valueGetter,
                         final SchemaUtil.ValueCreator<RuntimeSchemaT, T> valueCreator,
                         final SchemaUtil.ValuesSetter<RuntimeSchemaT,T> valuesSetter,
                         final SchemaUtil.StringGetter<T> stringGetter,
                         final SchemaUtil.TimestampConverter timestampConverter,
                         final TupleTag<T> outputMainTag,
                         final TupleTag<T> outputStateTag) {

                this.nested = nested;
                this.inputName = inputName;
                this.stateName = stateName;
                this.groupFields = groupFields;
                this.sequencers = sequencers;

                this.inputOutputSchema = inputOutputSchema;
                this.inputStateSchema = inputStateSchema;
                this.stateFields = stateFields;
                this.schemaConverter = schemaConverter;
                this.valueGetter = valueGetter;
                this.valueCreator = valueCreator;
                this.valuesSetter = valuesSetter;
                this.stringGetter = stringGetter;
                this.timestampConverter = timestampConverter;

                this.outputMainTag = outputMainTag;
                this.outputStateTag = outputStateTag;
            }

            protected void setup() {
                this.runtimeOutputSchema = schemaConverter.convert(inputOutputSchema);
                this.runtimeStateSchema = schemaConverter.convert(inputStateSchema);
                this.sequencers.forEach(Sequencer::setup);
            }

            protected void teardown() {

            }

            protected void processElement(
                    final KV<String, T> element,
                    final Instant timestamp,
                    final MultiOutputReceiver outputReceiver,
                    final ValueState<T> valueState) {

                processElement(element, timestamp, outputReceiver, valueState, null, false);
            }

            protected void processElement(
                    final KV<String, T> element,
                    final Instant timestamp,
                    final MultiOutputReceiver outputReceiver,
                    final ValueState<T> valueState,
                    final ValueState<Long> timestampState,
                    final boolean outputState) {

                final T input;
                final T state;
                if(nested) {
                    final T merged = element.getValue();
                    final String sourceName = stringGetter.getAsString(merged, Merge.DEFAULT_FIELD_NAME_SOURCE);
                    if(sourceName == null) {
                        throw new IllegalArgumentException("nested input data must have both source and timestamp fields. nested data: " + merged);
                    }
                    if(sourceName.equals(inputName)) {
                        if(timestampState != null) {
                            final Long stateCreatedMillis = Optional.ofNullable(timestampState.read()).orElse(0L);
                            if (timestamp.getMillis() < stateCreatedMillis) {
                                LOG.warn("input timestamp: " + timestamp + " is lesser than state timestamp: " + DateTimeUtil.toJodaInstant(stateCreatedMillis));
                                return;
                            }
                        }
                        input = getNestedInput(merged, inputName);
                        state = getState(valueState, merged, stateName);
                    } else if(sourceName.equals(stateName)) {
                        input = null;
                        state = getState(valueState, merged, stateName);
                        if(timestampState != null) {
                            final Long stateCreatedMillis = (Long) valueGetter.getValue(state, "__timestamp");
                            timestampState.write(stateCreatedMillis);
                        }
                    } else {
                        throw new IllegalArgumentException("Unknown source input: " + sourceName);
                    }
                } else {
                    input = element.getValue();
                    state = getState(valueState, null, stateName);
                }

                final Map<String, Object> updatingValues = new HashMap<>();

                for(final String stateField : stateFields) {
                    updatingValues.put(stateField, valueGetter.getValue(state, stateField));
                }

                boolean suspended = false;
                for (final Sequencer sequencer : sequencers) {
                    suspended = sequencer.suspend(input, updatingValues, valueGetter);
                    if (suspended) {
                        break;
                    }
                    sequencer.execute(input, state, updatingValues, timestamp, valueGetter, timestampConverter);
                }

                if(!suspended) {
                    final T output = valuesSetter.setValues(runtimeOutputSchema, input, updatingValues);
                    outputReceiver.get(outputMainTag).output(output);
                }

                for(final String groupField : groupFields) {
                    updatingValues.put(groupField, valueGetter.getValue(input != null ? input : state, groupField));
                }
                final T newState = updateState(state, updatingValues);
                valueState.write(newState);

                if(outputState) {
                    outputReceiver.get(outputStateTag).output(newState);
                }
            }

            T updateState(final T state, final Map<String,Object> values) {
                return valuesSetter.setValues(runtimeStateSchema, state, values);
            }

            private T getState(final ValueState<T> valueState, final T input, final String stateInputName) {
                final T state = valueState.read();
                if(state != null) {
                    return state;
                } else if(nested) {
                    final T output = getNestedInput(input, stateInputName);
                    if (output != null) {
                        return output;
                    }
                }
                return valueCreator.create(runtimeStateSchema, new HashMap<>());
            }

            private T getNestedInput(final T input, final String inputName) {
                final List<Object> list = (List<Object>)valueGetter.getValue(input, inputName);
                if(list != null && list.size() > 0) {
                    return (T)list.get(0);
                }
                return null;
            }

        }

        private class SequenceBatchDoFn extends SequenceDoFn<InputSchemaT,RuntimeSchemaT,T> {

            @StateId(STATE_ID_STATE)
            private final StateSpec<ValueState<T>> stateSpec;

            SequenceBatchDoFn(final Boolean nested,
                              final String inputName,
                              final String stateName,
                              final List<String> groupFields,
                              final List<Sequencer> sequencers,
                              final InputSchemaT inputSchema,
                              final InputSchemaT inputStateSchema,
                              final List<String> stateFields,
                              final SchemaUtil.SchemaConverter<InputSchemaT, RuntimeSchemaT> schemaConverter,
                              final SchemaUtil.ValueGetter<T> valueGetter,
                              final SchemaUtil.ValueCreator<RuntimeSchemaT,T> valueCreator,
                              final SchemaUtil.ValuesSetter<RuntimeSchemaT,T> valuesSetter,
                              final SchemaUtil.StringGetter<T> stringGetter,
                              final SchemaUtil.TimestampConverter timestampConverter,
                              final Coder<T> stateCoder,
                              final TupleTag<T> outputMainTag,
                              final TupleTag<T> outputStateTag) {

                super(nested, inputName, stateName, groupFields, sequencers,
                        inputSchema, inputStateSchema, stateFields, schemaConverter,
                        valueGetter, valueCreator, valuesSetter, stringGetter, timestampConverter,
                        outputMainTag, outputStateTag);

                this.stateSpec = StateSpecs.value(stateCoder);
            }

            @Setup
            public void setup() {
                super.setup();
            }

            @ProcessElement
            @RequiresTimeSortedInput
            public void processElement(
                    final @Element KV<String, T> element,
                    final @Timestamp Instant timestamp,
                    final DoFn.MultiOutputReceiver outputReceiver,
                    final @AlwaysFetched @StateId(STATE_ID_STATE) ValueState<T> stateSpec) {

                super.processElement(element, timestamp, outputReceiver, stateSpec);
            }

            @Teardown
            public void teardown() {
                super.teardown();
            }
        }

        private class SequenceStreamingDoFn extends SequenceDoFn<InputSchemaT,RuntimeSchemaT,T> {

            private static final String STATE_ID_STATE_TIMESTAMP = "sequenceStreamingStateTimestamp";

            @StateId(STATE_ID_STATE)
            private final StateSpec<ValueState<T>> stateSpec;
            @StateId(STATE_ID_STATE_TIMESTAMP)
            private final StateSpec<ValueState<Long>> stateTimestampSpec;


            SequenceStreamingDoFn(final Boolean nested,
                                  final String inputName,
                                  final String stateName,
                                  final List<String> groupFields,
                                  final List<Sequencer> sequencers,
                                  final InputSchemaT inputSchema,
                                  final InputSchemaT inputStateSchema,
                                  final List<String> stateFields,
                                  final SchemaUtil.SchemaConverter<InputSchemaT, RuntimeSchemaT> schemaConverter,
                                  final SchemaUtil.ValueGetter<T> valueGetter,
                                  final SchemaUtil.ValueCreator<RuntimeSchemaT,T> valueCreator,
                                  final SchemaUtil.ValuesSetter<RuntimeSchemaT,T> valuesSetter,
                                  final SchemaUtil.StringGetter<T> stringGetter,
                                  final SchemaUtil.TimestampConverter timestampConverter,
                                  final Coder<T> stateCoder,
                                  final TupleTag<T> outputMainTag,
                                  final TupleTag<T> outputStateTag) {

                super(nested, inputName, stateName, groupFields, sequencers,
                        inputSchema, inputStateSchema, stateFields, schemaConverter,
                        valueGetter, valueCreator, valuesSetter, stringGetter, timestampConverter,
                        outputMainTag, outputStateTag);

                this.stateSpec = StateSpecs.value(stateCoder);
                this.stateTimestampSpec = StateSpecs.value(VarLongCoder.of());
            }

            @Setup
            public void setup() {
                super.setup();
            }

            @ProcessElement
            public void processElement(
                    final @Element KV<String, T> element,
                    final @Timestamp Instant timestamp,
                    final DoFn.MultiOutputReceiver outputReceiver,
                    final @AlwaysFetched @StateId(STATE_ID_STATE) ValueState<T> stateSpec,
                    final @AlwaysFetched @StateId(STATE_ID_STATE_TIMESTAMP) ValueState<Long> timestampSpec) {

                super.processElement(element, timestamp, outputReceiver, stateSpec, timestampSpec, false);
            }

            @Teardown
            public void teardown() {
                super.teardown();
            }

        }

        private class SequenceStreamingBatchDoFn extends SequenceDoFn<InputSchemaT,RuntimeSchemaT,T> {

            private static final String STATE_ID_KEY = "sequenceStreamingBatchKey";
            private static final String STATE_ID_BUFFER = "sequenceStreamingBatchBuffer";

            @StateId(STATE_ID_KEY)
            private final StateSpec<ValueState<String>> keySpec;
            @StateId(STATE_ID_BUFFER)
            private final StateSpec<ValueState<List<KV<Instant,T>>>> bufferSpec;
            @StateId(STATE_ID_STATE)
            private final StateSpec<ValueState<T>> stateSpec;

            SequenceStreamingBatchDoFn(final Boolean nested,
                                       final String inputName,
                                       final String stateName,
                                       final List<String> groupFields,
                                       final List<Sequencer> sequencers,
                                       final InputSchemaT inputSchema,
                                       final InputSchemaT inputStateSchema,
                                       final List<String> stateFields,
                                       final SchemaUtil.SchemaConverter<InputSchemaT, RuntimeSchemaT> schemaConverter,
                                       final SchemaUtil.ValueGetter<T> valueGetter,
                                       final SchemaUtil.ValueCreator<RuntimeSchemaT,T> valueCreator,
                                       final SchemaUtil.ValuesSetter<RuntimeSchemaT,T> valuesSetter,
                                       final SchemaUtil.StringGetter<T> stringGetter,
                                       final SchemaUtil.TimestampConverter timestampConverter,
                                       final Coder<T> inputCoder,
                                       final Coder<T> stateCoder,
                                       final TupleTag<T> outputMainTag,
                                       final TupleTag<T> outputStateTag) {

                super(nested, inputName, stateName, groupFields, sequencers,
                        inputSchema, inputStateSchema, stateFields, schemaConverter,
                        valueGetter, valueCreator, valuesSetter, stringGetter, timestampConverter,
                        outputMainTag, outputStateTag);

                this.keySpec = StateSpecs.value(StringUtf8Coder.of());
                this.stateSpec = StateSpecs.value(stateCoder);
                this.bufferSpec = StateSpecs.value(ListCoder.of(KvCoder.of(InstantCoder.of(), inputCoder)));
            }

            @Setup
            public void setup() {
                super.setup();
            }

            @OnWindowExpiration
            public void onWindowExpiration(
                    final DoFn.MultiOutputReceiver outputReceiver,
                    final @AlwaysFetched @StateId(STATE_ID_KEY) ValueState<String> keySpec,
                    final @AlwaysFetched @StateId(STATE_ID_BUFFER) ValueState<List<KV<Instant,T>>> bufferSpec,
                    final @AlwaysFetched @StateId(STATE_ID_STATE) ValueState<T> stateSpec) {

                final String key = keySpec.read();
                if(key == null) {
                    LOG.error("onWindowExpiration: key is null");
                    return;
                }

                final List<KV<Instant,T>> inputs = Optional.ofNullable(bufferSpec.read()).orElseGet(ArrayList::new)
                        .stream()
                        .sorted(Comparator.comparing(KV::getKey))
                        .collect(Collectors.toList());
                for(final KV<Instant,T> input : inputs) {
                    super.processElement(KV.of(key, input.getValue()), input.getKey(), outputReceiver, stateSpec);
                }

                final T state = stateSpec.read();
                if(state == null) {
                    LOG.error("onWindowExpiration: state is null, inputs size: " + inputs.size());
                    return;
                }

                final Map<String,Object> values = new HashMap<>();
                final Instant timestamp = inputs.size() > 0 ? inputs.get(inputs.size() - 1).getKey() : new Instant();
                values.put("__timestamp", timestamp.getMillis());
                final T updatedState = updateState(state, values);
                outputReceiver.get(super.outputStateTag).output(updatedState);

                keySpec.clear();
                bufferSpec.clear();
                stateSpec.clear();
            }

            @ProcessElement
            public void processElement(
                    final @Element KV<String, T> element,
                    final @Timestamp Instant timestamp,
                    final @StateId(STATE_ID_KEY) ValueState<String> keySpec,
                    final @AlwaysFetched @StateId(STATE_ID_BUFFER) ValueState<List<KV<Instant,T>>> bufferSpec) {

                keySpec.write(element.getKey());

                final List<KV<Instant,T>> inputs = Optional.ofNullable(bufferSpec.read()).orElseGet(ArrayList::new);
                inputs.add(KV.of(timestamp, element.getValue()));
                bufferSpec.write(inputs);
            }

            @Teardown
            public void teardown() {
                super.teardown();
            }

        }

    }

    private static Schema createSequenceStateSchema(final List<Sequencer> sequencers, final Schema inputSchema, final List<String> groupFields) {
        final Map<String, Schema.FieldType> allStateTypes = new HashMap<>();
        for(final String field : groupFields) {
            allStateTypes.put(field, inputSchema.getField(field).getType().withNullable(true));
        }
        for(final Sequencer sequencer : sequencers) {
            final Map<String, Schema.FieldType> stateTypes = sequencer.stateTypes(inputSchema, allStateTypes);
            allStateTypes.putAll(stateTypes);
        }

        final Schema.Builder builder = Schema.builder();
        builder.addField("__timestamp", Schema.FieldType.INT64.withNullable(true));
        for(final Map.Entry<String, Schema.FieldType> stateType : allStateTypes.entrySet()) {
            builder.addField(stateType.getKey(), stateType.getValue());
        }
        return builder.build();
    }

    private static Schema createSequenceOutputSchema(final List<Sequencer> sequencers, final Schema inputSchema) {
        final Map<String, Schema.FieldType> allOutputTypes = new HashMap<>();
        final Map<String, Schema.FieldType> allStateTypes = new HashMap<>();
        for(final Sequencer sequencer : sequencers) {
            final Map<String, Schema.FieldType> outputTypes = sequencer.outputTypes(inputSchema, allStateTypes);
            final Map<String, Schema.FieldType> stateTypes = sequencer.stateTypes(inputSchema, allStateTypes);
            allOutputTypes.putAll(outputTypes);
            allStateTypes.putAll(stateTypes);
        }

        final Schema.Builder builder = RowSchemaUtil.toBuilder(inputSchema);
        for(final Map.Entry<String, Schema.FieldType> outputType : allOutputTypes.entrySet()) {
            builder.addField(outputType.getKey(), outputType.getValue());
        }
        return builder.build();
    }

}
