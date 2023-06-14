package com.mercari.solution.module.transform;

import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.mercari.solution.config.TransformConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.module.TransformModule;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.OptionUtil;
import com.mercari.solution.util.converter.RowToMutationConverter;
import com.mercari.solution.util.converter.RowToRecordConverter;
import com.mercari.solution.util.pipeline.TriggerUtil;
import com.mercari.solution.util.pipeline.WindowUtil;
import com.mercari.solution.util.pipeline.aggregation.Accumulator;
import com.mercari.solution.util.pipeline.aggregation.Aggregator;
import com.mercari.solution.util.pipeline.aggregation.Aggregators;
import com.mercari.solution.util.pipeline.union.Union;
import com.mercari.solution.util.pipeline.union.UnionValue;
import com.mercari.solution.util.schema.*;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;


public class AggregationTransform implements TransformModule {

    private static final Logger LOG = LoggerFactory.getLogger(AggregationTransform.class);

    public class AggregateTransformParameters implements Serializable {

        private List<String> groupFields;

        private WindowUtil.WindowParameters window;
        private TriggerUtil.TriggerParameters trigger;
        private WindowUtil.AccumulationMode accumulationMode;

        private List<AggregationDefinition> aggregations;

        private Boolean outputEmpty;
        private Boolean outputPaneInfo;

        private OutputType outputType;

        public List<String> getGroupFields() {
            return groupFields;
        }

        public void setGroupFields(List<String> groupFields) {
            this.groupFields = groupFields;
        }

        public WindowUtil.WindowParameters getWindow() {
            return window;
        }

        public void setWindow(WindowUtil.WindowParameters window) {
            this.window = window;
        }

        public TriggerUtil.TriggerParameters getTrigger() {
            return trigger;
        }

        public void setTrigger(TriggerUtil.TriggerParameters trigger) {
            this.trigger = trigger;
        }

        public WindowUtil.AccumulationMode getAccumulationMode() {
            return accumulationMode;
        }

        public void setAccumulationMode(WindowUtil.AccumulationMode accumulationMode) {
            this.accumulationMode = accumulationMode;
        }

        public List<AggregationDefinition> getAggregations() {
            return aggregations;
        }

        public void setAggregations(List<AggregationDefinition> aggregations) {
            this.aggregations = aggregations;
        }

        public Boolean getOutputEmpty() {
            return outputEmpty;
        }

        public void setOutputEmpty(Boolean outputEmpty) {
            this.outputEmpty = outputEmpty;
        }

        public Boolean getOutputPaneInfo() {
            return outputPaneInfo;
        }

        public void setOutputPaneInfo(Boolean outputPaneInfo) {
            this.outputPaneInfo = outputPaneInfo;
        }

        public OutputType getOutputType() {
            return outputType;
        }

        public void setOutputType(OutputType outputType) {
            this.outputType = outputType;
        }


        public List<String> validate() {
            final List<String> errorMessages = new ArrayList<>();
            if(this.aggregations == null || this.aggregations.size() == 0) {
                errorMessages.add("aggregations must not be null or size zero.");
            }
            if(this.window != null) {
                errorMessages.addAll(this.window.validate());
            }
            return errorMessages;
        }

        public void setDefaults() {
            if(this.window == null) {
                this.window = new WindowUtil.WindowParameters();
            }
            this.window.setDefaults();

            if(this.trigger != null) {
                this.trigger.setDefaults();
            }

            if(this.accumulationMode == null) {
                this.accumulationMode = WindowUtil.AccumulationMode.discarding;
            }

            if(this.outputEmpty == null) {
                this.outputEmpty = false;
            }
            if(this.outputPaneInfo == null) {
                this.outputPaneInfo = false;
            }
        }
    }

    private class AggregationDefinition implements Serializable {

        private String input;
        private JsonArray fields;

        public String getInput() {
            return input;
        }

        public void setInput(String input) {
            this.input = input;
        }

        public JsonArray getFields() {
            return fields;
        }

        public void setFields(JsonArray fields) {
            this.fields = fields;
        }

        public List<String> validate() {
            final List<String> errorMessages = new ArrayList<>();

            return errorMessages;
        }

    }

    private enum OutputType {
        row,
        avro,
        struct,
        document,
        entity;

        public DataType toDataType() {
            switch (this) {
                case avro:
                    return DataType.AVRO;
                case row:
                    return DataType.ROW;
                case struct:
                    return DataType.STRUCT;
                case document:
                    return DataType.DOCUMENT;
                case entity:
                    return DataType.ENTITY;
                default:
                    throw new IllegalArgumentException("Not supported type: " + this);
            }
        }
    }


    @Override
    public String getName() {
        return "aggregation";
    }

    @Override
    public Map<String, FCollection<?>> expand(List<FCollection<?>> inputs, TransformConfig config) {
        return Collections.singletonMap(config.getName(), transform(inputs, config));
    }

    public static FCollection<?> transform(final List<FCollection<?>> inputs, final TransformConfig config) {
        final AggregateTransformParameters parameters = new Gson().fromJson(config.getParameters(), AggregateTransformParameters.class);
        if (parameters == null) {
            throw new IllegalArgumentException("AggregateTransform config parameters must not be empty!");
        }

        final Map<String, KV<DataType, Schema>> inputSchemas = inputs.stream()
                .collect(Collectors
                        .toMap(FCollection::getName, f -> KV.of(f.getDataType(), f.getSchema())));

        parameters.validate();
        parameters.setDefaults();

        final List<Schema.Field> groupFields = new ArrayList<>();
        for(final Schema.Field groupField : inputs.get(0).getSchema().getFields()) {
            if(parameters.getGroupFields().contains(groupField.getName())) {
                groupFields.add(groupField);
            }
        }

        final List<Aggregators> aggregatorsList = new ArrayList<>();
        for(final AggregationDefinition definition : parameters.getAggregations()) {
            final KV<DataType, Schema> typeAndSchema = inputSchemas.get(definition.getInput());
            aggregatorsList.add(Aggregators.of(definition.getInput(), groupFields, typeAndSchema.getKey(), typeAndSchema.getValue(), definition.getFields()));
        }

        final List<TupleTag<?>> tags = new ArrayList<>();
        final List<String> inputNames = new ArrayList<>();
        final List<DataType> inputTypes = new ArrayList<>();

        final DataType outputType = outputType(parameters.getOutputType(), inputs, OptionUtil.isStreaming(inputs.get(0).getCollection()));

        PCollectionTuple tuple = PCollectionTuple.empty(inputs.get(0).getCollection().getPipeline());
        for(final FCollection<?> input : inputs){
            final TupleTag tag = new TupleTag<>(){};
            tags.add(tag);
            inputNames.add(input.getName());
            inputTypes.add(input.getDataType());

            tuple = tuple.and(tag, input.getCollection());
        }

        final KV<Schema, Set<String>> aggregateSchemaAndOutputNames = createAggregationOutputSchema(groupFields, aggregatorsList);
        final Schema outputSchema;
        if(parameters.getOutputPaneInfo()) {
            outputSchema = RowSchemaUtil.toBuilder(aggregateSchemaAndOutputNames.getKey())
                    .addField("paneFirst", Schema.FieldType.BOOLEAN)
                    .addField("paneLast", Schema.FieldType.BOOLEAN)
                    .addField("paneIndex", Schema.FieldType.INT64)
                    .addField("paneTiming", Schema.FieldType.STRING)
                    .addField("timestamp", Schema.FieldType.DATETIME)
                    .build();
        } else {
            outputSchema = RowSchemaUtil.toBuilder(aggregateSchemaAndOutputNames.getKey())
                    .addField("timestamp", Schema.FieldType.DATETIME)
                    .build();
        }

        final Map<String, Aggregators> aggregators = aggregatorsList.stream()
                .collect(Collectors.toMap(Aggregators::getInput, s -> s));

        switch (outputType) {
            case ROW: {
                final Transform<Schema,Schema,Row> transform = new Transform<>(
                        parameters,
                        s -> s,
                        RowSchemaUtil::convertPrimitive,
                        RowSchemaUtil::create,
                        outputSchema,
                        groupFields,
                        aggregators,
                        tags,
                        inputNames,
                        inputTypes);

                final PCollection<Row> output = tuple.apply(config.getName(), transform);
                return FCollection.of(config.getName(), output.setCoder(RowCoder.of(outputSchema)), DataType.ROW, outputSchema);
            }
            case AVRO: {
                final org.apache.avro.Schema outputAvroSchema = RowToRecordConverter.convertSchema(outputSchema);
                final Transform<String, org.apache.avro.Schema, GenericRecord> transform = new Transform<>(
                        parameters,
                        AvroSchemaUtil::convertSchema,
                        AvroSchemaUtil::convertPrimitive,
                        AvroSchemaUtil::create,
                        outputAvroSchema.toString(),
                        groupFields,
                        aggregators,
                        tags,
                        inputNames,
                        inputTypes);

                final PCollection<GenericRecord> output = tuple.apply(config.getName(), transform);
                return FCollection.of(config.getName(), output.setCoder(AvroCoder.of(outputAvroSchema)), DataType.AVRO, outputSchema);
            }
            case STRUCT: {
                final Type outputSpannerType = RowToMutationConverter.convertSchema(outputSchema);
                final Transform<Type, Type, Struct> transform = new Transform<>(
                        parameters,
                        t -> t,
                        StructSchemaUtil::convertPrimitive,
                        StructSchemaUtil::create,
                        outputSpannerType,
                        groupFields,
                        aggregators,
                        tags,
                        inputNames,
                        inputTypes);

                final PCollection<Struct> output = tuple.apply(config.getName(), transform);
                return FCollection.of(config.getName(), output.setCoder(SerializableCoder.of(Struct.class)), DataType.STRUCT, outputSpannerType);
            }
            default:
                throw new IllegalArgumentException();
        }
    }

    private static KV<Schema, Set<String>> createAggregationOutputSchema(
            final List<Schema.Field> groupFields,
            final List<Aggregators> aggregatorsList) {

        final Set<String> outputFieldNames = new HashSet<>();
        final Schema.Builder aggregateSchemaBuilder = Schema.builder();
        {
            for(final Schema.Field groupField : groupFields) {
                outputFieldNames.add(groupField.getName());
                aggregateSchemaBuilder.addField(groupField.getName(), groupField.getType());
            }
        }

        for(final Aggregators aggregators : aggregatorsList) {
            for(final Aggregator aggregation : aggregators.getAggregators()) {
                if(aggregation.getIgnore()) {
                    continue;
                }
                final List<Schema.Field> outputFields = aggregation.getOutputFields();
                for(final Schema.Field outputField : outputFields) {
                    outputFieldNames.add(outputField.getName());
                    aggregateSchemaBuilder.addField(outputField.getName(), outputField.getType());
                }
            }
        }

        return KV.of(aggregateSchemaBuilder.build(), outputFieldNames);
    }


    public static class Transform<InputSchemaT,RuntimeSchemaT,T> extends PTransform<PCollectionTuple, PCollection<T>> {

        private final SchemaUtil.SchemaConverter<InputSchemaT,RuntimeSchemaT> schemaConverter;
        private final SchemaUtil.PrimitiveValueConverter valueConverter;
        private final SchemaUtil.ValueCreator<RuntimeSchemaT,T> valueCreator;

        private final InputSchemaT inputOutputSchema;

        private final List<Schema.Field> groupFields;
        private final WindowUtil.WindowParameters windowParameters;
        private final TriggerUtil.TriggerParameters triggerParameters;
        private final WindowUtil.AccumulationMode accumulationMode;

        private final Boolean outputEmpty;
        private final Boolean outputPaneInfo;
        private final Map<String, Aggregators> aggregatorsMap;
        private final List<TupleTag<?>> tags;
        private final List<String> inputNames;
        private final List<DataType> inputTypes;

        Transform(final AggregateTransformParameters parameters,
                  final SchemaUtil.SchemaConverter<InputSchemaT,RuntimeSchemaT> schemaConverter,
                  final SchemaUtil.PrimitiveValueConverter valueConverter,
                  final SchemaUtil.ValueCreator<RuntimeSchemaT,T> valueCreator,
                  final InputSchemaT inputOutputSchema,
                  final List<Schema.Field> groupFields,
                  final Map<String, Aggregators> aggregatorsMap,
                  final List<TupleTag<?>> tags,
                  final List<String> inputNames,
                  final List<DataType> inputTypes) {

            this.schemaConverter = schemaConverter;
            this.valueConverter = valueConverter;
            this.valueCreator = valueCreator;
            this.inputOutputSchema = inputOutputSchema;

            this.groupFields = groupFields;
            this.windowParameters = parameters.getWindow();
            this.triggerParameters = parameters.getTrigger();
            this.accumulationMode = parameters.getAccumulationMode();
            this.aggregatorsMap = aggregatorsMap;

            this.outputEmpty = parameters.getOutputEmpty();
            this.outputPaneInfo = parameters.getOutputPaneInfo();

            this.tags = tags;
            this.inputNames = inputNames;
            this.inputTypes = inputTypes;
        }

        @Override
        public PCollection<T> expand(PCollectionTuple inputs) {

            Window<KV<String,UnionValue>> window = WindowUtil.createWindow(windowParameters);
            if(triggerParameters != null) {
                window = window.triggering(TriggerUtil.createTrigger(triggerParameters));
                if(WindowUtil.AccumulationMode.accumulating.equals(accumulationMode)) {
                    window = window.accumulatingFiredPanes();
                } else {
                    window = window.discardingFiredPanes();
                }

                if(windowParameters.getAllowedLateness() != null) {
                    window = window.withAllowedLateness(DateTimeUtil.getDuration(windowParameters.getUnit(), windowParameters.getAllowedLateness()));
                }
            }

            if(windowParameters.getTimestampCombiner() != null) {
                window = window.withTimestampCombiner(windowParameters.getTimestampCombiner());
            }

            final List<String> groupFieldNames = groupFields.stream()
                    .map(Schema.Field::getName)
                    .collect(Collectors.toList());

            return inputs
                    .apply("Union", Union.withKey(tags, inputTypes, groupFieldNames, inputNames))
                    .apply("WithWindow", window)
                    .apply("Aggregate", Combine.perKey(new AggregationCombineFn(inputNames, aggregatorsMap)))
                    .apply("Values", ParDo.of(new AggregationOutputDoFn(
                            inputOutputSchema, schemaConverter, valueConverter, valueCreator,
                            groupFields, aggregatorsMap, outputEmpty, outputPaneInfo)));
        }

        private static class AggregationCombineFn extends Combine.CombineFn<UnionValue, Accumulator, Accumulator> {

            private final List<String> inputNames;
            private final Map<String, Aggregators> aggregatorsMap;

            private transient Map<String, Aggregators> aggregators;

            AggregationCombineFn(final List<String> inputNames,
                                 final Map<String, Aggregators> aggregatorsMap) {

                this.inputNames = inputNames;
                this.aggregatorsMap = aggregatorsMap;
            }

            public void init() {
                if(this.aggregators == null) {
                    this.aggregators = aggregatorsMap
                            .entrySet()
                            .stream()
                            .collect(Collectors.toMap(
                                    Map.Entry::getKey,
                                    e -> e.getValue().setup()));
                }
            }

            @Override
            public Accumulator createAccumulator() {
                init();
                return Accumulator.of();
            }

            @Override
            public Accumulator addInput(Accumulator accum, final UnionValue input) {
                init();
                final String inputName = inputNames.get(input.getIndex());
                accum = aggregators.get(inputName).addInput(accum, input);
                return accum;
            }

            @Override
            public Accumulator mergeAccumulators(final Iterable<Accumulator> accums) {
                init();
                Accumulator accumulator = Accumulator.of();
                for(final Aggregators a : aggregators.values()) {
                    accumulator = a.mergeAccumulators(accumulator, accums);
                }
                return accumulator;
            }

            @Override
            public Accumulator extractOutput(final Accumulator accum) {
                return accum;
            }

            /*
            @Override
            public Coder<T> getAccumulatorCoder(CoderRegistry registry, Coder<T> inputCoder) {
                return this.accumulatorCoder;
            }

            @Override
            public Coder<T> getDefaultOutputCoder(CoderRegistry registry, Coder<T> inputCoder) {
                return this.outputCoder;
            }
             */

        }

        private class AggregationOutputDoFn extends DoFn<KV<String, Accumulator>, T> {

            private final InputSchemaT inputSchema;
            private final SchemaUtil.SchemaConverter<InputSchemaT,RuntimeSchemaT> schemaConverter;
            private final SchemaUtil.PrimitiveValueConverter valueConverter;
            private final SchemaUtil.ValueCreator<RuntimeSchemaT,T> valueCreator;

            private final List<Schema.Field> groupFields;
            private final Map<String, Aggregators> aggregatorsMap;
            private final Boolean outputEmpty;
            private final Boolean outputPaneInfo;

            private transient RuntimeSchemaT runtimeSchema;

            AggregationOutputDoFn(final InputSchemaT inputSchema,
                                  final SchemaUtil.SchemaConverter<InputSchemaT,RuntimeSchemaT> schemaConverter,
                                  final SchemaUtil.PrimitiveValueConverter valueConverter,
                                  final SchemaUtil.ValueCreator<RuntimeSchemaT,T> valueCreator,
                                  final List<Schema.Field> groupFields,
                                  final Map<String, Aggregators> aggregatorsMap,
                                  final Boolean outputEmpty,
                                  final Boolean outputPaneInfo) {

                this.inputSchema = inputSchema;
                this.schemaConverter = schemaConverter;
                this.valueConverter = valueConverter;
                this.valueCreator = valueCreator;

                this.groupFields = groupFields;
                this.aggregatorsMap = aggregatorsMap;
                this.outputEmpty = outputEmpty;
                this.outputPaneInfo = outputPaneInfo;
            }

            @Setup
            public void setup() {
                this.runtimeSchema = schemaConverter.convert(inputSchema);
                for(final Aggregators aggregators : aggregatorsMap.values()) {
                    aggregators.setup();
                }
            }

            @ProcessElement
            public void processElement(ProcessContext c) {
                final Accumulator accumulator = c.element().getValue();
                if(accumulator == null) {
                    return;
                }
                if(!outputEmpty && accumulator.empty) {
                    LOG.info("Skip empty aggregation result");
                    return;
                }

                Map<String, Object> values = new HashMap<>();
                // set common fields values
                for(final Schema.Field groupField : groupFields) {
                    final Object primitiveValue = accumulator.get(groupField.getType(), groupField.getName());
                    final Object value = valueConverter.convertPrimitive(groupField.getType(), primitiveValue);
                    values.put(groupField.getName(), value);
                }

                // set aggregation values
                for(final Aggregators aggregators : aggregatorsMap.values()) {
                    values = aggregators.extractOutput(accumulator, values, valueConverter);
                }

                // set timestamp and pane values
                final long epochMicros = c.timestamp().getMillis() * 1000L;
                values.put("timestamp", valueConverter.convertPrimitive(Schema.FieldType.DATETIME, epochMicros));

                if(outputPaneInfo) {
                    values.put("paneFirst", c.pane().isFirst());
                    values.put("paneLast", c.pane().isLast());
                    values.put("paneIndex", c.pane().getIndex());
                    values.put("paneTiming", c.pane().getTiming().name());
                }

                final T output = valueCreator.create(runtimeSchema, values);
                c.output(output);
            }

        }

    }

    private static DataType outputType(final OutputType initType, final List<FCollection<?>> fCollections, final boolean streaming) {
        if(initType != null) {
            return initType.toDataType();
        }
        final Set<DataType> inputTypes = fCollections
                .stream()
                .map(FCollection::getDataType)
                .collect(Collectors.toSet());
        if(inputTypes.size() == 1) {
            return inputTypes.stream().findFirst().orElseThrow();
        }

        if(streaming) {
            return DataType.ROW;
        } else {
            return DataType.AVRO;
        }
    }

}
