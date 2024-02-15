package com.mercari.solution.module.transform;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.mercari.solution.config.TransformConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.module.TransformModule;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.Filter;
import com.mercari.solution.util.OptionUtil;
import com.mercari.solution.util.converter.RowToRecordConverter;
import com.mercari.solution.util.pipeline.TriggerUtil;
import com.mercari.solution.util.pipeline.WindowUtil;
import com.mercari.solution.util.pipeline.aggregation.Accumulator;
import com.mercari.solution.util.pipeline.aggregation.Aggregator;
import com.mercari.solution.util.pipeline.aggregation.Aggregators;
import com.mercari.solution.util.pipeline.select.SelectFunction;
import com.mercari.solution.util.pipeline.union.Union;
import com.mercari.solution.util.pipeline.union.UnionValue;
import com.mercari.solution.util.schema.*;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
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

    public static class AggregateTransformParameters implements Serializable {

        private List<String> groupFields;
        private JsonElement filter;
        private JsonArray select;

        private WindowUtil.WindowParameters window;
        private TriggerUtil.TriggerParameters trigger;
        private WindowUtil.AccumulationMode accumulationMode;

        private List<AggregationDefinition> aggregations;

        private Integer fanout;

        private Boolean outputEmpty;
        private Boolean outputPaneInfo;

        private OutputType outputType;

        public List<String> getGroupFields() {
            return groupFields;
        }

        public JsonElement getFilter() {
            return filter;
        }

        public JsonArray getSelect() {
            return select;
        }

        public WindowUtil.WindowParameters getWindow() {
            return window;
        }

        public TriggerUtil.TriggerParameters getTrigger() {
            return trigger;
        }

        public WindowUtil.AccumulationMode getAccumulationMode() {
            return accumulationMode;
        }

        public List<AggregationDefinition> getAggregations() {
            return aggregations;
        }

        public Integer getFanout() {
            return fanout;
        }

        public Boolean getOutputEmpty() {
            return outputEmpty;
        }

        public Boolean getOutputPaneInfo() {
            return outputPaneInfo;
        }

        public OutputType getOutputType() {
            return outputType;
        }


        public void validate(String name, Map<String, KV<DataType, Schema>> inputSchemas) {
            final List<String> errorMessages = new ArrayList<>();
            if(this.aggregations == null || this.aggregations.size() == 0) {
                errorMessages.add("Aggregation transform module[" + name + "].aggregations parameter must not be null or size zero.");
            } else {
                for(int index=0; index < this.aggregations.size(); index++) {
                    final AggregationDefinition definition = this.aggregations.get(index);
                    errorMessages.addAll(definition.validate(name, inputSchemas, index));
                }
            }
            if(this.window != null) {
                errorMessages.addAll(this.window.validate());
            }

            if(!errorMessages.isEmpty()) {
                throw new IllegalArgumentException(String.join(", ", errorMessages));
            }
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

    private static class AggregationDefinition implements Serializable {

        private String input;
        private JsonArray fields;

        public String getInput() {
            return input;
        }

        public JsonArray getFields() {
            return fields;
        }

        public List<String> validate(String name, Map<String, KV<DataType, Schema>> inputSchemas, int index) {
            final List<String> errorMessages = new ArrayList<>();
            if(this.input == null) {
                errorMessages.add("Aggregation transform module[" + name + "].aggregations[" + index + "].input parameter must not be null.");
            } else if(!inputSchemas.containsKey(this.input)) {
                errorMessages.add("Aggregation transform module[" + name + "].aggregations[" + index + "].input not found in inputs: " + inputSchemas.keySet());
            }
            return errorMessages;
        }

    }

    private enum OutputType {
        row,
        avro;

        public DataType toDataType() {
            switch (this) {
                case avro:
                    return DataType.AVRO;
                case row:
                    return DataType.ROW;
                default:
                    throw new IllegalArgumentException("Not supported type: " + this);
            }
        }
    }

    private enum Op {
        current_timestamp
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

        parameters.validate(config.getName(), inputSchemas);
        parameters.setDefaults();

        final List<Schema.Field> groupFields = new ArrayList<>();
        for(final Schema.Field groupField : inputs.get(0).getSchema().getFields()) {
            if(parameters.getGroupFields().contains(groupField.getName())) {
                groupFields.add(groupField);
            }
        }

        final DataType outputType = outputType(parameters.getOutputType(), inputs, OptionUtil.isStreaming(inputs.get(0).getCollection()));

        final List<Aggregators> aggregatorsList = new ArrayList<>();
        for(final AggregationDefinition definition : parameters.getAggregations()) {
            final KV<DataType, Schema> typeAndSchema = inputSchemas.get(definition.getInput());
            aggregatorsList.add(Aggregators.of(definition.getInput(), groupFields, typeAndSchema.getKey(), outputType, typeAndSchema.getValue(), definition.getFields()));
        }

        final List<TupleTag<?>> tags = new ArrayList<>();
        final List<String> inputNames = new ArrayList<>();
        final List<DataType> inputTypes = new ArrayList<>();

        PCollectionTuple tuple = PCollectionTuple.empty(inputs.get(0).getCollection().getPipeline());
        for(final FCollection<?> input : inputs){
            final TupleTag tag = new TupleTag<>(){};
            tags.add(tag);
            inputNames.add(input.getName());
            inputTypes.add(input.getDataType());

            tuple = tuple.and(tag, input.getCollection());
        }

        final String filterJson;
        if(parameters.getFilter() == null || parameters.getFilter().isJsonNull()) {
            filterJson = null;
        } else {
            filterJson = parameters.getFilter().toString();
        }

        final List<Schema.Field> aggregationOutputFields = createAggregationOutputFields(
                groupFields, aggregatorsList, parameters.getOutputPaneInfo());
        final List<SelectFunction> selectFunctions = SelectFunction.of(parameters.getSelect(), aggregationOutputFields, outputType);

        final Schema outputSchema;
        if(selectFunctions.size() == 0) {
            outputSchema = Schema.builder().addFields(aggregationOutputFields).build();
        } else {
            outputSchema = SelectFunction.createSchema(selectFunctions);
        }

        final Map<String, Aggregators> aggregators = aggregatorsList.stream()
                .collect(Collectors.toMap(Aggregators::getInput, s -> s));

        switch (outputType) {
            case ROW -> {
                final Transform<Schema, Schema, Row> transform = new Transform<>(
                        parameters,
                        s -> s,
                        RowSchemaUtil::convertPrimitive,
                        RowSchemaUtil::create,
                        outputSchema,
                        groupFields,
                        filterJson,
                        selectFunctions,
                        aggregators,
                        tags,
                        inputNames,
                        inputTypes,
                        outputType);

                final PCollection<Row> output = tuple.apply(config.getName(), transform);
                return FCollection.of(config.getName(), output.setCoder(RowCoder.of(outputSchema)), DataType.ROW, outputSchema);
            }
            case AVRO -> {
                final org.apache.avro.Schema outputAvroSchema = RowToRecordConverter.convertSchema(outputSchema);
                final Transform<String, org.apache.avro.Schema, GenericRecord> transform = new Transform<>(
                        parameters,
                        AvroSchemaUtil::convertSchema,
                        AvroSchemaUtil::convertPrimitive,
                        AvroSchemaUtil::create,
                        outputAvroSchema.toString(),
                        groupFields,
                        filterJson,
                        selectFunctions,
                        aggregators,
                        tags,
                        inputNames,
                        inputTypes,
                        outputType);

                final PCollection<GenericRecord> output = tuple.apply(config.getName(), transform);
                return FCollection.of(config.getName(), output.setCoder(AvroCoder.of(outputAvroSchema)), DataType.AVRO, outputSchema);
            }
            default -> throw new IllegalArgumentException();
        }
    }


    private static List<Schema.Field> createAggregationOutputFields(
            final List<Schema.Field> groupFields,
            final List<Aggregators> aggregatorsList,
            final boolean outputPane) {

        final List<Schema.Field> aggregationOutputFields = new ArrayList<>();
        for(final Schema.Field groupField : groupFields) {
            aggregationOutputFields.add(Schema.Field.of(groupField.getName(), groupField.getType()));
        }

        for(final Aggregators aggregators : aggregatorsList) {
            for(final Aggregator aggregation : aggregators.getAggregators()) {
                if(aggregation.getIgnore()) {
                    continue;
                }
                final List<Schema.Field> outputFields = aggregation.getOutputFields();
                for(final Schema.Field outputField : outputFields) {
                    aggregationOutputFields.add(Schema.Field.of(outputField.getName(), outputField.getType()));
                }
            }
        }

        if(outputPane) {
            aggregationOutputFields.add(Schema.Field.of("paneFirst", Schema.FieldType.BOOLEAN));
            aggregationOutputFields.add(Schema.Field.of("paneLast", Schema.FieldType.BOOLEAN));
            aggregationOutputFields.add(Schema.Field.of("paneIndex", Schema.FieldType.INT64));
            aggregationOutputFields.add(Schema.Field.of("paneTiming", Schema.FieldType.STRING));
        }
        aggregationOutputFields.add(Schema.Field.of("timestamp", Schema.FieldType.DATETIME));

        return aggregationOutputFields;
    }

    public static class Transform<InputSchemaT,RuntimeSchemaT,T> extends PTransform<PCollectionTuple, PCollection<T>> {

        private final SchemaUtil.SchemaConverter<InputSchemaT,RuntimeSchemaT> schemaConverter;
        private final SchemaUtil.PrimitiveValueConverter valueConverter;
        private final SchemaUtil.ValueCreator<RuntimeSchemaT,T> valueCreator;

        private final InputSchemaT inputOutputSchema;

        private final List<Schema.Field> groupFields;
        private final String filterJson;
        private final List<SelectFunction> selectFunctions;
        private final WindowUtil.WindowParameters windowParameters;
        private final TriggerUtil.TriggerParameters triggerParameters;
        private final WindowUtil.AccumulationMode accumulationMode;
        private final Integer fanout;

        private final Boolean outputEmpty;
        private final Boolean outputPaneInfo;
        private final Map<String, Aggregators> aggregatorsMap;
        private final List<TupleTag<?>> tags;
        private final List<String> inputNames;
        private final List<DataType> inputTypes;
        private final DataType outputType;

        Transform(final AggregateTransformParameters parameters,
                  final SchemaUtil.SchemaConverter<InputSchemaT,RuntimeSchemaT> schemaConverter,
                  final SchemaUtil.PrimitiveValueConverter valueConverter,
                  final SchemaUtil.ValueCreator<RuntimeSchemaT,T> valueCreator,
                  final InputSchemaT inputOutputSchema,
                  final List<Schema.Field> groupFields,
                  final String filterJson,
                  final List<SelectFunction> selectFunctions,
                  final Map<String, Aggregators> aggregatorsMap,
                  final List<TupleTag<?>> tags,
                  final List<String> inputNames,
                  final List<DataType> inputTypes,
                  final DataType outputType) {

            this.schemaConverter = schemaConverter;
            this.valueConverter = valueConverter;
            this.valueCreator = valueCreator;
            this.inputOutputSchema = inputOutputSchema;

            this.groupFields = groupFields;
            this.filterJson = filterJson;
            this.selectFunctions = selectFunctions;
            this.windowParameters = parameters.getWindow();
            this.triggerParameters = parameters.getTrigger();
            this.accumulationMode = parameters.getAccumulationMode();
            this.aggregatorsMap = aggregatorsMap;
            this.fanout = parameters.getFanout();

            this.outputEmpty = parameters.getOutputEmpty();
            this.outputPaneInfo = parameters.getOutputPaneInfo();

            this.tags = tags;
            this.inputNames = inputNames;
            this.inputTypes = inputTypes;
            this.outputType = outputType;
        }

        @Override
        public PCollection<T> expand(PCollectionTuple inputs) {

            Window window = WindowUtil.createWindow(windowParameters);
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

            if(this.groupFields == null || this.groupFields.isEmpty()) {
                return flatten(inputs, window);
            } else {
                return withKey(inputs, window);
            }
        }

        private PCollection<T> withKey(final PCollectionTuple inputs, final Window<KV<String,UnionValue>> window) {
            final List<String> groupFieldNames = groupFields.stream()
                    .map(Schema.Field::getName)
                    .collect(Collectors.toList());

            final PCollection<KV<String, UnionValue>> withKey = inputs
                    .apply("UnionWithKey", Union.withKey(tags, inputTypes, groupFieldNames, inputNames))
                    .apply("WithWindow", window);

            final PCollection<KV<String,Accumulator>> output;
            if(fanout != null) {
                output = withKey
                        .apply("AggregateFanOut", Combine
                                .<String, UnionValue, Accumulator>perKey(new AggregationCombineFn(inputNames, aggregatorsMap))
                                .withHotKeyFanout(fanout));
            } else {
                output = withKey
                        .apply("Aggregate", Combine
                                .perKey(new AggregationCombineFn(inputNames, aggregatorsMap)));
            }

            return output
                    .setCoder(KvCoder.of(StringUtf8Coder.of(), Accumulator.coder()))
                    .apply("Values", ParDo.of(new AggregationOutputWithKeyDoFn(
                            inputOutputSchema, schemaConverter, valueConverter, valueCreator,
                            groupFields, filterJson, selectFunctions,
                            aggregatorsMap, outputEmpty, outputPaneInfo, outputType)));
        }

        private PCollection<T> flatten(final PCollectionTuple inputs, final Window<UnionValue> window) {
            final PCollection<UnionValue> flatten = inputs
                    .apply("UnionFlatten", Union.flatten(tags, inputTypes, inputNames))
                    .apply("WithWindow", window);

            final PCollection<Accumulator> output;
            if(fanout != null) {
                output = flatten
                        .apply("AggregateFanOut", Combine
                                .globally(new AggregationCombineFn(inputNames, aggregatorsMap))
                                .withFanout(fanout)
                                .withoutDefaults());
            } else {
                output = flatten
                        .apply("Aggregate", Combine
                                .globally(new AggregationCombineFn(inputNames, aggregatorsMap))
                                .withoutDefaults());
            }

            return output
                    .setCoder(Accumulator.coder())
                    .apply("Values", ParDo.of(new AggregationOutputFlattenDoFn(
                            inputOutputSchema, schemaConverter, valueConverter, valueCreator,
                            groupFields, filterJson, selectFunctions,
                            aggregatorsMap, outputEmpty, outputPaneInfo, outputType)));
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

            private void init() {
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
                return aggregators.get(inputName).addInput(accum, input);
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

            @Override
            public Coder<Accumulator> getAccumulatorCoder(CoderRegistry registry, Coder<UnionValue> input) {
                return Accumulator.coder();
            }

        }

        private class AggregationOutputWithKeyDoFn extends AggregationOutputDoFn<KV<String,Accumulator>> {

            AggregationOutputWithKeyDoFn(
                    final InputSchemaT inputSchema,
                    final SchemaUtil.SchemaConverter<InputSchemaT,RuntimeSchemaT> schemaConverter,
                    final SchemaUtil.PrimitiveValueConverter valueConverter,
                    final SchemaUtil.ValueCreator<RuntimeSchemaT,T> valueCreator,
                    final List<Schema.Field> groupFields,
                    final String filterJson,
                    final List<SelectFunction> selectFunctions,
                    final Map<String, Aggregators> aggregatorsMap,
                    final Boolean outputEmpty,
                    final Boolean outputPaneInfo,
                    final DataType outputType) {

                super(inputSchema, schemaConverter, valueConverter, valueCreator, groupFields, filterJson, selectFunctions, aggregatorsMap, outputEmpty, outputPaneInfo, outputType);
            }

            @Setup
            public void setup() {
                super.setup();
            }

            @ProcessElement
            public void processElement(ProcessContext c) {
                final Accumulator accumulator = c.element().getValue();
                super.process(accumulator, c);
            }

        }

        private class AggregationOutputFlattenDoFn extends AggregationOutputDoFn<Accumulator> {

            AggregationOutputFlattenDoFn(
                    final InputSchemaT inputSchema,
                    final SchemaUtil.SchemaConverter<InputSchemaT,RuntimeSchemaT> schemaConverter,
                    final SchemaUtil.PrimitiveValueConverter valueConverter,
                    final SchemaUtil.ValueCreator<RuntimeSchemaT,T> valueCreator,
                    final List<Schema.Field> groupFields,
                    final String filterJson,
                    final List<SelectFunction> selectFunctions,
                    final Map<String, Aggregators> aggregatorsMap,
                    final Boolean outputEmpty,
                    final Boolean outputPaneInfo,
                    final DataType outputType) {

                super(inputSchema, schemaConverter, valueConverter, valueCreator, groupFields, filterJson, selectFunctions, aggregatorsMap, outputEmpty, outputPaneInfo, outputType);
            }

            @Setup
            public void setup() {
                super.setup();
            }

            @ProcessElement
            public void processElement(ProcessContext c) {
                final Accumulator accumulator = c.element();
                super.process(accumulator, c);
            }

        }

        protected class AggregationOutputDoFn<InputT> extends DoFn<InputT, T> {

            private final InputSchemaT inputSchema;
            private final SchemaUtil.SchemaConverter<InputSchemaT,RuntimeSchemaT> schemaConverter;
            private final SchemaUtil.PrimitiveValueConverter valueConverter;
            private final SchemaUtil.ValueCreator<RuntimeSchemaT,T> valueCreator;

            private final List<Schema.Field> groupFields;
            private final String filterJson;
            private final List<SelectFunction> selectFunctions;
            private final Map<String, Aggregators> aggregatorsMap;
            private final Boolean outputEmpty;
            private final Boolean outputPaneInfo;
            private final DataType outputType;

            private transient RuntimeSchemaT runtimeSchema;
            private transient Filter.ConditionNode conditionNode;

            AggregationOutputDoFn(final InputSchemaT inputSchema,
                                  final SchemaUtil.SchemaConverter<InputSchemaT,RuntimeSchemaT> schemaConverter,
                                  final SchemaUtil.PrimitiveValueConverter valueConverter,
                                  final SchemaUtil.ValueCreator<RuntimeSchemaT,T> valueCreator,
                                  final List<Schema.Field> groupFields,
                                  final String filterJson,
                                  final List<SelectFunction> selectFunctions,
                                  final Map<String, Aggregators> aggregatorsMap,
                                  final Boolean outputEmpty,
                                  final Boolean outputPaneInfo,
                                  final DataType outputType) {

                this.inputSchema = inputSchema;
                this.schemaConverter = schemaConverter;
                this.valueConverter = valueConverter;
                this.valueCreator = valueCreator;

                this.groupFields = groupFields;
                this.filterJson = filterJson;
                this.selectFunctions = selectFunctions;
                this.aggregatorsMap = aggregatorsMap;
                this.outputEmpty = outputEmpty;
                this.outputPaneInfo = outputPaneInfo;
                this.outputType = outputType;
            }

            protected void setup() {
                this.runtimeSchema = schemaConverter.convert(inputSchema);
                for(final Aggregators aggregators : aggregatorsMap.values()) {
                    aggregators.setup();
                }
                if(filterJson != null) {
                    final JsonElement filterElement = new Gson().fromJson(filterJson, JsonElement.class);
                    if(!filterElement.isJsonNull()) {
                        this.conditionNode = Filter.parse(filterElement);
                    }
                }
                for(SelectFunction selectFunction: selectFunctions) {
                    selectFunction.setup();
                }
            }

            protected void process(Accumulator accumulator, ProcessContext c) {
                if(accumulator == null) {
                    LOG.info("Skip null aggregation result");
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

                values = SelectFunction.apply(selectFunctions, values, outputType);

                if(conditionNode == null || Filter.filter(conditionNode, values)) {
                    final T output = valueCreator.create(runtimeSchema, values);
                    c.output(output);
                }

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
