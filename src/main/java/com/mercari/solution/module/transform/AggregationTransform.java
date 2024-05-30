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
import org.apache.beam.sdk.state.*;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.*;
import org.joda.time.Instant;
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
        private AggregationLimit limit;

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

        public AggregationLimit getLimit() {
            return limit;
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


        public static AggregateTransformParameters of(
                final JsonElement jsonElement,
                final String name,
                final Map<String, KV<DataType, Schema>> inputSchemas) {

            final AggregateTransformParameters parameters = new Gson().fromJson(jsonElement, AggregateTransformParameters.class);
            if (parameters == null) {
                throw new IllegalArgumentException("AggregateTransform config parameters must not be empty!");
            }

            parameters.validate(name, inputSchemas);
            parameters.setDefaults();

            return parameters;
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
            if(this.filter != null && !this.filter.isJsonNull()) {
                if(this.filter.isJsonPrimitive()) {
                    errorMessages.add("Aggregation transform module[" + name + "].filter parameter must be array or object.");
                }
            }
            if(this.limit != null) {
                errorMessages.addAll(this.limit.validate(name));
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

            if(this.limit != null) {
                this.limit.setDefaults();
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

    private static class AggregationLimit implements Serializable {

        private Integer count;
        private String outputStartAt;

        public Integer getCount() {
            return count;
        }

        public Instant getOutputStartAt() {
            if(this.outputStartAt == null) {
                return null;
            }
            return Instant.parse(outputStartAt);
        }

        public List<String> validate(String name) {
            final List<String> errorMessages = new ArrayList<>();
            if(this.count == null && this.outputStartAt == null) {
                errorMessages.add("Aggregation transform module[" + name + "].limit.count parameter must not be null.");
            } else if(this.count != null) {
                if(this.count < 1) {
                    errorMessages.add("Aggregation transform module[" + name + "].limit.count parameter must not over zero.");
                }
            }
            return errorMessages;
        }

        public void setDefaults() {

        }

    }

    private enum OutputType {
        row,
        avro;

        public DataType toDataType() {
            return switch (this) {
                case avro -> DataType.AVRO;
                case row -> DataType.ROW;
                default -> throw new IllegalArgumentException("Not supported type: " + this);
            };
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
        final Map<String, KV<DataType, Schema>> inputSchemas = inputs.stream()
                .collect(Collectors
                        .toMap(FCollection::getName, f -> KV.of(f.getDataType(), f.getSchema())));

        final AggregateTransformParameters parameters = AggregateTransformParameters.of(config.getParameters(), config.getName(), inputSchemas);

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
                        outputType,
                        RowCoder.of(outputSchema));

                final PCollection<Row> output = tuple.apply(config.getName(), transform);
                return FCollection.of(config.getName(), output, DataType.ROW, outputSchema);
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
                        outputType,
                        AvroCoder.of(outputAvroSchema));

                final PCollection<GenericRecord> output = tuple.apply(config.getName(), transform);
                return FCollection.of(config.getName(), output, DataType.AVRO, outputSchema);
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
        private final AggregationLimit limit;
        private final Integer fanout;

        private final Boolean outputEmpty;
        private final Boolean outputPaneInfo;
        private final Map<String, Aggregators> aggregatorsMap;
        private final List<TupleTag<?>> tags;
        private final List<String> inputNames;
        private final List<DataType> inputTypes;
        private final DataType outputType;
        private final Coder<T> outputCoder;

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
                  final DataType outputType,
                  final Coder<T> outputCoder) {

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
            this.limit = parameters.getLimit();
            this.fanout = parameters.getFanout();

            this.outputEmpty = parameters.getOutputEmpty();
            this.outputPaneInfo = parameters.getOutputPaneInfo();

            this.tags = tags;
            this.inputNames = inputNames;
            this.inputTypes = inputTypes;
            this.outputType = outputType;

            this.outputCoder = outputCoder;
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

            final PCollection<KV<String, T>> aggregated;
            if(this.groupFields == null || this.groupFields.isEmpty()) {
                aggregated = flatten(inputs, window);
            } else {
                aggregated = withKey(inputs, window);
            }

            final PCollection<T> limited;
            if(limit != null) {
                final DoFn<KV<String,T>, T> limitDoFn;
                if(OptionUtil.isStreaming(inputs)) {
                    limitDoFn = new AggregationLimitStreamingDoFn<>(limit, outputCoder);
                } else {
                    limitDoFn = new AggregationLimitBatchDoFn<>(limit, outputCoder);
                }
                limited = aggregated.apply("Limit", ParDo.of(limitDoFn));
            } else {
                limited = aggregated.apply("Values", Values.create());
            }

            return limited.setCoder(outputCoder);
        }

        private PCollection<KV<String, T>> withKey(final PCollectionTuple inputs, final Window<KV<String,UnionValue>> window) {
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
                    .apply("Filter", ParDo.of(new AggregationOutputWithKeyDoFn(
                            inputOutputSchema, schemaConverter, valueConverter, valueCreator,
                            groupFields, filterJson, selectFunctions,
                            aggregatorsMap, outputEmpty, outputPaneInfo, outputType)))
                    .setCoder(KvCoder.of(StringUtf8Coder.of(), outputCoder));
        }

        private PCollection<KV<String, T>> flatten(final PCollectionTuple inputs, final Window<UnionValue> window) {
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
                    .apply("Filter", ParDo.of(new AggregationOutputFlattenDoFn(
                            inputOutputSchema, schemaConverter, valueConverter, valueCreator,
                            groupFields, filterJson, selectFunctions,
                            aggregatorsMap, outputEmpty, outputPaneInfo, outputType)))
                    .setCoder(KvCoder.of(StringUtf8Coder.of(), outputCoder));
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
                final String key = c.element().getKey();
                final Accumulator accumulator = c.element().getValue();
                super.process(accumulator, c, key);
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
                super.process(accumulator, c, "");
            }

        }

        protected class AggregationOutputDoFn<InputT> extends DoFn<InputT, KV<String,T>> {

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

            protected void process(Accumulator accumulator, ProcessContext c, String key) {
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

                values = SelectFunction.apply(selectFunctions, values, outputType, c.timestamp());

                if(conditionNode == null || Filter.filter(conditionNode, values)) {
                    final T output = valueCreator.create(runtimeSchema, values);
                    c.output(KV.of(key, output));
                }

            }

        }


        private static class AggregationLimitBatchDoFn<InputT> extends DoFn<KV<String,InputT>, InputT> {

            protected static final String STATEID_COUNT = "aggregationLimitCount";
            protected static final String STATEID_BUFFER = "aggregationLimitBuffer";

            @StateId(STATEID_COUNT)
            private final StateSpec<ValueState<Integer>> countSpec;
            @StateId(STATEID_BUFFER)
            private final StateSpec<ValueState<KV<Instant, InputT>>> bufferSpec;

            private final Integer limitCount;
            private final Instant outputStartAt;


            AggregationLimitBatchDoFn(
                    final AggregationLimit limit,
                    final Coder<InputT> coder) {

                this.countSpec = StateSpecs.value(VarIntCoder.of());
                this.bufferSpec = StateSpecs.value(KvCoder.of(InstantCoder.of(), coder));

                this.limitCount = limit.getCount();
                this.outputStartAt = limit.getOutputStartAt();


            }


            @Setup
            public void setup() {

            }

            @ProcessElement
            @RequiresTimeSortedInput
            public void processElement(
                    final ProcessContext c,
                    final @AlwaysFetched @StateId(STATEID_COUNT) ValueState<Integer> countValueState,
                    final @AlwaysFetched @StateId(STATEID_BUFFER) ValueState<KV<Instant, InputT>> bufferValueState) {

                if(c.element() == null) {
                    return;
                }

                final InputT input = c.element().getValue();
                final Instant timestamp = c.timestamp();

                if(this.outputStartAt != null) {
                    final KV<Instant, InputT> buffer = bufferValueState.read();
                    if(this.outputStartAt.isAfter(timestamp)) {
                        if(buffer == null || timestamp.isAfter(buffer.getKey())) {
                            bufferValueState.write(KV.of(timestamp, input));
                        }
                        return;
                    } else if(buffer != null) {

                    }
                }

                if(this.limitCount != null) {
                    final Integer outputCount = Optional
                            .ofNullable(countValueState.read())
                            .orElse(0);
                    if(this.limitCount > outputCount) {
                        c.output(input);
                        countValueState.write(outputCount + 1);
                    }
                } else {
                    c.output(input);
                }

            }

            /*
            @OnWindowExpiration
            public void onWindowExpiration(
                    final OutputReceiver<Row> receiver,
                    final @StateId(STATE_ID_BUFFER) BagState<MatchingEngineUtil.DataPoint> bufferState,
                    final @StateId(STATE_ID_BUFFER_SIZE) CombiningState<Long, long[], Long> bufferSizeState) {

                LOG.info("onWindowExpiration");

                flush(receiver, bufferState, bufferSizeState);
            }

             */


        }

        private static class AggregationLimitStreamingDoFn<InputT> extends DoFn<KV<String,InputT>, InputT> {

            protected static final String STATEID_COUNT = "aggregationLimitCount";
            protected static final String STATEID_BUFFER = "aggregationLimitBuffer";
            protected static final String TIMERID_OUTPUT = "aggregationLimitOutput";

            @StateId(STATEID_COUNT)
            private final StateSpec<ValueState<Integer>> countSpec;
            @StateId(STATEID_BUFFER)
            private final StateSpec<ValueState<KV<Instant, InputT>>> bufferSpec;

            @TimerId(TIMERID_OUTPUT)
            private final TimerSpec timer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

            private final Integer limitCount;
            private final Instant outputStartAt;


            AggregationLimitStreamingDoFn(final AggregationLimit limit, final Coder<InputT> coder) {
                this.countSpec = StateSpecs.value(VarIntCoder.of());
                this.bufferSpec = StateSpecs.value(KvCoder.of(InstantCoder.of(), coder));
                this.limitCount = limit.getCount();
                this.outputStartAt = limit.getOutputStartAt();
            }


            @Setup
            public void setup() {

            }

            @ProcessElement
            public void processElement(
                    final ProcessContext c,
                    final @AlwaysFetched @StateId(STATEID_COUNT) ValueState<Integer> countValueState,
                    final @AlwaysFetched @StateId(STATEID_BUFFER) ValueState<KV<Instant, InputT>> bufferValueState,
                    final @TimerId(TIMERID_OUTPUT) Timer timer) {

                if(c.element() == null) {
                    return;
                }

                final InputT input = c.element().getValue();
                final Instant timestamp = c.timestamp();

                if(this.outputStartAt != null && this.outputStartAt.isAfter(timestamp)) {
                    final KV<Instant, InputT> buffer = Optional
                            .ofNullable(bufferValueState.read())
                            .orElseGet(() -> KV.of(Instant.ofEpochMilli(0L), input));
                    if(timestamp.isAfter(buffer.getKey())) {
                        bufferValueState.write(KV.of(timestamp, input));
                        timer.set(this.outputStartAt);
                    }
                    LOG.info("buffer timestamp: " + timestamp);
                    return;
                }

                if(this.limitCount != null) {
                    final Integer outputCount = Optional
                            .ofNullable(countValueState.read())
                            .orElse(0);
                    if(this.limitCount > outputCount) {
                        c.output(input);
                        countValueState.write(outputCount + 1);
                        LOG.info("output count: " + outputCount);
                    } else {
                        LOG.info("limit count: " + outputCount);
                    }
                } else {
                    c.output(input);
                }

            }

            @OnTimer(TIMERID_OUTPUT)
            public void onTimer(final OnTimerContext c,
                                final @AlwaysFetched @StateId(STATEID_COUNT) ValueState<Integer> countValueState,
                                final @AlwaysFetched @StateId(STATEID_BUFFER) ValueState<KV<Instant,InputT>> bufferValueState) {

                final KV<Instant, InputT> buffer = bufferValueState.read();
                if(buffer != null) {
                    c.output(buffer.getValue());
                    final Integer outputCount = Optional
                            .ofNullable(countValueState.read())
                            .orElse(0);
                    countValueState.write(outputCount + 1);
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
