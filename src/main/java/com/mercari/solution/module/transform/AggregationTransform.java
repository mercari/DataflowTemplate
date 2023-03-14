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
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;


public class AggregationTransform implements TransformModule {

    private static final Logger LOG = LoggerFactory.getLogger(AggregationTransform.class);

    public class AggregateTransformParameters implements Serializable {

        private List<String> groupFields;

        private WindowParameters window;
        private TriggerParameters trigger;
        private AccumulationMode accumulationMode;

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

        public WindowParameters getWindow() {
            return window;
        }

        public void setWindow(WindowParameters window) {
            this.window = window;
        }

        public TriggerParameters getTrigger() {
            return trigger;
        }

        public void setTrigger(TriggerParameters trigger) {
            this.trigger = trigger;
        }

        public AccumulationMode getAccumulationMode() {
            return accumulationMode;
        }

        public void setAccumulationMode(AccumulationMode accumulationMode) {
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
                this.window = new WindowParameters();
            }
            this.window.setDefaults();

            if(this.trigger != null) {
                this.trigger.setDefaults();
            }

            if(this.accumulationMode == null) {
                this.accumulationMode = AccumulationMode.discarding;
            }

            if(this.outputEmpty == null) {
                this.outputEmpty = false;
            }
            if(this.outputPaneInfo == null) {
                this.outputPaneInfo = false;
            }
        }
    }

    public class WindowParameters implements Serializable {

        private WindowType type;
        private DateTimeUtil.TimeUnit unit;
        private Long size;
        private Long period;
        private Long gap;
        private Long offset;
        private String timezone;
        private String startDate;
        private Long allowedLateness;
        private TimestampCombiner timestampCombiner;


        public WindowType getType() {
            return type;
        }

        public void setType(WindowType type) {
            this.type = type;
        }

        public DateTimeUtil.TimeUnit getUnit() {
            return unit;
        }

        public void setUnit(DateTimeUtil.TimeUnit unit) {
            this.unit = unit;
        }

        public Long getSize() {
            return size;
        }

        public void setSize(Long size) {
            this.size = size;
        }

        public Long getPeriod() {
            return period;
        }

        public void setPeriod(Long period) {
            this.period = period;
        }

        public Long getGap() {
            return gap;
        }

        public void setGap(Long gap) {
            this.gap = gap;
        }

        public Long getOffset() {
            return offset;
        }

        public void setOffset(Long offset) {
            this.offset = offset;
        }

        public String getTimezone() {
            return timezone;
        }

        public void setTimezone(String timezone) {
            this.timezone = timezone;
        }

        public String getStartDate() {
            return startDate;
        }

        public void setStartDate(String startDate) {
            this.startDate = startDate;
        }

        public Long getAllowedLateness() {
            return allowedLateness;
        }

        public void setAllowedLateness(Long allowedLateness) {
            this.allowedLateness = allowedLateness;
        }

        public TimestampCombiner getTimestampCombiner() {
            return timestampCombiner;
        }

        public void setTimestampCombiner(TimestampCombiner timestampCombiner) {
            this.timestampCombiner = timestampCombiner;
        }

        private List<String> validate() {
            final List<String> errorMessages = new ArrayList<>();
            if(!WindowType.global.equals(this.type)) {
                if(this.size == null) {
                    if(WindowType.fixed.equals(this.type) || WindowType.sliding.equals(this.type)) {
                        errorMessages.add("Aggregation module.window requires size for fixed or sliding window");
                    }
                }
                if(this.period == null) {
                    if(WindowType.sliding.equals(this.type)) {
                        errorMessages.add("Aggregation module.window requires period for sliding window");
                    }
                }
                if(WindowType.session.equals(this.type)) {
                    if(this.gap == null) {
                        errorMessages.add("Aggregation module.window requires gap for session window");
                    }
                }
            }
            return errorMessages;
        }

        private void setDefaults() {
            if(this.type == null) {
                this.type = WindowType.global;
            }
            if(this.unit == null) {
                this.unit = DateTimeUtil.TimeUnit.second;
            }
            if(this.timezone == null) {
                this.timezone = "UTC";
            }
            if(this.startDate == null) {
                this.startDate = "1970-01-01";
            }
            if(this.offset == null) {
                this.offset = 0L;
            }
            if(this.allowedLateness == null) {
                this.allowedLateness = 0L;
            }
        }
    }

    public class TriggerParameters implements Serializable {

        private TriggerType type;

        // for watermark
        private TriggerParameters earlyFiringTrigger;
        private TriggerParameters lateFiringTrigger;

        // for composite triggers
        private List<TriggerParameters> childrenTriggers;

        // for repeatedly
        private TriggerParameters foreverTrigger;

        // for afterProcessingTime
        private Long pastFirstElementDelay;
        private DateTimeUtil.TimeUnit pastFirstElementDelayUnit;

        // for afterPane
        private Integer elementCountAtLeast;

        // final trigger
        private TriggerParameters finalTrigger;


        public TriggerType getType() {
            return type;
        }

        public void setType(TriggerType type) {
            this.type = type;
        }

        public TriggerParameters getEarlyFiringTrigger() {
            return earlyFiringTrigger;
        }

        public void setEarlyFiringTrigger(TriggerParameters earlyFiringTrigger) {
            this.earlyFiringTrigger = earlyFiringTrigger;
        }

        public TriggerParameters getLateFiringTrigger() {
            return lateFiringTrigger;
        }

        public void setLateFiringTrigger(TriggerParameters lateFiringTrigger) {
            this.lateFiringTrigger = lateFiringTrigger;
        }

        public List<TriggerParameters> getChildrenTriggers() {
            return childrenTriggers;
        }

        public void setChildrenTriggers(List<TriggerParameters> childrenTriggers) {
            this.childrenTriggers = childrenTriggers;
        }

        public TriggerParameters getForeverTrigger() {
            return foreverTrigger;
        }

        public void setForeverTrigger(TriggerParameters foreverTrigger) {
            this.foreverTrigger = foreverTrigger;
        }

        public Long getPastFirstElementDelay() {
            return pastFirstElementDelay;
        }

        public void setPastFirstElementDelay(Long pastFirstElementDelay) {
            this.pastFirstElementDelay = pastFirstElementDelay;
        }

        public DateTimeUtil.TimeUnit getPastFirstElementDelayUnit() {
            return pastFirstElementDelayUnit;
        }

        public void setPastFirstElementDelayUnit(DateTimeUtil.TimeUnit pastFirstElementDelayUnit) {
            this.pastFirstElementDelayUnit = pastFirstElementDelayUnit;
        }

        public Integer getElementCountAtLeast() {
            return elementCountAtLeast;
        }

        public void setElementCountAtLeast(Integer elementCountAtLeast) {
            this.elementCountAtLeast = elementCountAtLeast;
        }

        public TriggerParameters getFinalTrigger() {
            return finalTrigger;
        }

        public void setFinalTrigger(TriggerParameters finalTrigger) {
            this.finalTrigger = finalTrigger;
        }

        private void setDefaults() {
            if(this.type == null) {
                this.type = TriggerType.afterWatermark;
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

    private enum WindowType implements Serializable {
        global,
        fixed,
        sliding,
        session,
        calendar
    }

    public enum TriggerType {
        afterWatermark,
        afterProcessingTime,
        afterPane,
        repeatedly,
        afterEach,
        afterFirst,
        afterAll
    }

    public enum AccumulationMode {
        discarding,
        accumulating
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
        final List<SchemaUtil.StringGetter<Object>> stringGetters = new ArrayList<>();

        final DataType outputType = outputType(parameters.getOutputType(), inputs, OptionUtil.isStreaming(inputs.get(0).getCollection()));

        PCollectionTuple tuple = PCollectionTuple.empty(inputs.get(0).getCollection().getPipeline());
        for(final FCollection<?> input : inputs){
            final TupleTag tag = new TupleTag<>(){};
            tags.add(tag);
            inputNames.add(input.getName());
            inputTypes.add(input.getDataType());

            final SchemaUtil.StringGetter<Object> stringGetter;
            switch (input.getDataType()) {
                case ROW:
                    stringGetter = RowSchemaUtil::getAsString;
                    break;
                case AVRO:
                    stringGetter = AvroSchemaUtil::getAsString;
                    break;
                case STRUCT:
                    stringGetter = StructSchemaUtil::getAsString;
                    break;
                case DOCUMENT:
                    stringGetter = DocumentSchemaUtil::getAsString;
                    break;
                case ENTITY:
                    stringGetter = EntitySchemaUtil::getAsString;
                    break;
                default:
                    throw new IllegalArgumentException("Not supported input type: " + input.getDataType());
            }
            stringGetters.add(stringGetter);

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
                        inputTypes,
                        stringGetters);

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
                        inputTypes,
                        stringGetters);

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
                        inputTypes,
                        stringGetters);

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
        private final WindowParameters windowParameters;
        private final TriggerParameters triggerParameters;
        private final AccumulationMode accumulationMode;

        private final Boolean outputEmpty;
        private final Boolean outputPaneInfo;
        private final Map<String, Aggregators> aggregatorsMap;
        private final List<TupleTag<?>> tags;
        private final List<String> inputNames;
        private final List<DataType> inputTypes;
        private final List<SchemaUtil.StringGetter<Object>> stringGetters;

        Transform(final AggregateTransformParameters parameters,
                  final SchemaUtil.SchemaConverter<InputSchemaT,RuntimeSchemaT> schemaConverter,
                  final SchemaUtil.PrimitiveValueConverter valueConverter,
                  final SchemaUtil.ValueCreator<RuntimeSchemaT,T> valueCreator,
                  final InputSchemaT inputOutputSchema,
                  final List<Schema.Field> groupFields,
                  final Map<String, Aggregators> aggregatorsMap,
                  final List<TupleTag<?>> tags,
                  final List<String> inputNames,
                  final List<DataType> inputTypes,
                  final List<SchemaUtil.StringGetter<Object>> stringGetters) {

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
            this.stringGetters = stringGetters;
        }

        @Override
        public PCollection<T> expand(PCollectionTuple inputs) {

            //
            Window<KV<String,UnionValue>> window;
            switch (windowParameters.getType()) {
                case global: {
                    window = Window.into(new GlobalWindows());
                    break;
                }
                case fixed: {
                    window = Window.into(FixedWindows
                            .of(DateTimeUtil.getDuration(windowParameters.getUnit(), windowParameters.getSize()))
                            .withOffset(DateTimeUtil.getDuration(windowParameters.getUnit(), windowParameters.getOffset())));
                    break;
                }
                case sliding: {
                    window = Window.into(SlidingWindows
                            .of(DateTimeUtil.getDuration(windowParameters.getUnit(), windowParameters.getSize()))
                            .every(DateTimeUtil.getDuration(windowParameters.getUnit(), windowParameters.getPeriod()))
                            .withOffset(DateTimeUtil.getDuration(windowParameters.getUnit(), windowParameters.getOffset())));
                    break;
                }
                case session: {
                    window = Window.into(Sessions
                            .withGapDuration(DateTimeUtil.getDuration(windowParameters.getUnit(), windowParameters.getGap())));
                    break;
                }
                case calendar: {
                    final LocalDate startDate = parseStartDate(windowParameters.getStartDate());
                    switch (windowParameters.getUnit()) {
                        case day: {
                            window = Window.into(CalendarWindows
                                    .days(windowParameters.getSize().intValue())
                                    .withTimeZone(DateTimeZone.forID(windowParameters.getTimezone()))
                                    .withStartingDay(startDate.getYear(), startDate.getMonthValue(), startDate.getDayOfMonth()));
                            break;
                        }
                        case week: {
                            window = Window.into(CalendarWindows
                                    .weeks(windowParameters.getSize().intValue(), windowParameters.getOffset().intValue())
                                    .withTimeZone(DateTimeZone.forID(windowParameters.getTimezone()))
                                    .withStartingDay(startDate.getYear(), startDate.getMonthValue(), startDate.getDayOfMonth()));
                            break;
                        }
                        case month: {
                            window = Window.into(CalendarWindows
                                    .months(windowParameters.getSize().intValue())
                                    .withTimeZone(DateTimeZone.forID(windowParameters.getTimezone()))
                                    .withStartingMonth(startDate.getYear(), startDate.getMonthValue()));
                            break;
                        }
                        case year: {
                            window = Window.into(CalendarWindows
                                    .years(windowParameters.getSize().intValue())
                                    .withTimeZone(DateTimeZone.forID(windowParameters.getTimezone()))
                                    .withStartingYear(startDate.getYear()));
                            break;
                        }
                        case second:
                        case minute:
                        case hour:
                        default:
                            throw new IllegalArgumentException("Not supported calendar timeunit type: " + windowParameters.getType());
                    }
                    break;
                }
                default:
                    throw new IllegalArgumentException("Not supported window type: " + windowParameters.getType());
            }

            if(triggerParameters != null) {
                window = window.triggering(createTrigger(triggerParameters));
                if(AccumulationMode.accumulating.equals(accumulationMode)) {
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
                    .apply("Union", Union.withKey(tags, inputTypes, stringGetters, groupFieldNames, inputNames))
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

    private static Trigger createTrigger(final TriggerParameters parameter) {
        final Trigger trigger;
        switch (parameter.getType()) {
            case afterWatermark: {
                if(parameter.getEarlyFiringTrigger() != null && parameter.getLateFiringTrigger() != null) {
                    trigger = AfterWatermark.pastEndOfWindow()
                            .withEarlyFirings((Trigger.OnceTrigger) createTrigger(parameter.getEarlyFiringTrigger()))
                            .withLateFirings((Trigger.OnceTrigger) createTrigger(parameter.getLateFiringTrigger()));
                } else if(parameter.getEarlyFiringTrigger() != null) {
                    trigger = AfterWatermark.pastEndOfWindow()
                            .withEarlyFirings((Trigger.OnceTrigger) createTrigger(parameter.getEarlyFiringTrigger()));
                } else if(parameter.getLateFiringTrigger() != null) {
                    trigger = AfterWatermark.pastEndOfWindow()
                            .withLateFirings((Trigger.OnceTrigger) createTrigger(parameter.getLateFiringTrigger()));
                } else {
                    trigger = AfterWatermark.pastEndOfWindow();
                }
                break;
            }
            case afterProcessingTime: {
                final AfterProcessingTime afterProcessingTime = AfterProcessingTime.pastFirstElementInPane();
                trigger = afterProcessingTime.plusDelayOf(
                        DateTimeUtil.getDuration(parameter.getPastFirstElementDelayUnit(), parameter.getPastFirstElementDelay()));
                break;
            }
            case afterPane: {
                trigger = AfterPane.elementCountAtLeast(parameter.getElementCountAtLeast());
                break;
            }
            case afterFirst:
            case afterEach:
            case afterAll: {
                final List<Trigger> triggers = new ArrayList<>();
                for(final TriggerParameters child : parameter.getChildrenTriggers()) {
                    triggers.add(createTrigger(child));
                }
                switch (parameter.getType()) {
                    case afterFirst: {
                        trigger = AfterFirst.of(triggers);
                        break;
                    }
                    case afterEach: {
                        trigger = AfterEach.inOrder(triggers);
                        break;
                    }
                    case afterAll: {
                        trigger = AfterAll.of(triggers);
                        break;
                    }
                    default: {
                        throw new IllegalArgumentException("Not supported window trigger: " + parameter.getType());
                    }
                }
                break;
            }
            case repeatedly: {
                trigger = Repeatedly.forever(createTrigger(parameter.getForeverTrigger()));
                break;
            }
            default: {
                throw new IllegalArgumentException("Not supported window trigger: " + parameter.getType());
            }
        }

        if(parameter.getFinalTrigger() != null) {
            return trigger.orFinally((Trigger.OnceTrigger) createTrigger(parameter.getFinalTrigger()));
        }

        return trigger;
    }

    private static LocalDate parseStartDate(final String startDate) {
        final Integer year;
        final Integer month;
        final Integer day;
        if(startDate != null) {
            final String[] s = startDate.split("-");
            if(s.length == 1) {
                year = Integer.valueOf(s[0]);
                month = 1;
                day = 1;
            } else if(s.length == 2) {
                year = Integer.valueOf(s[0]);
                month = Integer.valueOf(s[1]);
                day = 1;
            } else if(s.length == 3) {
                year = Integer.valueOf(s[0]);
                month = Integer.valueOf(s[1]);
                day = Integer.valueOf(s[2]);
            } else {
                year = 1970;
                month = 1;
                day = 1;
            }
        } else {
            year = 1970;
            month = 1;
            day = 1;
        }

        return LocalDate.of(year, month, day);
    }

}
