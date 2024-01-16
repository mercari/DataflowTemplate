package com.mercari.solution.module.transform;

import com.google.cloud.spanner.Struct;
import com.google.datastore.v1.Entity;
import com.google.gson.Gson;
import com.mercari.solution.config.TransformConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.module.TransformModule;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.EntitySchemaUtil;
import com.mercari.solution.util.schema.RowSchemaUtil;
import com.mercari.solution.util.schema.StructSchemaUtil;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;

public class BarTransform  implements TransformModule {

    private static final Logger LOG = LoggerFactory.getLogger(BarTransform.class);

    private class BarTransformParameters implements Serializable {

        private String priceField;
        private String volumeField;
        private String timestampField;
        private List<String> symbolFields;

        private Type type;
        private Measure measure;
        private Unit unit;
        private Long size;
        private String timezone;

        private Boolean isVolumeAccumulative;
        private InformationType infoType;

        private Long intervalSeconds;

        public String getPriceField() {
            return priceField;
        }

        public void setPriceField(String priceField) {
            this.priceField = priceField;
        }

        public String getVolumeField() {
            return volumeField;
        }

        public void setVolumeField(String volumeField) {
            this.volumeField = volumeField;
        }

        public String getTimestampField() {
            return timestampField;
        }

        public void setTimestampField(String timestampField) {
            this.timestampField = timestampField;
        }

        public List<String> getSymbolFields() {
            return symbolFields;
        }

        public void setSymbolFields(List<String> symbolFields) {
            this.symbolFields = symbolFields;
        }

        public Type getType() {
            return type;
        }

        public void setType(Type type) {
            this.type = type;
        }

        public Measure getMeasure() {
            return measure;
        }

        public void setMeasure(Measure measure) {
            this.measure = measure;
        }

        public Unit getUnit() {
            return unit;
        }

        public void setUnit(Unit unit) {
            this.unit = unit;
        }

        public Long getSize() {
            return size;
        }

        public void setSize(Long size) {
            this.size = size;
        }

        public String getTimezone() {
            return timezone;
        }

        public void setTimezone(String timezone) {
            this.timezone = timezone;
        }

        public Boolean getIsVolumeAccumulative() {
            return isVolumeAccumulative;
        }

        public void setIsVolumeAccumulative(Boolean isVolumeAccumulative) {
            this.isVolumeAccumulative = isVolumeAccumulative;
        }

        public InformationType getInfoType() {
            return infoType;
        }

        public void setInfoType(InformationType infoType) {
            this.infoType = infoType;
        }

        public Long getIntervalSeconds() {
            return intervalSeconds;
        }

        public void setIntervalSeconds(Long intervalSeconds) {
            this.intervalSeconds = intervalSeconds;
        }
    }

    private enum Type implements Serializable {
        time,
        accumulation,
        information
    }

    private enum Measure implements Serializable {
        tick,
        volume,
        dollar
    }

    private enum Unit implements Serializable {
        second,
        minute,
        hour,
        day,
        week,
        month
    }

    private enum InformationType implements Serializable {
        imbalance,
        run
    }

    public String getName() {
        return "bar";
    }

    public Map<String, FCollection<?>> expand(List<FCollection<?>> inputs, TransformConfig config) {

        final BarTransformParameters parameters = new Gson().fromJson(config.getParameters(), BarTransformParameters.class);
        validateParameters(parameters);
        setDefaultParameters(parameters);

        switch (parameters.getType()) {
            case time:
                return transformTimeBar(inputs, config);
            case accumulation:
                return transformAccumulationBar(inputs, config);
            case information:
            default: {
                throw new IllegalArgumentException("Bar type: " + parameters.getType() + " is not supported");
            }
        }
    }

    private static void validateParameters(final BarTransformParameters parameters) {
        if(parameters == null) {
            throw new IllegalArgumentException("BarTransform config parameters must not be empty!");
        }

        final List<String> errorMessages = new ArrayList<>();
        if(parameters.getPriceField() == null) {
            errorMessages.add("BarTransform config parameters must contain priceField parameter.");
        }
        if(parameters.getTimestampField() == null) {
            errorMessages.add("BarTransform config parameters must contain timestampField parameter.");
        }

        if(errorMessages.size() > 0) {
            throw new IllegalArgumentException(String.join("\n", errorMessages));
        }
    }

    private static void setDefaultParameters(BarTransformParameters parameters) {
        if(parameters.getSymbolFields() == null) {
            parameters.setSymbolFields(new ArrayList<>());
        }
        if(parameters.getType() == null) {
            parameters.setType(Type.time);
        }
        if(parameters.getMeasure() == null) {
            parameters.setMeasure(Measure.tick);
        }
        if(parameters.getUnit() == null) {
            parameters.setUnit(Unit.day);
        }
        if(parameters.getSize() == null) {
            parameters.setSize(1L);
        }
        if(parameters.getTimezone() == null) {
            parameters.setTimezone("Etc/GMT");
        }
        if(parameters.getIsVolumeAccumulative() == null) {
            parameters.setIsVolumeAccumulative(false);
        }
        if(parameters.getInfoType() == null) {
            parameters.setInfoType(InformationType.imbalance);
        }
    }

    private static final Schema SCHEMA_OUTPUT = SchemaBuilder.record("bar").fields()
            .name("symbol").type(AvroSchemaUtil.REQUIRED_STRING).noDefault()
            .name("open").type(AvroSchemaUtil.REQUIRED_DOUBLE).noDefault()
            .name("close").type(AvroSchemaUtil.REQUIRED_DOUBLE).noDefault()
            .name("low").type(AvroSchemaUtil.REQUIRED_DOUBLE).noDefault()
            .name("high").type(AvroSchemaUtil.REQUIRED_DOUBLE).noDefault()
            .name("volume").type(AvroSchemaUtil.REQUIRED_DOUBLE).noDefault()
            .name("count").type(AvroSchemaUtil.REQUIRED_LONG).noDefault()
            .name("vwap").type(AvroSchemaUtil.REQUIRED_DOUBLE).noDefault()
            .name("sizeSecond").type(AvroSchemaUtil.REQUIRED_LONG).noDefault()
            .name("timestamp").type(AvroSchemaUtil.NULLABLE_LOGICAL_TIMESTAMP_MICRO_TYPE).noDefault()
            .endRecord();

    public static Map<String, FCollection<?>> transformTimeBar(final List<FCollection<?>> inputs, final TransformConfig config) {

        final BarTransformParameters parameters = new Gson().fromJson(config.getParameters(), BarTransformParameters.class);
        validateParameters(parameters);
        setDefaultParameters(parameters);

        final Schema outputSchema = SCHEMA_OUTPUT;
        final Map<String, FCollection<?>> results = new HashMap<>();
        for (final FCollection<?> input : inputs) {
            final String name = config.getName() + (config.getInputs().size() == 1 ? "" : "." + input.getName());
            final PCollection<GenericRecord> output;
            switch (input.getDataType()) {
                case AVRO: {
                    final FCollection<GenericRecord> inputCollection = (FCollection<GenericRecord>) input;
                    final TimeBarTransform<GenericRecord> transform = new TimeBarTransform<>(
                            parameters,
                            outputSchema.toString(),
                            AvroSchemaUtil::getAsString,
                            AvroSchemaUtil::getAsDouble,
                            AvroSchemaUtil::getTimestamp);
                    output = inputCollection.getCollection().apply(name, transform);
                    break;
                }
                case ROW: {
                    final FCollection<Row> inputCollection = (FCollection<Row>) input;
                    final TimeBarTransform<Row> transform = new TimeBarTransform<>(
                            parameters,
                            outputSchema.toString(),
                            RowSchemaUtil::getAsString,
                            RowSchemaUtil::getAsDouble,
                            RowSchemaUtil::getTimestamp);
                    output = inputCollection.getCollection().apply(name, transform);
                    break;
                }
                case STRUCT: {
                    final FCollection<Struct> inputCollection = (FCollection<Struct>) input;
                    final TimeBarTransform<Struct> transform = new TimeBarTransform<>(
                            parameters,
                            outputSchema.toString(),
                            StructSchemaUtil::getAsString,
                            StructSchemaUtil::getAsDouble,
                            StructSchemaUtil::getTimestamp);
                    output = inputCollection.getCollection().apply(name, transform);
                    break;
                }
                case ENTITY: {
                    final FCollection<Entity> inputCollection = (FCollection<Entity>) input;
                    final TimeBarTransform<Entity> transform = new TimeBarTransform<>(
                            parameters,
                            outputSchema.toString(),
                            EntitySchemaUtil::getAsString,
                            EntitySchemaUtil::getAsDouble,
                            EntitySchemaUtil::getTimestamp);
                    output = inputCollection.getCollection().apply(name, transform);
                    break;
                }
                default:
                    throw new IllegalStateException("");

            }
            results.put(name, FCollection.of(name, output.setCoder(AvroCoder.of(outputSchema)), DataType.AVRO, outputSchema));
        }

        return results;
    }

    public static Map<String, FCollection<?>> transformAccumulationBar(final List<FCollection<?>> inputs, final TransformConfig config) {

        final BarTransformParameters parameters = new Gson().fromJson(config.getParameters(), BarTransformParameters.class);
        validateParameters(parameters);
        setDefaultParameters(parameters);

        final Schema outputSchema = SCHEMA_OUTPUT;
        final Map<String, FCollection<?>> results = new HashMap<>();
        for (final FCollection<?> input : inputs) {
            final String name = config.getName() + (config.getInputs().size() == 1 ? "" : "." + input.getName());
            final PCollection<GenericRecord> output;
            switch (input.getDataType()) {
                case AVRO: {
                    final FCollection<GenericRecord> inputCollection = (FCollection<GenericRecord>) input;
                    final AccumulationBarTransform<GenericRecord> transform = new AccumulationBarTransform<>(
                            parameters,
                            outputSchema.toString(),
                            AvroSchemaUtil::getAsString,
                            AvroSchemaUtil::getAsDouble);
                    output = inputCollection.getCollection().apply(name, transform);
                    break;
                }
                case ROW: {
                    final FCollection<Row> inputCollection = (FCollection<Row>) input;
                    final AccumulationBarTransform<Row> transform = new AccumulationBarTransform<>(
                            parameters,
                            outputSchema.toString(),
                            RowSchemaUtil::getAsString,
                            RowSchemaUtil::getAsDouble);
                    output = inputCollection.getCollection().apply(name, transform);
                    break;
                }
                case STRUCT: {
                    final FCollection<Struct> inputCollection = (FCollection<Struct>) input;
                    final AccumulationBarTransform<Struct> transform = new AccumulationBarTransform<>(
                            parameters,
                            outputSchema.toString(),
                            StructSchemaUtil::getAsString,
                            StructSchemaUtil::getAsDouble);
                    output = inputCollection.getCollection().apply(name, transform);
                    break;
                }
                case ENTITY: {
                    final FCollection<Entity> inputCollection = (FCollection<Entity>) input;
                    final AccumulationBarTransform<Entity> transform = new AccumulationBarTransform<>(
                            parameters,
                            outputSchema.toString(),
                            EntitySchemaUtil::getAsString,
                            EntitySchemaUtil::getAsDouble);
                    output = inputCollection.getCollection().apply(name, transform);
                    break;
                }
                default:
                    throw new IllegalStateException("");

            }
            results.put(name, FCollection.of(name, output.setCoder(AvroCoder.of(outputSchema)), DataType.AVRO, outputSchema));
        }

        return results;
    }

    public static class TimeBarTransform<T> extends PTransform<PCollection<T>, PCollection<GenericRecord>> {

        private final BarTransformParameters parameters;
        private final String outputSchemaString;

        private final StringGetter<T> stringGetter;
        private final DoubleGetter<T> doubleGetter;
        private final TimestampGetter<T> timestampGetter;

        private TimeBarTransform(final BarTransformParameters parameters, final String outputSchemaString,
                                 final StringGetter<T> stringGetter, final DoubleGetter<T> doubleGetter, final TimestampGetter<T> timestampGetter) {
            this.parameters = parameters;
            this.outputSchemaString = outputSchemaString;
            this.stringGetter = stringGetter;
            this.doubleGetter = doubleGetter;
            this.timestampGetter = timestampGetter;
        }

        @Override
        public PCollection<GenericRecord> expand(final PCollection<T> input) {

            //
            final PCollection<T> withWindow;
            final WindowFn<? super T, ?> windowFn;
            switch (parameters.getUnit()) {
                case second: {
                    windowFn = FixedWindows.of(Duration.standardSeconds(parameters.getSize()));
                    break;
                }
                case minute: {
                    windowFn = FixedWindows.of(Duration.standardMinutes(parameters.getSize()));
                    break;
                }
                case hour: {
                    windowFn = FixedWindows.of(Duration.standardHours(parameters.getSize()));
                    break;
                }
                case day: {
                    windowFn = CalendarWindows
                            .days(parameters.getSize().intValue())
                            .withTimeZone(DateTimeZone.forID(parameters.getTimezone()));
                    break;
                }
                case week: {
                    windowFn = CalendarWindows
                            .weeks(parameters.getSize().intValue(), 0)
                            .withTimeZone(DateTimeZone.forID(parameters.getTimezone()));
                    break;
                }
                case month: {
                    windowFn = CalendarWindows
                            .months(parameters.getSize().intValue())
                            .withTimeZone(DateTimeZone.forID(parameters.getTimezone()));
                    break;
                }
                default:
                    throw new IllegalArgumentException("");
            }
            withWindow = input.apply("WithTimeFixedWindow", Window.into(windowFn));

            //
            final PCollection<KV<String, T>> withKey;
            if (parameters.getSymbolFields().size() > 0) {
                final List<String> symbolFields = parameters.getSymbolFields();
                withKey = withWindow.apply("WithSymbolsKey", WithKeys.of((T t) -> {
                    final StringBuilder sb = new StringBuilder();
                    for(String field : symbolFields) {
                        final String symbol = stringGetter.getAsString(t, field);
                        sb.append(symbol == null ? "" : symbol);
                        sb.append("#");
                    }
                    if(sb.length() > 0) {
                        sb.deleteCharAt(sb.length() - 1);
                    }
                    return sb.toString();
                }).withKeyType(TypeDescriptors.strings()));
            } else {
                withKey = withWindow.apply("WithFixedKey", WithKeys.of(""));
            }

            final PCollection<KV<String, Bar>> bars = withKey
                    .apply("Combine", Combine.perKey(new BarCombineFn<>(
                            parameters.getPriceField(),
                            parameters.getVolumeField(),
                            parameters.getTimestampField(),
                            parameters.getIsVolumeAccumulative(),
                            doubleGetter,
                            timestampGetter)))
                    .setCoder(KvCoder.of(StringUtf8Coder.of(), AvroCoder.of(Bar.class)))
                    .apply("WithGlobalWindow", Window.into(new GlobalWindows()));

            return bars.apply("BanditStreamingTransform", ParDo
                    .of(new BarDoFn(outputSchemaString)));
        }

        private static class BarDoFn extends DoFn<KV<String, Bar>, GenericRecord> {

            private final String schemaString;
            private transient Schema schema;

            BarDoFn(final String schemaString) {
                this.schemaString = schemaString;
            }

            @Setup
            public void setup() {
                this.schema = AvroSchemaUtil.convertSchema(schemaString);
            }

            @ProcessElement
            public void processElement(final ProcessContext c) {
                final String symbol = c.element().getKey();
                final Bar currentBar = c.element().getValue();
                c.output(createRecord(schema, symbol, currentBar, c.timestamp()));
            }

        }

    }

    public static class AccumulationBarTransform<T> extends PTransform<PCollection<T>, PCollection<GenericRecord>> {

        private final BarTransformParameters parameters;
        private final String outputSchemaString;

        private final StringGetter<T> stringGetter;
        private final DoubleGetter<T> doubleGetter;

        private AccumulationBarTransform(final BarTransformParameters parameters, final String outputSchemaString,
                                         final StringGetter<T> stringGetter, final DoubleGetter<T> doubleGetter) {
            this.parameters = parameters;
            this.outputSchemaString = outputSchemaString;
            this.stringGetter = stringGetter;
            this.doubleGetter = doubleGetter;
        }

        @Override
        public PCollection<GenericRecord> expand(final PCollection<T> input) {

            // Set bar window and trigger
            final PCollection<T> withWindow;
            if(parameters.getIntervalSeconds() != null) {
                withWindow = input
                        .apply("WithSessionWindow", Window.<T>into(Sessions
                                .withGapDuration(Duration.standardSeconds(parameters.getIntervalSeconds())))
                                .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1)))
                                .discardingFiredPanes());
            } else {
                withWindow = input
                        .apply("WithGlobalWindow", Window.<T>into(new GlobalWindows())
                                .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1)))
                                .discardingFiredPanes());
            }

            //
            final PCollection<KV<String, T>> withKey;
            if (parameters.getSymbolFields().size() > 0) {
                final List<String> symbolFields = parameters.getSymbolFields();
                withKey = withWindow.apply("WithSymbolsKey", WithKeys.of((T t) -> {
                    final StringBuilder sb = new StringBuilder();
                    for(String field : symbolFields) {
                        final String symbol = stringGetter.getAsString(t, field);
                        sb.append(symbol == null ? "" : symbol);
                        sb.append("#");
                    }
                    if(sb.length() > 0) {
                        sb.deleteCharAt(sb.length() - 1);
                    }
                    return sb.toString();
                }));
            } else {
                withKey = withWindow.apply("WithFixedKey", WithKeys.of(""));
            }

            if(input.getPipeline().getOptions().as(StreamingOptions.class).isStreaming()) {
                return withKey.apply("AccumulationBarTransform", ParDo
                        .of(new AccumulationBarStreamingDoFn<>(
                                parameters.getMeasure(), parameters.getSize(),
                                parameters.getPriceField(), parameters.getVolumeField(),
                                doubleGetter,
                                parameters.getIsVolumeAccumulative(),
                                outputSchemaString)));
            } else {
                return withKey.apply("AccumulationBarTransform", ParDo
                        .of(new AccumulationBarBatchDoFn<>(
                                parameters.getMeasure(), parameters.getSize(),
                                parameters.getPriceField(), parameters.getVolumeField(),
                                doubleGetter,
                                parameters.getIsVolumeAccumulative(),
                                outputSchemaString)));
            }
        }

        private static abstract class AccumulationBarDoFn<T> extends DoFn<KV<String, T>, GenericRecord> {

            static final String STATEID_ACCUMULATION = "accumulation";
            static final String STATEID_ACCUM = "accum";

            private final Measure measure;
            private final Long size;

            private final String priceField;
            private final String volumeField;
            private final DoubleGetter<T> doubleGetter;

            private final Boolean accumulative;
            private final String schemaString;

            private transient Schema schema;

            AccumulationBarDoFn(final Measure measure, final Long size,
                                final String priceField,
                                final String volumeField,
                                final DoubleGetter<T> doubleGetter,
                                final Boolean accumulative,
                                final String schemaString) {

                this.measure = measure;
                this.size = size;

                this.priceField = priceField;
                this.volumeField = volumeField;

                this.doubleGetter = doubleGetter;
                this.accumulative = accumulative;
                this.schemaString = schemaString;
            }

            @Setup
            public void setup() {
                this.schema = AvroSchemaUtil.convertSchema(schemaString);
            }

            void process(final ProcessContext c,
                         final ValueState<Bar> accumState,
                         final ValueState<Double> accumulationState) {

                final String symbol = c.element().getKey();
                final T element = c.element().getValue();
                final Double price = doubleGetter.getAsDouble(element, priceField);
                final Double volume = doubleGetter.getAsDouble(element, volumeField);

                final Double prevValue = Optional.ofNullable(accumulationState.read()).orElse(0D);
                final Double nextValue;
                switch (measure) {
                    case tick:
                        nextValue = prevValue + 1;
                        break;
                    case volume:
                        nextValue = accumulative ? Math.max(prevValue, volume) : prevValue + volume;
                        break;
                    case dollar:
                        double salesDiff = price * Math.max(0D, accumulative ? prevValue - volume : volume);
                        nextValue = prevValue + salesDiff;
                        break;
                    default:
                        throw new IllegalStateException("");
                }

                final Bar prevAccum = accumState.read();
                final Bar nextAccum = increment(prevAccum, price, volume, c.timestamp(), accumulative);

                if(nextValue >= size) {
                    c.output(createRecord(schema, symbol, nextAccum, c.timestamp()));
                    accumState.write(new Bar());
                    accumulationState.write(0D);
                } else {
                    accumState.write(nextAccum);
                    accumulationState.write(nextValue);
                }

            }

            private Bar increment(Bar prev,
                                  final Double price,
                                  final Double volume,
                                  final Instant timestamp,
                                  final boolean accumulative) {

                if(prev == null) {
                    prev = new Bar();
                }

                if(price < prev.low) {
                    prev.low = price;
                }
                if(price > prev.high) {
                    prev.high = price;
                }

                if(timestamp.compareTo(prev.openTimestamp) < 0) {
                    prev.open = price;
                    prev.openTimestamp = timestamp;
                }
                if(timestamp.compareTo(prev.closeTimestamp) > 0) {
                    prev.close = price;
                    prev.closeTimestamp = timestamp;
                }

                prev.count++;
                prev.volume = accumulative ? prev.volume + volume : Math.max(prev.volume, volume);

                return prev;
            }

        }

        private static class AccumulationBarStreamingDoFn<T> extends AccumulationBarDoFn<T> {

            @StateId(STATEID_ACCUM)
            private final StateSpec<ValueState<Bar>> accum;
            @StateId(STATEID_ACCUMULATION)
            private final StateSpec<ValueState<Double>> accumulation;

            AccumulationBarStreamingDoFn(final Measure measure, final Long size,
                                         final String priceField,
                                         final String volumeField,
                                         final DoubleGetter<T> doubleGetter,
                                         final Boolean accumulative,
                                         final String schemaString) {

                super(measure, size, priceField, volumeField, doubleGetter, accumulative, schemaString);
                this.accum = StateSpecs.value(AvroCoder.of(Bar.class));
                this.accumulation = StateSpecs.value(DoubleCoder.of());
            }

            @Setup
            public void setup() {
                super.setup();
            }

            @ProcessElement
            public void processElement(final ProcessContext c,
                                       final @StateId(STATEID_ACCUM) ValueState<Bar> accumState,
                                       final @StateId(STATEID_ACCUMULATION) ValueState<Double> accumulationState) {

                process(c, accumState, accumulationState);
            }
        }

        private static class AccumulationBarBatchDoFn<T> extends AccumulationBarDoFn<T> {

            @StateId(STATEID_ACCUM)
            private final StateSpec<ValueState<Bar>> accum;
            @StateId(STATEID_ACCUMULATION)
            private final StateSpec<ValueState<Double>> accumulation;

            AccumulationBarBatchDoFn(final Measure measure, final Long size,
                                     final String priceField,
                                     final String volumeField,
                                     final DoubleGetter<T> doubleGetter,
                                     final Boolean accumulative,
                                     final String schemaString) {

                super(measure, size, priceField, volumeField, doubleGetter, accumulative, schemaString);
                this.accum = StateSpecs.value(AvroCoder.of(Bar.class));
                this.accumulation = StateSpecs.value(DoubleCoder.of());
            }

            @Setup
            public void setup() {
                super.setup();
            }

            @ProcessElement
            @RequiresTimeSortedInput
            public void processElement(final ProcessContext c,
                                       final @StateId(STATEID_ACCUM) ValueState<Bar> accumState,
                                       final @StateId(STATEID_ACCUMULATION) ValueState<Double> accumulationState) {

                process(c, accumState, accumulationState);
            }
        }

    }

    public static class BarCombineFn<T> extends Combine.CombineFn<T, Bar, Bar> {

        private static final int SCALE = 32;

        private final String priceField;
        private final String volumeField;
        private final String timestampField;
        private final Boolean accumulative;
        private final DoubleGetter<T> doubleGetter;
        private final TimestampGetter<T> timestampGetter;

        BarCombineFn(final String priceField,
                     final String volumeField,
                     final String timestampField,
                     final Boolean accumulative,
                     final DoubleGetter<T> doubleGetter,
                     final TimestampGetter<T> timestampGetter) {

            this.priceField = priceField;
            this.volumeField = volumeField;
            this.timestampField = timestampField;
            this.accumulative = accumulative;
            this.doubleGetter = doubleGetter;
            this.timestampGetter = timestampGetter;
        }

        @Override
        public Bar createAccumulator() { return new Bar(); }

        @Override
        public Bar addInput(Bar accum, T input) {
            final Double price = doubleGetter.getAsDouble(input, priceField);
            if(price == null) {
                return accum;
            }
            final Double volume = doubleGetter.getAsDouble(input, volumeField);
            final Instant timestamp = timestampGetter.getAsTimestamp(input, timestampField, Instant.now());
            accum.count++;
            if(volume != null) {
                if(accumulative) {
                    accum.volume = Math.max(accum.volume, volume);
                } else {
                    accum.volume += volume;
                }
            }
            if(price.compareTo(accum.high) > 0) {
                accum.high = price;
            }
            if(price.compareTo(accum.low) < 0) {
                accum.low = price;
            }
            if(timestamp.compareTo(accum.openTimestamp) < 0) {
                accum.open = price;
                accum.openTimestamp = timestamp;
            }
            if(timestamp.compareTo(accum.closeTimestamp) > 0) {
                accum.close = price;
                accum.closeTimestamp = timestamp;
            }

            if(accumulative) {
                accum.vwap = accum.vwap
                        .multiply(BigDecimal.valueOf(accum.count - 1))
                        .divide(BigDecimal.valueOf(accum.count), SCALE, RoundingMode.HALF_UP)
                        .add(BigDecimal.valueOf(price)
                                .divide(BigDecimal.valueOf(accum.count), SCALE, RoundingMode.HALF_UP));
            } else if(accum.volume > 0) {
                accum.vwap = accum.vwap
                        .multiply(BigDecimal.valueOf(accum.volume - volume))
                        .divide(BigDecimal.valueOf(accum.volume), SCALE, RoundingMode.HALF_UP)
                        .add(BigDecimal.valueOf(price)
                                .multiply(BigDecimal.valueOf(volume))
                                .divide(BigDecimal.valueOf(accum.volume), SCALE, RoundingMode.HALF_UP));
            }

            return accum;
        }

        @Override
        public Bar mergeAccumulators(final Iterable<Bar> accums) {
            final Bar merged = createAccumulator();
            for (final Bar accum : accums) {
                merged.count += accum.count;
                if(accumulative) {
                    merged.volume = Math.max(merged.volume, accum.volume);
                } else {
                    merged.volume += accum.volume;
                }

                if(accum.high.compareTo(merged.high) > 0) {
                    merged.high = accum.high;
                }
                if(accum.low.compareTo(merged.low) < 0) {
                    merged.low = accum.low;
                }

                if(accum.openTimestamp.compareTo(merged.openTimestamp) < 0) {
                    merged.open = accum.open;
                    merged.openTimestamp = accum.openTimestamp;
                }
                if(accum.closeTimestamp.compareTo(merged.closeTimestamp) > 0) {
                    merged.close = accum.close;
                    merged.closeTimestamp = accum.closeTimestamp;
                }

                if(accumulative) {
                    if(merged.count.equals(accum.count)) {
                        merged.vwap = accum.vwap;
                    } else if(merged.count > 0) {
                        merged.vwap = merged.vwap
                                .multiply(BigDecimal.valueOf(merged.count - accum.count))
                                .divide(BigDecimal.valueOf(merged.count), SCALE, RoundingMode.HALF_UP)
                                .add(accum.vwap
                                        .multiply(BigDecimal.valueOf(accum.count))
                                        .divide(BigDecimal.valueOf(merged.count), SCALE, RoundingMode.HALF_UP)
                                );
                    }
                } else {
                    if(merged.volume.equals(accum.volume)) {
                        merged.vwap = accum.vwap;
                    } else if(merged.volume > 0) {
                        merged.vwap = merged.vwap
                                .multiply(BigDecimal.valueOf(merged.volume - accum.volume))
                                .divide(BigDecimal.valueOf(merged.volume), SCALE, RoundingMode.HALF_UP)
                                .add(accum.vwap
                                        .multiply(BigDecimal.valueOf(accum.volume))
                                        .divide(BigDecimal.valueOf(merged.volume), SCALE, RoundingMode.HALF_UP)
                                );
                    }
                }

            }
            return merged;
        }

        @Override
        public Bar extractOutput(final Bar accum) {
            return accum;
        }

    }

    @DefaultCoder(AvroCoder.class)
    public static class Bar implements Serializable {

        private static final Instant OPEN = Instant.parse("2100-12-31T00:00:00Z");
        private static final Instant CLOSE = Instant.parse("1900-01-01T00:00:00Z");

        private Double low;
        private Double high;
        private Double open;
        private Double close;
        private BigDecimal vwap;
        private Double volume;
        private Long count;

        private Instant openTimestamp;
        private Instant closeTimestamp;

        Bar() {
            this.low = Double.MAX_VALUE;
            this.high = Double.MIN_VALUE;
            this.open = 0D;
            this.close = 0D;
            this.vwap = BigDecimal.valueOf(0D);
            this.volume = 0D;
            this.count = 0L;

            this.openTimestamp = OPEN;
            this.closeTimestamp = CLOSE;
        }
    }

    private static GenericRecord createRecord(final Schema schema, final String symbol, final Bar accum, final Instant timestamp) {
        return new GenericRecordBuilder(schema)
                .set("symbol", symbol)
                .set("open", accum.open)
                .set("close", accum.close)
                .set("low", accum.low)
                .set("high", accum.high)
                .set("volume", accum.volume)
                .set("count", accum.count)
                .set("vwap", accum.vwap.doubleValue())
                .set("sizeSecond", new Duration(accum.openTimestamp, accum.closeTimestamp).getStandardSeconds())
                .set("timestamp", timestamp.getMillis() * 1000)
                .build();
    }

    private interface StringGetter<T> extends Serializable {
        String getAsString(final T value, final String field);
    }

    private interface DoubleGetter<T> extends Serializable {
        Double getAsDouble(final T value, final String field);
    }

    private interface TimestampGetter<T> extends Serializable {
        Instant getAsTimestamp(final T value, final String field, final Instant defaultTimestamp);
    }

}
