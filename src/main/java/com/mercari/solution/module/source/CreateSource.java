package com.mercari.solution.module.source;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.mercari.solution.config.SourceConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.module.SourceModule;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.Filter;
import com.mercari.solution.util.OptionUtil;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.converter.RowToRecordConverter;
import com.mercari.solution.util.pipeline.select.SelectFunction;
import com.mercari.solution.util.pipeline.union.UnionCoder;
import com.mercari.solution.util.pipeline.union.UnionValue;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroGenericCoder;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.SqlTypes;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.BoundedPerElement;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.*;


public class CreateSource implements SourceModule {

    private static final Logger LOG = LoggerFactory.getLogger(CreateSource.class);

    private static class CreateSourceParameters implements Serializable {

        private String type;
        private List<String> elements;
        private String from;
        private String to;
        private Integer interval;
        private DateTimeUtil.TimeUnit intervalUnit;

        private Long rate;
        private DateTimeUtil.TimeUnit rateUnit;

        private JsonElement filter;
        private JsonArray select;
        private String flattenField;

        private Integer splitSize;

        private Schema.FieldType elementType;
        private OutputType outputType;

        public List<String> getElements() {
            return elements;
        }

        public String getFrom() {
            return from;
        }

        public String getTo() {
            return to;
        }

        public String getType() {
            return type;
        }

        public Integer getInterval() {
            return interval;
        }

        public DateTimeUtil.TimeUnit getIntervalUnit() {
            return intervalUnit;
        }

        public Long getRate() {
            return rate;
        }

        public DateTimeUtil.TimeUnit getRateUnit() {
            return rateUnit;
        }

        public JsonElement getFilter() {
            return filter;
        }

        public JsonArray getSelect() {
            return select;
        }

        public String getFlattenField() {
            return flattenField;
        }

        public Integer getSplitSize() {
            return splitSize;
        }

        public Schema.FieldType getElementType() {
            return elementType;
        }

        public OutputType getOutputType() {
            return outputType;
        }

        private void validate(final String name) {

            final List<String> errorMessages = new ArrayList<>();
            if(elements == null && from == null) {
                errorMessages.add("create source module[" + name + "] requires either elements or from parameter");
            } else {
                if(type == null) {
                    errorMessages.add("create source module[" + name + "] requires type parameter in ['int','long','date','time','timestamp','string']");
                } else {
                    final Schema.FieldType elementType = getElementFieldType(type);
                    if(elementType == null) {
                        errorMessages.add("create source module[" + name + "] requires type parameter in ['int','long','date','time','timestamp','string']");
                    }
                }
            }

            if(rate != null) {
                if(rate < 0) {
                    errorMessages.add("create source module[" + name + "].rate parameter must be over zero");
                }
            } else if((elements == null || elements.isEmpty()) && to == null) {
                errorMessages.add("create source module[" + name + "].to parameter is required when rate is not set");
            }

            if(!errorMessages.isEmpty()) {
                throw new IllegalArgumentException(String.join(", ", errorMessages));
            }
        }

        private void setDefaults(final PInput input) {
            this.elementType = getElementFieldType(type);
            if(this.elements == null) {
                this.elements = new ArrayList<>();
            }
            if(this.elements.isEmpty()) {
                if(this.interval == null) {
                    this.interval = 1;
                }
                if(this.intervalUnit == null && this.type != null) {
                    this.intervalUnit = switch (this.type) {
                        case "date" -> DateTimeUtil.TimeUnit.day;
                        case "time", "timestamp" -> DateTimeUtil.TimeUnit.minute;
                        default -> null;
                    };
                }
            }

            if(this.from != null) {
                final Map<String, Object> values = new HashMap<>();
                TemplateUtil.setFunctions(values);
                this.from = TemplateUtil.executeStrictTemplate(from, values);
            }
            if(this.to != null) {
                final Map<String, Object> values = new HashMap<>();
                TemplateUtil.setFunctions(values);
                this.to = TemplateUtil.executeStrictTemplate(to, values);
            }

            if(this.rate == null) {
                this.rate = 0L;
            } else {
                if(this.rateUnit == null) {
                    this.rateUnit = DateTimeUtil.TimeUnit.minute;
                }
            }

            if(this.splitSize == null) {
                this.splitSize = 10;
            }

            if(this.outputType == null) {
                if(OptionUtil.isStreaming(input)) {
                    this.outputType = OutputType.row;
                } else {
                    this.outputType = OutputType.avro;
                }
            }

        }

        public static CreateSourceParameters of(
                final JsonElement jsonElement,
                final String name,
                final PInput input) {

            final CreateSourceParameters parameters = new Gson().fromJson(jsonElement, CreateSourceParameters.class);
            if (parameters == null) {
                throw new IllegalArgumentException("create source module[" + name + "].parameters must not be empty!");
            }

            parameters.validate(name);
            parameters.setDefaults(input);

            return parameters;
        }

    }



    public String getName() { return "create"; }


    private enum OutputType {
        row,
        avro
    }


    public Map<String, FCollection<?>> expand(PBegin begin, SourceConfig config, PCollection<Long> beats, List<FCollection<?>> waits) {
        return Collections.singletonMap(config.getName(), source(begin, config));
    }

    public static FCollection<?> source(final PBegin begin, final SourceConfig config) {

        final CreateSourceParameters parameters = CreateSourceParameters.of(config.getParameters(), config.getName(), begin);

        final Schema elementSchema = createElementRowSchema(parameters.getType());
        Schema outputSchema;
        if(parameters.getSelect() == null || !parameters.getSelect().isJsonArray()) {
            outputSchema = elementSchema;
        } else {
            outputSchema = SelectFunction.createSchema(parameters.getSelect(), elementSchema.getFields());
        }

        if(parameters.getFlattenField() != null) {
            outputSchema = SelectFunction.createFlattenSchema(outputSchema, parameters.getFlattenField());
        }

        final FCollection<?> fCollection = switch (parameters.getOutputType()) {
            case row -> {
                final Source<Row> source = new Source<>(parameters, DataType.ROW, outputSchema);
                final PCollection<Row> output = begin.apply(config.getName(), source);
                yield FCollection.of(config.getName(), output, DataType.ROW, outputSchema);
            }
            case avro -> {
                final Source<GenericRecord> source = new Source<>(parameters, DataType.AVRO, outputSchema);
                final PCollection<GenericRecord> output = begin.apply(config.getName(), source);
                yield FCollection.of(config.getName(), output, DataType.AVRO, outputSchema);
            }
        };

        return fCollection;
    }

    private static class Source<ElementT> extends PTransform<PBegin, PCollection<ElementT>> {

        private final String elementType;
        private final List<String> elements;
        private final String from;
        private final String to;
        private final Integer interval;
        private final DateTimeUtil.TimeUnit intervalUnit;

        private final Long rate;
        private final DateTimeUtil.TimeUnit rateUnit;

        private final String filterString;
        private final List<SelectFunction> selectFunctions;
        private final String flattenField;

        private final Integer splitSize;

        private final DataType outputType;
        private final Schema outputSchema;


        Source(final CreateSourceParameters parameters,
               final DataType outputType,
               final Schema outputSchema) {

            this.elementType = parameters.getType();
            this.elements = parameters.getElements();
            this.from = parameters.getFrom();
            this.to = parameters.getTo();
            this.interval = parameters.getInterval();
            this.intervalUnit = parameters.getIntervalUnit();
            this.rate = parameters.getRate();
            this.rateUnit = parameters.getRateUnit();

            if(parameters.getFilter() != null && (parameters.getFilter().isJsonObject() || parameters.getFilter().isJsonArray())) {
                this.filterString = parameters.getFilter().toString();
            } else {
                this.filterString = null;
            }
            if(parameters.getSelect() != null && parameters.getSelect().isJsonArray()) {
                final Schema elementRowSchema = createElementRowSchema(elementType);
                this.selectFunctions = SelectFunction.of(parameters.getSelect(), elementRowSchema.getFields(), outputType);
            } else {
                this.selectFunctions = new ArrayList<>();
            }
            this.flattenField = parameters.getFlattenField();

            this.splitSize = parameters.getSplitSize();

            this.outputType = outputType;
            this.outputSchema = outputSchema;
        }

        @Override
        public PCollection<ElementT> expand(PBegin begin) {
            final org.apache.avro.Schema elementSchema = createElementAvroSchema(elementType);
            final long elementSize = calculateElementSize(elementType, elements, from, to, interval, intervalUnit);
            final AvroGenericCoder elementCoder = AvroGenericCoder.of(elementSchema);
            final PCollection<UnionValue> seeds;
            if(this.rate > 0) {
                final DoFn<Long, UnionValue> elementDoFn = new SequenceElementDoFn(
                        elementType, elements, from, interval, intervalUnit);
                GenerateSequence generateSequence = GenerateSequence
                        .from(0)
                        .withRate(rate, DateTimeUtil.getDuration(rateUnit, 1L));
                if(elementSize > 0) {
                    generateSequence = generateSequence.to(elementSize);
                }
                seeds = begin
                        .apply("GenerateSequence", generateSequence)
                        .apply("Element", ParDo.of(elementDoFn))
                        .setCoder(UnionCoder.of(List.of(elementCoder)));
            } else {
                final DoFn<Long, UnionValue> elementDoFn;
                if(elementSize > 0) {
                    elementDoFn = new BatchElementDoFn(
                            elementType, elements, from, interval, intervalUnit, elementSize, splitSize);
                } else {
                    elementDoFn = new BatchElementDoFn(
                            elementType, elements, from, interval, intervalUnit, elementSize, splitSize);
                }
                seeds = begin
                        .apply("Seed", Create.of(0L).withCoder(VarLongCoder.of()))
                        .apply("Element", ParDo.of(elementDoFn))
                        .setCoder(UnionCoder.of(List.of(elementCoder)));
            }

            final PCollection<UnionValue> filtered;
            if(filterString == null) {
                filtered = seeds;
            } else {
                filtered = seeds
                        .apply("Filter", ParDo.of(new FilterDoFn(filterString)))
                        .setCoder(seeds.getCoder());
            }

            final Coder<ElementT> outputCoder = UnionValue.createCoderWithRowSchema(outputSchema, outputType);
            if(selectFunctions.isEmpty()) {
                return filtered
                        .apply("Values", ParDo.of(new ValueDoFn<ElementT>(outputType, outputSchema)))
                        .setCoder(outputCoder);
            } else {
                return filtered
                        .apply("Select", ParDo.of(new SelectDoFn<ElementT>(
                                outputType, outputSchema, selectFunctions, flattenField)))
                        .setCoder(outputCoder);
            }
        }

        private static long calculateElementSize(
                final String elementType,
                final List<String> elements,
                final String from,
                final String to,
                final Integer interval,
                final DateTimeUtil.TimeUnit intervalUnit) {

            if(!elements.isEmpty()) {
                return elements.size();
            }

            if(to == null) {
                return -1L;
            }
            switch (elementType) {
                case "float", "float32", "double", "float64" -> {
                    final double fromN = Double.parseDouble(from);
                    final double toN   = Double.parseDouble(to);
                    final Double size = (toN - fromN) / interval;
                    return size.longValue();
                }
                case "date" -> {
                    final LocalDate fromDate = DateTimeUtil.toLocalDate(from);
                    final LocalDate toDate   = DateTimeUtil.toLocalDate(to);
                    LocalDate currentDate = LocalDate.from(fromDate);
                    long count = 0;
                    while(currentDate.isBefore(toDate)) {
                        count++;
                        currentDate = switch (intervalUnit) {
                            case day -> currentDate.plusDays(interval);
                            case week -> currentDate.plusWeeks(interval);
                            case month -> currentDate.plusMonths(interval);
                            case year -> currentDate.plusYears(interval);
                            default -> throw new IllegalArgumentException();
                        };
                    }
                    return count;
                }
                case "time" -> {
                    final LocalTime fromTime = DateTimeUtil.toLocalTime(from);
                    final LocalTime toTime   = DateTimeUtil.toLocalTime(to);
                    LocalTime currentTime = LocalTime.from(fromTime);
                    long count = 0;
                    while(currentTime.isBefore(toTime)) {
                        count++;
                        switch (intervalUnit) {
                            case second -> currentTime = currentTime.plusSeconds(interval);
                            case minute -> currentTime = currentTime.plusMinutes(interval);
                            case hour -> currentTime = currentTime.plusHours(interval);
                            default -> throw new IllegalArgumentException();
                        }
                    }
                    return count;
                }
                case "timestamp" -> {
                    final Instant fromInstant = DateTimeUtil.toInstant(from);
                    final Instant toInstant   = DateTimeUtil.toInstant(to);
                    Instant currentInstant = Instant.from(fromInstant);
                    long count = 0;
                    while(currentInstant.isBefore(toInstant)) {
                        count++;
                        switch (intervalUnit) {
                            case second -> currentInstant = currentInstant.plusSeconds(interval);
                            case minute -> currentInstant = currentInstant.plus(interval, ChronoUnit.MINUTES);
                            case hour -> currentInstant = currentInstant.plus(interval, ChronoUnit.HOURS);
                            case day -> currentInstant = currentInstant.plus(interval, ChronoUnit.DAYS);
                            case week -> currentInstant = currentInstant.plus(interval, ChronoUnit.WEEKS);
                            case month -> currentInstant = currentInstant.plus(interval, ChronoUnit.MONTHS);
                            case year -> currentInstant = currentInstant.plus(interval, ChronoUnit.YEARS);
                            default -> throw new IllegalArgumentException();
                        }
                    }
                    return count;
                }
                default -> {
                    final long fromN = Long.parseLong(from);
                    final long toN   = Long.parseLong(to);
                    return (toN - fromN) / interval;
                }
            }
        }

        private static Object createElement(
                final String elementType,
                final List<String> elements,
                final String from,
                final Integer interval,
                final DateTimeUtil.TimeUnit intervalUnit,
                final Long sequence) {

            final String elementValue;
            if(!elements.isEmpty()) {
                elementValue = elements.get(sequence.intValue());
            } else {
                elementValue = switch (elementType) {
                    case "date" -> {
                        final LocalDate fromDate = DateTimeUtil.toLocalDate(from);
                        final long plus = interval * sequence;
                        final LocalDate lastDate = switch (intervalUnit) {
                            case day -> fromDate.plusDays(plus);
                            case week -> fromDate.plusWeeks(plus);
                            case month -> fromDate.plusMonths(plus);
                            case year -> fromDate.plusYears(plus);
                            default -> throw new IllegalArgumentException();
                        };
                        yield lastDate.toString();
                    }
                    case "time" -> {
                        final LocalTime fromTime = DateTimeUtil.toLocalTime(from);
                        final long plus = interval * sequence;
                        final LocalTime lastDate = switch (intervalUnit) {
                            case second -> fromTime.plusSeconds(plus);
                            case minute -> fromTime.plusMinutes(plus);
                            case hour -> fromTime.plusHours(plus);
                            default -> throw new IllegalArgumentException();
                        };
                        yield lastDate.toString();
                    }
                    case "timestamp" -> {
                        final Instant fromInstant = DateTimeUtil.toInstant(from);
                        final long plus = interval * sequence;
                        final Instant lastInstant = switch (intervalUnit) {
                            case second -> fromInstant.plusSeconds(plus);
                            case minute -> fromInstant.plus(plus, ChronoUnit.MINUTES);
                            case hour -> fromInstant.plus(plus, ChronoUnit.HOURS);
                            case day -> fromInstant.plus(plus, ChronoUnit.DAYS);
                            case week -> fromInstant.plus(plus, ChronoUnit.WEEKS);
                            case month -> fromInstant.plus(plus, ChronoUnit.MONTHS);
                            case year -> fromInstant.plus(plus, ChronoUnit.YEARS);
                            default -> throw new IllegalArgumentException();
                        };
                        yield lastInstant.toString();
                    }
                    case "float", "float32", "double", "float64" -> {
                        final double fromN = Double.parseDouble(from);
                        final double lastN = fromN + interval * sequence;
                        yield Double.toString(lastN);
                    }
                    default -> {
                        final long fromN = Long.parseLong(from);
                        final long lastN = fromN + interval * sequence;
                        yield Long.toString(lastN);
                    }
                };
            }

            return switch (elementType) {
                case "string" -> elementValue;
                case "date" -> Long.valueOf(DateTimeUtil.toLocalDate(elementValue).toEpochDay()).intValue();
                case "time" -> DateTimeUtil.toLocalTime(elementValue).toNanoOfDay() / 1000L;
                case "timestamp" -> DateTimeUtil.toJodaInstant(elementValue).getMillis() * 1000L;
                case "int32", "int", "integer" -> Integer.parseInt(elementValue);
                case "int64", "long" -> Long.parseLong(elementValue);
                case "float32", "float" -> Float.parseFloat(elementValue);
                case "float64", "double" -> Double.parseDouble(elementValue);
                default -> throw new IllegalArgumentException();
            };
        }

        @BoundedPerElement
        public static class BatchElementDoFn extends DoFn<Long, UnionValue> {

            private final String elementType;
            private final List<String> elements;
            private final String from;
            private final Integer interval;
            private final DateTimeUtil.TimeUnit intervalUnit;

            private final long size;
            private final boolean enableSplit;
            private final Integer splitSize;

            private transient org.apache.avro.Schema runtimeElementSchema;

            BatchElementDoFn(
                    final String elementType,
                    final List<String> elements,
                    final String from,
                    final int interval,
                    final DateTimeUtil.TimeUnit intervalUnit,
                    final long size,
                    final Integer splitSize) {

                this.elementType = elementType;
                this.elements = elements;
                this.from = from;
                this.interval = interval;
                this.intervalUnit = intervalUnit;

                this.size = size;
                this.splitSize = splitSize;
                this.enableSplit = true;
            }

            @Setup
            public void setup() {
                this.runtimeElementSchema = createElementAvroSchema(elementType);
            }

            @Teardown
            public void teardown() {

            }

            @ProcessElement
            public void processElement(
                    final ProcessContext c,
                    final RestrictionTracker<OffsetRange, Long> tracker) {

                final OffsetRange offsetRange = tracker.currentRestriction();
                long position = offsetRange.getFrom();
                while(tracker.tryClaim(position)) {
                    final Object elementValue = createElement(elementType, elements, from, interval, intervalUnit, position);
                    final UnionValue output = createElementUnionValue(runtimeElementSchema, elementValue, position);
                    c.output(output);
                    position++;
                }

            }

            @GetInitialRestriction
            public OffsetRange getInitialRestriction()  {
                final OffsetRange initialOffsetRange = new OffsetRange(0L, size);
                LOG.info("Initial restriction: {}", initialOffsetRange);
                return initialOffsetRange;
            }

            @GetRestrictionCoder
            public Coder<OffsetRange> getRestrictionCoder() {
                return OffsetRange.Coder.of();
            }

            @SplitRestriction
            public void splitRestriction(
                    @Restriction OffsetRange restriction,
                    OutputReceiver<OffsetRange> splitReceiver) {

                if(enableSplit) {
                    long size = (restriction.getTo() - restriction.getFrom()) / this.splitSize;
                    if(size == 0) {
                        LOG.info("Not split restriction because size is zero");
                        splitReceiver.output(new OffsetRange(restriction.getFrom(), restriction.getTo()));
                        return;
                    }
                    long start = restriction.getFrom();
                    for(int i=1; i<this.splitSize; i++) {
                        long end = i * size;
                        final OffsetRange childRestriction = new OffsetRange(start, end);
                        splitReceiver.output(childRestriction);
                        start = end;
                        LOG.info("create split restriction[{}]: {} for batch mode", i - 1, childRestriction);
                    }
                    final OffsetRange lastChildRestriction = new OffsetRange(start, restriction.getTo());
                    splitReceiver.output(lastChildRestriction);
                    LOG.info("create split restriction[{}]: {} for batch mode", this.splitSize - 1, lastChildRestriction);
                } else {
                    LOG.info("Not split restriction: {} for batch mode", restriction);
                    splitReceiver.output(restriction);
                }
            }

            @GetSize
            public double getSize(@Restriction OffsetRange restriction) throws Exception {
                final double size = restriction.getTo() - restriction.getFrom();
                LOG.info("SDF get size: {}", size);
                return size;
            }

        }

        private static class SequenceElementDoFn extends DoFn<Long, UnionValue> {

            private final String elementType;
            private final List<String> elements;
            private final String from;
            private final Integer interval;
            private final DateTimeUtil.TimeUnit intervalUnit;

            private transient org.apache.avro.Schema runtimeSchema;

            SequenceElementDoFn(
                    final String elementType,
                    final List<String> elements,
                    final String from,
                    final Integer interval,
                    final DateTimeUtil.TimeUnit intervalUnit) {

                this.elementType = elementType;
                this.elements = elements;
                this.from = from;
                this.interval = interval;
                this.intervalUnit = intervalUnit;
            }


            @Setup
            public void setup() {
                this.runtimeSchema = createElementAvroSchema(elementType);// UnionValue.convertRowSchema(elementSchema, outputType);
            }

            @ProcessElement
            public void processElement(final ProcessContext c) {
                final Long sequence = c.element();
                final Object value = createElement(elementType, elements, from, interval, intervalUnit, sequence);
                final UnionValue unionValue = createElementUnionValue(runtimeSchema, value, sequence);
                c.output(unionValue);
            }

        }

        private static class FilterDoFn extends DoFn<UnionValue, UnionValue> {

            private final String conditionJsons;

            private transient Filter.ConditionNode conditions;

            FilterDoFn(final String conditionJsons) {
                this.conditionJsons = conditionJsons;
            }

            @Setup
            public void setup() {
                this.conditions = Filter.parse(new Gson().fromJson(conditionJsons, JsonElement.class));
            }

            @ProcessElement
            public void processElement(ProcessContext c) {
                final UnionValue element = c.element();
                if(Filter.filter(element, UnionValue::getFieldValue, conditions)) {
                    c.output(element);
                }
            }
        }

        private static class SelectDoFn<ElementT> extends DoFn<UnionValue, ElementT> {

            private final DataType outputType;
            private final Schema inputSchema;
            private final List<SelectFunction> selectFunctions;
            private final String flattenField;

            private transient Object runtimeSchema;

            SelectDoFn(
                    final DataType outputType,
                    final Schema inputSchema,
                    final List<SelectFunction> selectFunctions,
                    final String flattenField) {

                this.outputType = outputType;
                this.inputSchema = inputSchema;
                this.selectFunctions = selectFunctions;
                this.flattenField = flattenField;
            }

            @Setup
            public void setup() {
                this.runtimeSchema = UnionValue.convertRowSchema(inputSchema, outputType);
                if(!selectFunctions.isEmpty()) {
                    for(final SelectFunction selectFunction : selectFunctions) {
                        selectFunction.setup();
                    }
                }
            }

            @ProcessElement
            public void processElement(final ProcessContext c) {
                final UnionValue element = c.element();
                if(element == null) {
                    return;
                }
                final Map<String, Object> values = SelectFunction.apply(selectFunctions, element, outputType, c.timestamp());
                if(flattenField == null) {
                    final ElementT output = UnionValue.create(runtimeSchema, values, outputType);
                    c.output(output);
                } else {
                    final List<?> flattenList = Optional.ofNullable((List<?>) values.get(flattenField)).orElseGet(ArrayList::new);
                    for(final Object value : flattenList) {
                        final Map<String, Object> flattenValues = new HashMap<>(values);
                        flattenValues.put(flattenField, value);
                        final ElementT output = UnionValue.create(runtimeSchema, flattenValues, outputType);
                        c.output(output);
                    }
                }
            }

        }

        private static class ValueDoFn<ElementT> extends DoFn<UnionValue, ElementT> {

            private final DataType outputType;
            private final Schema inputSchema;

            private transient Object runtimeSchema;

            ValueDoFn(
                    final DataType outputType,
                    final Schema inputSchema) {

                this.outputType = outputType;
                this.inputSchema = inputSchema;
            }

            @Setup
            public void setup() {
                this.runtimeSchema = UnionValue.convertRowSchema(inputSchema, outputType);
            }

            @ProcessElement
            public void processElement(final ProcessContext c) {
                final ElementT output = UnionValue.convert(runtimeSchema, c.element(), outputType);
                c.output(output);
            }

        }

        private static UnionValue createElementUnionValue(
                final org.apache.avro.Schema runtimeSchema,
                final Object value,
                final long sequence) {

            final org.joda.time.Instant timestamp = org.joda.time.Instant.now();
            final Map<String, Object> values = new HashMap<>();
            values.put("sequence", sequence);
            values.put("timestamp", timestamp.getMillis() * 1000L);
            values.put("value", value);

            final GenericRecord object = AvroSchemaUtil.create(runtimeSchema, values);
            return new UnionValue(0, DataType.AVRO, timestamp.getMillis(), object);
        }
    }

    private static Schema createElementRowSchema(final String elementType) {
        final List<Schema.Field> fields = new ArrayList<>();
        fields.add(Schema.Field.of("sequence", Schema.FieldType.INT64));
        fields.add(Schema.Field.of("timestamp", Schema.FieldType.DATETIME));
        final Schema.FieldType valueFieldType = getElementFieldType(elementType);
        fields.add(Schema.Field.of("value", valueFieldType));
        return Schema.builder().addFields(fields).build();
    }

    private static org.apache.avro.Schema createElementAvroSchema(final String elementType) {
        return RowToRecordConverter.convertSchema(createElementRowSchema(elementType));
    }

    private static Schema.FieldType getElementFieldType(final String elementType) {
        final Schema.FieldType elementFieldType = switch (elementType) {
            case "short", "int16" -> Schema.FieldType.INT16;
            case "int", "int32", "integer" -> Schema.FieldType.INT32;
            case "long", "int64" -> Schema.FieldType.INT64;
            case "float", "float32" -> Schema.FieldType.FLOAT;
            case "double", "float64" -> Schema.FieldType.DOUBLE;
            case "numeric", "decimal" -> Schema.FieldType.DECIMAL;
            case "bool", "boolean" -> Schema.FieldType.BOOLEAN;
            case "time" -> CalciteUtils.TIME;
            case "date" -> CalciteUtils.DATE;
            case "datetime" -> Schema.FieldType.logicalType(SqlTypes.DATETIME);
            case "timestamp" -> Schema.FieldType.DATETIME;
            default -> null;
        };

        if(elementFieldType == null) {
            return null;
        }

        return elementFieldType.withNullable(true);
    }

}
