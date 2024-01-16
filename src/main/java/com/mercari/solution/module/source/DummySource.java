package com.mercari.solution.module.source;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Mutation;
import com.google.common.hash.Hashing;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.config.SourceConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.module.SourceModule;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.OptionUtil;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.converter.*;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.SchemaUtil;
import freemarker.template.Template;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.gcp.spanner.MutationGroup;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.*;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.hbase.exceptions.IllegalArgumentIOException;
import org.joda.time.LocalDateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.time.Instant;
import java.util.*;


public class DummySource implements SourceModule {

    private static final Logger LOG = LoggerFactory.getLogger(DummySource.class);

    private static class DummySourceParameters implements Serializable {

        private JsonObject template;
        private Long throughput;
        private Long interval;
        private DateTimeUtil.TimeUnit intervalUnit;
        private Long sequenceFrom;
        private Long sequenceTo;

        // for output mutation and mutationGroup common
        private Mutation.Op mutationOp;
        private List<String> mutationKeyFields;

        // for output mutation specific
        private String mutationTable;

        // for output mutationGroup specific
        private String mutationPrimaryField;
        private List<String> mutationOtherFields;

        // output type
        private OutputType outputType;

        public JsonObject getTemplate() {
            return template;
        }

        public Long getThroughput() {
            return throughput;
        }

        public Long getInterval() {
            return interval;
        }

        public DateTimeUtil.TimeUnit getIntervalUnit() {
            return intervalUnit;
        }

        public Long getSequenceFrom() {
            return sequenceFrom;
        }

        public Long getSequenceTo() {
            return sequenceTo;
        }

        public Mutation.Op getMutationOp() {
            return mutationOp;
        }

        public void setMutationOp(Mutation.Op mutationOp) {
            this.mutationOp = mutationOp;
        }

        public List<String> getMutationKeyFields() {
            return mutationKeyFields;
        }

        public String getMutationTable() {
            return mutationTable;
        }

        public String getMutationPrimaryField() {
            return mutationPrimaryField;
        }

        public List<String> getMutationOtherFields() {
            return mutationOtherFields;
        }

        public OutputType getOutputType() {
            return outputType;
        }


        public void validate(PInput input) {
            final List<String> errorMessages = new ArrayList<>();
            if(template == null || (!template.isJsonObject() && !template.isJsonArray())) {
                errorMessages.add("dummy source module config requires template parameter");
            }
            if(OptionUtil.isStreaming(input)) {
                if(throughput == null) {
                    errorMessages.add("dummy source module config requires throughput parameter for streaming mode");
                }
            } else {
                if(throughput != null) {
                    LOG.warn("dummy source module config not need throughput parameter for batch mode");
                }
                if(interval != null) {
                    LOG.warn("dummy source module config not need interval parameter for batch mode");
                }
                if(sequenceTo == null) {
                    errorMessages.add("dummy source module config requires sequenceTo parameter for batch mode");
                } else if(Optional.ofNullable(sequenceFrom).orElse(0L) > sequenceTo) {
                    errorMessages.add("dummy source module sequenceTo parameter must be greater than sequenceFrom");
                }
            }

            if(OutputType.mutation.equals(outputType)) {
                if(mutationTable == null) {
                    errorMessages.add("dummy source module config requires sequenceTo parameter for batch mode");
                }
                if(Mutation.Op.DELETE.equals(mutationOp) && (mutationKeyFields == null || mutationKeyFields.size() == 0)) {
                    errorMessages.add("dummy source module config requires mutationKeyFields parameter if mutationOp is DELETE");
                }
            } else if(OutputType.mutationGroup.equals(outputType)) {
                if(mutationPrimaryField == null) {
                    errorMessages.add("dummy source module config requires mutationPrimaryField parameter if outputType is mutationGroup");
                }
                if(Mutation.Op.DELETE.equals(mutationOp) && (mutationKeyFields == null || mutationKeyFields.size() == 0)) {
                    errorMessages.add("dummy source module config requires mutationKeyFields parameter if mutationOp is DELETE");
                }
            }
            if(errorMessages.size() > 0) {
                throw new IllegalArgumentException(String.join(", ", errorMessages));
            }
        }

        public void setDefaults() {
            if(sequenceFrom == null) {
                sequenceFrom = 0L;
            }
            if(interval == null) {
                interval = 1L;
            }
            if(intervalUnit == null) {
                intervalUnit = DateTimeUtil.TimeUnit.second;
            }
            if(outputType == null) {
                outputType = OutputType.row;
            }
            if(mutationOp == null) {
                mutationOp = Mutation.Op.INSERT_OR_UPDATE;
            }
            if(mutationOtherFields == null) {
                mutationOtherFields = new ArrayList<>();
            }
        }

    }

    private enum OutputType {
        row,
        avro,
        mutation,
        mutationGroup
    }

    public String getName() { return "dummy"; }

    public Map<String, FCollection<?>> expand(PBegin begin, SourceConfig config, PCollection<Long> beats, List<FCollection<?>> waits) {
        final DummySourceParameters parameters = new Gson().fromJson(config.getParameters(), DummySourceParameters.class);
        if(parameters == null) {
            throw new IllegalArgumentException("Dummy source module parameters must not be empty!");
        }
        parameters.validate(begin);
        parameters.setDefaults();

        if(config.getMicrobatch() != null && config.getMicrobatch()) {
            throw new IllegalArgumentException("Dummy source module does not support microbatch mode");
        }

        return Collections.singletonMap(config.getName(), dummy(begin, config, parameters));
    }

    public static FCollection<?> dummy(final PBegin begin, final SourceConfig config, final DummySourceParameters parameters) {
        final Schema schema = SourceConfig.convertSchema(config.getSchema());
        switch (parameters.getOutputType()) {
            case row -> {
                final DummyStream<Schema, Schema, Row> dummyStream = new DummyStream<>(
                        parameters,
                        schema,
                        s -> s,
                        JsonToRowConverter::convert);
                final PCollection<Row> dummies = begin
                        .apply(config.getName(), dummyStream)
                        .setCoder(RowCoder.of(schema));
                return FCollection
                        .of(config.getName(), dummies, DataType.ROW, schema);
            }
            case avro -> {
                final org.apache.avro.Schema avroSchema = RowToRecordConverter.convertSchema(schema);
                final DummyStream<String, org.apache.avro.Schema, GenericRecord> dummyStream = new DummyStream<>(
                        parameters,
                        avroSchema.toString(),
                        AvroSchemaUtil::convertSchema,
                        JsonToRecordConverter::convert);
                final PCollection<GenericRecord> dummies = begin
                        .apply(config.getName(), dummyStream)
                        .setCoder(AvroCoder.of(avroSchema));
                return FCollection
                        .of(config.getName(), dummies, DataType.AVRO, avroSchema);
            }
            case mutationGroup -> {
                final DummyStream<Schema, Schema, Row> dummyStream = new DummyStream<>(
                        parameters,
                        schema,
                        s -> s,
                        JsonToRowConverter::convert);
                final PCollection<MutationGroup> dummies = begin
                        .apply(config.getName(), dummyStream)
                        .setCoder(RowCoder.of(schema))
                        .apply("ToMutationGroup", ParDo
                                .of(new MutationGroupDoFn(
                                        parameters.getMutationOp(),
                                        parameters.getMutationKeyFields(),
                                        parameters.getMutationPrimaryField(),
                                        parameters.getMutationOtherFields())));
                return FCollection
                        .of(config.getName(), dummies, DataType.MUTATIONGROUP, schema);
            }
            default -> {
                throw new IllegalArgumentException("Dummy source module: " + config.getName() + " does not support outputType: " + parameters.getOutputType());
            }
        }
    }

    private static class DummyStream<InputSchemaT, RuntimeSchemaT, ElementT> extends PTransform<PBegin, PCollection<ElementT>> {

        private static final Logger LOG = LoggerFactory.getLogger(DummyStream.class);

        private final String templateString;
        private final InputSchemaT inputSchema;
        private final SchemaUtil.SchemaConverter<InputSchemaT, RuntimeSchemaT> schemaConverter;
        private final SchemaUtil.JsonConverter<RuntimeSchemaT,ElementT> jsonConverter;

        private final Long throughput;
        private final Long interval;
        private final DateTimeUtil.TimeUnit intervalUnit;
        private final Long sequenceFrom;
        private final Long sequenceTo;

        private DummyStream(final DummySourceParameters parameters,
                            final InputSchemaT inputSchema,
                            final SchemaUtil.SchemaConverter<InputSchemaT, RuntimeSchemaT> schemaConverter,
                            final SchemaUtil.JsonConverter<RuntimeSchemaT, ElementT> jsonConverter) {

            this.templateString = parameters.getTemplate().toString();
            this.throughput = parameters.getThroughput();
            this.interval = parameters.getInterval();
            this.intervalUnit = parameters.getIntervalUnit();
            this.sequenceFrom = parameters.getSequenceFrom();
            this.sequenceTo = parameters.getSequenceTo();

            this.inputSchema = inputSchema;
            this.schemaConverter = schemaConverter;
            this.jsonConverter = jsonConverter;

        }

        public PCollection<ElementT> expand(final PBegin begin) {

            GenerateSequence generateSequence = GenerateSequence
                    .from(sequenceFrom)
                    .withRate(throughput, DateTimeUtil.getDuration(intervalUnit, interval));
            if(sequenceTo != null) {
                generateSequence = generateSequence.to(sequenceTo);
            }

            final PCollection<Long> sequence = begin.apply("GenerateSequence", generateSequence);
            return sequence
                    .apply("GenerateDummies", ParDo.of(new DummyGenerateDoFn<>(
                            templateString, inputSchema, schemaConverter, jsonConverter, sequenceTo)));
        }

        private static class DummyGenerateDoFn<InputSchemaT, RuntimeSchemaT, ElementT> extends DoFn<Long, ElementT> {

            private final String templateString;
            private final InputSchemaT inputSchema;
            private final SchemaUtil.SchemaConverter<InputSchemaT, RuntimeSchemaT> schemaConverter;
            private final SchemaUtil.JsonConverter<RuntimeSchemaT,ElementT> jsonConverter;
            private final Long sequenceTo;
            private transient Template template;
            private transient RuntimeSchemaT runtimeSchema;


            DummyGenerateDoFn(final String templateString,
                              final InputSchemaT inputSchema,
                              final SchemaUtil.SchemaConverter<InputSchemaT, RuntimeSchemaT> schemaConverter,
                              final SchemaUtil.JsonConverter<RuntimeSchemaT,ElementT> jsonConverter,
                              final Long sequenceTo) {

                this.templateString = templateString;
                this.inputSchema = inputSchema;
                this.schemaConverter = schemaConverter;
                this.jsonConverter = jsonConverter;
                this.sequenceTo = sequenceTo;
            }

            @Setup
            public void setup() {
                this.template = TemplateUtil.createStrictTemplate("", templateString);
                this.runtimeSchema = schemaConverter.convert(this.inputSchema);
            }

            @ProcessElement
            public void processElement(final ProcessContext c) throws IOException {
                if(sequenceTo != null && sequenceTo.compareTo(Objects.requireNonNull(c.element())) < 0) {
                    LOG.warn("skip duplicated sequence: " + c.element());
                    return;
                }
                final Map<String, Object> data = new HashMap<>();
                data.put("_SEQUENCE", c.element());
                data.put("_UUID", UUID.randomUUID().toString());
                //data.put("_DateTimeUtil", datetimeUtils);
                data.put("_EVENTTIME", Instant.ofEpochMilli(c.timestamp().getMillis()));

                final String jsonText = TemplateUtil.executeStrictTemplate(template, data);
                final JsonElement jsonElement = new Gson().fromJson(jsonText, JsonElement.class);
                if(jsonElement.isJsonObject()) {
                    final ElementT output = jsonConverter.convert(runtimeSchema, jsonElement.getAsJsonObject());
                    c.output(output);
                } else if(jsonElement.isJsonArray()) {
                    for(final JsonElement child : jsonElement.getAsJsonArray()) {
                        if(child.isJsonObject()) {
                            final ElementT output = jsonConverter.convert(runtimeSchema, child.getAsJsonObject());
                            c.output(output);
                        } else {
                            throw new IllegalArgumentIOException();
                        }
                    }
                } else {
                    throw new IllegalArgumentIOException();
                }
            }

        }

    }


    private static class MutationGroupDoFn extends DoFn<Row, MutationGroup> {

        private final Mutation.Op mutationOp;
        private final List<String> mutationKeyFields;
        private final String mutationPrimaryField;
        private final List<String> mutationOtherFields;

        MutationGroupDoFn(final Mutation.Op mutationOp,
                          final List<String> mutationKeyFields,
                          final String mutationPrimaryField,
                          final List<String> mutationOtherFields) {

            this.mutationOp = mutationOp;
            this.mutationKeyFields = mutationKeyFields;
            this.mutationPrimaryField = mutationPrimaryField;
            this.mutationOtherFields = mutationOtherFields;
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final Row row = c.element();
            final MutationGroup mutationGroup = createMutationGroup(row);
            c.output(mutationGroup);
        }

        private MutationGroup createMutationGroup(final Row row) {
            final Schema schema = row.getSchema();

            // create primary mutation
            final Row primaryRow = row.getRow(mutationPrimaryField);
            final Mutation primary = RowToMutationConverter.convert(primaryRow.getSchema(), primaryRow,
                    mutationPrimaryField, mutationOp.name(), mutationKeyFields,
                    null, null, null);

            // create other mutations
            final List<Mutation> others = new ArrayList<>();
            for(final String mutationOtherField : mutationOtherFields) {
                final Schema.Field otherField = schema.getField(mutationOtherField);
                switch (otherField.getType().getTypeName()) {
                    case ROW: {
                        final Row otherRow = row.getRow(mutationOtherField);
                        final Mutation other = RowToMutationConverter.convert(otherRow.getSchema(), otherRow,
                                mutationOtherField, mutationOp.name(), mutationKeyFields,
                                null, null, null);
                        others.add(other);
                        break;
                    }
                    case ARRAY:
                    case ITERABLE: {
                        final Collection<Row> otherRows = row.getArray(mutationOtherField);
                        for(final Row otherRow : otherRows) {
                            final Mutation other = RowToMutationConverter.convert(otherRow.getSchema(), otherRow,
                                    mutationOtherField, mutationOp.name(), mutationKeyFields,
                                    null, null, null);
                            others.add(other);
                        }
                        break;
                    }
                    default:
                        throw new IllegalArgumentException();
                }

            }

            return MutationGroup.create(primary, others);
        }

    }

    private interface DummyGenerator {

        Random random = new Random();

        static DummyGenerator of(String fieldType, String fieldName, boolean isPrimary, boolean isNullable, List<String> range, int randomRate) {
            if(fieldType.startsWith("STRING") || fieldType.startsWith("BYTES")) {
                final String sizeString = fieldType.substring(fieldType.indexOf("(") + 1, fieldType.indexOf(")"));
                final Integer lengthLimit = "MAX".equals(sizeString) ? Integer.MAX_VALUE : Integer.valueOf(sizeString);
                if(fieldType.startsWith("BYTES")) {
                    return new BytesDummyGenerator(fieldName, lengthLimit, isPrimary, isNullable, randomRate);
                }
                return new StringDummyGenerator(fieldName, lengthLimit, range, isPrimary, isNullable, randomRate);
            } else if(fieldType.equals("INT64")) {
                return new IntDummyGenerator(fieldName, range, isPrimary, isNullable, randomRate);
            } else if(fieldType.equals("FLOAT64")) {
                return new FloatDummyGenerator(fieldName, range, isPrimary, isNullable, randomRate);
            } else if(fieldType.equals("BOOL")) {
                return new BoolDummyGenerator(fieldName, isNullable, randomRate);
            } else if(fieldType.equals("DATE")) {
                return new DateDummyGenerator(fieldName, range, isPrimary, isNullable, randomRate);
            } else if(fieldType.equals("TIMESTAMP")) {
                return new TimestampDummyGenerator(fieldName, range, isPrimary, isNullable, randomRate);
            } else if(fieldType.startsWith("ARRAY")) {
                final String arrayType = fieldType.substring(fieldType.indexOf("<") + 1, fieldType.indexOf(">"));
                final DummyGenerator elementGenerator = of(arrayType, fieldName, false, isNullable, range, randomRate);
                return new ArrayDummyGenerator(fieldName, elementGenerator, isNullable, randomRate);
            }
            throw new IllegalArgumentException(String.format("Illegal fieldType: %s, %s", fieldType, fieldName));
        }

        static boolean randomNull(boolean isNullable, int randomRate) {
            return isNullable && random.nextInt(100) < randomRate;
        }

        static <T> List<T> generateValues(DummyGenerator generator, long value) {
            final List<T> randomValues = new ArrayList<>();
            for(int i=0; i<10; i++) {
                final T randomValue = (T)generator.generateValue(value);
                randomValues.add(randomValue);
            }
            return randomValues;
        }

        Object generateValue(long value);

        Mutation.WriteBuilder putDummyValue(Mutation.WriteBuilder builder, long value);

        Mutation.WriteBuilder putDummyValues(Mutation.WriteBuilder builder, long value);

    }

    private static class StringDummyGenerator implements DummyGenerator {

        private final String fieldName;
        private final List<String> range;
        private final Integer maxLength;
        private final Boolean isPrimary;
        private final Boolean isNullable;
        private final int randomRate;

        public StringDummyGenerator(String fieldName, Integer maxLength, List<String> range, Boolean isPrimary, Boolean isNullable, int randomRate) {
            this.fieldName = fieldName;
            if(range != null && range.size() == 1 && NumberUtils.isNumber(range.get(0))) {
                List<String> r = new ArrayList<>();
                for(int i=0; i<Integer.valueOf(range.get(0)); i++) {
                    String randomString = UUID.randomUUID().toString() + UUID.randomUUID().toString();
                    randomString = maxLength > randomString.length() ? randomString : randomString.substring(0, maxLength);
                    r.add(randomString);
                }
                this.range = r;
            } else {
                this.range = range;
            }
            this.maxLength = maxLength;
            this.isPrimary = isPrimary;
            this.isNullable = isNullable;
            this.randomRate = randomRate;
        }

        public String generateValue(long value) {
            if(!isPrimary && DummyGenerator.randomNull(this.isNullable, this.randomRate)) {
                return null;
            }
            final String randomString = range != null && range.size() > 0 ?
                    range.get(random.nextInt(range.size())) :
                    UUID.randomUUID().toString() + UUID.randomUUID().toString();
            return maxLength > randomString.length() ? randomString : randomString.substring(0, maxLength);
        }

        public Mutation.WriteBuilder putDummyValue(Mutation.WriteBuilder builder, long value) {
            final String randomString = generateValue(value);
            return builder.set(this.fieldName).to(randomString);
        }

        public Mutation.WriteBuilder putDummyValues(Mutation.WriteBuilder builder, long value) {
            final List<String> randomValues = DummyGenerator.generateValues(this, value);
            return builder.set(this.fieldName).toStringArray(randomValues);
        }

    }

    private static class BytesDummyGenerator implements DummyGenerator {

        private final String fieldName;
        private final Integer maxLength;
        private final Boolean isPrimary;
        private final Boolean isNullable;
        private final int randomRate;

        public BytesDummyGenerator(String fieldName, Integer maxLength, Boolean isPrimary, Boolean isNullable, int randomRate) {
            this.fieldName = fieldName;
            this.maxLength = maxLength;
            this.isPrimary = isPrimary;
            this.isNullable = isNullable;
            this.randomRate = randomRate;
        }

        public ByteArray generateValue(long value) {
            if(!isPrimary && DummyGenerator.randomNull(this.isNullable, this.randomRate)) {
                return null;
            }
            final byte[] randomBytes = this.isPrimary ? Long.toString(value).getBytes() : Hashing.sha512().hashLong(value).asBytes();
            return ByteArray.copyFrom(randomBytes.length < maxLength ? randomBytes : Arrays.copyOf(randomBytes, maxLength));
        }

        public Mutation.WriteBuilder putDummyValue(Mutation.WriteBuilder builder, long value) {
            return builder.set(this.fieldName).to(generateValue(value));
        }

        public Mutation.WriteBuilder putDummyValues(Mutation.WriteBuilder builder, long value) {
            final List<ByteArray> randomValues = DummyGenerator.generateValues(this, value);
            return builder.set(this.fieldName).toBytesArray(randomValues);
        }

    }

    private static class IntDummyGenerator implements DummyGenerator {

        private final Random random;
        private final String fieldName;
        private final Long min;
        private final Long max;
        private final Boolean isPrimary;
        private final Boolean isNullable;
        private final int randomRate;

        public IntDummyGenerator(String fieldName, List<String> range, Boolean isPrimary, Boolean isNullable, int randomRate) {
            this.random = new Random();
            this.fieldName = fieldName;
            this.min = range != null && range.size() > 0 ? Long.valueOf(range.get(0)) : 0L;
            this.max = range != null && range.size() > 1 ? Long.valueOf(range.get(1)) : Long.MAX_VALUE;
            this.isPrimary = isPrimary;
            this.isNullable = isNullable;
            this.randomRate = randomRate;
        }

        public Long generateValue(long value) {
            if(!isPrimary && DummyGenerator.randomNull(this.isNullable, this.randomRate)) {
                return null;
            }
            return isPrimary ? value : Math.abs(random.nextLong()) % (this.max - this.min) + this.min;
        }

        public Mutation.WriteBuilder putDummyValue(Mutation.WriteBuilder builder, long value) {
            return builder.set(this.fieldName).to(generateValue(value));
        }

        public Mutation.WriteBuilder putDummyValues(Mutation.WriteBuilder builder, long value) {
            List<Long> randomValues = DummyGenerator.generateValues(this, value);
            return builder.set(this.fieldName).toInt64Array(randomValues);
        }
    }

    private static class FloatDummyGenerator implements DummyGenerator {

        private final Random random;
        private final String fieldName;
        private final Double min;
        private final Double max;
        private final Boolean isPrimary;
        private final Boolean isNullable;
        private final int randomRate;

        public FloatDummyGenerator(String fieldName, List<String> range, Boolean isPrimary, Boolean isNullable, int randomRate) {
            this.random = new Random();
            this.fieldName = fieldName;
            this.min = range != null && range.size() > 0 ? Double.valueOf(range.get(0)) : Double.MIN_VALUE;
            this.max = range != null && range.size() > 1 ? Double.valueOf(range.get(1)) : Double.MAX_VALUE;
            this.isPrimary = isPrimary;
            this.isNullable = isNullable;
            this.randomRate = randomRate;
        }

        public Double generateValue(long value) {
            if(!isPrimary && DummyGenerator.randomNull(this.isNullable, this.randomRate)) {
                return null;
            }
            return isPrimary ? (double)value : random.nextDouble() * (max - min) + min;
        }

        public Mutation.WriteBuilder putDummyValue(Mutation.WriteBuilder builder, long value) {
            return builder.set(this.fieldName).to(generateValue(value));
        }

        public Mutation.WriteBuilder putDummyValues(Mutation.WriteBuilder builder, long value) {
            final List<Double> randomValues = DummyGenerator.generateValues(this, value);
            return builder.set(this.fieldName).toFloat64Array(randomValues);
        }

    }

    private static class BoolDummyGenerator implements DummyGenerator {

        private final Random random;
        private final String fieldName;
        private final Boolean isNullable;
        private final int randomRate;

        public BoolDummyGenerator(String fieldName, Boolean isNullable, int randomRate) {
            this.random = new Random();
            this.fieldName = fieldName;
            this.isNullable = isNullable;
            this.randomRate = randomRate;
        }

        public Boolean generateValue(long value) {
            if(DummyGenerator.randomNull(this.isNullable, this.randomRate)) {
                return null;
            }
            return this.random.nextBoolean();
        }

        public Mutation.WriteBuilder putDummyValue(Mutation.WriteBuilder builder, long value) {
            return builder.set(this.fieldName).to(generateValue(value));
        }

        public Mutation.WriteBuilder putDummyValues(Mutation.WriteBuilder builder, long value) {
            List<Boolean> randomValues = DummyGenerator.generateValues(this, value);
            return builder.set(this.fieldName).toBoolArray(randomValues);
        }

    }

    private static class DateDummyGenerator implements DummyGenerator {

        private final Random random;
        private final String fieldName;
        private final Integer bound;
        private final LocalDateTime startDate;
        private final Boolean isPrimary;
        private final Boolean isNullable;
        private final int randomRate;

        public DateDummyGenerator(String fieldName, List<String> range, Boolean isPrimary, Boolean isNullable, int randomRate) {
            this.random = new Random();
            this.fieldName = fieldName;
            final String startDateStr = range != null && range.size() > 0 ? range.get(0) : "1970-01-01T00:00:00";
            final String endDateStr = range != null && range.size() > 1 ? range.get(1) : "2020-12-31T23:59:59";
            this.startDate = LocalDateTime.parse(startDateStr.length() == 10 ? startDateStr + "T00:00:00" : startDateStr);
            final LocalDateTime ldt2 = LocalDateTime.parse(endDateStr.length() == 10 ? endDateStr + "T00:00:00" : endDateStr);
            this.bound = 0;// (int)(ChronoUnit.DAYS.between(this.startDate.toLocalDate(), ldt2.toLocalDate()));
            this.isPrimary = isPrimary;
            this.isNullable = isNullable;
            this.randomRate = randomRate;
        }

        public Date generateValue(long value) {
            if(!isPrimary && DummyGenerator.randomNull(this.isNullable, this.randomRate)) {
                return null;
            }
            //final LocalDateTime nextDate = this.startDate.plusDays(isPrimary ? value : (long)this.random.nextInt(this.bound));
            //return Date.fromYearMonthDay(nextDate.getYear(), nextDate.getMonthValue(), nextDate.getDayOfMonth());
            return null;
        }

        public Mutation.WriteBuilder putDummyValue(Mutation.WriteBuilder builder, long value) {
            return builder.set(fieldName).to(generateValue(value));
        }

        public Mutation.WriteBuilder putDummyValues(Mutation.WriteBuilder builder, long value) {
            final List<Date> randomValues = DummyGenerator.generateValues(this, value);
            return builder.set(this.fieldName).toDateArray(randomValues);
        }

    }

    private static class TimestampDummyGenerator implements DummyGenerator {

        private final Random random;
        private final String fieldName;
        private final Long bound;
        private final LocalDateTime startDateTime;
        private final Boolean isPrimary;
        private final Boolean isNullable;
        private final int randomRate;

        public TimestampDummyGenerator(String fieldName, List<String> range, Boolean isPrimary, Boolean isNullable, int randomRate) {
            this.random = new Random();
            this.fieldName = fieldName;
            final String startDateStr = range != null && range.size() > 0 ? range.get(0) : "1970-01-01T00:00:00";
            final String endDateStr = range != null && range.size() > 1 ? range.get(1) : "2020-12-31T23:59:59";
            this.startDateTime = LocalDateTime.parse(startDateStr.length() == 10 ? startDateStr + "T00:00:00" : startDateStr);
            final LocalDateTime endDateTime = LocalDateTime.parse(endDateStr.length() == 10 ? endDateStr + "T00:00:00" : endDateStr);
            this.bound = 0L;//java.time.Duration.between(this.startDateTime, endDateTime).getSeconds();
            this.isPrimary = isPrimary;
            this.isNullable = isNullable;
            this.randomRate = randomRate;
        }

        public Timestamp generateValue(long value) {
            final LocalDateTime nextTime;
            if(this.isPrimary) {
                //nextTime = this.startDateTime.plusSeconds(value);
                //return Timestamp.ofTimeSecondsAndNanos(nextTime.toEpochSecond(ZoneOffset.UTC),0);
            } else if(DummyGenerator.randomNull(this.isNullable, this.randomRate)) {
                return null;
            } else {
                //nextTime = this.startDateTime.plusSeconds(Math.abs(this.random.nextLong()) % this.bound);
                //return Timestamp.ofTimeSecondsAndNanos(nextTime.toEpochSecond(ZoneOffset.UTC),0);
            }
            return null;
        }

        public Mutation.WriteBuilder putDummyValue(Mutation.WriteBuilder builder, long value) {
            return builder.set(fieldName).to(generateValue(value));
        }

        public Mutation.WriteBuilder putDummyValues(Mutation.WriteBuilder builder, long value) {
            final List<Timestamp> randomValues = DummyGenerator.generateValues(this, value);
            return builder.set(this.fieldName).toTimestampArray(randomValues);
        }

    }


    private static class ArrayDummyGenerator implements DummyGenerator {

        private final String fieldName;
        private final DummyGenerator elementGenerator;
        private final Boolean isNullable;
        private final int randomRate;

        public ArrayDummyGenerator(String fieldName, DummyGenerator elementGenerator, Boolean isNullable, int randomRate) {
            this.fieldName = fieldName;
            this.elementGenerator = elementGenerator;
            this.isNullable = isNullable;
            this.randomRate = randomRate;
        }

        public List generateValue(long value) {
            if(DummyGenerator.randomNull(this.isNullable, this.randomRate)) {
                return null;
            }
            return DummyGenerator.generateValues(elementGenerator, value);
        }

        public Mutation.WriteBuilder putDummyValue(Mutation.WriteBuilder builder, long value) {
            return elementGenerator.putDummyValues(builder, value);
        }

        public Mutation.WriteBuilder putDummyValues(Mutation.WriteBuilder builder, long value) {
            throw new IllegalArgumentException(String.format("Field: %s, Impossible Nested Array (Array in Array) for Cloud Spanner.", fieldName));
        }

    }

}
