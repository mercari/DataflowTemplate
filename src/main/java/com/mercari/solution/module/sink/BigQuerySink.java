package com.mercari.solution.module.sink;

import com.google.api.services.bigquery.model.*;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Struct;
import com.google.datastore.v1.Entity;
import com.google.firestore.v1.Document;
import com.google.gson.Gson;
import com.mercari.solution.config.SinkConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.module.SinkModule;
import com.mercari.solution.util.pipeline.union.Union;
import com.mercari.solution.util.pipeline.union.UnionValue;
import com.mercari.solution.util.schema.*;
import com.mercari.solution.util.OptionUtil;
import com.mercari.solution.util.converter.*;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.gcp.bigquery.*;
import org.apache.beam.sdk.io.gcp.spanner.MutationGroup;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;


public class BigQuerySink implements SinkModule {

    private static final Logger LOG = LoggerFactory.getLogger(BigQuerySink.class);

    private static class BigQuerySinkParameters implements Serializable {

        private String table;
        private BigQueryIO.Write.WriteDisposition writeDisposition;
        private BigQueryIO.Write.CreateDisposition createDisposition;
        private BigQueryIO.Write.Method method;
        private RowMutationInformation.MutationType mutationType;

        private String partitioning;
        private String partitioningField;
        private List<String> clusteringFields;
        private List<String> primaryKeyFields;
        private Boolean skipInvalidRows;
        private Boolean ignoreUnknownValues;
        private Boolean ignoreInsertIds;
        private Boolean withExtendedErrorInfo;
        private FailedInsertRetryPolicy failedInsertRetryPolicy;

        private String dynamicDestination;

        private List<BigQueryIO.Write.SchemaUpdateOption> schemaUpdateOptions;
        private Boolean autoSchemaUpdate;

        private String kmsKey;

        private Boolean optimizedWrites;
        private Boolean autoSharding;
        private Long triggeringFrequencySecond;
        private Integer numStorageWriteApiStreams;
        private Boolean withoutValidation;
        private WriteFormat writeFormat;

        @Deprecated
        private String clustering;


        public String getTable() {
            return table;
        }

        public void setTable(String table) {
            this.table = table;
        }

        public BigQueryIO.Write.WriteDisposition getWriteDisposition() {
            return writeDisposition;
        }

        public BigQueryIO.Write.CreateDisposition getCreateDisposition() {
            return createDisposition;
        }

        public BigQueryIO.Write.Method getMethod() {
            return method;
        }

        public RowMutationInformation.MutationType getMutationType() {
            return mutationType;
        }

        public String getPartitioning() {
            return partitioning;
        }

        public String getPartitioningField() {
            return partitioningField;
        }

        public List<String> getClusteringFields() {
            return clusteringFields;
        }

        public List<String> getPrimaryKeyFields() {
            return primaryKeyFields;
        }

        public String getDynamicDestination() {
            return dynamicDestination;
        }

        public String getClustering() {
            return clustering;
        }

        public Boolean getSkipInvalidRows() {
            return skipInvalidRows;
        }

        public Boolean getIgnoreUnknownValues() {
            return ignoreUnknownValues;
        }

        public Boolean getIgnoreInsertIds() {
            return ignoreInsertIds;
        }

        public Boolean getWithExtendedErrorInfo() {
            return withExtendedErrorInfo;
        }

        public FailedInsertRetryPolicy getFailedInsertRetryPolicy() {
            return failedInsertRetryPolicy;
        }

        public List<BigQueryIO.Write.SchemaUpdateOption> getSchemaUpdateOptions() {
            return schemaUpdateOptions;
        }

        public Boolean getAutoSchemaUpdate() {
            return autoSchemaUpdate;
        }

        public String getKmsKey() {
            return kmsKey;
        }

        public Boolean getOptimizedWrites() {
            return optimizedWrites;
        }

        public Boolean getAutoSharding() {
            return autoSharding;
        }

        public Long getTriggeringFrequencySecond() {
            return triggeringFrequencySecond;
        }

        public Integer getNumStorageWriteApiStreams() {
            return numStorageWriteApiStreams;
        }

        public Boolean getWithoutValidation() {
            return withoutValidation;
        }

        public WriteFormat getWriteFormat() {
            return writeFormat;
        }

        private void validate() {
            if(this.getTable() == null) {
                throw new IllegalArgumentException("BigQuery output module requires table parameter!");
            }
        }

        private void setDefaults(final PInput input) {
            if(this.writeDisposition == null) {
                this.writeDisposition = BigQueryIO.Write.WriteDisposition.WRITE_EMPTY;
            }
            if(this.createDisposition == null) {
                this.createDisposition = BigQueryIO.Write.CreateDisposition.CREATE_NEVER;
            }
            if(this.method == null) {
                this.method = BigQueryIO.Write.Method.DEFAULT;
            }
            if(this.clusteringFields == null) {
                this.clusteringFields = new ArrayList<>();
                if(this.clustering != null) {
                    Collections.addAll(this.clusteringFields, this.clustering.split(","));
                }
            }
            if(this.primaryKeyFields == null) {
                this.primaryKeyFields = new ArrayList<>();
            }

            if(this.skipInvalidRows == null) {
                this.skipInvalidRows = false;
            }
            if(this.ignoreUnknownValues == null) {
                this.ignoreUnknownValues = false;
            }
            if(this.ignoreInsertIds == null) {
                this.ignoreInsertIds = false;
            }
            if(this.withExtendedErrorInfo == null) {
                this.withExtendedErrorInfo = false;
            }
            if(this.failedInsertRetryPolicy == null) {
                this.failedInsertRetryPolicy = FailedInsertRetryPolicy.always;
            }

            if(this.optimizedWrites == null) {
                this.optimizedWrites = false;
            }
            if(BigQueryIO.Write.Method.FILE_LOADS.equals(this.method)
                    || BigQueryIO.Write.Method.STORAGE_WRITE_API.equals(this.method)
                    || BigQueryIO.Write.Method.STORAGE_API_AT_LEAST_ONCE.equals(this.method)) {
                if(this.triggeringFrequencySecond == null) {
                    if(OptionUtil.isStreaming(input)) {
                        this.triggeringFrequencySecond = 10L;
                    }
                } else {
                    if(!OptionUtil.isStreaming(input)) {
                        LOG.warn("triggeringFrequencySecond must not be set in batch mode");
                        this.triggeringFrequencySecond = null;
                    }
                }
                if(this.numStorageWriteApiStreams == null) {
                    if(!BigQueryIO.Write.Method.FILE_LOADS.equals(this.method)
                            && OptionUtil.isStreaming(input)) {
                        LOG.warn("numStorageWriteApiStreams must be set when using storage write api");
                        this.autoSharding = true;
                    }
                }
            }
            if(this.autoSharding == null) {
                this.autoSharding = false;
            }
            if(this.withoutValidation == null) {
                this.withoutValidation = false;
            }

        }
    }

    private enum FailedInsertRetryPolicy {
        always,
        never,
        retryTransientErrors
    }

    private enum WriteFormat {
        json,
        row,
        avro,
        avrofile
    }

    @Override
    public String getName() { return "bigquery"; }

    @Override
    public Map<String, FCollection<?>> expand(List<FCollection<?>> inputs, SinkConfig config, List<FCollection<?>> waits) {
        if(inputs == null || inputs.size() == 0) {
            throw new IllegalArgumentException("bigquery sink module requires input parameter");
        }

        final BigQuerySinkParameters parameters = new Gson().fromJson(config.getParameters(), BigQuerySinkParameters.class);
        final Schema inputSchema = inputs.get(0).getAvroSchema();
        try {
            config.outputAvroSchema(inputSchema);
        } catch (Exception e) {
            LOG.error("Failed to output avro schema for " + config.getName() + " to path: " + config.getOutputAvroSchema(), e);
        }

        final FCollection<?> output;
        if(inputs.size() == 1) {
            output = write(inputs.get(0), config, parameters, waits);
        } else {
            output = writeMulti(inputs, config, parameters, waits);
        }
        final Map<String, FCollection<?>> outputs = new HashMap<>();
        outputs.put(config.getName(), output);
        final String failuresName = config.getName() + ".failures";
        outputs.put(failuresName, FCollection.of(failuresName, output.getCollection(), output.getDataType(), output.getAvroSchema()));
        return outputs;
    }

    public static FCollection<?> write(final FCollection<?> collection,
                                       final SinkConfig config,
                                       final BigQuerySinkParameters parameters,
                                       final List<FCollection<?>> waitCollections) {

        final String destinationField = parameters.getDynamicDestination();
        final DataType inputType = collection.getDataType();
        switch (inputType) {
            case AVRO -> {
                final BigQueryWrite<GenericRecord> write = new BigQueryWrite<>(
                        config.getName(),
                        collection,
                        parameters,
                        (s, r) -> r,
                        RecordToRowConverter::convert,
                        RecordToTableRowConverter::convert,
                        s -> s.get(destinationField) == null ? "" : s.get(destinationField).toString(),
                        waitCollections);
                final PCollection<GenericRecord> input = (PCollection<GenericRecord>) collection.getCollection();
                final PCollection<GenericRecord> output = input.apply(config.getName(), write);
                return FCollection.of(config.getName(), output, DataType.AVRO, ((AvroCoder) output.getCoder()).getSchema());
            }
            case ROW -> {
                final BigQueryWrite<Row> write = new BigQueryWrite<>(
                        config.getName(),
                        collection,
                        parameters,
                        RowToRecordConverter::convert,
                        (s, r) -> r,
                        RowToTableRowConverter::convert,
                        s -> s.getValue(destinationField) == null ? "" : s.getValue(destinationField).toString(),
                        waitCollections);
                final PCollection<Row> input = (PCollection<Row>) collection.getCollection();
                final PCollection<GenericRecord> output = input.apply(config.getName(), write);
                return FCollection.of(config.getName(), output, DataType.AVRO, ((AvroCoder) output.getCoder()).getSchema());
            }
            case STRUCT -> {
                final BigQueryWrite<Struct> write = new BigQueryWrite<>(
                        config.getName(),
                        collection,
                        parameters,
                        StructToRecordConverter::convert,
                        StructToRowConverter::convert,
                        StructToTableRowConverter::convert,
                        s -> s.isNull(destinationField) ? "" : s.getString(destinationField),
                        waitCollections);
                final PCollection<Struct> input = (PCollection<Struct>) collection.getCollection();
                final PCollection<GenericRecord> output = input.apply(config.getName(), write);
                return FCollection.of(config.getName(), output, DataType.AVRO, ((AvroCoder) output.getCoder()).getSchema());
            }
            case DOCUMENT -> {
                final boolean withName = collection.getSchema().hasField("__name__");
                final boolean withCreateTime = collection.getSchema().hasField("__createtime__");
                final boolean withUpdateTime = collection.getSchema().hasField("__updatetime__");
                final SerializableFunction<Document, TableRow> convertTableRowFunction = DocumentToTableRowConverter
                        .createConverter(withName, withCreateTime, withUpdateTime);
                final BigQueryWrite<Document> write = new BigQueryWrite<>(
                        config.getName(),
                        collection,
                        parameters,
                        DocumentToRecordConverter::convert,
                        DocumentToRowConverter::convert,
                        convertTableRowFunction,
                        s -> OptionUtil.ifnull(DocumentSchemaUtil.getAsString(s.getFieldsOrDefault(destinationField, null)), ""),
                        waitCollections);
                final PCollection<Document> input = (PCollection<Document>) collection.getCollection();
                final PCollection<GenericRecord> output = input.apply(config.getName(), write);
                return FCollection.of(config.getName(), output, DataType.AVRO, ((AvroCoder) output.getCoder()).getSchema());
            }
            case ENTITY -> {
                final SerializableFunction<Entity, TableRow> convertTableRowFunction;
                if (collection.getSchema().hasField("__key__")) {
                    convertTableRowFunction = EntityToTableRowConverter::convertWithKey;
                } else {
                    convertTableRowFunction = EntityToTableRowConverter::convertWithoutKey;
                }
                final BigQueryWrite<Entity> write = new BigQueryWrite<>(
                        config.getName(),
                        collection,
                        parameters,
                        EntityToRecordConverter::convert,
                        EntityToRowConverter::convert,
                        convertTableRowFunction,
                        s -> OptionUtil.ifnull(EntitySchemaUtil.getAsString(s.getPropertiesOrDefault(destinationField, null)), ""),
                        waitCollections);
                final PCollection<Entity> input = (PCollection<Entity>) collection.getCollection();
                final PCollection<GenericRecord> output = input.apply(config.getName(), write);
                return FCollection.of(config.getName(), output, DataType.AVRO, ((AvroCoder) output.getCoder()).getSchema());
            }
            case MUTATION -> {
                final BigQueryWrite<Mutation> write = new BigQueryWrite<>(
                        config.getName(),
                        collection,
                        parameters,
                        MutationToRecordConverter::convert,
                        MutationToRowConverter::convert,
                        MutationToTableRowConverter::convert,
                        s -> "",
                        waitCollections);
                final PCollection<Mutation> input = (PCollection<Mutation>) collection.getCollection();
                final PCollection<GenericRecord> output = input.apply(config.getName(), write);
                return FCollection.of(config.getName(), output, DataType.AVRO, ((AvroCoder) output.getCoder()).getSchema());
            }
            case MUTATIONGROUP -> {
                final BigQueryWrite<Mutation> write = new BigQueryWrite<>(
                        config.getName(),
                        collection,
                        parameters,
                        MutationToRecordConverter::convertMutationRecord,
                        MutationToRowConverter::convertMutationRecord,
                        MutationToTableRowConverter::convertMutationRecord,
                        s -> "",
                        waitCollections);
                final PCollection<MutationGroup> input = (PCollection<MutationGroup>) collection.getCollection();
                final PCollection<Mutation> mutations = input.apply("FlattenGroup", ParDo.of(new FlattenGroupMutationDoFn()));
                final PCollection<GenericRecord> output = mutations.apply(config.getName(), write);
                return FCollection.of(config.getName(), output, DataType.AVRO, ((AvroCoder) output.getCoder()).getSchema());
            }
            default -> throw new IllegalArgumentException("Not supported type: " + inputType + " for BigQuerySink.");
        }
    }

    public static FCollection<?> writeMulti(
            final List<FCollection<?>> inputs,
            final SinkConfig config,
            final BigQuerySinkParameters parameters,
            final List<FCollection<?>> waitCollections) {

        final List<TupleTag<?>> inputTags = new ArrayList<>();
        final List<String> inputNames = new ArrayList<>();
        final List<DataType> inputTypes = new ArrayList<>();
        final Map<String, org.apache.beam.sdk.schemas.Schema> inputSchemas = new HashMap<>();

        PCollectionTuple tuple = PCollectionTuple.empty(inputs.get(0).getCollection().getPipeline());
        for (final FCollection<?> input : inputs) {
            final TupleTag inputTag = new TupleTag<>() {};
            inputTags.add(inputTag);
            inputNames.add(input.getName());
            inputTypes.add(input.getDataType());
            inputSchemas.put(input.getName(), input.getSchema());
            tuple = tuple.and(inputTag, input.getCollection());
        }

        final Schema inputSchema = inputs.get(0).getAvroSchema();

        final BigQueryMultiWrite write = new BigQueryMultiWrite(
            config.getName(),
            inputSchema,
            parameters,
            inputTags,
            inputNames,
            inputTypes,
            waitCollections);
        final PCollection<GenericRecord> output = tuple.apply(config.getName(), write);
        return FCollection.of(config.getName(), output, DataType.AVRO, ((AvroCoder) output.getCoder()).getSchema());
    }

    public static class BigQueryWrite<T> extends PTransform<PCollection<T>, PCollection<GenericRecord>> {

        private final String name;
        private final BigQuerySinkParameters parameters;
        private final SchemaUtil.DataConverter<Schema,T,GenericRecord> converter;
        private final SchemaUtil.DataConverter<org.apache.beam.sdk.schemas.Schema,T,Row> rowConverter;
        private final SerializableFunction<T, TableRow> convertTableRowFunction;
        private final SerializableFunction<T, String> destinationFunction;
        private final List<FCollection<?>> waitCollections;

        private FCollection<?> collection;

        private BigQueryWrite(final String name,
                              final FCollection<?> collection,
                              final BigQuerySinkParameters parameters,
                              final SchemaUtil.DataConverter<Schema,T,GenericRecord> converter,
                              final SchemaUtil.DataConverter<org.apache.beam.sdk.schemas.Schema,T,Row> rowConverter,
                              final SerializableFunction<T, TableRow> convertTableRowFunction,
                              final SerializableFunction<T, String> destinationFunction,
                              final List<FCollection<?>> waitCollections) {

            this.name = name;
            this.collection = collection;
            this.parameters = parameters;
            this.converter = converter;
            this.rowConverter = rowConverter;
            this.convertTableRowFunction = convertTableRowFunction;
            this.destinationFunction = destinationFunction;
            this.waitCollections = waitCollections;
        }

        public PCollection<GenericRecord> expand(final PCollection<T> input) {
            this.parameters.validate();
            this.parameters.setDefaults(input);

            final PCollection<T> waited;
            if(waitCollections == null) {
                waited = input;
            } else {
                final List<PCollection<?>> waits = waitCollections.stream()
                        .map(FCollection::getCollection)
                        .collect(Collectors.toList());
                waited = input
                        .apply("Wait", Wait.on(waits))
                        .setCoder(input.getCoder());
            }

            final boolean isStreaming = OptionUtil.isStreaming(input);
            final TableSchema tableSchema = collection.getTableSchema();
            final String destinationField = parameters.getDynamicDestination();

            final WriteResult writeResult;
            final WriteFormat writeDataType = writeDataType(
                    parameters.getMethod(),
                    collection.getDataType(),
                    collection.getAvroSchema(),
                    parameters.getWriteFormat(),
                    isStreaming);
            switch (writeDataType) {
                case row -> {
                    BigQueryIO.Write<Row> write = BigQueryIO
                            .<Row>write()
                            .useBeamSchema();
                    final SerializableFunction<Row, String> destinationFunction = s -> s.getValue(destinationField) == null ? "" : s.getValue(destinationField).toString();
                    write = applyParameters(write, parameters, tableSchema, isStreaming, destinationFunction);
                    if(DataType.ROW.equals(collection.getDataType())) {
                        writeResult = ((PCollection<Row>)waited)
                                .apply("WriteRow", write);
                    } else {
                        writeResult = waited
                                .apply("ConvertToRow", ParDo.of(new ToRowDoFn<>(collection.getSchema(), rowConverter)))
                                .setCoder(RowCoder.of(collection.getSchema()))
                                .apply("WriteRow", write);
                    }
                }
                case avro -> {
                    BigQueryIO.Write<GenericRecord> write = BigQueryIO
                            .writeGenericRecords()
                            .useAvroLogicalTypes();
                    final SerializableFunction<GenericRecord, String> destinationFunction = (s) -> s.get(destinationField) == null ? "" : s.get(destinationField).toString();
                    write = applyParameters(write, parameters, tableSchema, isStreaming, destinationFunction);
                    if(DataType.AVRO.equals(collection.getDataType())) {
                        writeResult = ((PCollection<GenericRecord>)waited)
                                .apply("WriteAvro", write);
                    } else {
                        writeResult = waited
                                .apply("ConvertToAvro", ParDo.of(new ToRecordDoFn<>(collection.getAvroSchema().toString(), converter)))
                                .setCoder(AvroCoder.of(collection.getAvroSchema()))
                                .apply("WriteAvro", write);
                    }
                }
                case avrofile, json -> {
                    BigQueryIO.Write<T> write = BigQueryIO
                            .<T>write()
                            .withFormatFunction(convertTableRowFunction);
                    write = applyParameters(write, parameters, tableSchema, isStreaming, destinationFunction);
                    writeResult = waited.apply("WriteTableRow", write);
                }
                default -> throw new IllegalArgumentException();
            }

            final boolean isStreamingInsert =
                    BigQueryIO.Write.Method.STREAMING_INSERTS.equals(parameters.getMethod())
                            || (BigQueryIO.Write.Method.DEFAULT.equals(parameters.getMethod()) && isStreaming);
            final boolean isStorageApiInsert =
                    BigQueryIO.Write.Method.STORAGE_WRITE_API.equals(parameters.getMethod())
                            || BigQueryIO.Write.Method.STORAGE_API_AT_LEAST_ONCE.equals(parameters.getMethod());

            if(isStreamingInsert) {
                if(parameters.getWithExtendedErrorInfo()) {
                    return writeResult.getFailedInsertsWithErr()
                            .apply("ConvertFailureRecordWithError", ParDo.of(new FailedRecordWithErrorDoFn(name, collection.getAvroSchema().toString())))
                            .setCoder(AvroCoder.of(FailedRecordWithErrorDoFn.createOutputSchema(collection.getAvroSchema())));
                } else {
                    return writeResult.getFailedInserts()
                            .apply("ConvertFailureRecord", ParDo.of(new FailedRecordDoFn(name, collection.getAvroSchema().toString())))
                            .setCoder(AvroCoder.of(collection.getAvroSchema()));
                }
            } else if(isStorageApiInsert) {
                return writeResult.getFailedStorageApiInserts()
                        .apply("ConvertFailureSARecord", ParDo.of(new FailedStorageApiRecordDoFn(name, collection.getAvroSchema().toString())))
                        .setCoder(AvroCoder.of(collection.getAvroSchema()));
            } else {
                return writeResult.getFailedInserts()
                        .apply("ConvertFailureRecord", ParDo.of(new FailedRecordDoFn(name, collection.getAvroSchema().toString())))
                        .setCoder(AvroCoder.of(collection.getAvroSchema()));
            }
        }

        private static <InputT> SerializableFunction<InputT, RowMutationInformation> createRowMutationInformationFunction(
                final RowMutationInformation.MutationType mutationType,
                final SchemaUtil.StringGetter<InputT> stringGetter,
                final SchemaUtil.LongGetter<InputT> longGetter,
                final String typeField,
                final String sequenceField) {

            return (InputT input) -> {
                final RowMutationInformation.MutationType type;
                if(mutationType != null) {
                    type = mutationType;
                } else if(typeField != null) {
                    final String typeString = stringGetter.getAsString(input, typeField);
                    type = RowMutationInformation.MutationType.valueOf(typeString);
                } else {
                    throw new IllegalStateException();
                }

                final long sequenceNumber = longGetter.getAsLong(input, sequenceField);

                return RowMutationInformation.of(type, sequenceNumber);
            };
        }

        private static class ToRecordDoFn<T> extends DoFn<T, GenericRecord> {

            private final String schemaString;
            private final SchemaUtil.DataConverter<Schema,T,GenericRecord> converter;

            private transient Schema schema;

            ToRecordDoFn(final String schemaString, final SchemaUtil.DataConverter<Schema,T,GenericRecord> converter) {
                this.schemaString = schemaString;
                this.converter = converter;
            }

            @Setup
            public void setup() {
                this.schema = AvroSchemaUtil.convertSchema(schemaString);
            }

            @ProcessElement
            public void processElement(ProcessContext c) {
                final T element = c.element();
                final GenericRecord record = converter.convert(schema, element);
                c.output(record);
            }

        }

        private static class ToRowDoFn<T> extends DoFn<T, Row> {

            private final org.apache.beam.sdk.schemas.Schema schema;
            private final SchemaUtil.DataConverter<org.apache.beam.sdk.schemas.Schema,T,Row> converter;

            ToRowDoFn(final org.apache.beam.sdk.schemas.Schema schema, final SchemaUtil.DataConverter<org.apache.beam.sdk.schemas.Schema,T,Row> converter) {
                this.schema = schema;
                this.converter = converter;
            }

            @ProcessElement
            public void processElement(ProcessContext c) {
                final T element = c.element();
                final Row row = converter.convert(schema, element);
                c.output(row);
            }

        }

    }

    public static class BigQueryMultiWrite extends PTransform<PCollectionTuple, PCollection<GenericRecord>> {

        private final String name;
        private final BigQuerySinkParameters parameters;

        private final Schema schema;
        private final List<TupleTag<?>> inputTags;
        private final List<String> inputNames;
        private final List<DataType> inputTypes;
        private final List<FCollection<?>> waitCollections;

        private BigQueryMultiWrite(
                final String name,
                final Schema schema,
                final BigQuerySinkParameters parameters,
                final List<TupleTag<?>> inputTags,
                final List<String> inputNames,
                final List<DataType> inputTypes,
                final List<FCollection<?>> waitCollections) {

            this.name = name;
            this.parameters = parameters;
            this.schema = schema;
            this.inputTags = inputTags;
            this.inputNames = inputNames;
            this.inputTypes = inputTypes;
            this.waitCollections = waitCollections;
        }

        public PCollection<GenericRecord> expand(final PCollectionTuple inputs) {
            this.parameters.validate();
            this.parameters.setDefaults(inputs);

            final PCollection<UnionValue> unionValues = inputs
                    .apply("Union", Union.flatten(inputTags, inputTypes, inputNames));

            final PCollection<UnionValue> waited;
            if(waitCollections == null) {
                waited = unionValues;
            } else {
                final List<PCollection<?>> waits = waitCollections.stream()
                        .map(FCollection::getCollection)
                        .collect(Collectors.toList());
                waited = unionValues
                        .apply("Wait", Wait.on(waits))
                        .setCoder(unionValues.getCoder());
            }

            final boolean isStreaming = OptionUtil.isStreaming(inputs);
            final TableSchema tableSchema = RecordToTableRowConverter.convertSchema(schema);
            final String destinationField = parameters.getDynamicDestination();

            final WriteFormat writeDataType = writeDataType(parameters.getMethod(), inputTypes.get(0), schema, parameters.getWriteFormat(), isStreaming);
            final WriteResult writeResult = switch (writeDataType) {
                case row -> {
                    BigQueryIO.Write<Row> write = BigQueryIO
                            .<Row>write()
                            .useBeamSchema();
                    final SerializableFunction<Row, String> destinationFunction = s -> s.getValue(destinationField) == null ? "" : s.getValue(destinationField).toString();
                    write = applyParameters(write, parameters, tableSchema, isStreaming, destinationFunction);
                    final org.apache.beam.sdk.schemas.Schema rowSchema = RecordToRowConverter.convertSchema(schema);
                    yield waited
                            .apply("ConvertToRow", ParDo.of(new Union.ToRowDoFn(rowSchema)))
                            .setCoder(RowCoder.of(rowSchema))
                            .apply("WriteRow", write);
                }
                case avro -> {
                    BigQueryIO.Write<GenericRecord> write = BigQueryIO
                            .writeGenericRecords()
                            .useAvroLogicalTypes();
                    final SerializableFunction<GenericRecord, String> destinationFunction = (s) -> s.get(destinationField) == null ? "" : s.get(destinationField).toString();
                    write = applyParameters(write, parameters, tableSchema, isStreaming, destinationFunction);
                    yield waited
                            .apply("ConvertToAvro", ParDo.of(new Union.ToRecordDoFn(schema.toString())))
                            .setCoder(AvroCoder.of(schema))
                            .apply("WriteAvro", write);
                }
                case avrofile -> {
                    BigQueryIO.Write<UnionValue> write = BigQueryIO
                            .<UnionValue>write()
                            .withAvroFormatFunction((AvroWriteRequest<UnionValue> r) -> switch (r.getElement().getType()) {
                                case ROW -> RowToRecordConverter.convert(r.getSchema(), (Row) r.getElement().getValue());
                                case AVRO -> (GenericRecord) r.getElement().getValue();
                                case STRUCT -> StructToRecordConverter.convert(r.getSchema(), (Struct) r.getElement().getValue());
                                case DOCUMENT -> DocumentToRecordConverter.convert(r.getSchema(), (Document) r.getElement().getValue());
                                case ENTITY -> EntityToRecordConverter.convert(r.getSchema(), (Entity) r.getElement().getValue());
                                case MUTATION -> MutationToRecordConverter.convert(schema, (Mutation) r.getElement().getValue());
                                default -> throw new IllegalArgumentException();
                            })
                            .useAvroLogicalTypes();
                    write = applyParameters(write, parameters, tableSchema, isStreaming, createDestinationFunction(destinationField));
                    yield waited.apply("WriteAvro", write);
                }
                case json -> {
                    BigQueryIO.Write<UnionValue> write = BigQueryIO
                            .<UnionValue>write()
                            .withFormatFunction(convertTableRowFunction);
                    write = applyParameters(write, parameters, tableSchema, isStreaming, createDestinationFunction(destinationField));
                    yield waited.apply("WriteTableRow", write);
                }
            };

            final boolean isStreamingInsert =
                    BigQueryIO.Write.Method.STREAMING_INSERTS.equals(parameters.getMethod())
                            || (BigQueryIO.Write.Method.DEFAULT.equals(parameters.getMethod()) && isStreaming);
            final boolean isStorageApiInsert =
                    BigQueryIO.Write.Method.STORAGE_WRITE_API.equals(parameters.getMethod())
                            || BigQueryIO.Write.Method.STORAGE_API_AT_LEAST_ONCE.equals(parameters.getMethod());

            if(isStreamingInsert) {
                if(parameters.getWithExtendedErrorInfo()) {
                    return writeResult.getFailedInsertsWithErr()
                            .apply("ConvertFailureRecordWithError", ParDo.of(new FailedRecordWithErrorDoFn(name, schema.toString())))
                            .setCoder(AvroCoder.of(FailedRecordWithErrorDoFn.createOutputSchema(schema)));
                } else {
                    return writeResult.getFailedInserts()
                            .apply("ConvertFailureRecord", ParDo.of(new FailedRecordDoFn(name, schema.toString())))
                            .setCoder(AvroCoder.of(schema));
                }
            } else if(isStorageApiInsert) {
                return writeResult.getFailedStorageApiInserts()
                        .apply("ConvertFailureSARecord", ParDo.of(new FailedStorageApiRecordDoFn(name, schema.toString())))
                        .setCoder(AvroCoder.of(schema));
            } else {
                return writeResult.getFailedInserts()
                        .apply("ConvertFailureRecord", ParDo.of(new FailedRecordDoFn(name, schema.toString())))
                        .setCoder(AvroCoder.of(schema));
            }
        }

        private static final SerializableFunction<UnionValue, TableRow> convertTableRowFunction = (UnionValue unionValue) -> switch (unionValue.getType()) {
            case ROW -> RowToTableRowConverter.convert((Row) unionValue.getValue());
            case AVRO -> RecordToTableRowConverter.convert((GenericRecord) unionValue.getValue());
            case STRUCT -> StructToTableRowConverter.convert((Struct) unionValue.getValue());
            case DOCUMENT -> DocumentToTableRowConverter.convert((Document) unionValue.getValue());
            case ENTITY -> EntityToTableRowConverter.convertWithoutKey((Entity) unionValue.getValue());
            default -> throw new IllegalArgumentException();
        };

        private static SerializableFunction<UnionValue, String> createDestinationFunction(final String field) {
            return (UnionValue unionValue) -> Optional.ofNullable(
                    switch (unionValue.getType()) {
                        case ROW -> RowSchemaUtil.getAsString((Row) unionValue.getValue(), field);
                        case AVRO -> AvroSchemaUtil.getAsString((GenericRecord) unionValue.getValue(), field);
                        case STRUCT -> StructSchemaUtil.getAsString((Struct) unionValue.getValue(), field);
                        case DOCUMENT -> DocumentSchemaUtil.getAsString((Document) unionValue.getValue(), field);
                        case ENTITY -> EntitySchemaUtil.getAsString((Entity) unionValue.getValue(), field);
                        default -> throw new IllegalArgumentException();
                    })
                    .orElse("");
        }

    }

    private static WriteFormat writeDataType(
            final BigQueryIO.Write.Method method,
            final DataType inputDataType,
            final Schema inputSchema,
            final WriteFormat writeFormat,
            final boolean isStreaming) {

        if(writeFormat != null) {
            return writeFormat;
        }

        if(isStreaming) {
            if(BigQueryIO.Write.Method.STREAMING_INSERTS.equals(method)
                    || BigQueryIO.Write.Method.DEFAULT.equals(method)) {

                return WriteFormat.json;
            } else {
                return WriteFormat.row;
            }
        } else {
            if(BigQueryIO.Write.Method.FILE_LOADS.equals(method)
                    || BigQueryIO.Write.Method.DEFAULT.equals(method)) {

                if(!AvroSchemaUtil.isNestedSchema(inputSchema)) {
                    return WriteFormat.avrofile;
                } else {
                    return WriteFormat.json;
                }
            } else if(inputDataType == null || DataType.ROW.equals(inputDataType)) {
                return WriteFormat.row;
            } else {
                return WriteFormat.row;
            }
        }
    }

    private static <InputT> BigQueryIO.Write<InputT> applyParameters(
            final BigQueryIO.Write<InputT> base,
            final BigQuerySinkParameters parameters,
            final TableSchema tableSchema,
            final boolean isStreaming,
            final SerializableFunction<InputT, String> destinationFunction) {

        final String table = parameters.getTable();

        BigQueryIO.Write<InputT> write = base
                .withTableDescription("Auto Generated at " + Instant.now())
                .withWriteDisposition(parameters.getWriteDisposition())
                .withCreateDisposition(parameters.getCreateDisposition())
                .withMethod(parameters.getMethod());

        if(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED.equals(parameters.getCreateDisposition())) {
            write = write.withSchema(tableSchema);
        }

        if(parameters.getPartitioning() != null) {
            final String partitioningType = parameters.getPartitioning().trim();
            final String partitioningField = parameters.getPartitioningField();
            final TimePartitioning timePartitioning = new TimePartitioning().setType(partitioningType).setField(partitioningField);
            write = write.withTimePartitioning(timePartitioning);
        }

        if(parameters.getClusteringFields().size() > 0) {
            final Clustering clustering = new Clustering().setFields(parameters.getClusteringFields());
            write = write.withClustering(clustering);
        }

        if(parameters.getPrimaryKeyFields().size() > 0) {
            write = write.withPrimaryKey(parameters.getPrimaryKeyFields());
        }

        if(parameters.getKmsKey() != null) {
            write = write.withKmsKey(parameters.getKmsKey().trim());
        }

        if(parameters.getDynamicDestination() != null) {
            write = write.to(new DynamicDestinationFunc<>(table, tableSchema, parameters.getDynamicDestination(), destinationFunction));
        } else {
            write = write.to(table);
        }

        if(parameters.getOptimizedWrites()) {
            write = write.optimizedWrites();
        }

        if(parameters.getSchemaUpdateOptions() != null && parameters.getSchemaUpdateOptions().size() > 0) {
            write = write.withSchemaUpdateOptions(new HashSet<>(parameters.getSchemaUpdateOptions()));
        }
        if(parameters.getAutoSchemaUpdate() != null) {
            if(BigQueryIO.Write.Method.STORAGE_WRITE_API.equals(parameters.getMethod())) {
                write = write.withAutoSchemaUpdate(parameters.getAutoSchemaUpdate());
            } else {
                LOG.warn("BigQuery sink autoSchemaUpdate parameter is applicable for only STORAGE_WRITE_API method");
            }
        }

        if(parameters.getWithoutValidation() != null && parameters.getWithoutValidation()) {
            write = write.withoutValidation();
        }

        if(isStreaming) {
            // For streaming mode options
            if(parameters.getSkipInvalidRows()) {
                write = write.skipInvalidRows();
            }
            if(parameters.getIgnoreUnknownValues()) {
                write = write.ignoreUnknownValues();
            }
            if(parameters.getIgnoreInsertIds()) {
                write = write.ignoreInsertIds();
            }
            if(parameters.getWithExtendedErrorInfo()) {
                write = write.withExtendedErrorInfo();
            }
            if(!BigQueryIO.Write.Method.FILE_LOADS.equals(parameters.getMethod())) {
                if(parameters.getFailedInsertRetryPolicy().equals(FailedInsertRetryPolicy.always)) {
                    write = write.withFailedInsertRetryPolicy(InsertRetryPolicy.alwaysRetry());
                } else if(parameters.getFailedInsertRetryPolicy().equals(FailedInsertRetryPolicy.never)) {
                    write = write.withFailedInsertRetryPolicy(InsertRetryPolicy.neverRetry());
                } else {
                    write = write.withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors());
                }
            }

            switch (parameters.getMethod()) {
                case FILE_LOADS, STORAGE_WRITE_API -> {
                    write = write
                            .withTriggeringFrequency(Duration.standardSeconds(parameters.getTriggeringFrequencySecond()));
                    if (parameters.getAutoSharding()) {
                        write = write.withAutoSharding();
                    } else if (parameters.getNumStorageWriteApiStreams() != null) {
                        write = write.withNumStorageWriteApiStreams(parameters.getNumStorageWriteApiStreams());
                    }
                }
                case DEFAULT, STREAMING_INSERTS -> {
                    if (parameters.getAutoSharding()) {
                        write = write.withAutoSharding();
                    }
                }
            }

        } else {
            // For batch mode options
            switch (parameters.getMethod()) {
                case STORAGE_WRITE_API, STORAGE_API_AT_LEAST_ONCE -> {
                    if (parameters.getNumStorageWriteApiStreams() != null) {
                        write = write.withNumStorageWriteApiStreams(parameters.getNumStorageWriteApiStreams());
                    }
                }
            }
        }

        return write;
    }

    private static class FailedRecordDoFn extends DoFn<TableRow, GenericRecord> {

        private final Counter errorCounter;
        private final String recordSchemaString;
        private transient Schema recordSchema;

        FailedRecordDoFn(final String name, final String schemaString) {
            this.errorCounter = Metrics.counter(name, "elements_insert_error");
            this.recordSchemaString = schemaString;
        }

        @Setup
        public void setup() {
            this.recordSchema = AvroSchemaUtil.convertSchema(recordSchemaString);
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            errorCounter.inc();
            final TableRow tableRow = c.element();
            LOG.error("FailedProcessElement: " + tableRow.toString());
            final GenericRecord record = TableRowToRecordConverter.convert(recordSchema, tableRow);
            c.output(record);

            LOG.error("Failed insert record: " + RecordToJsonConverter.convert(record));
        }

    }

    private static class FailedRecordWithErrorDoFn extends DoFn<BigQueryInsertError, GenericRecord> {

        private final Counter errorCounter;
        private final String recordSchemaString;
        private transient Schema recordSchema;
        private transient Schema outputSchema;
        private transient Schema errorSchema;

        FailedRecordWithErrorDoFn(final String name, final String schemaString) {
            this.errorCounter = Metrics.counter(name, "elements_insert_error");
            this.recordSchemaString = schemaString;
        }

        @Setup
        public void setup() {
            this.recordSchema = AvroSchemaUtil.convertSchema(recordSchemaString);
            this.outputSchema = createOutputSchema(recordSchema);
            this.errorSchema = AvroSchemaUtil.unnestUnion(outputSchema.getField("errors").schema()).getElementType();
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            errorCounter.inc();
            final TableRow tableRow = c.element().getRow();
            final GenericRecord record = TableRowToRecordConverter.convert(recordSchema, tableRow);
            final TableDataInsertAllResponse.InsertErrors errors = c.element().getError();
            final List<GenericRecord> errorMessages = errors.getErrors() == null ? null : errors.getErrors().stream()
                    .map(error -> new GenericRecordBuilder(errorSchema)
                            .set("message", error.getMessage())
                            .set("reason", error.getReason())
                            .set("location", error.getLocation())
                            .set("debugInfo", error.getDebugInfo())
                            .build())
                    .collect(Collectors.toList());
            final GenericRecord errorRecord = new GenericRecordBuilder(this.outputSchema)
                    .set("index", errors.getIndex())
                    .set("errors", errorMessages)
                    .set("record", record)
                    .build();

            c.output(errorRecord);

            LOG.error("Failed insert record: " + RecordToJsonConverter.convert(errorRecord));
        }

        static Schema createOutputSchema(final Schema recordSchema) {
            final Schema errorSchema = Schema.createRecord("errorMessage", "", "extendedErrorInfo", false, Arrays.asList(
                    new Schema.Field("reason", AvroSchemaUtil.NULLABLE_STRING, "", (Object)null),
                    new Schema.Field("location", AvroSchemaUtil.NULLABLE_STRING, "", (Object)null),
                    new Schema.Field("message", AvroSchemaUtil.NULLABLE_STRING, "", (Object)null),
                    new Schema.Field("debugInfo", AvroSchemaUtil.NULLABLE_STRING, "", (Object)null)
            ));
            return Schema.createRecord("extendedErrorInfo", "", "root", false, Arrays.asList(
                    new Schema.Field("index", AvroSchemaUtil.NULLABLE_LONG, "", (Object)null),
                    new Schema.Field("errors", Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.createArray(errorSchema)), "", (Object)null),
                    new Schema.Field("record", recordSchema, "", (Object)null)
            ));
        }

    }

    private static class FailedStorageApiRecordDoFn extends DoFn<BigQueryStorageApiInsertError, GenericRecord> {

        private final Counter errorCounter;
        private final String recordSchemaString;
        private transient Schema recordSchema;

        FailedStorageApiRecordDoFn(final String name, final String schemaString) {
            this.errorCounter = Metrics.counter(name, "elements_insert_error");
            this.recordSchemaString = schemaString;
        }

        @Setup
        public void setup() {
            this.recordSchema = AvroSchemaUtil.convertSchema(recordSchemaString);
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            errorCounter.inc();
            final BigQueryStorageApiInsertError error = c.element();
            final TableRow tableRow = error.getRow();
            LOG.error("FailedProcessElement: " + tableRow.toString() + " with error message: " + error.getErrorMessage());
            final GenericRecord record = TableRowToRecordConverter.convert(recordSchema, tableRow);
            c.output(record);
        }

    }

    private static class DynamicDestinationFunc<T> extends DynamicDestinations<T, String> {

        private final String table;
        private final TableSchema tableSchema;
        private final String dynamicDestination;
        private final boolean isTimePartitioning;
        private final SerializableFunction<T, String> destinationFunction;

        public DynamicDestinationFunc(final String table, final TableSchema tableSchema,
                                      final String dynamicDestination,
                                      final SerializableFunction<T, String> destinationFunction) {

            this.table = table;
            this.tableSchema = tableSchema;
            this.dynamicDestination = dynamicDestination;
            this.isTimePartitioning = dynamicDestination.startsWith("$");
            this.destinationFunction = destinationFunction;
        }

        @Override
        public String getDestination(ValueInSingleWindow<T> element) {
            return table + (isTimePartitioning ? dynamicDestination : destinationFunction.apply(element.getValue()));
        }
        @Override
        public TableDestination getTable(String destination) {
            if(isTimePartitioning) {
                final TimePartitioning timePartitioning = new TimePartitioning();
                return new TableDestination(destination, null, timePartitioning);
            } else {
                return new TableDestination(destination, null);
            }
        }
        @Override
        public TableSchema getSchema(String destination) {
            return tableSchema;
        }

    }

    private static class FlattenGroupMutationDoFn extends DoFn<MutationGroup, Mutation> {

        @ProcessElement
        public void processElement(ProcessContext c) {
            final MutationGroup mutationGroup = c.element();
            c.output(mutationGroup.primary());
            for(Mutation mutation : mutationGroup.attached()) {
                c.output(mutation);
            }
        }

    }

}
