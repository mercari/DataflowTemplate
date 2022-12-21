package com.mercari.solution.module.sink;

import com.google.api.services.bigquery.model.*;
import com.google.cloud.spanner.Struct;
import com.google.datastore.v1.Entity;
import com.google.gson.Gson;
import com.mercari.solution.config.SinkConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.module.SinkModule;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.OptionUtil;
import com.mercari.solution.util.converter.*;
import com.mercari.solution.util.schema.EntitySchemaUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.io.gcp.bigquery.*;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;


public class BigQuerySink implements SinkModule {

    private static final Logger LOG = LoggerFactory.getLogger(BigQuerySink.class);

    private class BigQuerySinkParameters {

        private String table;
        private String writeDisposition;
        private String createDisposition;
        private BigQueryIO.Write.Method method;

        private String partitioning;
        private String partitioningField;
        private String clustering;

        private Boolean skipInvalidRows;
        private Boolean ignoreUnknownValues;
        private Boolean ignoreInsertIds;
        private Boolean withExtendedErrorInfo;
        private FailedInsertRetryPolicy failedInsertRetryPolicy;

        private String dynamicDestination;

        private List<BigQueryIO.Write.SchemaUpdateOption> schemaUpdateOptions;

        private String kmsKey;

        private Boolean optimizedWrites;
        private Boolean autoSharding;
        private Long triggeringFrequencySecond;
        private Integer numStorageWriteApiStreams;

        public String getTable() {
            return table;
        }

        public void setTable(String table) {
            this.table = table;
        }

        public String getWriteDisposition() {
            return writeDisposition;
        }

        public void setWriteDisposition(String writeDisposition) {
            this.writeDisposition = writeDisposition;
        }

        public String getCreateDisposition() {
            return createDisposition;
        }

        public void setCreateDisposition(String createDisposition) {
            this.createDisposition = createDisposition;
        }

        public BigQueryIO.Write.Method getMethod() {
            return method;
        }

        public void setMethod(BigQueryIO.Write.Method method) {
            this.method = method;
        }

        public String getPartitioning() {
            return partitioning;
        }

        public void setPartitioning(String partitioning) {
            this.partitioning = partitioning;
        }

        public String getPartitioningField() {
            return partitioningField;
        }

        public void setPartitioningField(String partitioningField) {
            this.partitioningField = partitioningField;
        }

        public String getDynamicDestination() {
            return dynamicDestination;
        }

        public void setDynamicDestination(String dynamicDestination) {
            this.dynamicDestination = dynamicDestination;
        }

        public String getClustering() {
            return clustering;
        }

        public void setClustering(String clustering) {
            this.clustering = clustering;
        }

        public Boolean getSkipInvalidRows() {
            return skipInvalidRows;
        }

        public void setSkipInvalidRows(Boolean skipInvalidRows) {
            this.skipInvalidRows = skipInvalidRows;
        }

        public Boolean getIgnoreUnknownValues() {
            return ignoreUnknownValues;
        }

        public void setIgnoreUnknownValues(Boolean ignoreUnknownValues) {
            this.ignoreUnknownValues = ignoreUnknownValues;
        }

        public Boolean getIgnoreInsertIds() {
            return ignoreInsertIds;
        }

        public void setIgnoreInsertIds(Boolean ignoreInsertIds) {
            this.ignoreInsertIds = ignoreInsertIds;
        }

        public Boolean getWithExtendedErrorInfo() {
            return withExtendedErrorInfo;
        }

        public void setWithExtendedErrorInfo(Boolean withExtendedErrorInfo) {
            this.withExtendedErrorInfo = withExtendedErrorInfo;
        }

        public FailedInsertRetryPolicy getFailedInsertRetryPolicy() {
            return failedInsertRetryPolicy;
        }

        public void setFailedInsertRetryPolicy(FailedInsertRetryPolicy failedInsertRetryPolicy) {
            this.failedInsertRetryPolicy = failedInsertRetryPolicy;
        }

        public List<BigQueryIO.Write.SchemaUpdateOption> getSchemaUpdateOptions() {
            return schemaUpdateOptions;
        }

        public void setSchemaUpdateOptions(List<BigQueryIO.Write.SchemaUpdateOption> schemaUpdateOptions) {
            this.schemaUpdateOptions = schemaUpdateOptions;
        }

        public String getKmsKey() {
            return kmsKey;
        }

        public void setKmsKey(String kmsKey) {
            this.kmsKey = kmsKey;
        }

        public Boolean getOptimizedWrites() {
            return optimizedWrites;
        }

        public void setOptimizedWrites(Boolean optimizedWrites) {
            this.optimizedWrites = optimizedWrites;
        }

        public Boolean getAutoSharding() {
            return autoSharding;
        }

        public void setAutoSharding(Boolean autoSharding) {
            this.autoSharding = autoSharding;
        }

        public Long getTriggeringFrequencySecond() {
            return triggeringFrequencySecond;
        }

        public void setTriggeringFrequencySecond(Long triggeringFrequencySecond) {
            this.triggeringFrequencySecond = triggeringFrequencySecond;
        }

        public Integer getNumStorageWriteApiStreams() {
            return numStorageWriteApiStreams;
        }

        public void setNumStorageWriteApiStreams(Integer numStorageWriteApiStreams) {
            this.numStorageWriteApiStreams = numStorageWriteApiStreams;
        }

        private void validate() {
            if(this.getTable() == null) {
                throw new IllegalArgumentException("BigQuery output module requires table parameter!");
            }
        }

        private void setDefaults(final PInput input) {
            if(this.getWriteDisposition() == null) {
                this.setWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_EMPTY.name());
            } else {
                this.setWriteDisposition(this.getWriteDisposition().trim().toUpperCase());
            }

            if(this.getCreateDisposition() == null) {
                this.setCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER.name());
            } else {
                this.setCreateDisposition(this.getCreateDisposition().trim().toUpperCase());
            }

            if(this.getMethod() == null) {
                this.setMethod(BigQueryIO.Write.Method.DEFAULT);
            }

            if(this.getSkipInvalidRows() == null) {
                this.setSkipInvalidRows(false);
            }
            if(this.getIgnoreUnknownValues() == null) {
                this.setIgnoreUnknownValues(false);
            }
            if(this.getIgnoreInsertIds() == null) {
                this.setIgnoreInsertIds(false);
            }
            if(this.getWithExtendedErrorInfo() == null) {
                this.setWithExtendedErrorInfo(false);
            }
            if(this.getFailedInsertRetryPolicy() == null) {
                this.setFailedInsertRetryPolicy(FailedInsertRetryPolicy.always);
            }

            if(this.getOptimizedWrites() == null) {
                this.setOptimizedWrites(false);
            }
            if(BigQueryIO.Write.Method.FILE_LOADS.equals(this.getMethod())
                    || BigQueryIO.Write.Method.STORAGE_WRITE_API.equals(this.getMethod())
                    || BigQueryIO.Write.Method.STORAGE_API_AT_LEAST_ONCE.equals(this.getMethod())) {
                if(this.getTriggeringFrequencySecond() == null) {
                    if(OptionUtil.isStreaming(input)) {
                        this.setTriggeringFrequencySecond(10L);
                    }
                } else {
                    if(!OptionUtil.isStreaming(input)) {
                        LOG.warn("triggeringFrequencySecond must not be set in batch mode");
                        this.setTriggeringFrequencySecond(null);
                    }
                }
                if(this.getNumStorageWriteApiStreams() == null) {
                    this.setAutoSharding(true);
                }
            }
            if(this.getAutoSharding() == null) {
                this.setAutoSharding(false);
            }

        }
    }

    private enum FailedInsertRetryPolicy {
        always,
        never,
        retryTransientErrors
    }

    public String getName() { return "bigquery"; }

    public Map<String, FCollection<?>> expand(FCollection<?> input, SinkConfig config, List<FCollection<?>> waits, List<FCollection<?>> sideInputs) {
        final Map<String, FCollection<?>> outputs = new HashMap<>();
        final FCollection<?> output = BigQuerySink.write(input, config, waits, sideInputs);
        outputs.put(config.getName(), output);
        final String failuresName = config.getName() + ".failures";
        outputs.put(failuresName, FCollection.of(failuresName, output.getCollection(), output.getDataType(), output.getAvroSchema()));
        return outputs;
    }

    public static FCollection<?> write(final FCollection<?> collection, final SinkConfig config) {
        return write(collection, config, null, null);
    }

    public static FCollection<?> write(final FCollection<?> collection,
                                       final SinkConfig config,
                                       final List<FCollection<?>> waitCollections,
                                       final List<FCollection<?>> sideInputs) {

        final BigQuerySinkParameters parameters = new Gson().fromJson(config.getParameters(), BigQuerySinkParameters.class);

        final String destinationField = parameters.getDynamicDestination();
        final DataType inputType = collection.getDataType();
        try {
            config.outputAvroSchema(collection.getAvroSchema());
        } catch (Exception e) {
            LOG.error("Failed to output avro schema for " + config.getName() + " to path: " + config.getOutputAvroSchema(), e);
        }
        switch (inputType) {
            case AVRO: {
                final BigQueryWrite<GenericRecord> write = new BigQueryWrite<>(
                        config.getName(),
                        collection,
                        parameters,
                        AvroWriteRequest::getElement,
                        RecordToRowConverter::convert,
                        RecordToTableRowConverter::convert,
                        s -> s.get(destinationField) == null ? "" : s.get(destinationField).toString(),
                        waitCollections);
                final PCollection<GenericRecord> input = (PCollection<GenericRecord>) collection.getCollection();
                final PCollection<GenericRecord> output = input.apply(config.getName(), write);
                return FCollection.of(config.getName(), output, DataType.AVRO, ((AvroCoder)output.getCoder()).getSchema());
            }
            case ROW: {
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
                return FCollection.of(config.getName(), output, DataType.AVRO, ((AvroCoder)output.getCoder()).getSchema());
            }
            case STRUCT: {
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
                return FCollection.of(config.getName(), output, DataType.AVRO, ((AvroCoder)output.getCoder()).getSchema());
            }
            case ENTITY: {
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
                return FCollection.of(config.getName(), output, DataType.AVRO, ((AvroCoder)output.getCoder()).getSchema());
            }
            default:
                throw new IllegalArgumentException("Not supported type: " + inputType + " for BigQuerySink.");
        }
    }

    public static class BigQueryWrite<T> extends PTransform<PCollection<T>, PCollection<GenericRecord>> {

        private final String name;
        private final BigQuerySinkParameters parameters;
        private final SerializableFunction<AvroWriteRequest<T>, GenericRecord> convertAvroFunction;
        private final RowConverter<T> convertRowFunction;
        private final SerializableFunction<T, TableRow> convertTableRowFunction;
        private final SerializableFunction<T, String> destinationFunction;
        private final List<FCollection<?>> waitCollections;

        private FCollection<?> collection;

        private BigQueryWrite(final String name,
                              final FCollection<?> collection,
                              final BigQuerySinkParameters parameters,
                              final SerializableFunction<AvroWriteRequest<T>, GenericRecord> convertAvroFunction,
                              final RowConverter<T> convertRowFunction,
                              final SerializableFunction<T, TableRow> convertTableRowFunction,
                              final SerializableFunction<T, String> destinationFunction,
                              final List<FCollection<?>> waitCollections) {

            this.name = name;
            this.collection = collection;
            this.parameters = parameters;
            this.convertAvroFunction = convertAvroFunction;
            this.convertRowFunction = convertRowFunction;
            this.convertTableRowFunction = convertTableRowFunction;
            this.destinationFunction = destinationFunction;
            this.waitCollections = waitCollections;
        }

        public PCollection<GenericRecord> expand(final PCollection<T> input) {
            this.parameters.validate();
            this.parameters.setDefaults(input);

            final boolean isStreaming = OptionUtil.isStreaming(input);
            final boolean useRow = BigQueryIO.Write.Method.STORAGE_WRITE_API.equals(parameters.getMethod())
                    || BigQueryIO.Write.Method.STORAGE_API_AT_LEAST_ONCE.equals(parameters.getMethod());

            final WriteResult writeResult;
            if(useRow) {
                final String destinationField = parameters.getDynamicDestination();
                final ToRowDoFn<T> dofn = new ToRowDoFn(collection.getSchema(), convertRowFunction);
                final PCollection<Row> rows = input
                        .apply("ToBeamRow", ParDo.of(dofn))
                        .setCoder(RowCoder.of(collection.getSchema()))
                        .setRowSchema(collection.getSchema());
                final BigQueryIO.Write<Row> write = createWrite(
                        isStreaming, null, null,
                        s -> s.getValue(destinationField) == null ? "" : s.getValue(destinationField).toString());
                if(waitCollections == null) {
                    writeResult = rows.apply("WriteBigQuery", write);
                } else {
                    final List<PCollection<?>> waits = waitCollections.stream()
                            .map(FCollection::getCollection)
                            .collect(Collectors.toList());
                    writeResult = rows
                            .apply("Wait", Wait.on(waits))
                            .setCoder(rows.getCoder())
                            .apply("WriteTable", write);
                }
            } else {
                final BigQueryIO.Write<T> write = createWrite(
                        isStreaming, convertAvroFunction, convertTableRowFunction, destinationFunction);
                if(waitCollections == null) {
                    writeResult = input.apply("WriteBigQuery", write);
                } else {
                    final List<PCollection<?>> waits = waitCollections.stream()
                            .map(FCollection::getCollection)
                            .collect(Collectors.toList());
                    writeResult = input
                            .apply("Wait", Wait.on(waits))
                            .setCoder(input.getCoder())
                            .apply("WriteTable", write);
                }
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

        private <T> BigQueryIO.Write<T> createWrite(
                final boolean isStreaming,
                final SerializableFunction<AvroWriteRequest<T>, GenericRecord> convertAvroFunction,
                final SerializableFunction<T, TableRow> convertTableRowFunction,
                final SerializableFunction<T, String> destinationFunction ) {

            final String table = this.parameters.getTable();
            final TableSchema tableSchema = collection.getTableSchema();

            BigQueryIO.Write<T> write = BigQueryIO.<T>write()
                    .withSchema(tableSchema)
                    .withTableDescription("Auto Generated at " + Instant.now())
                    .withWriteDisposition(BigQueryIO.Write.WriteDisposition.valueOf(this.parameters.getWriteDisposition()))
                    .withCreateDisposition(BigQueryIO.Write.CreateDisposition.valueOf(this.parameters.getCreateDisposition()))
                    .withMethod(this.parameters.getMethod());

            if(this.parameters.getPartitioning() != null) {
                final String partitioningType = this.parameters.getPartitioning().trim();
                final String partitioningField = this.parameters.getPartitioningField();
                final TimePartitioning timePartitioning = new TimePartitioning().setType(partitioningType).setField(partitioningField);
                write = write.withTimePartitioning(timePartitioning);
            }

            if(this.parameters.getClustering() != null) {
                final List<String> clusteringFields = Arrays.asList(this.parameters.getClustering().trim().split(","));
                final Clustering clustering = new Clustering().setFields(clusteringFields);
                write = write.withClustering(clustering);
            }
            if(this.parameters.getKmsKey() != null) {
                write = write.withKmsKey(this.parameters.getKmsKey().trim());
            }

            if(this.parameters.getDynamicDestination() != null) {
                write = write.to(new DynamicDestinationFunc<>(table, tableSchema, this.parameters.getDynamicDestination(), destinationFunction));
            } else {
                write = write.to(table);
            }

            if(this.parameters.getOptimizedWrites()) {
                write = write.optimizedWrites();
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

                if(parameters.getSchemaUpdateOptions() != null && parameters.getSchemaUpdateOptions().size() > 0) {
                    write = write.withSchemaUpdateOptions(new HashSet<>(parameters.getSchemaUpdateOptions()));
                }

                switch (parameters.getMethod()) {
                    case FILE_LOADS:
                    case STORAGE_WRITE_API: {
                        write = write
                                .withTriggeringFrequency(Duration.standardSeconds(parameters.getTriggeringFrequencySecond()))
                                .useBeamSchema();
                        if(parameters.getAutoSharding()) {
                            write = write.withAutoSharding();
                        } else if(parameters.getNumStorageWriteApiStreams() != null) {
                            write = write.withNumStorageWriteApiStreams(parameters.getNumStorageWriteApiStreams());
                        }
                        break;
                    }
                    case STORAGE_API_AT_LEAST_ONCE: {
                        write = write.useBeamSchema();
                        break;
                    }
                    case DEFAULT:
                    case STREAMING_INSERTS: {
                        write = write.withFormatFunction(convertTableRowFunction);
                        if(parameters.getAutoSharding()) {
                            write = write.withAutoSharding();
                        }
                        break;
                    }
                }

            } else {
                // For batch mode options
                if(AvroSchemaUtil.isNestedSchema(collection.getAvroSchema())) {
                    LOG.info("BigQuerySink: TableRowWrite mode.");
                    write = write
                            .withFormatFunction(convertTableRowFunction);
                } else {
                    LOG.info("BigQuerySink: AvroWrite mode.");
                    switch (parameters.getMethod()) {
                        case FILE_LOADS:
                            write = write
                                    .withAvroSchemaFactory(TableRowToRecordConverter::convertSchema)
                                    .withAvroFormatFunction(convertAvroFunction)
                                    .useAvroLogicalTypes();
                            break;
                        case STORAGE_WRITE_API:
                        case STORAGE_API_AT_LEAST_ONCE: {
                            write = write.useBeamSchema();
                            if(parameters.getAutoSharding()) {
                                write = write.withAutoSharding();
                            } else if(parameters.getNumStorageWriteApiStreams() != null) {
                                write = write.withNumStorageWriteApiStreams(parameters.getNumStorageWriteApiStreams());
                            }
                            break;
                        }
                        case DEFAULT:
                        case STREAMING_INSERTS: {
                            write = write.withFormatFunction(convertTableRowFunction);
                            if(parameters.getAutoSharding()) {
                                write = write.withAutoSharding();
                            }
                            break;
                        }
                    }

                }

                if(parameters.getSchemaUpdateOptions() != null && parameters.getSchemaUpdateOptions().size() > 0) {
                    write = write.withSchemaUpdateOptions(new HashSet<>(parameters.getSchemaUpdateOptions()));
                }
            }

            return write;
        }

        private static class ToRowDoFn<T> extends DoFn<T, Row> {

            private final org.apache.beam.sdk.schemas.Schema schema;
            private final RowConverter<T> rowConverter;

            ToRowDoFn(final org.apache.beam.sdk.schemas.Schema schema, final RowConverter<T> rowConverter) {
                this.schema = schema;
                this.rowConverter = rowConverter;
            }

            @ProcessElement
            public void processElement(ProcessContext c) {
                final T element = c.element();
                final Row row = rowConverter.convert(schema, element);
                c.output(row);
            }

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

    private interface RowConverter<T> extends Serializable {
        Row convert(final org.apache.beam.sdk.schemas.Schema schema, final T element);
    }

}
