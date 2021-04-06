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
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.gcp.bigquery.*;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;


public class BigQuerySink implements SinkModule {

    private static final Logger LOG = LoggerFactory.getLogger(BigQuerySink.class);

    private class BigQuerySinkParameters {

        private String table;
        private String writeDisposition;
        private String createDisposition;
        private String method;

        private String partitioning;
        private String partitioningField;
        private String clustering;

        private Boolean skipInvalidRows;
        private Boolean ignoreUnknownValues;
        private Boolean ignoreInsertIds;
        private Boolean withExtendedErrorInfo;
        private FailedInsertRetryPolicy failedInsertRetryPolicy;

        private String dynamicDestination;

        private String kmsKey;

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

        public String getMethod() {
            return method;
        }

        public void setMethod(String method) {
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

        public String getKmsKey() {
            return kmsKey;
        }

        public void setKmsKey(String kmsKey) {
            this.kmsKey = kmsKey;
        }

    }

    private enum FailedInsertRetryPolicy {
        always,
        never,
        retryTransientErrors
    }

    public String getName() { return "bigquery"; }

    public Map<String, FCollection<?>> expand(FCollection<?> input, SinkConfig config, List<FCollection<?>> waits) {
        final Map<String, FCollection<?>> outputs = new HashMap<>();
        final FCollection<?> output = BigQuerySink.write(input, config, waits);
        outputs.put(config.getName(), output);
        final String failuresName = config.getName() + ".failures";
        outputs.put(failuresName, FCollection.of(failuresName, output.getCollection(), output.getDataType(), output.getAvroSchema()));
        return outputs;
    }

    public static FCollection<?> write(final FCollection<?> collection, final SinkConfig config) {
        return write(collection, config, null);
    }

    public static FCollection<?> write(final FCollection<?> collection,
                                       final SinkConfig config,
                                       final List<FCollection<?>> waitCollections) {

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
                        StructToTableRowConverter::convert,
                        s -> s.isNull(destinationField) ? "" : s.getString(destinationField),
                        waitCollections);
                final PCollection<Struct> input = (PCollection<Struct>) collection.getCollection();
                final PCollection<GenericRecord> output = input.apply(config.getName(), write);
                return FCollection.of(config.getName(), output, DataType.AVRO, ((AvroCoder)output.getCoder()).getSchema());
            }
            case ENTITY: {
                final BigQueryWrite<Entity> write = new BigQueryWrite<>(
                        config.getName(),
                        collection,
                        parameters,
                        EntityToRecordConverter::convert,
                        EntityToTableRowConverter::convert,
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
        private final SerializableFunction<T, TableRow> convertTableRowFunction;
        private final SerializableFunction<T, String> destinationFunction;
        private final List<FCollection<?>> waitCollections;

        private FCollection<?> collection;

        private BigQueryWrite(final String name,
                              final FCollection<?> collection,
                              final BigQuerySinkParameters parameters,
                              final SerializableFunction<AvroWriteRequest<T>, GenericRecord> convertAvroFunction,
                              final SerializableFunction<T, TableRow> convertTableRowFunction,
                              final SerializableFunction<T, String> destinationFunction,
                              final List<FCollection<?>> waitCollections) {

            this.name = name;
            this.collection = collection;
            this.parameters = parameters;
            this.convertAvroFunction = convertAvroFunction;
            this.convertTableRowFunction = convertTableRowFunction;
            this.destinationFunction = destinationFunction;
            this.waitCollections = waitCollections;
        }

        public PCollection<GenericRecord> expand(final PCollection<T> rows) {
            validateParameters();
            setDefaultParameters();

            final String table = this.parameters.getTable();
            final TableSchema tableSchema = collection.getTableSchema();

            BigQueryIO.Write<T> write = BigQueryIO.<T>write()
                    .withSchema(tableSchema)
                    .withTableDescription("Auto Generated");

            if(this.parameters.getDynamicDestination() != null) {
                write = write.to(new DynamicDestinationFunc<>(table, tableSchema, this.parameters.getDynamicDestination(), destinationFunction));
            } else {
                write = write.to(table);
            }

            if(this.parameters.getPartitioning() != null) {
                final String partitioningType = this.parameters.getPartitioning().trim();
                final String partitioningField = this.parameters.getPartitioningField();
                final TimePartitioning timePartitioning = new TimePartitioning().setType(partitioningType).setField(partitioningField);
                write = write.withTimePartitioning(timePartitioning);
            }

            write = write.withWriteDisposition(BigQueryIO.Write.WriteDisposition.valueOf(this.parameters.getWriteDisposition()));
            write = write.withCreateDisposition(BigQueryIO.Write.CreateDisposition.valueOf(this.parameters.getCreateDisposition()));
            write = write.withMethod(BigQueryIO.Write.Method.valueOf(this.parameters.getMethod()));

            if(this.parameters.getClustering() != null) {
                final List<String> clusteringFields = Arrays.asList(this.parameters.getClustering().trim().split(","));
                final Clustering clustering = new Clustering().setFields(clusteringFields);
                write = write.withClustering(clustering);
            }
            if(this.parameters.getKmsKey() != null) {
                write = write.withKmsKey(this.parameters.getKmsKey().trim());
            }

            if(rows.getPipeline().getOptions().as(DataflowPipelineOptions.class).isStreaming()) {
                LOG.info("BigQuerySink: TableRowWrite mode.");
                write = write
                        .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
                        .withFormatFunction(convertTableRowFunction);
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
                if(parameters.getFailedInsertRetryPolicy().equals(FailedInsertRetryPolicy.always)) {
                    write = write.withFailedInsertRetryPolicy(InsertRetryPolicy.alwaysRetry());
                } else if(parameters.getFailedInsertRetryPolicy().equals(FailedInsertRetryPolicy.never)) {
                    write = write.withFailedInsertRetryPolicy(InsertRetryPolicy.neverRetry());
                } else {
                    write = write.withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors());
                }
            } else {
                if(AvroSchemaUtil.isNestedSchema(collection.getAvroSchema())) {
                    LOG.info("BigQuerySink: TableRowWrite mode.");
                    write = write
                            .withFormatFunction(convertTableRowFunction);
                } else {
                    LOG.info("BigQuerySink: AvroWrite mode.");
                    write = write
                            .withAvroSchemaFactory(TableRowToRecordConverter::convertSchema)
                            .withAvroFormatFunction(convertAvroFunction)
                            .useAvroLogicalTypes();
                }
            }

            final WriteResult writeResult;
            if(waitCollections == null) {
                writeResult = rows.apply("WriteTable", write);
            } else {
                final List<PCollection<?>> waits = waitCollections.stream()
                        .map(f -> f.getCollection())
                        .collect(Collectors.toList());
                writeResult = rows
                        .apply("Wait", Wait.on(waits))
                        .setCoder(rows.getCoder())
                        .apply("WriteTable", write);
            }

            if(rows.getPipeline().getOptions().as(DataflowPipelineOptions.class).isStreaming()
                    && parameters.getWithExtendedErrorInfo()) {

                return writeResult.getFailedInsertsWithErr()
                        .apply("ConvertFailureRecordWithError", ParDo.of(new FailedRecordWithErrorDoFn(name, collection.getAvroSchema().toString())))
                        .setCoder(AvroCoder.of(FailedRecordWithErrorDoFn.createOutputSchema(collection.getAvroSchema())));
            } else {
                return writeResult.getFailedInserts()
                        .apply("ConvertFailureRecord", ParDo.of(new FailedRecordDoFn(name, collection.getAvroSchema().toString())))
                        .setCoder(AvroCoder.of(collection.getAvroSchema()));
            }
        }

        private void validateParameters() {
            if(this.parameters.getTable() == null) {
                throw new IllegalArgumentException("BigQuery output module requires table parameter!");
            }
        }

        private void setDefaultParameters() {
            if(this.parameters.getWriteDisposition() == null) {
                this.parameters.setWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_EMPTY.name());
            } else {
                this.parameters.setWriteDisposition(this.parameters.getWriteDisposition().trim().toUpperCase());
            }

            if(this.parameters.getCreateDisposition() == null) {
                this.parameters.setCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER.name());
            } else {
                this.parameters.setCreateDisposition(this.parameters.getCreateDisposition().trim().toUpperCase());
            }

            if(this.parameters.getMethod() == null) {
                this.parameters.setMethod(BigQueryIO.Write.Method.DEFAULT.name());
            } else {
                this.parameters.setMethod(this.parameters.getMethod().trim().toUpperCase());
            }

            if(parameters.getSkipInvalidRows() == null) {
                parameters.setSkipInvalidRows(false);
            }
            if(parameters.getIgnoreUnknownValues() == null) {
                parameters.setIgnoreUnknownValues(false);
            }
            if(parameters.getIgnoreInsertIds() == null) {
                parameters.setIgnoreInsertIds(false);
            }
            if(parameters.getWithExtendedErrorInfo() == null) {
                parameters.setWithExtendedErrorInfo(false);
            }
            if(parameters.getFailedInsertRetryPolicy() == null) {
                parameters.setFailedInsertRetryPolicy(FailedInsertRetryPolicy.retryTransientErrors);
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


}
