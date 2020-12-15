package com.mercari.solution.module.sink;

import com.google.api.services.bigquery.model.Clustering;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TimePartitioning;
import com.google.cloud.spanner.Struct;
import com.google.datastore.v1.Entity;
import com.google.gson.Gson;
import com.mercari.solution.config.SinkConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.util.AvroSchemaUtil;
import com.mercari.solution.util.OptionUtil;
import com.mercari.solution.util.converter.*;
import com.mercari.solution.util.gcp.DatastoreUtil;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.io.gcp.bigquery.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;


public class BigQuerySink {

    private static final Logger LOG = LoggerFactory.getLogger(BigQuerySink.class);

    private class BigQuerySinkParameters {

        private String table;
        private String partitioning;
        private String partitioningField;
        private String dynamicDestination;
        private String writeDisposition;
        private String createDisposition;
        private String method;
        private String clustering;
        private String kmsKey;
        private String outputError;

        public String getTable() {
            return table;
        }

        public void setTable(String table) {
            this.table = table;
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

        public String getClustering() {
            return clustering;
        }

        public void setClustering(String clustering) {
            this.clustering = clustering;
        }

        public String getKmsKey() {
            return kmsKey;
        }

        public void setKmsKey(String kmsKey) {
            this.kmsKey = kmsKey;
        }

        public String getOutputError() {
            return outputError;
        }

        public void setOutputError(String outputError) {
            this.outputError = outputError;
        }
    }

    public static FCollection<?> write(final FCollection<?> collection, final SinkConfig config) {
        return write(collection, config, null);
    }

    public static FCollection<?> write(final FCollection<?> collection, final SinkConfig config,
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
                        collection,
                        parameters,
                        AvroWriteRequest::getElement,
                        RecordToTableRowConverter::convert,
                        s -> s.get(destinationField) == null ? "" : s.get(destinationField).toString(),
                        waitCollections);
                final PCollection<GenericRecord> input = (PCollection<GenericRecord>) collection.getCollection();
                final PCollection output = input.apply(config.getName(), write);
                return FCollection.update(write.collection, output);
            }
            case ROW: {
                final BigQueryWrite<Row> write = new BigQueryWrite<>(
                        collection,
                        parameters,
                        RowToRecordConverter::convert,
                        RowToTableRowConverter::convert,
                        s -> s.getValue(destinationField) == null ? "" : s.getValue(destinationField).toString(),
                        waitCollections);
                final PCollection<Row> input = (PCollection<Row>) collection.getCollection();
                final PCollection output = input.apply(config.getName(), write);
                return FCollection.update(write.collection, output);
            }
            case STRUCT: {
                final BigQueryWrite<Struct> write = new BigQueryWrite<>(
                        collection,
                        parameters,
                        StructToRecordConverter::convert,
                        StructToTableRowConverter::convert,
                        s -> s.isNull(destinationField) ? "" : s.getString(destinationField),
                        waitCollections);
                final PCollection<Struct> input = (PCollection<Struct>) collection.getCollection();
                final PCollection output = input.apply(config.getName(), write);
                return FCollection.update(write.collection, output);
            }
            case ENTITY: {
                final BigQueryWrite<Entity> write = new BigQueryWrite<>(
                        collection,
                        parameters,
                        EntityToRecordConverter::convert,
                        EntityToTableRowConverter::convert,
                        s -> OptionUtil.ifnull(DatastoreUtil.getAsString(s.getPropertiesOrDefault(destinationField, null)), ""),
                        waitCollections);
                final PCollection<Entity> input = (PCollection<Entity>) collection.getCollection();
                final PCollection output = input.apply(config.getName(), write);
                return FCollection.update(write.collection, output);
            }
            default:
                throw new IllegalArgumentException("Not supported type: " + inputType + " for BigQuerySink.");
        }
    }

    public static class BigQueryWrite<T> extends PTransform<PCollection<T>, PCollection<TableRow>> {

        private final BigQuerySinkParameters parameters;
        private final SerializableFunction<AvroWriteRequest<T>, GenericRecord> convertAvroFunction;
        private final SerializableFunction<T, TableRow> convertTableRowFunction;
        private final SerializableFunction<T, String> destinationFunction;
        private final List<FCollection<?>> waitCollections;

        private FCollection<?> collection;

        private BigQueryWrite(final FCollection<?> collection,
                              final BigQuerySinkParameters parameters,
                              final SerializableFunction<AvroWriteRequest<T>, GenericRecord> convertAvroFunction,
                              final SerializableFunction<T, TableRow> convertTableRowFunction,
                              final SerializableFunction<T, String> destinationFunction,
                              final List<FCollection<?>> waitCollections) {

            this.collection = collection;
            this.parameters = parameters;
            this.convertAvroFunction = convertAvroFunction;
            this.convertTableRowFunction = convertTableRowFunction;
            this.destinationFunction = destinationFunction;
            this.waitCollections = waitCollections;
        }

        public PCollection<TableRow> expand(final PCollection<T> rows) {
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
            if(this.parameters.getOutputError() != null) {
                write = write.withExtendedErrorInfo();
            }

            if(rows.getPipeline().getOptions().as(DataflowPipelineOptions.class).isStreaming()) {
                LOG.info("BigQuerySink: TableRowWrite mode.");
                write = write
                        .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
                        .withFormatFunction(convertTableRowFunction);
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

            if(this.parameters.getOutputError() != null) {
                writeResult.getFailedInsertsWithErr()
                        .apply("OutputError", ParDo.of(new DoFn<BigQueryInsertError, String>() {
                            @ProcessElement
                            public void processElement(ProcessContext c) {
                                //c.element().getRow()
                            }
                        }));
            }

            //return rows.getPipeline()
            //        .apply(Create.of("wait").withCoder(StringUtf8Coder.of()))
            //        .apply(Wait.on(writeResult.getFailedInserts()));
            return writeResult.getFailedInserts();
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
