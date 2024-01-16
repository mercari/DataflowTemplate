package com.mercari.solution.module.source;

import com.google.bigtable.v2.RowFilter;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.mercari.solution.config.SourceConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.module.SourceModule;
import com.mercari.solution.util.OptionUtil;
import com.mercari.solution.util.converter.BigtableRowToRecordConverter;
import com.mercari.solution.util.gcp.BigtableUtil;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.SchemaUtil;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.io.range.ByteKeyRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class BigtableSource implements SourceModule {

    private static final Logger LOG = LoggerFactory.getLogger(BigtableSource.class);

    private static class BigtableSourceParameters implements Serializable {

        private String projectId;
        private String instanceId;
        private String tableId;

        // for batch
        private JsonElement rowFilter;
        private JsonElement keyRanges;


        private OutputType outputType;

        public String getProjectId() {
            return projectId;
        }

        public String getInstanceId() {
            return instanceId;
        }

        public String getTableId() {
            return tableId;
        }

        public JsonElement getRowFilter() {
            return rowFilter;
        }

        public JsonElement getKeyRanges() {
            return keyRanges;
        }

        public OutputType getOutputType() {
            return outputType;
        }

        private void validate(final PBegin begin) {

            // check required parameters filled
            final List<String> errorMessages = new ArrayList<>();

            if(this.projectId == null) {
                errorMessages.add("bigtable source module parameter projectId must not be null");
            }
            if(this.instanceId == null) {
                errorMessages.add("bigtable source module parameter instanceId must not be null");
            }
            if(this.tableId == null) {
                errorMessages.add("bigtable source module parameter tableId must not be null");
            }

            if (errorMessages.size() > 0) {
                throw new IllegalArgumentException(errorMessages.stream().collect(Collectors.joining(", ")));
            }
        }

        private void setDefaults() {
            if(this.outputType == null) {
                this.outputType = OutputType.avro;
            }
        }

    }

    private enum OutputType {
        avro,
        row,
        mutation,
        cells
    }

    public String getName() { return "bigtable"; }

    public Map<String, FCollection<?>> expand(PBegin begin, SourceConfig config, PCollection<Long> beats, List<FCollection<?>> waits) {
        if(OptionUtil.isStreaming(begin.getPipeline().getOptions())) {
            // TODO
            return Collections.emptyMap();
        } else {
            return Collections.singletonMap(config.getName(), batch(begin, config));
        }
    }

    public static FCollection<?> batch(PBegin begin, SourceConfig config) {
        final BigtableSourceParameters parameters = new Gson().fromJson(config.getParameters(), BigtableSourceParameters.class);
        if(parameters == null) {
            throw new IllegalArgumentException("bigtable source module parameters must not be empty!");
        }
        parameters.validate(begin);
        parameters.setDefaults();

        switch (parameters.getOutputType()) {
            case avro: {
                final org.apache.avro.Schema outputAvroSchema = SourceConfig.convertAvroSchema(config.getSchema());
                final BatchSource<String, org.apache.avro.Schema, GenericRecord> source = new BatchSource<>(
                        parameters,
                        outputAvroSchema.toString(),
                        AvroSchemaUtil::convertSchema,
                        BigtableRowToRecordConverter::convert);
                final PCollection<GenericRecord> records = begin
                        .apply(config.getName(), source)
                        .setCoder(AvroCoder.of(outputAvroSchema));
                return FCollection.of(config.getName(), records, DataType.AVRO, outputAvroSchema);
            }
            default:
                throw new IllegalArgumentException("bigtable source module not support format: " + parameters.getOutputType());
        }
    }

    private static class BatchSource<InputSchemaT,RuntimeSchemaT,T> extends PTransform<PBegin, PCollection<T>> {

        private final String projectId;
        private final String instanceId;
        private final String tableId;

        private final List<ByteKeyRange> keyRanges;
        private final RowFilter rowFilter;

        private final InputSchemaT inputSchema;
        private final SchemaUtil.SchemaConverter<InputSchemaT,RuntimeSchemaT> schemaConverter;
        private final SchemaUtil.DataConverter<RuntimeSchemaT, com.google.bigtable.v2.Row, T> converter;

        BatchSource(final BigtableSourceParameters parameters,
                    final InputSchemaT inputSchema,
                    final SchemaUtil.SchemaConverter<InputSchemaT,RuntimeSchemaT> schemaConverter,
                    final SchemaUtil.DataConverter<RuntimeSchemaT, com.google.bigtable.v2.Row, T> converter) {

            this.projectId = parameters.getProjectId();
            this.instanceId = parameters.getInstanceId();
            this.tableId = parameters.getTableId();

            if(parameters.getKeyRanges() == null || parameters.getKeyRanges().isJsonNull()) {
                keyRanges = null;
            } else {
                keyRanges = BigtableUtil.createKeyRanges(parameters.getKeyRanges());
            }

            if(parameters.getRowFilter() == null || parameters.getRowFilter().isJsonNull()) {
                rowFilter = null;
            } else {
                rowFilter = BigtableUtil.createRowFilter(parameters.getRowFilter());
            }

            this.inputSchema = inputSchema;
            this.schemaConverter = schemaConverter;
            this.converter = converter;
        }

        @Override
        public PCollection<T> expand(PBegin begin) {
            BigtableIO.Read read = BigtableIO.read()
                    .withProjectId(projectId)
                    .withInstanceId(instanceId)
                    .withTableId(tableId);

            if(keyRanges != null && keyRanges.size() > 0) {
                read = read.withKeyRanges(keyRanges);
            } else if(rowFilter != null) {
                read = read.withRowFilter(rowFilter);
            }

            final PCollection<com.google.bigtable.v2.Row> rows = begin
                    .apply("ReadBigtable", read);

            return rows.apply("Convert", ParDo.of(new ConvertDoFn(inputSchema, schemaConverter, converter)));
        }

        private class ConvertDoFn extends DoFn<com.google.bigtable.v2.Row, T> {

            private final InputSchemaT inputSchema;
            private final SchemaUtil.SchemaConverter<InputSchemaT,RuntimeSchemaT> schemaConverter;
            private final SchemaUtil.DataConverter<RuntimeSchemaT, com.google.bigtable.v2.Row, T> converter;


            private transient RuntimeSchemaT runtimeSchema;

            ConvertDoFn(final InputSchemaT inputSchema,
                                     final SchemaUtil.SchemaConverter<InputSchemaT,RuntimeSchemaT> schemaConverter,
                                     final SchemaUtil.DataConverter<RuntimeSchemaT, com.google.bigtable.v2.Row, T> converter) {

                this.inputSchema = inputSchema;
                this.schemaConverter = schemaConverter;
                this.converter = converter;
            }

            @Setup
            public void setup() {
                this.runtimeSchema = schemaConverter.convert(inputSchema);
            }

            @ProcessElement
            public void processElement(ProcessContext c) {
                final com.google.bigtable.v2.Row row = c.element();
                final T output = converter.convert(runtimeSchema, row);
                c.output(output);
            }

        }
    }

}
