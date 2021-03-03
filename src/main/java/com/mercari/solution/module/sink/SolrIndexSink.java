package com.mercari.solution.module.sink;

import com.google.cloud.spanner.Struct;
import com.google.datastore.v1.Entity;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.mercari.solution.config.SinkConfig;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.module.SinkModule;
import com.mercari.solution.module.sink.fileio.SolrSink;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.FixedFileNaming;
import com.mercari.solution.util.schema.EntitySchemaUtil;
import com.mercari.solution.util.schema.RowSchemaUtil;
import com.mercari.solution.util.converter.EntityToSolrDocumentConverter;
import com.mercari.solution.util.converter.RecordToSolrDocumentConverter;
import com.mercari.solution.util.converter.RowToSolrDocumentConverter;
import com.mercari.solution.util.converter.StructToSolrDocumentConverter;
import com.mercari.solution.util.gcp.StorageUtil;
import com.mercari.solution.util.schema.StructSchemaUtil;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.WriteFilesResult;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class SolrIndexSink implements SinkModule {

    private static final Logger LOG = LoggerFactory.getLogger(SolrIndexSink.class);

    private class SolrIndexSinkParameters implements Serializable {

        private String output;
        private String dynamicSplitField;
        private String tempDirectory;

        private String datetimeFormat;
        private String datetimeFormatZone;
        private Boolean useOnlyEndDatetime;

        private String coreName;
        private JsonElement indexSchema;
        private JsonElement config;

        private String outputSchema;

        public String getOutput() {
            return output;
        }

        public void setOutput(String output) {
            this.output = output;
        }

        public String getDynamicSplitField() {
            return dynamicSplitField;
        }

        public void setDynamicSplitField(String dynamicSplitField) {
            this.dynamicSplitField = dynamicSplitField;
        }

        public String getTempDirectory() {
            return tempDirectory;
        }

        public void setTempDirectory(String tempDirectory) {
            this.tempDirectory = tempDirectory;
        }

        public String getDatetimeFormat() {
            return datetimeFormat;
        }

        public void setDatetimeFormat(String datetimeFormat) {
            this.datetimeFormat = datetimeFormat;
        }

        public String getDatetimeFormatZone() {
            return datetimeFormatZone;
        }

        public void setDatetimeFormatZone(String datetimeFormatZone) {
            this.datetimeFormatZone = datetimeFormatZone;
        }

        public Boolean getUseOnlyEndDatetime() {
            return useOnlyEndDatetime;
        }

        public void setUseOnlyEndDatetime(Boolean useOnlyEndDatetime) {
            this.useOnlyEndDatetime = useOnlyEndDatetime;
        }

        public String getCoreName() {
            return coreName;
        }

        public void setCoreName(String coreName) {
            this.coreName = coreName;
        }

        public JsonElement getIndexSchema() {
            return indexSchema;
        }

        public void setIndexSchema(JsonElement indexSchema) {
            this.indexSchema = indexSchema;
        }

        public String getOutputSchema() {
            return outputSchema;
        }

        public void setOutputSchema(String outputSchema) {
            this.outputSchema = outputSchema;
        }

    }

    public String getName() { return "solrindex"; }

    public Map<String, FCollection<?>> expand(FCollection<?> input, SinkConfig config, List<FCollection<?>> waits) {
        return Collections.singletonMap(config.getName(), SolrIndexSink.write(input, config, waits));
    }

    public static FCollection<?> write(final FCollection<?> collection, final SinkConfig config) {
        return write(collection, config, null);
    }

    public static FCollection<?> write(final FCollection<?> collection, final SinkConfig config, final List<FCollection<?>> waits) {
        final SolrIndexSinkParameters parameters = new Gson().fromJson(config.getParameters(), SolrIndexSinkParameters.class);
        final SolrWrite write = new SolrWrite(collection, parameters, waits);
        final PCollection output = collection.getCollection().apply(config.getName(), write);
        final FCollection<?> fcollection = FCollection.update(collection, output);
        try {
            config.outputAvroSchema(collection.getAvroSchema());
        } catch (Exception e) {
            LOG.error("Failed to output avro schema for " + config.getName() + " to path: " + config.getOutputAvroSchema(), e);
        }
        return fcollection;
    }

    public static class SolrWrite extends PTransform<PCollection<?>, PCollection<KV>> {

        private FCollection<?> collection;
        private final SolrIndexSinkParameters parameters;
        private final List<FCollection<?>> waits;

        private SolrWrite(final FCollection<?> collection,
                             final SolrIndexSinkParameters parameters,
                             final List<FCollection<?>> waits) {
            this.collection = collection;
            this.parameters = parameters;
            this.waits = waits;
        }

        public PCollection<KV> expand(final PCollection<?> inputP) {

            validateParameters();
            setDefaultParameters();

            final PCollection<?> input;
            if(waits == null || waits.size() == 0) {
                input = inputP;
            } else {
                final List<PCollection<?>> wait = waits.stream()
                        .map(FCollection::getCollection)
                        .collect(Collectors.toList());
                input = inputP
                        .apply("Wait", Wait.on(wait))
                        .setCoder((Coder)inputP.getCoder());
            }

            final String destinationField = this.parameters.getDynamicSplitField();
            final String schemaString;
            WriteFilesResult writeResult;
            switch (collection.getDataType()) {
                case AVRO: {
                    final FileIO.Write<String, GenericRecord> write = createWrite(
                            parameters, e -> AvroSchemaUtil.getAsString(e, destinationField));
                    if(parameters.getIndexSchema() == null) {
                        schemaString = RecordToSolrDocumentConverter.convertSchema(collection.getAvroSchema());
                    } else if(parameters.getIndexSchema().isJsonPrimitive() && parameters.getIndexSchema().getAsJsonPrimitive().isString()) {
                        schemaString = StorageUtil.readString(parameters.getIndexSchema().getAsString());
                    } else if(parameters.getIndexSchema().isJsonObject()) {
                        schemaString = RecordToSolrDocumentConverter.convertSchema(collection.getAvroSchema());
                    } else {
                        schemaString = RecordToSolrDocumentConverter.convertSchema(collection.getAvroSchema());
                    }
                    writeResult = ((PCollection<GenericRecord>)input)
                            .apply(write.via(SolrSink
                                    .of(parameters.getCoreName(), schemaString, RecordToSolrDocumentConverter::convert)));
                    break;
                }
                case ROW: {
                    final FileIO.Write<String, Row> write = createWrite(
                            parameters, e -> RowSchemaUtil.getAsString(e, destinationField));
                    if(parameters.getIndexSchema() == null) {
                        schemaString = RecordToSolrDocumentConverter.convertSchema(collection.getAvroSchema());
                    } else if(parameters.getIndexSchema().isJsonPrimitive() && parameters.getIndexSchema().getAsJsonPrimitive().isString()) {
                        schemaString = StorageUtil.readString(parameters.getIndexSchema().getAsString());
                    } else if(parameters.getIndexSchema().isJsonObject()) {
                        schemaString = RecordToSolrDocumentConverter.convertSchema(collection.getAvroSchema());
                    } else {
                        schemaString = RecordToSolrDocumentConverter.convertSchema(collection.getAvroSchema());
                    }
                    writeResult = ((PCollection<Row>)input)
                            .apply(write.via(SolrSink
                                    .of(parameters.getCoreName(), schemaString, RowToSolrDocumentConverter::convert)));
                    break;
                }
                case STRUCT: {
                    final FileIO.Write<String, Struct> write = createWrite(
                            parameters, e -> StructSchemaUtil.getAsString(e, destinationField));
                    if(parameters.getIndexSchema() == null) {
                        schemaString = RecordToSolrDocumentConverter.convertSchema(collection.getAvroSchema());
                    } else if(parameters.getIndexSchema().isJsonPrimitive() && parameters.getIndexSchema().getAsJsonPrimitive().isString()) {
                        schemaString = StorageUtil.readString(parameters.getIndexSchema().getAsString());
                    } else if(parameters.getIndexSchema().isJsonObject()) {
                        schemaString = RecordToSolrDocumentConverter.convertSchema(collection.getAvroSchema());
                    } else {
                        schemaString = RecordToSolrDocumentConverter.convertSchema(collection.getAvroSchema());
                    }
                    writeResult = ((PCollection<Struct>)input)
                            .apply(write.via(SolrSink
                                    .of(parameters.getCoreName(), schemaString, StructToSolrDocumentConverter::convert)));
                    break;
                }
                case ENTITY: {
                    final FileIO.Write<String, Entity> write = createWrite(
                            parameters, e -> EntitySchemaUtil.getAsString(e.getPropertiesOrDefault(destinationField, null)));
                    if(parameters.getIndexSchema() == null) {
                        schemaString = RecordToSolrDocumentConverter.convertSchema(collection.getAvroSchema());
                    } else if(parameters.getIndexSchema().isJsonPrimitive() && parameters.getIndexSchema().getAsJsonPrimitive().isString()) {
                        schemaString = StorageUtil.readString(parameters.getIndexSchema().getAsString());
                    } else if(parameters.getIndexSchema().isJsonObject()) {
                        schemaString = RecordToSolrDocumentConverter.convertSchema(collection.getAvroSchema());
                    } else {
                        schemaString = RecordToSolrDocumentConverter.convertSchema(collection.getAvroSchema());
                    }
                    writeResult = ((PCollection<Entity>)input)
                            .apply(write.via(SolrSink
                                    .of(parameters.getCoreName(), schemaString, EntityToSolrDocumentConverter::convert)));
                    break;
                }
                default:
                    throw new IllegalArgumentException("Solr not supported input type: " + collection.getDataType());
            }

            if(parameters.getOutputSchema() != null && parameters.getOutputSchema().startsWith("gs://")) {
                try {
                    StorageUtil.writeString(parameters.getOutputSchema(), schemaString);
                } catch (IOException e) {
                    throw new IllegalArgumentException("SolrIndex module parameter outputSchema is illegal.", e);
                }
            }

            return writeResult.getPerDestinationOutputFilenames();
        }

        private <InputT> FileIO.Write<String, InputT> createWrite(
                final SolrIndexSinkParameters parameters,
                final SerializableFunction<InputT, String> destinationFunction) {

            final String output = parameters.getOutput();

            FileIO.Write<String, InputT> write;
            if(parameters.getDynamicSplitField() != null) {
                write = FileIO.<String, InputT>writeDynamic()
                        .to(output)
                        .by(d -> Optional.ofNullable(destinationFunction.apply(d)).orElse(""))
                        .withDestinationCoder(StringUtf8Coder.of());
            } else {
                final String outdir = StorageUtil.removeDirSuffix(output);
                write = FileIO.<String, InputT>writeDynamic()
                        .to(outdir)
                        .by(d -> "")
                        .withDestinationCoder(StringUtf8Coder.of());
            }

            write = write.withNumShards(1);
            final String datetimeFormat = parameters.getDatetimeFormat();
            final String datetimeFormatZone = parameters.getDatetimeFormatZone();
            final Boolean useOnlyEndDatetime = parameters.getUseOnlyEndDatetime();
            if(parameters.getDynamicSplitField() != null) {
                write = write
                        .withNaming(key -> FixedFileNaming.of(
                                StorageUtil.addFilePrefix(output, (String)key), "",
                                datetimeFormat, datetimeFormatZone, useOnlyEndDatetime));
            } else {
                write = write
                        .withNaming(key -> FixedFileNaming.of(
                                StorageUtil.addFilePrefix(output, ""), "",
                                datetimeFormat, datetimeFormatZone, useOnlyEndDatetime));
            }


            if(parameters.getTempDirectory() != null) {
                write = write.withTempDirectory(parameters.getTempDirectory());
            }

            return write;
        }

        private void validateParameters() {

        }

        private void setDefaultParameters() {
            if(this.parameters.getDatetimeFormat() == null) {
                this.parameters.setDatetimeFormat("yyyyMMddHHmmss");
            }
            if(this.parameters.getDatetimeFormatZone() == null) {
                this.parameters.setDatetimeFormatZone("Etc/GMT");
            }
            if(this.parameters.getUseOnlyEndDatetime() == null) {
                this.parameters.setUseOnlyEndDatetime(false);
            }
        }

    }

}
