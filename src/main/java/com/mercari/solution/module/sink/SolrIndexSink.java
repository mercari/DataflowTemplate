package com.mercari.solution.module.sink;

import com.google.cloud.spanner.Struct;
import com.google.datastore.v1.Entity;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.mercari.solution.config.SinkConfig;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.module.SinkModule;
import com.mercari.solution.module.sink.fileio.SolrSink;
import com.mercari.solution.util.TemplateFileNaming;
import com.mercari.solution.util.XmlUtil;
import com.mercari.solution.util.schema.*;
import com.mercari.solution.util.converter.EntityToSolrDocumentConverter;
import com.mercari.solution.util.converter.RecordToSolrDocumentConverter;
import com.mercari.solution.util.converter.RowToSolrDocumentConverter;
import com.mercari.solution.util.converter.StructToSolrDocumentConverter;
import com.mercari.solution.util.gcp.StorageUtil;
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
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;


public class SolrIndexSink implements SinkModule {

    private static final Logger LOG = LoggerFactory.getLogger(SolrIndexSink.class);

    private class SolrIndexSinkParameters implements Serializable {

        private String output;
        private String coreName;
        private SolrSchemaUtil.SolrConfig solrconfig;
        private JsonElement indexSchema;
        private List<CustomConfigFileParameters> customConfigFiles;

        private List<String> groupFields;
        private String tempDirectory;

        public String getOutput() {
            return output;
        }

        public void setOutput(String output) {
            this.output = output;
        }

        public String getCoreName() {
            return coreName;
        }

        public void setCoreName(String coreName) {
            this.coreName = coreName;
        }

        public SolrSchemaUtil.SolrConfig getSolrconfig() {
            return solrconfig;
        }

        public void setSolrconfig(SolrSchemaUtil.SolrConfig solrconfig) {
            this.solrconfig = solrconfig;
        }

        public JsonElement getIndexSchema() {
            return indexSchema;
        }

        public void setIndexSchema(JsonElement indexSchema) {
            this.indexSchema = indexSchema;
        }

        public List<CustomConfigFileParameters> getCustomConfigFiles() {
            return customConfigFiles;
        }

        public void setCustomConfigFiles(List<CustomConfigFileParameters> customConfigFiles) {
            this.customConfigFiles = customConfigFiles;
        }

        public List<String> getGroupFields() {
            return groupFields;
        }

        public void setGroupFields(List<String> groupFields) {
            this.groupFields = groupFields;
        }

        public String getTempDirectory() {
            return tempDirectory;
        }

        public void setTempDirectory(String tempDirectory) {
            this.tempDirectory = tempDirectory;
        }


        public void validate() {
            final List<String> errorMessages = new ArrayList<>();
            if(this.coreName == null) {
                errorMessages.add("Solrindex source module requires `coreName` parameter.");
            }
            if(this.solrconfig != null) {
                errorMessages.addAll(this.solrconfig.validate());
            }
            if(this.getCustomConfigFiles() != null) {
                for(int i=0; i<this.customConfigFiles.size(); i++) {
                    errorMessages.addAll(this.customConfigFiles.get(i).validate(i));
                }
            }

            if(errorMessages.size() > 0) {
                throw new IllegalArgumentException(errorMessages.stream().collect(Collectors.joining(", ")));
            }
        }

        public void setDefaults() {

            if(this.solrconfig == null) {
                this.solrconfig = SolrSchemaUtil.createSolrConfig();
            }

            if(this.customConfigFiles == null) {
                this.customConfigFiles = new ArrayList<>();
            } else {
                for(final CustomConfigFileParameters customConfigFileParameter : this.customConfigFiles) {
                    customConfigFileParameter.setDefaults();
                }
            }

            if(this.groupFields == null) {
                this.groupFields = new ArrayList<>();
            }
        }

    }

    private class CustomConfigFileParameters implements Serializable {

        private String input;
        private String filename;

        public String getInput() {
            return input;
        }

        public void setInput(String input) {
            this.input = input;
        }

        public String getFilename() {
            return filename;
        }

        public void setFilename(String filename) {
            this.filename = filename;
        }

        public List<String> validate(int index) {
            final List<String> errorMessages = new ArrayList<>();
            if(this.input == null) {
                errorMessages.add("solrindex.customConfigFiles[" + index + "].input parameter must not be null.");
            }
            if(this.filename == null) {
                errorMessages.add("solrindex.customConfigFiles[" + index + "].filename parameter must not be null.");
            }

            return errorMessages;
        }

        public void setDefaults() {

        }
    }


    public String getName() { return "solrindex"; }


    public Map<String, FCollection<?>> expand(FCollection<?> input, SinkConfig config, List<FCollection<?>> waits, List<FCollection<?>> sideInputs) {
        return Collections.singletonMap(config.getName(), SolrIndexSink.write(input, config, waits, sideInputs));
    }

    public static FCollection<?> write(
            final FCollection<?> collection,
            final SinkConfig config,
            final List<FCollection<?>> waits,
            final List<FCollection<?>> sideInputs) {

        final SolrIndexSinkParameters parameters = new Gson().fromJson(config.getParameters(), SolrIndexSinkParameters.class);
        if(parameters == null) {
            throw new IllegalArgumentException("SolrIndexSink parameters must not be empty!");
        }
        parameters.setDefaults();


        PCollectionTuple inputs = PCollectionTuple.of(collection.getName(), collection.getCollection());
        final PCollection output = inputs.apply(config.getName(), new SolrWrite(collection, parameters, waits));
        final FCollection<?> fcollection = FCollection.update(collection, output);
        try {
            config.outputAvroSchema(collection.getAvroSchema());
        } catch (Exception e) {
            LOG.error("Failed to output avro schema for " + config.getName() + " to path: " + config.getOutputAvroSchema(), e);
        }
        return fcollection;
    }

    public static class SolrWrite extends PTransform<PCollectionTuple, PCollection<KV>> {

        private FCollection<?> collection;
        private final SolrIndexSinkParameters parameters;
        private final List<FCollection<?>> waits;

        private final String schemaXml;
        private final String solrconfigXml;

        private SolrWrite(final FCollection<?> collection,
                          final SolrIndexSinkParameters parameters,
                          final List<FCollection<?>> waits) {

            this.collection = collection;
            this.parameters = parameters;

            this.waits = waits;

            if (parameters.getIndexSchema() == null) {
                schemaXml = RecordToSolrDocumentConverter.convertSchema(collection.getAvroSchema());
            } else if (parameters.getIndexSchema().isJsonPrimitive() && parameters.getIndexSchema().getAsJsonPrimitive().isString()) {
                schemaXml = StorageUtil.readString(parameters.getIndexSchema().getAsString());
            } else if (parameters.getIndexSchema().isJsonObject()) {
                schemaXml = RecordToSolrDocumentConverter.convertSchema(collection.getAvroSchema());
            } else {
                schemaXml = RecordToSolrDocumentConverter.convertSchema(collection.getAvroSchema());
            }

            final Document solrconfig = parameters.getSolrconfig().createDocument();
            this.solrconfigXml = XmlUtil.toString(solrconfig);

            LOG.info("Create solrconfig.xml: " + solrconfigXml);
            LOG.info("Create schema.xml: " + schemaXml);
        }

        public PCollection<KV> expand(final PCollectionTuple inputs) {

            final PCollection<?> input;
            final List<KV<String,String>> customConfigFilePaths = new ArrayList<>();
            if(parameters.getCustomConfigFiles().size() > 0) {
                for(final CustomConfigFileParameters customConfigFile : parameters.getCustomConfigFiles()) {
                    customConfigFilePaths.add(KV.of(customConfigFile.getFilename(), customConfigFile.getInput()));
                }
            }

            if((waits == null || waits.size() == 0)) {
                input = inputs.get(collection.getName());
            } else {
                final List<PCollection<?>> wait = new ArrayList<>();
                if(waits != null && waits.size() > 0) {
                    wait.addAll(waits.stream()
                            .map(FCollection::getCollection)
                            .collect(Collectors.toList()));
                }
                input = inputs.get(collection.getName())
                        .apply("Wait", Wait.on(wait))
                        .setCoder((Coder)(inputs.get(collection.getName()).getCoder()));
            }

            final String name = "WriteIndexFile";
            final List<String> groupFields = this.parameters.getGroupFields();
            WriteFilesResult writeResult;
            switch (collection.getDataType()) {
                case AVRO: {
                    final FileIO.Write<String, GenericRecord> write = createWrite(
                            parameters, SchemaUtil.createGroupKeysFunction(AvroSchemaUtil::getAsString, groupFields));
                    writeResult = ((PCollection<GenericRecord>)input)
                            .apply(name, write.via(SolrSink
                                    .of(parameters.getCoreName(), schemaXml, solrconfigXml, customConfigFilePaths, RecordToSolrDocumentConverter::convert)));
                    break;
                }
                case ROW: {
                    final FileIO.Write<String, Row> write = createWrite(
                            parameters, SchemaUtil.createGroupKeysFunction(RowSchemaUtil::getAsString, groupFields));
                    writeResult = ((PCollection<Row>)input)
                            .apply(name, write.via(SolrSink
                                    .of(parameters.getCoreName(), schemaXml, solrconfigXml, customConfigFilePaths, RowToSolrDocumentConverter::convert)));
                    break;
                }
                case STRUCT: {
                    final FileIO.Write<String, Struct> write = createWrite(
                            parameters, SchemaUtil.createGroupKeysFunction(StructSchemaUtil::getAsString, groupFields));
                    writeResult = ((PCollection<Struct>)input)
                            .apply(name, write.via(SolrSink
                                    .of(parameters.getCoreName(), schemaXml, solrconfigXml, customConfigFilePaths, StructToSolrDocumentConverter::convert)));
                    break;
                }
                case ENTITY: {
                    final FileIO.Write<String, Entity> write = createWrite(
                            parameters, SchemaUtil.createGroupKeysFunction(EntitySchemaUtil::getAsString, groupFields));
                    writeResult = ((PCollection<Entity>)input)
                            .apply(name, write.via(SolrSink
                                    .of(parameters.getCoreName(), schemaXml, solrconfigXml, customConfigFilePaths, EntityToSolrDocumentConverter::convert)));
                    break;
                }
                default:
                    throw new IllegalArgumentException("Solr not supported input type: " + collection.getDataType());
            }

            return writeResult.getPerDestinationOutputFilenames();
        }

        private <InputT> FileIO.Write<String, InputT> createWrite(
                final SolrIndexSinkParameters parameters,
                final SerializableFunction<InputT, String> destinationFunction) {

            final String output = parameters.getOutput();

            FileIO.Write<String, InputT> write;
            if(parameters.getGroupFields().size() > 0) {
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

            final String filename = StorageUtil.addFilePrefix(output, "");
            write = write
                    .withNumShards(1)
                    .withNoSpilling()
                    .withNaming(key -> TemplateFileNaming.of(filename, key));

            if(parameters.getTempDirectory() != null) {
                write = write.withTempDirectory(parameters.getTempDirectory());
            }

            return write;
        }

    }

}
