package com.mercari.solution.module.sink;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.mercari.solution.config.SinkConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.module.SinkModule;
import com.mercari.solution.module.sink.fileio.SolrSink2;
import com.mercari.solution.util.XmlUtil;
import com.mercari.solution.util.converter.*;
import com.mercari.solution.util.domain.search.ZipFileUtil;
import com.mercari.solution.util.gcp.StorageUtil;
import com.mercari.solution.util.pipeline.union.Union;
import com.mercari.solution.util.pipeline.union.UnionValue;
import com.mercari.solution.util.schema.*;
import org.apache.avro.Schema;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.WriteFilesResult;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

public class LocalSolrSink implements SinkModule {

    private static final Logger LOG = LoggerFactory.getLogger(LocalSolrSink.class);

    private static class LocalSolrSinkParameters implements Serializable {

        private String input;
        private String output;
        private List<CoreParameter> cores;
        private List<String> groupFields;
        private String tempDirectory;

        public String getInput() {
            return input;
        }

        public String getOutput() {
            return output;
        }

        public List<CoreParameter> getCores() {
            return cores;
        }

        public List<String> getGroupFields() {
            return groupFields;
        }

        public String getTempDirectory() {
            return tempDirectory;
        }

        public void validate(String name) {
            final List<String> errorMessages = new ArrayList<>();
            if(this.output == null) {
                errorMessages.add("localSolr sink module: " + name + " requires `output` parameter.");
            }
            if(this.cores == null || this.cores.size() == 0) {
                errorMessages.add("localSolr sink module: " + name + " requires `cores` parameter.");
            } else {
                for(int i=0; i<this.cores.size(); i++) {
                    errorMessages.addAll(this.cores.get(i).validate(name, i));
                }
            }

            if(errorMessages.size() > 0) {
                throw new IllegalArgumentException(String.join(", ", errorMessages));
            }
        }

        public void setDefaults() {
            if(this.input == null) {
                this.input = output;
            }
            if(this.groupFields == null) {
                this.groupFields = new ArrayList<>();
            }
            for(final CoreParameter coreParameter : this.cores) {
                coreParameter.setDefaults();
            }
        }

    }

    public static class CoreParameter implements Serializable {

        private String name;
        private String input;
        private JsonElement schema;
        private JsonElement config;
        private List<CustomConfigFileParameter> customConfigFiles;

        public List<String> validate(String moduleName, int i) {
            final List<String> errorMessages = new ArrayList<>();
            if(name == null) {
                errorMessages.add("localSolr sink module: " + moduleName + ".cores[" + i + "] requires `name` parameter.");
            }
            if(input == null) {
                errorMessages.add("localSolr sink module: " + moduleName + ".cores[" + i + "] requires `input` parameter.");
            }
            if(customConfigFiles != null) {
                for(int j=0; j<customConfigFiles.size(); j++) {
                    errorMessages.addAll(customConfigFiles.get(j).validate(i, j));
                }
            }

            return errorMessages;
        }

        public void setDefaults() {
            if(this.customConfigFiles == null) {
                this.customConfigFiles = new ArrayList<>();
            } else {
                for(final CustomConfigFileParameter customConfigFile : customConfigFiles) {
                    customConfigFile.setDefaults();
                }
            }
        }

        public Core toCore(Map<String, String> avroSchemaStrings) {
            final Core core = new Core();
            core.name = name;
            core.input = input;

            // solr schema
            if(schema == null || schema.isJsonNull()) {
                final String avroSchemaString = avroSchemaStrings.get(input);
                final Schema avroSchema = AvroSchemaUtil.convertSchema(avroSchemaString);
                core.schema = RecordToSolrDocumentConverter.convertSchema(avroSchema);
            } else if(schema.isJsonPrimitive()) {
                if(schema.getAsString().replaceAll("\"", "").startsWith("gs://")) {
                    core.schema = StorageUtil.readString(schema.getAsString().replaceAll("\"", ""));
                } else {
                    core.schema = schema.getAsString();
                }
            } else if(schema.isJsonObject()) {
                core.schema = XmlUtil.toString(SolrSchemaUtil.convertIndexSchemaJsonToXml(schema.getAsJsonObject()));
            } else {
                throw new IllegalArgumentException();
            }

            // solr config
            if(config == null || config.isJsonNull()) {
                core.config = XmlUtil.toString(SolrSchemaUtil.createSolrConfig().createDocument());
            } else if(config.isJsonPrimitive()) {
                if(config.getAsString().replaceAll("\"", "").startsWith("gs://")) {
                    core.config = StorageUtil.readString(config.getAsString().replaceAll("\"", ""));
                } else {
                    throw new IllegalArgumentException();
                }
            } else if(config.isJsonObject()) {
                final SolrSchemaUtil.SolrConfig solrConfig = new Gson().fromJson(config.getAsJsonObject(), SolrSchemaUtil.SolrConfig.class);
                core.config = XmlUtil.toString(solrConfig.createDocument());
            } else {
                throw new IllegalArgumentException();
            }

            // custom config files
            final List<KV<String,String>> customConfigFilePaths = new ArrayList<>();
            if(customConfigFiles.size() > 0) {
                for(final CustomConfigFileParameter customConfigFile : customConfigFiles) {
                    customConfigFilePaths.add(KV.of(customConfigFile.getFilename(), customConfigFile.getInput()));
                }
            }
            core.customConfigFiles = customConfigFilePaths;

            return core;
        }
    }

    private static class CustomConfigFileParameter implements Serializable {

        private String input;
        private String filename;

        public String getInput() {
            return input;
        }

        public String getFilename() {
            return filename;
        }

        public List<String> validate(int coreIndex, int index) {
            final List<String> errorMessages = new ArrayList<>();
            if(this.input == null) {
                errorMessages.add("localSolr.cores[" + coreIndex + "].customConfigFiles[" + index + "].input parameter must not be null.");
            }
            return errorMessages;
        }

        public void setDefaults() {
            if(this.filename == null) {
                String[] paths = this.input.split("/");
                this.filename = paths[paths.length - 1];
            }
        }
    }

    public static class Core implements Serializable {

        private String name;
        private String input;
        private String schema;
        private String config;
        private List<String> fields;
        private List<KV<String,String>> customConfigFiles;


        public String getName() {
            return name;
        }

        public String getInput() {
            return input;
        }

        public String getSchema() {
            return schema;
        }

        public String getConfig() {
            return config;
        }

        public List<String> getFields() {
            return fields;
        }

        public List<KV<String,String>> getCustomConfigFiles() {
            return customConfigFiles;
        }
    }


    @Override
    public String getName() { return "localSolr"; }


    @Override
    public Map<String, FCollection<?>> expand(List<FCollection<?>> inputs, SinkConfig config, List<FCollection<?>> waits) {
        if(inputs == null || inputs.size() == 0) {
            throw new IllegalArgumentException("localSolr sink module requires input parameter");
        }

        final LocalSolrSinkParameters parameters = new Gson().fromJson(config.getParameters(), LocalSolrSinkParameters.class);
        if(parameters == null) {
            throw new IllegalArgumentException("localSolr sink parameters must not be empty!");
        }
        parameters.validate(config.getName());
        parameters.setDefaults();

        final List<TupleTag<?>> tags = new ArrayList<>();
        final List<String> inputNames = new ArrayList<>();
        final List<DataType> inputTypes = new ArrayList<>();
        final Map<String, String> avroSchemaStrings = new HashMap<>();

        PCollectionTuple tuple = PCollectionTuple.empty(inputs.get(0).getCollection().getPipeline());
        for(final FCollection<?> input : inputs){
            final TupleTag tag = new TupleTag<>(){};
            tags.add(tag);
            inputNames.add(input.getName());
            inputTypes.add(input.getDataType());
            avroSchemaStrings.put(input.getName(), input.getAvroSchema().toString());

            tuple = tuple.and(tag, input.getCollection());
        }

        final SolrIndexWrite write = new SolrIndexWrite(config.getName(), parameters, tags, inputNames, inputTypes, avroSchemaStrings, waits);
        final PCollection output = tuple.apply(config.getName(), write);
        return Collections.singletonMap(config.getName(), FCollection.of(config.getName(), output, DataType.AVRO, inputs.get(0).getAvroSchema()));
    }

    public static class SolrIndexWrite extends PTransform<PCollectionTuple, PCollection<KV>> {

        private String name;
        private String output;
        private List<String> groupFields;
        private String tempDirectory;
        private List<Core> cores;

        private final List<TupleTag<?>> tags;
        private final List<String> inputNames;
        private final List<DataType> inputTypes;

        private final List<FCollection<?>> waits;

        private SolrIndexWrite(
                final String name,
                final LocalSolrSinkParameters parameters,
                final List<TupleTag<?>> tags,
                final List<String> inputNames,
                final List<DataType> inputTypes,
                final Map<String, String> avroSchemaStrings,
                final List<FCollection<?>> waits) {

            this.name = name;
            this.output = parameters.getOutput();
            this.groupFields = parameters.getGroupFields();
            this.tempDirectory = parameters.getTempDirectory();
            this.cores = parameters.cores.stream()
                    .map(c -> c.toCore(avroSchemaStrings))
                    .collect(Collectors.toList());
            this.tags = tags;
            this.inputNames = inputNames;
            this.inputTypes = inputTypes;
            this.waits = waits;
        }

        public PCollection<KV> expand(final PCollectionTuple inputs) {

            final PCollection<UnionValue> union = inputs
                    .apply("Union", Union.flatten(tags, inputTypes, inputNames));

            final PCollection<UnionValue> input;
            if((waits == null || waits.size() == 0)) {
                input = union;
            } else {
                final List<PCollection<?>> waitsList = waits.stream()
                        .map(FCollection::getCollection)
                        .collect(Collectors.toList());
                input = union
                        .apply("Wait", Wait.on(waitsList))
                        .setCoder(union.getCoder());
            }

            final FileIO.Write<String, UnionValue> write = ZipFileUtil.createSingleFileWrite(
                    output,
                    groupFields,
                    tempDirectory,
                    SchemaUtil.createGroupKeysFunction(UnionValue::getAsString, groupFields));
            final WriteFilesResult writeResult = input
                    .apply("Write", write.via(SolrSink2.of(
                            cores,
                            inputNames)));

            return writeResult.getPerDestinationOutputFilenames();
        }
    }

}
