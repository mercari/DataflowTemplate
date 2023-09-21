package com.mercari.solution.module.sink;

import com.google.gson.Gson;
import com.mercari.solution.config.SinkConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.module.SinkModule;
import com.mercari.solution.module.sink.fileio.Neo4jSink;
import com.mercari.solution.util.domain.search.Neo4jUtil;
import com.mercari.solution.util.domain.search.ZipFileUtil;
import com.mercari.solution.util.pipeline.union.Union;
import com.mercari.solution.util.pipeline.union.UnionValue;
import com.mercari.solution.util.schema.*;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.WriteFilesResult;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public class LocalNeo4jSink implements SinkModule {

    private static final Logger LOG = LoggerFactory.getLogger(LocalNeo4jSink.class);

    private static class LocalNeo4jSinkParameters implements Serializable {

        private String output;
        private String input;
        private String database;
        private String conf;
        private List<Neo4jUtil.NodeConfig> nodes;
        private List<Neo4jUtil.RelationshipConfig> relationships;
        private List<String> setupCyphers;
        private List<String> teardownCyphers;
        private Integer bufferSize;
        private Neo4jUtil.Format format;

        private List<String> groupFields;
        private String tempDirectory;

        public String getOutput() {
            return output;
        }

        public String getInput() {
            return input;
        }

        public String getDatabase() {
            return database;
        }

        public String getConf() {
            return conf;
        }

        public List<Neo4jUtil.NodeConfig> getNodes() {
            return nodes;
        }

        public List<Neo4jUtil.RelationshipConfig> getRelationships() {
            return relationships;
        }

        public List<String> getSetupCyphers() {
            return setupCyphers;
        }

        public List<String> getTeardownCyphers() {
            return teardownCyphers;
        }

        public Integer getBufferSize() {
            return bufferSize;
        }

        public Neo4jUtil.Format getFormat() {
            return format;
        }

        public List<String> getGroupFields() {
            return groupFields;
        }

        public String getTempDirectory() {
            return tempDirectory;
        }

        public void validate() {
            final List<String> errorMessages = new ArrayList<>();
            if(this.output == null) {
                errorMessages.add("localNeo4j sink module requires `output` parameter.");
            }
            if(this.input != null) {
                if(!this.input.startsWith("gs://")) {
                    errorMessages.add("localNeo4j sink module `input` parameter must be gcs path (must start with gs://).");
                }
            }
            if(this.conf != null) {
                if(!this.conf.startsWith("gs://")) {
                    errorMessages.add("localNeo4j sink module `conf` parameter must be gcs path (must start with gs://).");
                }
            }
            if((nodes == null || nodes.isEmpty()) && (relationships == null || relationships.isEmpty())) {
                errorMessages.add("localNeo4j sink module requires `nodes` or `relationships` parameter.");
            } else {
                if(nodes != null) {
                    for(int i=0; i<nodes.size(); i++) {
                        errorMessages.addAll(nodes.get(i).validate(i));
                    }
                }
                if(relationships != null) {
                    for(int i=0; i<relationships.size(); i++) {
                        errorMessages.addAll(relationships.get(i).validate(i));
                    }
                }
            }

            if(errorMessages.size() > 0) {
                throw new IllegalArgumentException(String.join(", ", errorMessages));
            }
        }

        public void setDefaults() {

            if(this.database == null) {
                this.database = Neo4jUtil.DEFAULT_DATABASE_NAME;
            }
            if(this.groupFields == null) {
                this.groupFields = new ArrayList<>();
            }
            if(this.nodes == null) {
                this.nodes = new ArrayList<>();
            } else {
                for(final Neo4jUtil.NodeConfig node : nodes) {
                    node.setDefaults();
                }
            }
            if(this.relationships == null) {
                this.relationships = new ArrayList<>();
            } else {
                for(final Neo4jUtil.RelationshipConfig relationship : relationships) {
                    relationship.setDefaults();
                }
            }
            if(this.setupCyphers == null) {
                this.setupCyphers = new ArrayList<>();
            }
            if(this.teardownCyphers == null) {
                this.teardownCyphers = new ArrayList<>();
            }
            if(this.bufferSize == null) {
                this.bufferSize = 1000;
            }
            if(this.format == null) {
                this.format = Neo4jUtil.Format.dump;
            }
        }

    }


    public String getName() { return "localNeo4j"; }


    @Override
    public Map<String, FCollection<?>> expand(List<FCollection<?>> inputs, SinkConfig config, List<FCollection<?>> waits) {

        if(inputs == null || inputs.size() == 0) {
            throw new IllegalArgumentException("localNeo4j sink module requires inputs");
        }

        final LocalNeo4jSinkParameters parameters = new Gson().fromJson(config.getParameters(), LocalNeo4jSinkParameters.class);
        if(parameters == null) {
            throw new IllegalArgumentException("localNeo4j sink parameters must not be empty!");
        }
        parameters.validate();
        parameters.setDefaults();

        final List<TupleTag<?>> tags = new ArrayList<>();
        final List<String> inputNames = new ArrayList<>();
        final List<DataType> inputTypes = new ArrayList<>();

        PCollectionTuple tuple = PCollectionTuple.empty(inputs.get(0).getCollection().getPipeline());
        for(final FCollection<?> input : inputs){
            final TupleTag tag = new TupleTag<>(){};
            tags.add(tag);
            inputNames.add(input.getName());
            inputTypes.add(input.getDataType());

            tuple = tuple.and(tag, input.getCollection());
        }

        final PCollection output = tuple.apply(config.getName(), new Neo4jIndexWrite(config.getName(), parameters, tags, inputNames, inputTypes, waits));
        final FCollection<?> fcollection = FCollection.of(config.getName(), output, DataType.AVRO, inputs.get(0).getAvroSchema());
        return Collections.singletonMap(config.getName(), fcollection);
    }

    public static class Neo4jIndexWrite extends PTransform<PCollectionTuple, PCollection<KV>> {

        private final String name;
        private final LocalNeo4jSinkParameters parameters;

        private final List<TupleTag<?>> tags;
        private final List<String> inputNames;
        private final List<DataType> inputTypes;

        private final List<FCollection<?>> waits;

        private Neo4jIndexWrite(final String name,
                                final LocalNeo4jSinkParameters parameters,
                                final List<TupleTag<?>> tags,
                                final List<String> inputNames,
                                final List<DataType> inputTypes,
                                final List<FCollection<?>> waits) {

            this.name = name;
            this.parameters = parameters;
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
                    parameters.getOutput(),
                    parameters.getGroupFields(),
                    parameters.getTempDirectory(),
                    SchemaUtil.createGroupKeysFunction(UnionValue::getAsString, parameters.getGroupFields()));
            final WriteFilesResult writeResult = input
                            .apply("Write", write.via(Neo4jSink.of(
                                    name,
                                    parameters.getInput(), parameters.getDatabase(), parameters.getConf(),
                                    parameters.getNodes(), parameters.getRelationships(),
                                    parameters.getSetupCyphers(), parameters.getTeardownCyphers(),
                                    parameters.getBufferSize(), parameters.getFormat(),
                                    inputNames)));

            return writeResult.getPerDestinationOutputFilenames();
        }

    }

}
