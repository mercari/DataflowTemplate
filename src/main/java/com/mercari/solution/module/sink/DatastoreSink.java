package com.mercari.solution.module.sink;

import com.google.datastore.v1.Entity;
import com.google.gson.Gson;
import com.mercari.solution.config.SinkConfig;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.module.SinkModule;
import com.mercari.solution.util.converter.DataTypeTransform;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreV1;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;


public class DatastoreSink implements SinkModule {

    private static final Logger LOG = LoggerFactory.getLogger(DatastoreSink.class);

    private class DatastoreSinkParameters {

        private String projectId;
        private String kind;
        private List<String> keyFields;
        private Boolean delete;

        public String getProjectId() {
            return projectId;
        }

        public void setProjectId(String projectId) {
            this.projectId = projectId;
        }

        public String getKind() {
            return kind;
        }

        public void setKind(String kind) {
            this.kind = kind;
        }

        public List<String> getKeyFields() {
            return keyFields;
        }

        public void setKeyFields(List<String> keyFields) {
            this.keyFields = keyFields;
        }

        public Boolean getDelete() {
            return delete;
        }

        public void setDelete(Boolean delete) {
            this.delete = delete;
        }
    }

    public String getName() { return "datastore"; }

    public Map<String, FCollection<?>> expand(FCollection<?> input, SinkConfig config, List<FCollection<?>> waits) {
        return Collections.singletonMap(config.getName(), DatastoreSink.write(input, config, waits));
    }

    public static FCollection<?> write(final FCollection<?> collection, final SinkConfig config) {
        return write(collection, config, null);
    }

    public static FCollection<?> write(final FCollection<?> collection, final SinkConfig config, final List<FCollection<?>> waitCollections) {
        final DatastoreSinkParameters parameters = new Gson().fromJson(config.getParameters(), DatastoreSinkParameters.class);
        final DatastoreWrite write = new DatastoreWrite(collection, parameters, waitCollections);
        final PDone output = collection.getCollection().apply(config.getName(), write);
        try {
            config.outputAvroSchema(collection.getAvroSchema());
        } catch (Exception e) {
            LOG.error("Failed to output avro schema for " + config.getName() + " to path: " + config.getOutputAvroSchema(), e);
        }
        return FCollection.update(collection, config.getName(), (PCollection) collection.getCollection());
    }

    public static class DatastoreWrite extends PTransform<PCollection<?>, PDone> {

        private final FCollection<?> inputCollection;
        private final DatastoreSinkParameters parameters;
        private final List<FCollection<?>> waitCollections;

        public FCollection<?> getCollection() {
            return inputCollection;
        }

        private DatastoreWrite(final FCollection<?> inputCollection,
                               final DatastoreSinkParameters parameters,
                               final List<FCollection<?>> waitCollections) {

            this.inputCollection = inputCollection;
            this.parameters = parameters;
            this.waitCollections = waitCollections;
        }

        public PDone expand(final PCollection<?> input) {
            validateParameters();
            setDefaultParameters();

            final String execEnvProject = input.getPipeline().getOptions().as(GcpOptions.class).getProject();

            final PCollection<Entity> entities = input
                    .apply(DataTypeTransform.datastoreEntity(inputCollection, parameters.getKind(), parameters.getKeyFields()));

            if(parameters.getDelete()) {
                final DatastoreV1.DeleteEntity delete = DatastoreIO.v1().deleteEntity()
                        .withProjectId(Optional.ofNullable(parameters.getProjectId()).orElse(execEnvProject));
                if(waitCollections == null) {
                    return entities.apply("DeleteEntity", delete);
                } else {
                    final List<PCollection<?>> waits = waitCollections.stream()
                            .map(f -> f.getCollection())
                            .collect(Collectors.toList());
                    return entities
                            .apply("Wait", Wait.on(waits))
                            .setCoder(entities.getCoder())
                            .apply("DeleteEntity", delete);
                }
            } else {
                final DatastoreV1.Write write = DatastoreIO.v1().write()
                        .withProjectId(Optional.ofNullable(parameters.getProjectId()).orElse(execEnvProject));
                if(waitCollections == null) {
                    return entities.apply("WriteEntity", write);
                } else {
                    final List<PCollection<?>> waits = waitCollections.stream()
                            .map(f -> f.getCollection())
                            .collect(Collectors.toList());
                    return entities
                            .apply("Wait", Wait.on(waits))
                            .setCoder(entities.getCoder())
                            .apply("WriteEntity", write);
                }
            }
        }

        private void validateParameters() {
            if(this.parameters.getProjectId() == null) {
                throw new IllegalArgumentException("Datastore output module requires projectId parameter!");
            }
        }

        private void setDefaultParameters() {
            if(parameters.getKeyFields() == null) {
                parameters.setKeyFields(new ArrayList<>());
            }
            if(parameters.getDelete() == null) {
                parameters.setDelete(false);
            }
        }

    }

}
